use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratatui::crossterm::event;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui_textarea::{Input, Key as TextKey, TextArea};
use tokio::sync::mpsc;
use tuirealm::application::{Application, PollStrategy};
use tuirealm::command::{Cmd, CmdResult};
use tuirealm::component::{AppComponent, Component};
use tuirealm::event::{Event, Key, KeyEvent, KeyModifiers, MouseEventKind};
use tuirealm::listener::{EventListenerCfg, PollAsync, PortResult};
use tuirealm::props::{AttrValue, Attribute, QueryResult};
use tuirealm::state::State;
use tuirealm::subscription::{EventClause, Sub, SubClause};
use tuirealm::terminal::{CrosstermTerminalAdapter, TerminalAdapter};
use vlinder_core::domain::{HarnessEvent, RunResult, ToolCall, ToolTrace};

use super::theme;
use super::types::{Entry, Role, ToolTraceDisplay};

const LOGO: &[&str] = &[
    "       _ _           _           ",
    "__   _| (_)_ __   __| | ___ _ __ ",
    "\\ \\ / / | | '_ \\ / _` |/ _ \\ '__|",
    " \\ V /| | | | | | (_| |  __/ |   ",
    "  \\_/ |_|_|_| |_|\\__,_|\\___|_|   ",
];

#[derive(Debug, Eq, PartialEq, Clone, Hash)]
enum Id {
    Transcript,
    Input,
    Status,
    Hint,
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum UserEvent {
    External,
}

#[derive(Debug, PartialEq)]
enum Msg {
    UserSubmit(String),
    UserExit,
    ScrollUp(u16),
    ScrollDown(u16),
    JumpTop,
    JumpBottom,
    ToolsToggle,
    SpinnerTick,
    ExternalWake,
}

enum ExternalEvent {
    Harness(HarnessEvent),
    ProcessResult(RunResult),
}

type ExternalQueue = Arc<Mutex<VecDeque<ExternalEvent>>>;

struct Model {
    app: Application<Id, Msg, UserEvent>,
    terminal: CrosstermTerminalAdapter,
    quit: bool,
    redraw: bool,
    process_tx: mpsc::UnboundedSender<String>,
    external_queue: ExternalQueue,
    pending_call_indices: Vec<usize>,
    received_assistant_content: bool,
}

impl Model {
    fn new(
        initial_transcript: Vec<Entry>,
        process_tx: mpsc::UnboundedSender<String>,
        external_rx: mpsc::UnboundedReceiver<ExternalEvent>,
        terminal: CrosstermTerminalAdapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let external_queue = Arc::new(Mutex::new(VecDeque::new()));
        let listener_cfg = EventListenerCfg::default()
            .with_handle(tokio::runtime::Handle::current())
            .async_crossterm_input_listener(Duration::ZERO, 64)
            .add_async_port(
                Box::new(ExternalPort::new(external_rx, Arc::clone(&external_queue))),
                Duration::ZERO,
                1,
            )
            .async_tick(true)
            .tick_interval(theme::SPINNER_TICK);
        let mut app = Application::init(listener_cfg);
        app.mount(
            Id::Transcript,
            Box::new(TranscriptComponent::new(initial_transcript)),
            Vec::default(),
        )?;
        app.mount(Id::Input, Box::new(InputComponent::new()), Vec::default())?;
        app.mount(
            Id::Status,
            Box::new(StatusComponent::default()),
            vec![
                Sub::new(EventClause::Tick, SubClause::Always),
                Sub::new(EventClause::User(UserEvent::External), SubClause::Always),
            ],
        )?;
        app.mount(Id::Hint, Box::new(HintComponent), Vec::default())?;
        app.active(&Id::Input)?;

        Ok(Self {
            app,
            terminal,
            quit: false,
            redraw: true,
            process_tx,
            external_queue,
            pending_call_indices: Vec::new(),
            received_assistant_content: false,
        })
    }

    fn update(&mut self, msg: Msg) {
        self.redraw = true;
        match msg {
            Msg::UserSubmit(input) => self.submit(input),
            Msg::UserExit => self.quit = true,
            Msg::ScrollUp(rows) => {
                self.transcript_mut().scroll_up(rows);
                self.sync_scrolled_status();
            }
            Msg::ScrollDown(rows) => {
                self.transcript_mut().scroll_down(rows);
                self.sync_scrolled_status();
            }
            Msg::JumpTop => {
                self.transcript_mut().jump_top();
                self.sync_scrolled_status();
            }
            Msg::JumpBottom => {
                self.transcript_mut().jump_bottom();
                self.sync_scrolled_status();
            }
            Msg::ToolsToggle => {
                self.transcript_mut().toggle_tools();
                self.sync_scrolled_status();
            }
            Msg::SpinnerTick => self.status_mut().tick(),
            Msg::ExternalWake => self.drain_external(),
        }
    }

    fn submit(&mut self, input: String) {
        if input == "exit" || input == "quit" {
            self.quit = true;
            return;
        }
        self.transcript_mut().push_user(input.clone());
        self.sync_scrolled_status();
        self.input_mut().clear();
        self.status_mut().set_spinning(true);
        self.pending_call_indices.clear();
        self.received_assistant_content = false;
        let _ = self.process_tx.send(input);
    }

    fn drain_external(&mut self) {
        loop {
            let event = self
                .external_queue
                .lock()
                .ok()
                .and_then(|mut q| q.pop_front());
            let Some(event) = event else { break };
            match event {
                ExternalEvent::Harness(event) => self.handle_harness_event(event),
                ExternalEvent::ProcessResult(result) => self.handle_process_result(result),
            }
        }
    }

    fn handle_harness_event(&mut self, event: HarnessEvent) {
        match event {
            HarnessEvent::AssistantContent { text, .. } => {
                self.received_assistant_content = true;
                self.transcript_mut().push_assistant(text);
            }
            HarnessEvent::ToolCallStarted { tool_call } => {
                let idx = self.transcript_mut().push_tool_call_pending(&tool_call);
                self.pending_call_indices.push(idx);
            }
            HarnessEvent::ToolCallCompleted { trace } => {
                if self.pending_call_indices.is_empty() {
                    self.transcript_mut().push_tool_call(&trace);
                } else {
                    let idx = self.pending_call_indices.remove(0);
                    self.transcript_mut().finalize_tool_call(idx, &trace);
                }
            }
            HarnessEvent::RunStarted { .. }
            | HarnessEvent::TurnStarted { .. }
            | HarnessEvent::ToolCallApprovalRequired { .. }
            | HarnessEvent::TurnCompleted { .. }
            | HarnessEvent::RunCompleted { .. } => {}
        }
    }

    fn handle_process_result(&mut self, result: RunResult) {
        self.status_mut().set_spinning(false);
        if !self.received_assistant_content {
            if let Some(text) = result.content {
                self.transcript_mut().push_assistant(text);
            }
        }
        self.transcript_mut().jump_bottom();
        self.sync_scrolled_status();
    }

    fn view(&mut self) {
        let _ = self.terminal.draw(|frame| {
            let input_height = self
                .app
                .get_component_mut(&Id::Input)
                .and_then(|c| c.as_any_mut().downcast_mut::<InputComponent>())
                .map_or(3, |input| input.height(frame.area().width));
            let chunks = Layout::vertical([
                Constraint::Min(1),
                Constraint::Length(1),
                Constraint::Length(1),
                Constraint::Length(input_height),
                Constraint::Length(1),
            ])
            .split(frame.area());
            self.app.view(&Id::Transcript, frame, chunks[0]);
            self.app.view(&Id::Status, frame, chunks[2]);
            self.app.view(&Id::Input, frame, chunks[3]);
            self.app.view(&Id::Hint, frame, chunks[4]);
        });
    }

    fn transcript_mut(&mut self) -> &mut TranscriptComponent {
        self.app
            .get_component_mut(&Id::Transcript)
            .and_then(|c| c.as_any_mut().downcast_mut::<TranscriptComponent>())
            .expect("transcript component mounted")
    }

    fn input_mut(&mut self) -> &mut InputComponent {
        self.app
            .get_component_mut(&Id::Input)
            .and_then(|c| c.as_any_mut().downcast_mut::<InputComponent>())
            .expect("input component mounted")
    }

    fn status_mut(&mut self) -> &mut StatusComponent {
        self.app
            .get_component_mut(&Id::Status)
            .and_then(|c| c.as_any_mut().downcast_mut::<StatusComponent>())
            .expect("status component mounted")
    }

    fn sync_scrolled_status(&mut self) {
        let scrolled_up = !self.transcript_mut().follow_tail;
        self.status_mut().set_scrolled_up(scrolled_up);
    }
}

struct ExternalPort {
    rx: mpsc::UnboundedReceiver<ExternalEvent>,
    queue: ExternalQueue,
}

impl ExternalPort {
    fn new(rx: mpsc::UnboundedReceiver<ExternalEvent>, queue: ExternalQueue) -> Self {
        Self { rx, queue }
    }
}

#[tuirealm::async_trait]
impl PollAsync<UserEvent> for ExternalPort {
    async fn poll(&mut self) -> PortResult<Option<Event<UserEvent>>> {
        match self.rx.recv().await {
            Some(event) => {
                if let Ok(mut queue) = self.queue.lock() {
                    queue.push_back(event);
                }
                Ok(Some(Event::User(UserEvent::External)))
            }
            None => Ok(None),
        }
    }
}

struct TranscriptComponent {
    output: Vec<Entry>,
    scroll_offset: u16,
    follow_tail: bool,
    tools_expanded: bool,
}

impl TranscriptComponent {
    fn new(output: Vec<Entry>) -> Self {
        Self {
            output,
            scroll_offset: 0,
            follow_tail: true,
            tools_expanded: false,
        }
    }

    fn push_user(&mut self, text: String) {
        self.output.push(Entry {
            role: Role::User,
            text,
            tool: None,
        });
        self.jump_bottom();
    }

    fn push_assistant(&mut self, text: String) {
        self.output.push(Entry {
            role: Role::Assistant,
            text,
            tool: None,
        });
        self.jump_bottom();
    }

    fn push_tool_call_pending(&mut self, tool_call: &ToolCall) -> usize {
        let idx = self.output.len();
        let args = serde_json::to_string(&tool_call.arguments)
            .unwrap_or_else(|_| tool_call.arguments.to_string());
        self.output.push(Entry {
            role: Role::ToolCall,
            text: format!("⏵ {}(…) [running…]", tool_call.name),
            tool: Some(ToolTraceDisplay {
                name: tool_call.name.clone(),
                args,
                result: String::new(),
                duration_ms: 0,
                is_error: false,
            }),
        });
        self.jump_bottom();
        idx
    }

    fn finalize_tool_call(&mut self, idx: usize, trace: &ToolTrace) {
        if idx >= self.output.len() {
            return;
        }
        let result = String::from_utf8_lossy(&trace.result.content).to_string();
        let result_preview = result.chars().take(80).collect::<String>();
        let args = serde_json::to_string_pretty(&trace.tool_call.arguments)
            .unwrap_or_else(|_| trace.tool_call.arguments.to_string());
        self.output[idx] = Entry {
            role: Role::ToolCall,
            text: format!(
                "⏵ {}(…) [{}ms] {}",
                trace.tool_call.name,
                trace.duration_ms,
                result_preview.trim()
            ),
            tool: Some(ToolTraceDisplay {
                name: trace.tool_call.name.clone(),
                args,
                result,
                duration_ms: trace.duration_ms,
                is_error: false,
            }),
        };
        self.jump_bottom();
    }

    fn push_tool_call(&mut self, trace: &ToolTrace) {
        let args = serde_json::to_string_pretty(&trace.tool_call.arguments)
            .unwrap_or_else(|_| trace.tool_call.arguments.to_string());
        let result = String::from_utf8_lossy(&trace.result.content).to_string();
        let result_preview = result.chars().take(40).collect::<String>();
        self.output.push(Entry {
            role: Role::ToolCall,
            text: format!(
                "{}(…) → \"{}…\"",
                trace.tool_call.name,
                result_preview.trim()
            ),
            tool: Some(ToolTraceDisplay {
                name: trace.tool_call.name.clone(),
                args,
                result,
                duration_ms: trace.duration_ms,
                is_error: false,
            }),
        });
        self.jump_bottom();
    }

    fn scroll_up(&mut self, n: u16) {
        self.follow_tail = false;
        self.scroll_offset = self.scroll_offset.saturating_sub(n);
    }

    fn scroll_down(&mut self, n: u16) {
        self.scroll_offset = self.scroll_offset.saturating_add(n);
    }

    fn jump_top(&mut self) {
        self.follow_tail = false;
        self.scroll_offset = 0;
    }

    fn jump_bottom(&mut self) {
        self.follow_tail = true;
    }

    fn toggle_tools(&mut self) {
        self.tools_expanded = !self.tools_expanded;
        self.jump_bottom();
    }

    fn clamp_scroll(&mut self, max_scroll: u16) {
        if self.follow_tail {
            self.scroll_offset = max_scroll;
        } else {
            let clamped = self.scroll_offset.min(max_scroll);
            if clamped == max_scroll {
                self.follow_tail = true;
            }
            self.scroll_offset = clamped;
        }
    }
}

impl Component for TranscriptComponent {
    fn view(&mut self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let lines = if self.output.is_empty() {
            welcome_lines()
        } else {
            transcript_lines(&self.output, area.width, self.tools_expanded)
        };
        let total_rows = count_wrapped_rows(&lines, area.width);
        let max_scroll = total_rows.saturating_sub(area.height);
        self.clamp_scroll(max_scroll);
        let paragraph = Paragraph::new(Text::from(lines))
            .wrap(Wrap { trim: false })
            .scroll((self.scroll_offset, 0));
        frame.render_widget(paragraph, area);
    }

    fn query(&self, _attr: Attribute) -> Option<QueryResult<'_>> {
        None
    }

    fn attr(&mut self, _attr: Attribute, _value: AttrValue) {}

    fn state(&self) -> State {
        State::None
    }

    fn perform(&mut self, cmd: Cmd) -> CmdResult {
        CmdResult::Invalid(cmd)
    }
}

impl AppComponent<Msg, UserEvent> for TranscriptComponent {
    fn on(&mut self, _event: &Event<UserEvent>) -> Option<Msg> {
        None
    }
}

struct InputComponent {
    textarea: TextArea<'static>,
}

impl InputComponent {
    fn new() -> Self {
        let mut textarea = TextArea::default();
        theme::configure_input(&mut textarea);
        Self { textarea }
    }

    fn clear(&mut self) {
        *self = Self::new();
    }

    fn height(&self, terminal_width: u16) -> u16 {
        let inner_w = terminal_width.saturating_sub(2).max(1) as usize;
        let visual_rows: usize = self
            .textarea
            .lines()
            .iter()
            .map(|line| line.chars().count().max(1).div_ceil(inner_w))
            .sum();
        u16::try_from(visual_rows.max(1))
            .unwrap_or(u16::MAX)
            .saturating_add(2)
            .clamp(3, 10)
    }

    fn submit_text(&self) -> Option<String> {
        let input = self.textarea.lines().join("\n").trim().to_string();
        (!input.is_empty()).then_some(input)
    }
}

impl Component for InputComponent {
    fn view(&mut self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        frame.render_widget(&self.textarea, area);
    }

    fn query(&self, _attr: Attribute) -> Option<QueryResult<'_>> {
        None
    }

    fn attr(&mut self, _attr: Attribute, _value: AttrValue) {}

    fn state(&self) -> State {
        State::None
    }

    fn perform(&mut self, cmd: Cmd) -> CmdResult {
        CmdResult::Invalid(cmd)
    }
}

impl AppComponent<Msg, UserEvent> for InputComponent {
    fn on(&mut self, event: &Event<UserEvent>) -> Option<Msg> {
        match event {
            Event::Keyboard(key) => self.on_key(*key),
            Event::Mouse(mouse) => match mouse.kind {
                MouseEventKind::ScrollUp => Some(Msg::ScrollUp(3)),
                MouseEventKind::ScrollDown => Some(Msg::ScrollDown(3)),
                _ => None,
            },
            _ => None,
        }
    }
}

impl InputComponent {
    fn on_key(&mut self, key: KeyEvent) -> Option<Msg> {
        match key.code {
            Key::Enter if key.modifiers == KeyModifiers::NONE => {
                self.submit_text().map(Msg::UserSubmit)
            }
            Key::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
                self.textarea.insert_newline();
                None
            }
            Key::Char('c' | 'd') if key.modifiers == KeyModifiers::CONTROL => Some(Msg::UserExit),
            Key::Char('o') if key.modifiers == KeyModifiers::CONTROL => Some(Msg::ToolsToggle),
            Key::PageUp => Some(Msg::ScrollUp(5)),
            Key::PageDown => Some(Msg::ScrollDown(5)),
            Key::Home if key.modifiers == KeyModifiers::CONTROL => Some(Msg::JumpTop),
            Key::End if key.modifiers == KeyModifiers::CONTROL => Some(Msg::JumpBottom),
            Key::CtrlHome => Some(Msg::JumpTop),
            Key::CtrlEnd => Some(Msg::JumpBottom),
            Key::Up if key.modifiers == KeyModifiers::NONE && self.textarea.lines().len() <= 1 => {
                Some(Msg::ScrollUp(1))
            }
            Key::Down
                if key.modifiers == KeyModifiers::NONE && self.textarea.lines().len() <= 1 =>
            {
                Some(Msg::ScrollDown(1))
            }
            _ => {
                self.textarea.input(to_textarea_input(key));
                None
            }
        }
    }
}

#[derive(Default)]
struct StatusComponent {
    spinning: bool,
    spinner_frame: usize,
    scrolled_up: bool,
}

impl StatusComponent {
    fn set_spinning(&mut self, spinning: bool) {
        self.spinning = spinning;
        if spinning {
            self.spinner_frame = 0;
        }
    }

    fn tick(&mut self) {
        if self.spinning {
            self.spinner_frame = (self.spinner_frame + 1) % theme::SPINNER_FRAMES.len();
        }
    }

    fn set_scrolled_up(&mut self, scrolled_up: bool) {
        self.scrolled_up = scrolled_up;
    }
}

impl Component for StatusComponent {
    fn view(&mut self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let line = if self.spinning {
            let glyph = theme::SPINNER_FRAMES[self.spinner_frame % theme::SPINNER_FRAMES.len()];
            Line::from(vec![
                Span::styled(format!("  {glyph} "), theme::spinner_style()),
                Span::styled("thinking...", theme::spinner_style()),
            ])
        } else if self.scrolled_up {
            Line::from(Span::styled(
                "  (scrolled up — press Ctrl+End to jump to latest)",
                theme::scrolled_up_style(),
            ))
        } else {
            Line::from("")
        };
        frame.render_widget(Paragraph::new(line), area);
    }

    fn query(&self, _attr: Attribute) -> Option<QueryResult<'_>> {
        None
    }

    fn attr(&mut self, _attr: Attribute, _value: AttrValue) {}

    fn state(&self) -> State {
        State::None
    }

    fn perform(&mut self, cmd: Cmd) -> CmdResult {
        CmdResult::Invalid(cmd)
    }
}

impl AppComponent<Msg, UserEvent> for StatusComponent {
    fn on(&mut self, event: &Event<UserEvent>) -> Option<Msg> {
        match event {
            Event::Tick => Some(Msg::SpinnerTick),
            Event::User(UserEvent::External) => Some(Msg::ExternalWake),
            _ => None,
        }
    }
}

struct HintComponent;

impl Component for HintComponent {
    fn view(&mut self, frame: &mut ratatui::Frame<'_>, area: Rect) {
        let hint = Line::from(Span::styled(
            "  ↵ send · shift+↵ newline · ctrl-o tools · wheel/pgup-pgdn scroll · ctrl-c quit",
            theme::hint_style(),
        ));
        frame.render_widget(Paragraph::new(hint), area);
    }

    fn query(&self, _attr: Attribute) -> Option<QueryResult<'_>> {
        None
    }

    fn attr(&mut self, _attr: Attribute, _value: AttrValue) {}

    fn state(&self) -> State {
        State::None
    }

    fn perform(&mut self, cmd: Cmd) -> CmdResult {
        CmdResult::Invalid(cmd)
    }
}

impl AppComponent<Msg, UserEvent> for HintComponent {
    fn on(&mut self, _event: &Event<UserEvent>) -> Option<Msg> {
        None
    }
}

pub async fn run<F, Fut>(
    process: F,
    event_rx: mpsc::Receiver<HarnessEvent>,
    initial_transcript: Vec<Entry>,
) where
    F: FnMut(String) -> Fut + Send + 'static,
    Fut: Future<Output = RunResult> + Send + 'static,
{
    let (input_tx, input_rx) = mpsc::unbounded_channel();
    let (external_tx, external_rx) = mpsc::unbounded_channel();
    spawn_harness_forwarder(event_rx, external_tx.clone());
    spawn_process_worker(process, input_rx, external_tx);

    let Ok(mut terminal) = init_terminal() else {
        return;
    };
    splash_phase(&mut terminal);
    let Ok(mut model) = Model::new(initial_transcript, input_tx, external_rx, terminal) else {
        return;
    };

    while !model.quit {
        match model
            .app
            .tick(PollStrategy::UpTo(64, Duration::from_millis(20)))
        {
            Ok(messages) => {
                for msg in messages {
                    model.update(msg);
                }
            }
            Err(_) => model.quit = true,
        }

        if model.redraw {
            model.view();
            model.redraw = false;
        }
    }

    let _ = model.terminal.restore();
}

fn init_terminal() -> Result<CrosstermTerminalAdapter, Box<dyn std::error::Error>> {
    let mut terminal = CrosstermTerminalAdapter::new()?;
    terminal.enable_raw_mode()?;
    terminal.enter_alternate_screen()?;
    Ok(terminal)
}

fn spawn_harness_forwarder(
    mut event_rx: mpsc::Receiver<HarnessEvent>,
    external_tx: mpsc::UnboundedSender<ExternalEvent>,
) {
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            let _ = external_tx.send(ExternalEvent::Harness(event));
        }
    });
}

fn spawn_process_worker<F, Fut>(
    mut process: F,
    mut input_rx: mpsc::UnboundedReceiver<String>,
    external_tx: mpsc::UnboundedSender<ExternalEvent>,
) where
    F: FnMut(String) -> Fut + Send + 'static,
    Fut: Future<Output = RunResult> + Send + 'static,
{
    tokio::spawn(async move {
        while let Some(input) = input_rx.recv().await {
            let result = process(input).await;
            let _ = external_tx.send(ExternalEvent::ProcessResult(result));
        }
    });
}

fn splash_phase<T: TerminalAdapter>(terminal: &mut T) {
    let deadline = std::time::Instant::now() + theme::SPLASH_DURATION;
    loop {
        let _ = terminal.draw(draw_splash);
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return;
        }
        let timeout = remaining.min(Duration::from_millis(80));
        if event::poll(timeout).unwrap_or(false) {
            let _ = event::read();
            return;
        }
    }
}

fn draw_splash(frame: &mut ratatui::Frame<'_>) {
    let area = frame.area();
    let mut lines: Vec<Line> = LOGO
        .iter()
        .map(|row| centered(Span::styled(*row, theme::logo_style()), area.width))
        .collect();
    lines.push(Line::from(""));
    lines.push(Line::from(""));
    lines.push(centered(
        Span::styled("AI agents that can time travel.", theme::tagline_style()),
        area.width,
    ));
    lines.push(Line::from(""));
    lines.push(centered(
        Span::styled("reason · debug · experiment · prove", theme::motto_style()),
        area.width,
    ));
    lines.push(Line::from(""));
    lines.push(Line::from(""));
    lines.push(centered(
        Span::styled("(press any key to continue)", theme::splash_hint_style()),
        area.width,
    ));

    let content_height = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let top_pad = area.height.saturating_sub(content_height) / 2;
    let mut padded: Vec<Line> = (0..top_pad).map(|_| Line::from("")).collect();
    padded.extend(lines);
    frame.render_widget(Paragraph::new(Text::from(padded)), area);
}

fn centered(content: Span<'_>, width: u16) -> Line<'_> {
    let content_w = u16::try_from(content.content.chars().count()).unwrap_or(u16::MAX);
    let pad = width.saturating_sub(content_w) / 2;
    Line::from(vec![Span::raw(" ".repeat(usize::from(pad))), content])
}

fn welcome_lines() -> Vec<Line<'static>> {
    vec![
        Line::from(Span::styled(
            "Welcome to Vlinder.",
            theme::welcome_title_style(),
        )),
        Line::from(""),
        Line::from(Span::styled(
            "Type a message below and press Enter to send.",
            theme::welcome_body_style(),
        )),
    ]
}

fn transcript_lines(output: &[Entry], width: u16, tools_expanded: bool) -> Vec<Line<'static>> {
    let mut acc = Vec::new();
    for entry in output {
        acc.extend(render_entry(entry, width, tools_expanded));
    }
    acc
}

fn render_entry(entry: &Entry, width: u16, tools_expanded: bool) -> Vec<Line<'static>> {
    match entry.role {
        Role::User => render_user_entry(&entry.text, width),
        Role::Assistant => render_assistant_entry(&entry.text, width),
        Role::ToolCall => {
            if let Some(ref display) = entry.tool {
                if tools_expanded {
                    render_tool_call_expanded(display, width)
                } else {
                    render_tool_call_collapsed(display, width)
                }
            } else {
                render_assistant_entry(&entry.text, width)
            }
        }
    }
}

fn wrap_to_width(text: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![text.to_string()];
    }
    let mut result = Vec::new();
    for paragraph in text.split('\n') {
        if paragraph.is_empty() {
            result.push(String::new());
            continue;
        }
        let mut current = String::new();
        let mut current_w = 0;
        for word in paragraph.split_whitespace() {
            let word_w = word.chars().count();
            if word_w > width {
                if current_w > 0 {
                    result.push(std::mem::take(&mut current));
                    current_w = 0;
                }
                let chars: Vec<char> = word.chars().collect();
                for chunk in chars.chunks(width) {
                    result.push(chunk.iter().collect());
                }
            } else if current_w == 0 {
                current = word.to_string();
                current_w = word_w;
            } else if current_w + 1 + word_w <= width {
                current.push(' ');
                current.push_str(word);
                current_w += 1 + word_w;
            } else {
                result.push(std::mem::take(&mut current));
                current = word.to_string();
                current_w = word_w;
            }
        }
        if !current.is_empty() {
            result.push(current);
        }
    }
    if result.is_empty() {
        result.push(String::new());
    }
    result
}

fn bg_row(bg: Style, width: u16) -> Line<'static> {
    Line::from(Span::styled(" ".repeat(width as usize), bg))
}

fn render_user_entry(text: &str, width: u16) -> Vec<Line<'static>> {
    const PROMPT_WIDTH: usize = 2;
    let bg_pad = theme::user_pad_style();
    let w = width as usize;
    let inner_w = w.saturating_sub(PROMPT_WIDTH).max(1);
    let mut lines = vec![bg_row(bg_pad, width)];
    let mut overall_idx = 0;
    for paragraph in text.split('\n') {
        let visuals = if paragraph.is_empty() {
            vec![String::new()]
        } else {
            wrap_to_width(paragraph, inner_w)
        };
        for visual in visuals {
            let marker = if overall_idx == 0 { "> " } else { "  " };
            let used = PROMPT_WIDTH + visual.chars().count();
            let pad = w.saturating_sub(used);
            lines.push(Line::from(vec![
                Span::styled(marker.to_string(), theme::user_prompt_style()),
                Span::styled(visual, theme::user_text_style()),
                Span::styled(" ".repeat(pad), bg_pad),
            ]));
            overall_idx += 1;
        }
    }
    lines.push(bg_row(bg_pad, width));
    lines
}

fn render_assistant_entry(text: &str, width: u16) -> Vec<Line<'static>> {
    let bg_text = theme::assistant_text_style();
    let bg_pad = theme::assistant_pad_style();
    let w = width as usize;
    let mut lines = vec![bg_row(bg_pad, width)];
    for visual in wrap_to_width(text, w) {
        let used = visual.chars().count();
        let pad = w.saturating_sub(used);
        lines.push(Line::from(vec![
            Span::styled(visual, bg_text),
            Span::styled(" ".repeat(pad), bg_pad),
        ]));
    }
    lines.push(bg_row(bg_pad, width));
    lines
}

fn render_tool_call_collapsed(display: &ToolTraceDisplay, width: u16) -> Vec<Line<'static>> {
    let bg_style = theme::tool_call_row_style();
    let glyph_style = if display.is_error {
        theme::tool_glyph_error_style()
    } else {
        theme::tool_glyph_style()
    };
    let prefix = if display.is_error { "✗" } else { "⏵" };
    let duration_s = format!("[{}ms]", display.duration_ms);
    let w = width as usize;
    let name_w = display.name.chars().count();
    let dur_w = duration_s.chars().count();
    let fixed = 9_usize;
    let remaining = w.saturating_sub(fixed + name_w + dur_w);
    let args_budget = (remaining / 2).min(40);
    let result_budget = remaining.saturating_sub(args_budget).min(40);
    let args_trunc: String = display.args.trim().chars().take(args_budget).collect();
    let result_trunc: String = display.result.trim().chars().take(result_budget).collect();
    let used = fixed + name_w + args_trunc.chars().count() + result_trunc.chars().count() + dur_w;
    let pad = w.saturating_sub(used);

    vec![
        bg_row(bg_style, width),
        Line::from(vec![
            Span::styled(prefix, glyph_style.patch(bg_style)),
            Span::styled(" ", bg_style),
            Span::styled(
                display.name.clone(),
                theme::tool_name_style().patch(bg_style),
            ),
            Span::styled("(", theme::tool_args_style().patch(bg_style)),
            Span::styled(args_trunc, theme::tool_args_style().patch(bg_style)),
            Span::styled(")", theme::tool_args_style().patch(bg_style)),
            Span::styled(" → ", bg_style),
            Span::styled("\"", bg_style),
            Span::styled(result_trunc, bg_style),
            Span::styled("\"", bg_style),
            Span::styled(" ".repeat(pad), bg_style),
            Span::styled(" ", bg_style),
            Span::styled(duration_s, theme::tool_duration_style().patch(bg_style)),
        ]),
        bg_row(bg_style, width),
    ]
}

fn render_tool_call_expanded(display: &ToolTraceDisplay, width: u16) -> Vec<Line<'static>> {
    fn pad_line(line: &mut Line<'static>, width: u16, bg_style: Style) {
        let used: usize = line.spans.iter().map(|s| s.content.chars().count()).sum();
        let pad = usize::from(width).saturating_sub(used);
        if pad > 0 {
            line.spans.push(Span::styled(" ".repeat(pad), bg_style));
        }
    }

    let glyph_style = if display.is_error {
        theme::tool_glyph_error_style()
    } else {
        theme::tool_glyph_style()
    };
    let prefix = if display.is_error { "✗" } else { "⏵" };
    let duration_s = format!("[{}ms]", display.duration_ms);
    let pad = usize::from(width)
        .saturating_sub(display.name.chars().count() + duration_s.chars().count() + 4);
    let bg_style = theme::tool_call_row_style();
    let mut lines = vec![bg_row(bg_style, width)];
    let mut header = Line::from(vec![
        Span::styled(prefix, glyph_style.patch(bg_style)),
        Span::styled(" ", bg_style),
        Span::styled(
            display.name.clone(),
            theme::tool_name_style().patch(bg_style),
        ),
        Span::styled(" ".repeat(pad), bg_style),
        Span::styled(duration_s, theme::tool_duration_style().patch(bg_style)),
    ]);
    pad_line(&mut header, width, bg_style);
    lines.push(header);
    for (idx, arg_line) in display.args.lines().enumerate() {
        let mut line = if idx == 0 {
            Line::from(vec![
                Span::styled("  args:   ", theme::tool_args_label_style().patch(bg_style)),
                Span::styled(
                    arg_line.to_string(),
                    theme::tool_args_style().patch(bg_style),
                ),
            ])
        } else {
            Line::from(Span::styled(
                format!("          {arg_line}"),
                theme::tool_args_style().patch(bg_style),
            ))
        };
        pad_line(&mut line, width, bg_style);
        lines.push(line);
    }
    for (idx, res_line) in display.result.lines().enumerate() {
        let mut line = if idx == 0 {
            Line::from(vec![
                Span::styled(
                    "  result: ",
                    theme::tool_result_label_style().patch(bg_style),
                ),
                Span::styled(
                    res_line.to_string(),
                    theme::tool_result_style().patch(bg_style),
                ),
            ])
        } else {
            Line::from(Span::styled(
                format!("          {res_line}"),
                theme::tool_result_style().patch(bg_style),
            ))
        };
        pad_line(&mut line, width, bg_style);
        lines.push(line);
    }
    lines.push(bg_row(bg_style, width));
    lines
}

fn count_wrapped_rows(lines: &[Line<'_>], width: u16) -> u16 {
    let w = width.max(1) as usize;
    let total: usize = lines
        .iter()
        .map(|line| {
            let chars: usize = line.spans.iter().map(|sp| sp.content.chars().count()).sum();
            chars.max(1).div_ceil(w)
        })
        .sum();
    u16::try_from(total).unwrap_or(u16::MAX)
}

fn to_textarea_input(key: KeyEvent) -> Input {
    Input {
        key: to_textarea_key(key.code),
        ctrl: key.modifiers.contains(KeyModifiers::CONTROL),
        alt: key.modifiers.contains(KeyModifiers::ALT),
        shift: key.modifiers.contains(KeyModifiers::SHIFT),
    }
}

fn to_textarea_key(key: Key) -> TextKey {
    match key {
        Key::Char(ch) => TextKey::Char(ch),
        Key::Function(idx) => TextKey::F(idx),
        Key::Backspace => TextKey::Backspace,
        Key::Enter => TextKey::Enter,
        Key::Left => TextKey::Left,
        Key::Right => TextKey::Right,
        Key::Up => TextKey::Up,
        Key::Down => TextKey::Down,
        Key::Tab => TextKey::Tab,
        Key::Delete => TextKey::Delete,
        Key::Home => TextKey::Home,
        Key::End => TextKey::End,
        Key::PageUp => TextKey::PageUp,
        Key::PageDown => TextKey::PageDown,
        Key::Esc => TextKey::Esc,
        _ => TextKey::Null,
    }
}
