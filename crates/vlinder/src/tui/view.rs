//! Rendering. Pure top-down: takes immutable-ish state (`&mut App` only so
//! the transcript can clamp scroll once it knows the wrapped row count) and
//! paints a frame. All visual choices come from `theme`.

use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::text::{Line, Span, Text};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use super::app::{App, Entry, Role, ToolTraceDisplay};
use super::theme;

/// ASCII wordmark for `vlinder` (lowercase ASCII-art block letters).
const LOGO: &[&str] = &[
    "       _ _           _           ",
    "__   _| (_)_ __   __| | ___ _ __ ",
    "\\ \\ / / | | '_ \\ / _` |/ _ \\ '__|",
    " \\ V /| | | | | | (_| |  __/ |   ",
    "  \\_/ |_|_|_| |_|\\__,_|\\___|_|   ",
];

/// Paint the splash screen: centered wordmark, tagline, motto, dismiss hint.
pub fn draw_splash(frame: &mut Frame<'_>) {
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

    // Vertical centering: prepend blank lines to push the block to the middle.
    let content_height = u16::try_from(lines.len()).unwrap_or(u16::MAX);
    let top_pad = area.height.saturating_sub(content_height) / 2;
    let mut padded: Vec<Line> = (0..top_pad).map(|_| Line::from("")).collect();
    padded.extend(lines);

    frame.render_widget(Paragraph::new(Text::from(padded)), area);
}

/// Build a line with `content` horizontally centered in `width` columns.
fn centered(content: Span<'_>, width: u16) -> Line<'_> {
    let content_w = u16::try_from(content.content.chars().count()).unwrap_or(u16::MAX);
    let pad = width.saturating_sub(content_w) / 2;
    Line::from(vec![Span::raw(" ".repeat(usize::from(pad))), content])
}

/// Paint one frame. Top-down layout: transcript, spacer, status, input, hint.
pub fn draw(frame: &mut Frame<'_>, app: &mut App) {
    let area = frame.area();

    // Input height grows with content (+2 for the rounded border).
    let inner_w = (frame.area().width.saturating_sub(2)).max(1) as usize;
    let visual_rows: usize = app
        .textarea
        .lines()
        .iter()
        .map(|line| line.chars().count().max(1).div_ceil(inner_w))
        .sum();
    let input_inner = u16::try_from(visual_rows.max(1)).unwrap_or(u16::MAX);
    let input_height = input_inner.saturating_add(2).clamp(3, 10);

    let chunks = Layout::vertical([
        Constraint::Min(1),               // transcript
        Constraint::Length(1),            // breathing room above status
        Constraint::Length(1),            // status / spinner
        Constraint::Length(input_height), // input
        Constraint::Length(1),            // hint
    ])
    .split(area);

    render_transcript(frame, chunks[0], app);
    render_status(frame, chunks[2], app);
    frame.render_widget(&app.textarea, chunks[3]);
    render_hint(frame, chunks[4]);
}

fn render_transcript(frame: &mut Frame<'_>, area: Rect, app: &mut App) {
    let lines = if app.output.is_empty() {
        welcome_lines()
    } else {
        transcript_lines(&app.output, area.width, app.tools_expanded)
    };

    let total_rows = count_wrapped_rows(&lines, area.width);
    let max_scroll = total_rows.saturating_sub(area.height);
    app.clamp_scroll(max_scroll);

    let paragraph = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .scroll((app.scroll_offset, 0));
    frame.render_widget(paragraph, area);
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
    let mut acc: Vec<Line> = Vec::new();
    for (idx, entry) in output.iter().enumerate() {
        acc.extend(render_entry(entry, width, tools_expanded));
        if idx + 1 < output.len() {
            acc.push(Line::from(""));
        }
    }
    acc
}

fn render_entry(entry: &Entry, width: u16, tools_expanded: bool) -> Vec<Line<'static>> {
    match entry.role {
        Role::User => render_user_entry(&entry.text, width),
        Role::Assistant => render_assistant_entry(&entry.text),
        Role::ToolCall => {
            if let Some(ref display) = entry.tool {
                if tools_expanded {
                    render_tool_call_expanded(display, width)
                } else {
                    render_tool_call_collapsed(display, width)
                }
            } else {
                render_assistant_entry(&entry.text)
            }
        }
    }
}

/// User messages render as a tinted full-width bar with a `> ` prompt
/// marker and 2-space hanging indent on continuation lines.
fn render_user_entry(text: &str, width: u16) -> Vec<Line<'static>> {
    const PROMPT_WIDTH: usize = 2;
    let width = width as usize;

    text.split('\n')
        .enumerate()
        .map(|(i, line)| {
            let marker = if i == 0 { "> " } else { "  " };
            let used = PROMPT_WIDTH + line.chars().count();
            let pad = width.saturating_sub(used);
            Line::from(vec![
                Span::styled(marker.to_string(), theme::user_prompt_style()),
                Span::styled(line.to_string(), theme::user_text_style()),
                Span::styled(" ".repeat(pad), theme::user_pad_style()),
            ])
        })
        .collect()
}

fn render_assistant_entry(text: &str) -> Vec<Line<'static>> {
    text.split('\n')
        .map(|line| Line::from(Span::raw(line.to_string())))
        .collect()
}

/// Collapsed tool-call showing name, truncated args/result, and duration.
fn render_tool_call_collapsed(display: &ToolTraceDisplay, width: u16) -> Vec<Line<'static>> {
    let glyph_style = if display.is_error {
        theme::tool_glyph_error_style()
    } else {
        theme::tool_glyph_style()
    };
    let prefix = if display.is_error { "✗" } else { "⏵" };
    let left = format!(
        "{} {}({:.40}) → \"{:.40}\"",
        prefix,
        display.name,
        display.args.trim(),
        display.result.trim()
    );
    let duration_s = format!("[{}ms]", display.duration_ms);
    let left_w = left.chars().count();
    let pad = usize::from(width).saturating_sub(left_w + duration_s.chars().count() + 2);
    vec![Line::from(vec![
        Span::styled(prefix, glyph_style),
        Span::raw(" "),
        Span::styled(display.name.clone(), theme::tool_name_style()),
        Span::styled(
            format!("({:.40})", display.args.trim()),
            theme::tool_args_style(),
        ),
        Span::raw(" → "),
        Span::raw(format!("\"{:.40}\"", display.result.trim())),
        Span::styled(" ".repeat(pad), theme::hint_style()),
        Span::styled(duration_s, theme::tool_duration_style()),
    ])]
}

/// Expanded tool-call: header with name + duration, indented args block,
/// indented result block.
fn render_tool_call_expanded(display: &ToolTraceDisplay, width: u16) -> Vec<Line<'static>> {
    let glyph_style = if display.is_error {
        theme::tool_glyph_error_style()
    } else {
        theme::tool_glyph_style()
    };
    let prefix = if display.is_error { "✗" } else { "⏵" };
    let duration_s = format!("[{}ms]", display.duration_ms);
    let pad = usize::from(width)
        .saturating_sub(display.name.chars().count() + duration_s.chars().count() + 4);

    let mut lines = Vec::new();
    // Header: glyph name [duration]
    lines.push(Line::from(vec![
        Span::styled(prefix, glyph_style),
        Span::raw(" "),
        Span::styled(display.name.clone(), theme::tool_name_style()),
        Span::styled(" ".repeat(pad), theme::hint_style()),
        Span::styled(duration_s, theme::tool_duration_style()),
    ]));
    // Args: "  args:   {pretty-printed JSON}"
    for (i, arg_line) in display.args.lines().enumerate() {
        if i == 0 {
            lines.push(Line::from(vec![
                Span::styled("  args:   ", theme::tool_args_label_style()),
                Span::styled(arg_line.to_string(), theme::tool_args_style()),
            ]));
        } else {
            // Continuation lines of args are indented further
            lines.push(Line::from(Span::styled(
                format!("          {arg_line}"),
                theme::tool_args_style(),
            )));
        }
    }
    // Result: "  result: {result content}"
    for (i, res_line) in display.result.lines().enumerate() {
        if i == 0 {
            lines.push(Line::from(vec![
                Span::styled("  result: ", theme::tool_result_label_style()),
                Span::styled(res_line.to_string(), theme::tool_result_style()),
            ]));
        } else {
            lines.push(Line::from(Span::styled(
                format!("          {res_line}"),
                theme::tool_result_style(),
            )));
        }
    }
    lines
}

fn render_status(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let line = if app.spinning {
        let glyph = theme::SPINNER_FRAMES[app.spinner_frame % theme::SPINNER_FRAMES.len()];
        Line::from(vec![
            Span::styled(format!("  {glyph} "), theme::spinner_style()),
            Span::styled("thinking...", theme::spinner_style()),
        ])
    } else if app.follow_tail {
        Line::from("")
    } else {
        Line::from(Span::styled(
            "  (scrolled up — press Ctrl+End to jump to latest)",
            theme::scrolled_up_style(),
        ))
    };
    frame.render_widget(Paragraph::new(line), area);
}

fn render_hint(frame: &mut Frame<'_>, area: Rect) {
    let hint = Line::from(Span::styled(
        "  ↵ send · shift+↵ newline · ctrl-o tools · wheel/pgup-pgdn scroll · ctrl-c quit",
        theme::hint_style(),
    ));
    frame.render_widget(Paragraph::new(hint), area);
}

/// Approximate the number of visual rows a wrapped paragraph will occupy,
/// for plain text under `Wrap { trim: false }`: each source line takes
/// `ceil(chars / width)` rows, with a minimum of one.
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

#[cfg(test)]
mod tests {
    use ratatui::backend::TestBackend;
    use ratatui::Terminal;

    use super::super::app::App;
    use super::draw;

    #[test]
    fn test_input_soft_wrap_grows_box() {
        let mut app = App::new();
        // 300 'A's in an 80-wide terminal -> wraps to ~4 visual rows
        // at 78-wide inner width (80 - 2 for borders).
        let long_str = "A".repeat(300);
        app.textarea.insert_str(&long_str);

        let mut terminal = Terminal::new(TestBackend::new(80, 40)).unwrap();
        terminal.draw(|f| draw(f, &mut app)).unwrap();
        let buffer = terminal.backend().buffer();

        // Find the input box via its rounded border characters.
        let mut top_y = None;
        let mut bot_y = None;
        for y in 0..40u16 {
            let cell = &buffer[(0, y)];
            let ch = cell.symbol().chars().next().unwrap_or(' ');
            if ch == '\u{256d}' {
                top_y = Some(y);
            }
            if ch == '\u{2570}' {
                bot_y = Some(y);
            }
        }

        let top = top_y.expect("input box top border not found");
        let bot = bot_y.expect("input box bottom border not found");
        let height = bot - top + 1;

        // Without soft-wrap, the box would be 3 rows (1 logical line + 2
        // border). With wrap enabled and 300 chars at 78-wide inner width,
        // it wraps to ceil(300/78) = 4 visual rows + 2 border = 6 rows.
        assert!(
            height > 3,
            "input box height {height} should grow beyond 3 with long input",
        );

        // Verify that content actually wraps: the first character 'A' should
        // appear on the first content row (rather than being scrolled away by
        // horizontal scrolling).
        let first_content_row = top + 1;
        let first_char = buffer[(1, first_content_row)].symbol();
        assert_eq!(
            first_char, "A",
            "first char should be visible with soft-wrap"
        );

        // Verify that characters from a wrapped portion appear on a
        // subsequent row. With 78-wide inner, index 78 lands on row 2 of
        // the content area.
        let second_content_row = first_content_row + 1;
        let char_on_wrapped = buffer[(1, second_content_row)].symbol();
        assert_eq!(
            char_on_wrapped, "A",
            "wrapped content should appear on row below"
        );
    }
}
