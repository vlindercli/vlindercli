//! Async event loop. Orchestrates rendering, event handling, and the
//! turn lifecycle (push user → draw → drive process → animate → push
//! response). Doesn't know about colors, keybindings, or how a frame is
//! painted — only when to do each.

use std::future::Future;
use std::time::Duration;

use ratatui::crossterm::event;
use ratatui::prelude::Backend;
use ratatui::Terminal;
use tokio::sync::mpsc;
use tokio::time::Instant;
use vlinder_core::domain::{HarnessEvent, RunResult};

use super::app::{App, Entry};
use super::event::{handle_event, EventOutcome};
use super::theme::{SPINNER_FRAMES, SPINNER_TICK, SPLASH_DURATION};
use super::view::{draw, draw_splash};

/// Run an interactive REPL loop using ratatui.
///
/// `process` is invoked once per submitted line and yields a future whose
/// output is appended to the transcript. `event_rx` receives `HarnessEvent`
/// messages during execution — the caller constructs the channel and captures
/// the sender in the `process` closure.
///
/// `initial_transcript` is a pre-populated list of entries from a resumed
/// session, projected from the DAG chain by the caller. Pass an empty `Vec`
/// to start a fresh session.
///
/// The future is polled concurrently with a spinner-animation timer and the
/// event channel so "thinking..." animates while a response is in flight and
/// tool-call entries appear incrementally.
pub async fn run<F, Fut>(
    mut process: F,
    mut event_rx: mpsc::Receiver<HarnessEvent>,
    initial_transcript: Vec<Entry>,
) where
    F: FnMut(String) -> Fut,
    Fut: Future<Output = RunResult>,
{
    let mut terminal = ratatui::init();
    splash_phase(&mut terminal);

    let mut app = App::with_initial_transcript(initial_transcript);

    'main: loop {
        let _ = terminal.draw(|frame| draw(frame, &mut app));

        // Block on the next event; we're idle here so blocking is fine.
        let Ok(ev) = event::read() else { continue };
        match handle_event(&ev, &mut app) {
            EventOutcome::Continue => {}
            EventOutcome::Exit => break 'main,
            EventOutcome::Submit(input) => {
                if input == "exit" || input == "quit" {
                    break 'main;
                }
                run_turn(&mut terminal, &mut app, &mut process, input, &mut event_rx).await;
            }
        }
    }

    ratatui::restore();
}

/// Execute one user → response turn. Pushes the user message, drives
/// `process` to completion while animating the spinner and draining the
/// event channel, and pushes the response. Drains events during the wait
/// so scroll / quit work in flight.
async fn run_turn<B, F, Fut>(
    terminal: &mut Terminal<B>,
    app: &mut App,
    process: &mut F,
    input: String,
    event_rx: &mut mpsc::Receiver<HarnessEvent>,
) where
    B: Backend,
    F: FnMut(String) -> Fut,
    Fut: Future<Output = RunResult>,
{
    app.push_user(input.clone());
    app.clear_input();
    app.spinning = true;
    app.spinner_frame = 0;
    app.jump_bottom();
    let _ = terminal.draw(|frame| draw(frame, app));

    let fut = process(input);
    tokio::pin!(fut);

    let mut next_tick = Instant::now() + SPINNER_TICK;

    let mut pending_call_indices: Vec<usize> = Vec::new();
    let mut received_assistant_content = false;

    let response = loop {
        // biased: poll the response first so a ready future preempts a stale tick.
        let outcome = tokio::select! {
            biased;
            response = &mut fut => Some(response),
            Some(event) = event_rx.recv() => {
                handle_harness_event(
                    app,
                    event,
                    &mut pending_call_indices,
                    &mut received_assistant_content,
                );
                None
            },
            () = tokio::time::sleep_until(next_tick) => None,
        };

        if let Some(r) = outcome {
            break r;
        }

        app.spinner_frame = (app.spinner_frame + 1) % SPINNER_FRAMES.len();
        next_tick += SPINNER_TICK;

        if drain_events(app) == DrainOutcome::Exit {
            app.spinning = false;
            return;
        }

        let _ = terminal.draw(|frame| draw(frame, app));
    };

    app.spinning = false;

    // Events are authoritative during streaming — assistant content and tool
    // calls were already rendered as they arrived. The fallback only fires for
    // the error path: agent.rs synthesizes a RunResult with `content: Some("[error] ...")`
    // when the harness call fails, without emitting any events.
    if !received_assistant_content {
        if let Some(text) = response.content {
            app.push_assistant(text);
        }
    }
    app.jump_bottom();
}

/// Process a `HarnessEvent` from the event channel, mutating `App` state.
fn handle_harness_event(
    app: &mut App,
    event: HarnessEvent,
    pending_indices: &mut Vec<usize>,
    received_assistant_content: &mut bool,
) {
    match event {
        HarnessEvent::AssistantContent { text, .. } => {
            *received_assistant_content = true;
            app.push_assistant(text);
        }
        HarnessEvent::ToolCallStarted { tool_call } => {
            let idx = app.push_tool_call_pending(&tool_call);
            pending_indices.push(idx);
        }
        HarnessEvent::ToolCallCompleted { trace } => {
            if pending_indices.is_empty() {
                // No pending index — push as completed directly.
                app.push_tool_call(&trace);
            } else {
                // Update the oldest pending entry.
                let idx = pending_indices.remove(0);
                app.finalize_tool_call(idx, &trace);
            }
        }
        // Other events are not wired in this branch.
        HarnessEvent::RunStarted { .. }
        | HarnessEvent::TurnStarted { .. }
        | HarnessEvent::ToolCallApprovalRequired { .. }
        | HarnessEvent::TurnCompleted { .. }
        | HarnessEvent::RunCompleted { .. } => {}
    }
}

/// Show the splash screen until either a key is pressed or
/// `SPLASH_DURATION` elapses. Synchronous because `event::poll` is sync; the
/// tiny blocking window is fine inside our async caller.
fn splash_phase<B: Backend>(terminal: &mut Terminal<B>) {
    let deadline = std::time::Instant::now() + SPLASH_DURATION;
    loop {
        let _ = terminal.draw(draw_splash);

        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            return;
        }

        // Cap each poll at 80ms so the splash redraws don't appear frozen if
        // the terminal is resized mid-splash.
        let timeout = remaining.min(Duration::from_millis(80));
        if event::poll(timeout).unwrap_or(false) {
            let _ = event::read();
            return;
        }
    }
}

#[derive(PartialEq, Eq)]
enum DrainOutcome {
    Continued,
    Exit,
}

/// Drain pending terminal events without blocking. Scroll/quit work while
/// thinking; submits and edits are dropped (the input is already disabled).
fn drain_events(app: &mut App) -> DrainOutcome {
    while event::poll(Duration::from_millis(0)).unwrap_or(false) {
        let Ok(ev) = event::read() else { continue };
        match handle_event(&ev, app) {
            EventOutcome::Exit => return DrainOutcome::Exit,
            EventOutcome::Submit(_) | EventOutcome::Continue => {}
        }
    }
    DrainOutcome::Continued
}
