//! Terminal event handling. Translates crossterm key/mouse events into
//! state mutations on `App` and high-level outcomes for the run loop.
//! Knows nothing about rendering.

use ratatui::crossterm::event::{
    Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind,
};

use super::app::App;

/// What the run loop should do after an event is dispatched.
pub enum EventOutcome {
    /// Stay in the loop.
    Continue,
    /// Exit the REPL cleanly.
    Exit,
    /// Submit the buffered input as a new turn.
    Submit(String),
}

/// Dispatch one terminal event.
pub fn handle_event(ev: &Event, app: &mut App) -> EventOutcome {
    match ev {
        Event::Key(key) => handle_key(*key, app),
        Event::Mouse(mouse) => {
            handle_mouse(*mouse, app);
            EventOutcome::Continue
        }
        _ => EventOutcome::Continue,
    }
}

fn handle_mouse(mouse: MouseEvent, app: &mut App) {
    match mouse.kind {
        MouseEventKind::ScrollUp => app.scroll_up(3),
        MouseEventKind::ScrollDown => app.scroll_down(3),
        _ => {}
    }
}

fn handle_key(key: KeyEvent, app: &mut App) -> EventOutcome {
    match key.code {
        KeyCode::Enter if key.modifiers.is_empty() => {
            let input = app.textarea.lines().join("\n").trim().to_string();
            if input.is_empty() {
                EventOutcome::Continue
            } else {
                EventOutcome::Submit(input)
            }
        }
        KeyCode::Enter if key.modifiers.contains(KeyModifiers::SHIFT) => {
            app.textarea.insert_newline();
            EventOutcome::Continue
        }
        KeyCode::Char('c' | 'd') if key.modifiers == KeyModifiers::CONTROL => EventOutcome::Exit,

        KeyCode::PageUp => {
            app.scroll_up(5);
            EventOutcome::Continue
        }
        KeyCode::PageDown => {
            app.scroll_down(5);
            EventOutcome::Continue
        }
        KeyCode::Home if key.modifiers == KeyModifiers::CONTROL => {
            app.jump_top();
            EventOutcome::Continue
        }
        KeyCode::End if key.modifiers == KeyModifiers::CONTROL => {
            app.jump_bottom();
            EventOutcome::Continue
        }
        KeyCode::Char('o') if key.modifiers == KeyModifiers::CONTROL => {
            app.tools_expanded = !app.tools_expanded;
            app.jump_bottom();
            EventOutcome::Continue
        }

        _ => {
            app.textarea.input(key);
            EventOutcome::Continue
        }
    }
}
