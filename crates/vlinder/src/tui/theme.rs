//! Visual theme: colors, style constructors, spinner cadence, and the
//! styling applied to the input editor. The single source of truth for
//! "how the TUI looks." Tweak appearance here, not in render code.

use std::time::Duration;

use ratatui::style::{Color, Modifier, Style};
use ratatui::widgets::{Block, BorderType, Borders};
use ratatui_textarea::{TextArea, WrapMode};

// ── Spinner ──────────────────────────────────────────────────────────────

/// Braille spinner frames (10-frame cycle).
pub const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Tick interval for the spinner animation.
pub const SPINNER_TICK: Duration = Duration::from_millis(80);

/// How long the splash screen displays before auto-dismissing. Any keypress
/// dismisses it earlier.
pub const SPLASH_DURATION: Duration = Duration::from_millis(1800);

// ── Color palette ────────────────────────────────────────────────────────

/// Background tint for user-message rows. Cool dark blue-gray.
const USER_BG: Color = Color::Rgb(40, 42, 54);

/// Background tint for assistant-message rows. Neutral-cool dark green-gray —
/// chosen to be perceptibly distinct from `USER_BG` while staying subtle.
const ASSISTANT_BG: Color = Color::Rgb(38, 52, 48);

/// Background tint for tool-call rows. Warm dark amber/brown — clearly
/// distinct from User and Assistant.
const TOOL_CALL_BG: Color = Color::Rgb(58, 46, 32);

/// Accent color — prompt marker, spinner, welcome heading.
const ACCENT: Color = Color::Cyan;

/// Muted color — hints, borders, secondary text.
const DIM: Color = Color::DarkGray;

/// Warning color — used for the "scrolled up" status indicator.
const WARN: Color = Color::Yellow;

// ── Style constructors ───────────────────────────────────────────────────

/// Style for the cyan `>` prompt marker on a user message.
pub fn user_prompt_style() -> Style {
    Style::default()
        .bg(USER_BG)
        .fg(ACCENT)
        .add_modifier(Modifier::BOLD)
}

/// Style for the body text of a user message.
pub fn user_text_style() -> Style {
    Style::default().bg(USER_BG).add_modifier(Modifier::BOLD)
}

/// Style for the trailing space-padding that extends the user-row bar to
/// the right margin.
pub fn user_pad_style() -> Style {
    Style::default().bg(USER_BG)
}

/// Style for the body text of an assistant message.
pub fn assistant_text_style() -> Style {
    Style::default().bg(ASSISTANT_BG)
}

/// Style for the trailing space-padding that extends the assistant-row bar
/// to the right margin.
pub fn assistant_pad_style() -> Style {
    Style::default().bg(ASSISTANT_BG)
}

/// Style for tool-call rows (collapsed or expanded). Applies a subtle warm
/// background to distinguish them from assistant entries.
pub fn tool_call_row_style() -> Style {
    Style::default().bg(TOOL_CALL_BG)
}

/// Style for the welcome heading shown in an empty transcript.
pub fn welcome_title_style() -> Style {
    Style::default().fg(ACCENT)
}

/// Style for the welcome body text.
pub fn welcome_body_style() -> Style {
    Style::default().fg(DIM)
}

/// Style for the spinner glyph and "thinking..." label.
pub fn spinner_style() -> Style {
    Style::default().fg(ACCENT)
}

/// Style for the "(scrolled up — press Ctrl+End to jump to latest)" status.
pub fn scrolled_up_style() -> Style {
    Style::default().fg(WARN)
}

/// Style for the keybinding hint at the bottom of the screen.
pub fn hint_style() -> Style {
    Style::default().fg(DIM)
}

// ── Tool call rendering ──────────────────────────────────────────────────

/// Style for the glyph (⏵ / ✗) in tool-call entries.
pub fn tool_glyph_style() -> Style {
    Style::default().fg(ACCENT)
}

/// Style for the glyph when the tool call errored.
pub fn tool_glyph_error_style() -> Style {
    Style::default().fg(Color::Red)
}

/// Style for the tool name in tool-call entries.
pub fn tool_name_style() -> Style {
    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
}

/// Style for tool arguments.
pub fn tool_args_style() -> Style {
    Style::default().fg(DIM)
}

/// Style for the "args:" label prefix.
pub fn tool_args_label_style() -> Style {
    Style::default().fg(DIM).add_modifier(Modifier::BOLD)
}

/// Style for tool results.
pub fn tool_result_style() -> Style {
    Style::default()
}

/// Style for the "result:" label prefix.
pub fn tool_result_label_style() -> Style {
    Style::default().fg(DIM).add_modifier(Modifier::BOLD)
}

/// Style for the duration display in tool-call entries.
pub fn tool_duration_style() -> Style {
    Style::default().fg(DIM)
}

// ── Splash screen ────────────────────────────────────────────────────────

/// Style for the big ASCII wordmark on the splash.
pub fn logo_style() -> Style {
    Style::default().fg(ACCENT).add_modifier(Modifier::BOLD)
}

/// Style for the tagline below the wordmark.
pub fn tagline_style() -> Style {
    Style::default().add_modifier(Modifier::BOLD)
}

/// Style for the motto line.
pub fn motto_style() -> Style {
    Style::default().fg(DIM)
}

/// Style for the "(press any key)" hint on the splash.
pub fn splash_hint_style() -> Style {
    Style::default().fg(DIM)
}

// ── Input editor ─────────────────────────────────────────────────────────

/// Apply the input-editor theme: rounded dim border, default cursor line,
/// and placeholder copy.
pub fn configure_input(textarea: &mut TextArea<'static>) {
    textarea.set_block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(DIM)),
    );
    textarea.set_cursor_line_style(Style::default());
    textarea.set_placeholder_text("Type a message, then press Enter...");
    textarea.set_wrap_mode(WrapMode::WordOrGlyph);
}
