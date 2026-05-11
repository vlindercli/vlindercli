//! Interactive REPL TUI built on ratatui.
//!
//! The only public surface is the `run` function. Internal modules are
//! private so callers don't depend on the layout:
//!
//! - [`app`] — UI state (transcript, input, scroll, spinner).
//! - [`event`] — terminal event → state-mutation dispatcher.
//! - [`view`] — frame rendering.
//! - [`theme`] — colors, spinner cadence, input styling.
//! - [`run`] — async event loop that wires everything together.

mod app;
mod event;
mod run;
mod theme;
mod view;

pub use run::run;
