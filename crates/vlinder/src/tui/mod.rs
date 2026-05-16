//! Interactive REPL TUI built on ratatui.
//!
//! Most modules are private so callers don't depend on the layout.
//! The `projection` module is public because `commands/agent.rs` and
//! `commands/fleet.rs` need to walk the chain before entering the TUI.
//!
//! - [`app`] — UI state (transcript, input, scroll, spinner).
//! - [`event`] — terminal event → state-mutation dispatcher.
//! - [`projection`] — chain-to-transcript projection (public).
//! - [`run`] — async event loop that wires everything together.
//! - [`theme`] — colors, spinner cadence, input styling.
//! - [`view`] — frame rendering.

mod app;
mod event;
pub mod projection;
mod run;
mod theme;
mod view;

pub use run::run;
