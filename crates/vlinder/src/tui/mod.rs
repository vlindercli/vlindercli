//! Interactive REPL TUI built on tuirealm.
//!
//! Most modules are private so callers don't depend on the layout.
//! The `projection` module is public because `commands/agent.rs` and
//! `commands/fleet.rs` need to walk the chain before entering the TUI.
//!
//! - [`projection`] — chain-to-transcript projection (public).
//! - [`realm`] — tuirealm model, components, and event integration.
//! - [`theme`] — colors, spinner cadence, input styling.

pub mod projection;
mod realm;
mod theme;
mod types;

pub use realm::run;
