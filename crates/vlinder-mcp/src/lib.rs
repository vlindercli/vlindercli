//! Vlinder MCP service worker.

pub mod mcp_client;
pub mod worker;

pub use worker::run_mcp_worker;
