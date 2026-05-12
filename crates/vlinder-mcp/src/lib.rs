//! Vlinder MCP service worker.

pub mod boundary;
pub mod mcp_client;
pub mod protocol;
pub mod worker;

pub use protocol::McpProtocol;
pub use worker::run_mcp_worker;
