//! `McpProtocol` — concrete `ToolCallProtocol` implementation for MCP.
//!
//! Delegates to `crate::boundary` functions for serialization
//! and extraction.

use serde_json::Value;
use vlinder_core::domain::ToolCallProtocol;

use crate::boundary;

/// MCP protocol adapter — converts between opaque `Vec<u8>` and
/// MCP-specific types at the boundary.
pub struct McpProtocol;

impl ToolCallProtocol for McpProtocol {
    fn encode_tool_call(&self, name: &str, arguments: &Value) -> Vec<u8> {
        boundary::serialize_request(name, arguments)
    }

    fn decode_tool_result(&self, payload: &[u8]) -> String {
        boundary::extract_tool_content(payload)
    }
}
