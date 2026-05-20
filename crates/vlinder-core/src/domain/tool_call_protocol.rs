//! Protocol boundary trait for tool call encoding and result decoding.
//!
//! The MCP crate owns the protocol-specific logic. The harness and
//! provider server work only with protocol-agnostic types carrying
//! `Vec<u8>`. This trait defines the boundary between them.

use serde_json::Value;

/// Protocol boundary for tool call encoding and result decoding.
pub trait ToolCallProtocol: Send + Sync {
    /// Encode a tool name and arguments into opaque bytes for the queue.
    fn encode_tool_call(&self, name: &str, arguments: &Value) -> Vec<u8>;
    /// Decode opaque payload bytes back into a text string for LLM serialization.
    fn decode_tool_result(&self, payload: &[u8]) -> String;
}
