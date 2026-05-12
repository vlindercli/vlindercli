use super::ToolCallId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A tool call in an agent's response, requesting the harness to dispatch.
///
/// Canonical internal representation. All vendor wire formats (`OpenAI`,
/// `Anthropic`, `Gemini`) map to this type. Provider-specific parsers in the
/// sidecar handle string↔object translation at the boundary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    /// Unique identifier for this tool call within the turn.
    pub id: ToolCallId,
    /// Tool name (e.g., `delegate_agent`, `call_inference`).
    pub name: String,
    /// Parsed JSON arguments for the tool.
    pub arguments: Value,
}
