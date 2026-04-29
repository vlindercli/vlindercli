use serde::{Deserialize, Serialize};

/// Result of dispatching a tool call, returned to the agent on re-invocation.
///
/// Currently carries text content only. Multi-modal content blocks will
/// be added when a concrete need arises.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolResult {
    /// Correlates to `ToolCall::id`.
    pub tool_call_id: String,
    /// The result content.
    pub content: String,
    /// Whether the tool execution failed.
    #[serde(default)]
    pub is_error: bool,
}
