use crate::domain::{Message, ToolCall};

/// Parsed result of an agent's HTTP response.
///
/// Produced by the sidecar from the agent's raw response payload.
/// Carried on `CompleteMessage` so the harness can route tool calls
/// without knowing the provider's wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedResponse {
    /// The agent's text content (may be present alongside `tool_calls`).
    pub content: Option<String>,
    /// Tool calls in the agent's response, if any.
    pub tool_calls: Option<Vec<ToolCall>>,
}

/// Parser for provider-specific agent response payloads.
///
/// Trait in vlinder-core (domain contract), implementations in provider
/// crates (wire-format knowledge). The sidecar is the consumer — it calls
/// `parse_response` when it receives the agent's HTTP response, and
/// `serialize_conversation` when it needs to build the HTTP body for
/// the next invocation.
pub trait ToolCallParser: Send + Sync {
    /// Parse an agent's HTTP response payload.
    ///
    /// Returns `Ok(ParsedResponse)` with `tool_calls = Some(...)` if the
    /// agent's response contains tool calls, or `tool_calls = None` for
    /// a final text response. Returns `Err` if the payload could not be
    /// parsed.
    fn parse_response(&self, payload: &[u8]) -> Result<ParsedResponse, ParseError>;

    /// Serialize a conversation to a provider-specific HTTP body.
    ///
    /// The sidecar calls this before `POSTing` to the agent container.
    /// Takes a flat slice of `Message` entries (history + `current_input`
    /// combined by the sidecar) and produces the provider-specific JSON
    /// that the agent's LLM SDK expects.
    fn serialize_conversation(&self, messages: &[Message]) -> Vec<u8>;
}

#[derive(Debug)]
pub enum ParseError {
    Json(serde_json::Error),
    InvalidStructure(String),
}
