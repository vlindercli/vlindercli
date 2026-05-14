//! `CompleteMessage`: Runtime → Harness (submission finished).

use crate::domain::ToolCall;
use serde::{Deserialize, Serialize};

use super::super::diagnostics::RuntimeDiagnostics;
use super::identity::{DagNodeId, MessageId};

/// Data-plane complete payload — everything NOT in the subject (ADR 121).
///
/// The subject carries routing (session, branch, submission, agent, harness)
/// and protocol version. This struct carries the domain data that goes in the
/// NATS payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteMessage {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub dag_parent: DagNodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub diagnostics: RuntimeDiagnostics,
    /// The agent's text content from this response.
    /// Populated by the sidecar using the provider's `ToolCallParser`.
    /// None if the agent returned only tool calls with no text.
    #[serde(default)]
    pub content: Option<String>,
    /// Parsed tool calls from the agent's response.
    /// Populated by the sidecar using the provider's `ToolCallParser`.
    /// None if the agent returned a final text response (no tool calls).
    #[serde(default)]
    pub tool_calls: Option<Vec<ToolCall>>,
    #[serde(with = "super::base64_serde")]
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_json_round_trip() {
        let msg = CompleteMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some("state-abc".to_string()),
            diagnostics: RuntimeDiagnostics::placeholder(42),
            content: None,
            tool_calls: None,
            payload: b"hello world".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: CompleteMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn complete_payload_is_base64() {
        let msg = CompleteMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            content: None,
            tool_calls: None,
            payload: b"hello world".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(raw["payload"].is_string());
        assert_eq!(raw["payload"].as_str().unwrap(), "aGVsbG8gd29ybGQ=");
    }

    #[test]
    fn complete_omits_none_state() {
        let msg = CompleteMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            content: None,
            tool_calls: None,
            payload: b"test".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert!(raw.get("state").is_none());
    }
}
