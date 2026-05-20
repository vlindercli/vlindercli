//! `InvokeMessage`: Data-plane invoke payload (ADR 121).

use serde::{Deserialize, Serialize};

use super::super::diagnostics::InvokeDiagnostics;
use super::identity::{DagNodeId, MessageId};
use crate::domain::Message;

/// Data-plane invoke payload — everything NOT in the subject.
///
/// The subject carries routing (session, branch, submission, harness, runtime, agent)
/// and protocol version. This struct carries the domain data that goes in the
/// NATS payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InvokeMessage {
    pub id: MessageId,
    /// Content-addressed DAG node ID. Computed by the recording queue
    /// before publish; empty on initial construction.
    #[serde(default = "DagNodeId::root")]
    pub dag_id: DagNodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub diagnostics: InvokeDiagnostics,
    pub dag_parent: DagNodeId,
    /// The new input that triggered this invocation.
    pub current_input: Vec<Message>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_message_json_round_trip() {
        let msg = InvokeMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            state: Some("abc123".to_string()),
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            current_input: vec![Message::User {
                content: "hello world".to_string(),
            }],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let back: InvokeMessage = serde_json::from_str(&json).unwrap();

        assert_eq!(back, msg);
    }

    #[test]
    fn invoke_message_omits_none_state() {
        let msg = InvokeMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            current_input: vec![Message::User {
                content: "test".to_string(),
            }],
        };

        let json = serde_json::to_string(&msg).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert!(raw.get("state").is_none());
    }
}
