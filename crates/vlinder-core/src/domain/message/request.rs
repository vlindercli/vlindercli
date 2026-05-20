//! `RequestMessage`: data-plane request payload (ADR 121).

use serde::{Deserialize, Serialize};

use super::super::diagnostics::RequestDiagnostics;
use super::identity::{DagNodeId, MessageId};

/// Data-plane request payload — everything NOT in the subject (ADR 121).
///
/// The subject carries routing (session, branch, submission, agent, service,
/// operation, sequence) and protocol version. This struct carries the domain
/// data that goes in the NATS payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestMessage {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub dag_parent: DagNodeId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub diagnostics: RequestDiagnostics,
    #[serde(with = "super::base64_serde")]
    pub payload: Vec<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_json_round_trip() {
        let msg = RequestMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some("state-abc".to_string()),
            diagnostics: RequestDiagnostics {
                sequence: 1,
                endpoint: "/kv".to_string(),
                request_bytes: 42,
                received_at_ms: 1000,
            },
            payload: b"hello".to_vec(),
            checkpoint: Some("cp-1".to_string()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: RequestMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn request_payload_is_base64() {
        let msg = RequestMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: RequestDiagnostics {
                sequence: 1,
                endpoint: "/kv".to_string(),
                request_bytes: 0,
                received_at_ms: 0,
            },
            payload: b"hello".to_vec(),
            checkpoint: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(raw["payload"].as_str().unwrap(), "aGVsbG8=");
    }
}
