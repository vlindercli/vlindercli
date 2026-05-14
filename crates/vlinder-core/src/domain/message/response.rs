//! `ResponseMessage`: data-plane response payload (ADR 121).

use serde::{Deserialize, Serialize};

use super::super::diagnostics::ServiceDiagnostics;
#[cfg(test)]
use super::super::operation::Operation;
use super::identity::{DagNodeId, MessageId};

/// Data-plane response payload — everything NOT in the subject (ADR 121).
///
/// The subject carries routing (session, branch, submission, agent, service,
/// operation, sequence) and protocol version. This struct carries the domain
/// data that goes in the NATS payload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseMessage {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub dag_parent: DagNodeId,
    pub correlation_id: MessageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    pub diagnostics: ServiceDiagnostics,
    #[serde(with = "super::base64_serde")]
    pub payload: Vec<u8>,
    pub status_code: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_json_round_trip() {
        let msg = ResponseMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: MessageId::from("req-1".to_string()),
            state: Some("state-abc".to_string()),
            diagnostics: ServiceDiagnostics::storage(
                super::super::super::ServiceType::Kv,
                "sqlite",
                Operation::Get,
                42,
                10,
            ),
            payload: b"hello".to_vec(),
            status_code: 200,
            checkpoint: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ResponseMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn response_payload_is_base64() {
        let msg = ResponseMessage {
            id: MessageId::from("msg-1".to_string()),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: MessageId::from("req-1".to_string()),
            state: None,
            diagnostics: ServiceDiagnostics::storage(
                super::super::super::ServiceType::Kv,
                "sqlite",
                Operation::Get,
                0,
                0,
            ),
            payload: b"hello".to_vec(),
            status_code: 200,
            checkpoint: None,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let raw: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(raw["payload"].as_str().unwrap(), "aGVsbG8=");
    }
}
