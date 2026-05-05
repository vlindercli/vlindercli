//! `ResponseV2`: harness-mediated service response payload.

use serde::{Deserialize, Serialize};

use super::identity::{DagNodeId, MessageId};
use crate::domain::SvcResponseDiagnostics;

/// Harness‑mediated service response — returned by a service worker
/// via NATS `svc_response` subjects.
///
/// Unlike `ResponseMessage` (sidecar‑mediated, opaque payload + HTTP status),
/// `ResponseV2` carries opaque payload bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseV2 {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub correlation_id: MessageId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default)]
    pub diagnostics: SvcResponseDiagnostics,
    #[serde(with = "super::base64_serde")]
    pub payload: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_v2_json_round_trip() {
        let msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"result content".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ResponseV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn response_v2_payload_is_base64_in_json() {
        let msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"hello".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        // payload should be base64-encoded in the JSON
        assert!(json.contains("\"payload\":\""));
        assert!(json.len() > 50); // should have base64 content
    }

    #[test]
    fn response_v2_state_omitted_when_none() {
        let msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"ok".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("state"), "state should be omitted when None");
    }

    #[test]
    fn response_v2_state_serialized_when_present() {
        let msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: MessageId::new(),
            state: Some("state-hash".to_string()),
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"ok".to_vec(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("state"), "state should appear when present");
        let back: ResponseV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back.state, Some("state-hash".to_string()));
    }
}
