//! `ResponseV2`: harness-mediated service response payload.

use serde::{Deserialize, Serialize};

use super::identity::{DagNodeId, MessageId};

/// Harness‑mediated service response — returned by a service worker
/// via NATS `svc_response` subjects.
///
/// Unlike `ResponseMessage` (sidecar‑mediated, opaque payload + HTTP status),
/// `ResponseV2` carries structured tool result data: the content string
/// and an error flag, matching `ToolResult`'s shape.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResponseV2 {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub correlation_id: MessageId,
    pub content: String,
    #[serde(default)]
    pub is_error: bool,
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
            content: "result content".to_string(),
            is_error: false,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ResponseV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn response_v2_is_error_defaults_to_false() {
        // Serialize without is_error field, deserialize should default to false
        let json = format!(
            r#"{{
                "id": "{}",
                "dag_id": "",
                "correlation_id": "{}",
                "content": "ok"
            }}"#,
            MessageId::new(),
            MessageId::new()
        );
        let back: ResponseV2 = serde_json::from_str(&json).unwrap();
        assert!(!back.is_error);
        assert_eq!(back.content, "ok");
    }

    #[test]
    fn response_v2_is_error_true_serializes() {
        let msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: MessageId::new(),
            content: "error".to_string(),
            is_error: true,
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: ResponseV2 = serde_json::from_str(&json).unwrap();
        assert!(back.is_error);
        assert_eq!(back.content, "error");
    }
}
