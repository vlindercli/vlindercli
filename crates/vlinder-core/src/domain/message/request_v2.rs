//! `RequestV2`: harness-mediated service request payload.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::identity::{DagNodeId, MessageId, ToolCallId};
use crate::domain::SvcRequestDiagnostics;

/// Harness-mediated service request — carries tool call parameters
/// to a service worker via NATS `svc_request` subjects.
///
/// Unlike `RequestMessage` (sidecar-mediated, opaque payload),
/// `RequestV2` carries structured call data. Routing metadata
/// (service type, provider, operation) lives in the NATS subject
/// and the parsed `SvcRoutingKey` — the worker reads it from there.
/// This payload is just the call parameters.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequestV2 {
    pub id: MessageId,
    pub dag_id: DagNodeId,
    pub tool_call_id: ToolCallId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
    #[serde(default)]
    pub diagnostics: SvcRequestDiagnostics,
    pub arguments: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_v2_json_round_trip() {
        let msg = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            arguments: json!({"key": "value"}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: RequestV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, msg);
    }

    #[test]
    fn request_v2_arguments_preserved() {
        let msg = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            arguments: json!({"nested": {"array": [1, 2, 3]}}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: RequestV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back.arguments, msg.arguments);
    }

    #[test]
    fn request_v2_state_serialized_when_present() {
        let msg = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: ToolCallId::new(),
            state: Some("state-hash-abc".to_string()),
            diagnostics: SvcRequestDiagnostics::default(),
            arguments: json!({"key": "value"}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(
            json.contains("state"),
            "state should appear in JSON when present"
        );
        let back: RequestV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back.state, Some("state-hash-abc".to_string()));
    }

    #[test]
    fn request_v2_state_omitted_when_none() {
        let msg = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            arguments: json!({"key": "value"}),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(!json.contains("state"), "state should be omitted when None");
    }
}
