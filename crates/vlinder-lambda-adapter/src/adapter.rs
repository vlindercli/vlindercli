//! Adapter core logic — testable without real infrastructure.
//!
//! Pure functions for deserializing invocations, building diagnostics,
//! and constructing complete messages. The main module wires these to
//! the Lambda Runtime API and real HTTP.

use vlinder_core::domain::{DataRoutingKey, InvokeMessage, RuntimeDiagnostics, RuntimeInfo};

/// Lambda invocation payload — routing key + message, serialized together.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct LambdaInvokePayload {
    pub key: DataRoutingKey,
    pub msg: InvokeMessage,
}

/// Deserialize a Lambda invocation body.
///
/// The daemon serializes a `LambdaInvokePayload` as the Lambda payload.
pub fn deserialize_invoke(body: &[u8]) -> Result<LambdaInvokePayload, String> {
    serde_json::from_slice(body)
        .map_err(|e| format!("failed to deserialize LambdaInvokePayload: {e}"))
}

/// Build Lambda-specific runtime diagnostics.
pub fn build_lambda_diagnostics(
    function_name: &str,
    region: &str,
    duration_ms: u64,
) -> RuntimeDiagnostics {
    RuntimeDiagnostics {
        stderr: Vec::new(),
        runtime: RuntimeInfo::Lambda {
            function_name: function_name.to_string(),
            region: region.to_string(),
        },
        duration_ms,
        health: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{
        AgentName, BranchId, DagNodeId, DataMessageKind, DataRoutingKey, HarnessType,
        InvokeDiagnostics, InvokeMessage, MessageId, RuntimeType, SessionId, SubmissionId,
    };

    /// Build a test `LambdaInvokePayload` and serialize it to JSON, simulating
    /// what the daemon sends as the Lambda payload.
    fn make_invoke_json(payload: &[u8]) -> Vec<u8> {
        let invoke = LambdaInvokePayload {
            key: DataRoutingKey {
                session: SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string())
                    .unwrap(),
                branch: BranchId::from(1),
                submission: SubmissionId::from("sub-test".to_string()),
                kind: DataMessageKind::Invoke {
                    harness: HarnessType::Cli,
                    runtime: RuntimeType::Lambda,
                    agent: AgentName::new("echo-lambda"),
                },
            },
            msg: InvokeMessage {
                id: MessageId::new(),
                dag_id: DagNodeId::root(),
                state: Some("state-abc".to_string()),
                diagnostics: InvokeDiagnostics {
                    harness_version: "0.1.0".to_string(),
                },
                dag_parent: DagNodeId::root(),
                payload: payload.to_vec(),
            },
        };
        serde_json::to_vec(&invoke).unwrap()
    }

    #[test]
    fn deserialize_invoke_round_trips() {
        let json = make_invoke_json(b"hello from lambda");
        let inv = deserialize_invoke(&json).unwrap();

        let DataMessageKind::Invoke { agent, runtime, .. } = &inv.key.kind else {
            panic!("expected Invoke");
        };
        assert_eq!(agent.as_str(), "echo-lambda");
        assert_eq!(*runtime, RuntimeType::Lambda);
        assert_eq!(inv.msg.payload, b"hello from lambda");
        assert_eq!(inv.msg.state, Some("state-abc".to_string()));
        assert_eq!(inv.msg.diagnostics.harness_version, "0.1.0");
    }

    #[test]
    fn deserialize_invoke_rejects_invalid_json() {
        let result = deserialize_invoke(b"not json at all");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to deserialize"));
    }

    #[test]
    fn deserialize_invoke_rejects_wrong_schema() {
        let result = deserialize_invoke(b"{\"foo\": \"bar\"}");
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_invoke_preserves_empty_payload() {
        let json = make_invoke_json(b"");
        let inv = deserialize_invoke(&json).unwrap();
        assert!(inv.msg.payload.is_empty());
    }

    #[test]
    fn deserialize_invoke_preserves_binary_payload() {
        let binary: Vec<u8> = (0..=255).collect();
        let json = make_invoke_json(&binary);
        let inv = deserialize_invoke(&json).unwrap();
        assert_eq!(inv.msg.payload, binary);
    }

    #[test]
    fn build_lambda_diagnostics_populates_fields() {
        let diag = build_lambda_diagnostics("my-function", "eu-west-1", 450);

        assert_eq!(diag.duration_ms, 450);
        assert!(diag.stderr.is_empty());
        match &diag.runtime {
            RuntimeInfo::Lambda {
                function_name,
                region,
            } => {
                assert_eq!(function_name, "my-function");
                assert_eq!(region, "eu-west-1");
            }
            other @ RuntimeInfo::Container { .. } => panic!("expected Lambda, got {other:?}"),
        }
    }

    #[test]
    fn complete_carries_output_and_state() {
        let diag = build_lambda_diagnostics("fn", "us-east-1", 100);

        let complete = vlinder_core::domain::CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: Some("final-state-hash".to_string()),
            diagnostics: diag,
            payload: b"output bytes".to_vec(),
        };

        assert_eq!(complete.payload, b"output bytes");
        assert_eq!(complete.state, Some("final-state-hash".to_string()));
    }

    #[test]
    fn complete_with_no_state() {
        let diag = build_lambda_diagnostics("fn", "us-east-1", 50);

        let complete = vlinder_core::domain::CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: diag,
            payload: b"out".to_vec(),
        };

        assert!(complete.state.is_none());
    }
}
