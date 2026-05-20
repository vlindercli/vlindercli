//! Conversions between domain types and protobuf types for the harness service.
//!
//! The bidi streaming protocol uses typed proto messages instead of the
//! JSON-over-string interim. These conversions are used by both the server
//! bridge (commit 4) and the client bridge (commit 5).

use vlinder_core::domain::{RunResult, ToolCall, ToolCallId, ToolResult, ToolTrace, TurnTrace};

use crate::harness_service::proto;

// =============================================================================
// ToolCall ↔ ToolCallProto
// =============================================================================

impl From<ToolCall> for proto::ToolCallProto {
    fn from(tc: ToolCall) -> Self {
        proto::ToolCallProto {
            id: tc.id.to_string(),
            name: tc.name,
            arguments: serde_json::to_vec(&tc.arguments).unwrap_or_default(),
        }
    }
}

impl From<proto::ToolCallProto> for ToolCall {
    fn from(p: proto::ToolCallProto) -> Self {
        let arguments = serde_json::from_slice(&p.arguments).unwrap_or_default();
        ToolCall {
            id: ToolCallId::from(p.id.clone()),
            name: p.name,
            arguments,
        }
    }
}

// =============================================================================
// ToolResult ↔ ToolResultProto
// =============================================================================

impl From<ToolResult> for proto::ToolResultProto {
    fn from(tr: ToolResult) -> Self {
        proto::ToolResultProto {
            tool_call_id: tr.tool_call_id.to_string(),
            content: tr.content,
        }
    }
}

impl From<proto::ToolResultProto> for ToolResult {
    fn from(p: proto::ToolResultProto) -> Self {
        ToolResult {
            tool_call_id: ToolCallId::from(p.tool_call_id),
            content: p.content,
        }
    }
}

// =============================================================================
// ToolTrace ↔ ToolTraceProto
// =============================================================================

impl From<ToolTrace> for proto::ToolTraceProto {
    fn from(t: ToolTrace) -> Self {
        proto::ToolTraceProto {
            tool_call: Some(t.tool_call.into()),
            result: Some(t.result.into()),
            duration_ms: t.duration_ms,
        }
    }
}

impl From<proto::ToolTraceProto> for ToolTrace {
    fn from(p: proto::ToolTraceProto) -> Self {
        ToolTrace {
            tool_call: p.tool_call.map_or_else(
                || ToolCall {
                    id: ToolCallId::from(String::new()),
                    name: String::new(),
                    arguments: serde_json::Value::Null,
                },
                Into::into,
            ),
            result: p.result.map_or_else(
                || ToolResult {
                    tool_call_id: ToolCallId::from(String::new()),
                    content: Vec::new(),
                },
                Into::into,
            ),
            duration_ms: p.duration_ms,
        }
    }
}

// =============================================================================
// TurnTrace ↔ TurnTraceProto
// =============================================================================

impl From<TurnTrace> for proto::TurnTraceProto {
    fn from(t: TurnTrace) -> Self {
        proto::TurnTraceProto {
            assistant_content: t.assistant_content,
            tool_calls: t.tool_calls.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<proto::TurnTraceProto> for TurnTrace {
    fn from(p: proto::TurnTraceProto) -> Self {
        TurnTrace {
            assistant_content: p.assistant_content,
            tool_calls: p.tool_calls.into_iter().map(Into::into).collect(),
        }
    }
}

// =============================================================================
// RunResult ↔ RunResultProto
// =============================================================================

impl From<RunResult> for proto::RunResultProto {
    fn from(r: RunResult) -> Self {
        proto::RunResultProto {
            content: r.content,
            pending_tool_calls: r
                .pending_tool_calls
                .unwrap_or_default()
                .into_iter()
                .map(Into::into)
                .collect(),
            turns: r.turns.into_iter().map(Into::into).collect(),
            state_snapshot: r.state_snapshot,
        }
    }
}

impl From<proto::RunResultProto> for RunResult {
    fn from(p: proto::RunResultProto) -> Self {
        let pending = p
            .pending_tool_calls
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        RunResult {
            content: p.content,
            pending_tool_calls: if pending.is_empty() {
                None
            } else {
                Some(pending)
            },
            turns: p.turns.into_iter().map(Into::into).collect(),
            state_snapshot: p.state_snapshot,
        }
    }
}

// =============================================================================
// ApprovalMode ↔ ApprovalModeProto
// =============================================================================

impl From<vlinder_core::domain::ApprovalMode> for proto::ApprovalModeProto {
    fn from(m: vlinder_core::domain::ApprovalMode) -> Self {
        match m {
            vlinder_core::domain::ApprovalMode::Autonomous => proto::ApprovalModeProto::Autonomous,
            vlinder_core::domain::ApprovalMode::RequireApproval => {
                proto::ApprovalModeProto::RequireApproval
            }
        }
    }
}

impl From<proto::ApprovalModeProto> for vlinder_core::domain::ApprovalMode {
    fn from(p: proto::ApprovalModeProto) -> Self {
        match p {
            proto::ApprovalModeProto::Autonomous => vlinder_core::domain::ApprovalMode::Autonomous,
            proto::ApprovalModeProto::RequireApproval => {
                vlinder_core::domain::ApprovalMode::RequireApproval
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_tool_call() -> ToolCall {
        ToolCall {
            id: ToolCallId::from(String::from("call-1")),
            name: "get_weather".to_string(),
            arguments: json!({"location": "SF"}),
        }
    }

    fn sample_tool_result() -> ToolResult {
        ToolResult {
            tool_call_id: ToolCallId::from(String::from("call-1")),
            content: b"62F".to_vec(),
        }
    }

    #[test]
    fn tool_call_round_trip() {
        let tc = sample_tool_call();
        let proto: proto::ToolCallProto = tc.clone().into();
        let back: ToolCall = proto.into();
        assert_eq!(tc.id, back.id);
        assert_eq!(tc.name, back.name);
        assert_eq!(tc.arguments, back.arguments);
    }

    #[test]
    fn tool_result_round_trip() {
        let tr = sample_tool_result();
        let proto: proto::ToolResultProto = tr.clone().into();
        let back: ToolResult = proto.into();
        assert_eq!(tr.tool_call_id, back.tool_call_id);
        assert_eq!(tr.content, back.content);
    }

    #[test]
    fn tool_trace_round_trip() {
        let trace = ToolTrace {
            tool_call: sample_tool_call(),
            result: sample_tool_result(),
            duration_ms: 203,
        };
        let proto: proto::ToolTraceProto = trace.clone().into();
        let back: ToolTrace = proto.into();
        assert_eq!(trace.tool_call.name, back.tool_call.name);
        assert_eq!(trace.result.content, back.result.content);
        assert_eq!(trace.duration_ms, back.duration_ms);
    }

    #[test]
    fn turn_trace_round_trip() {
        let turn = TurnTrace {
            assistant_content: Some("Checking weather".to_string()),
            tool_calls: vec![ToolTrace {
                tool_call: sample_tool_call(),
                result: sample_tool_result(),
                duration_ms: 203,
            }],
        };
        let proto: proto::TurnTraceProto = turn.clone().into();
        let back: TurnTrace = proto.into();
        assert_eq!(turn.assistant_content, back.assistant_content);
        assert_eq!(turn.tool_calls.len(), back.tool_calls.len());
    }

    #[test]
    fn run_result_round_trip() {
        let result = RunResult {
            content: Some("The weather is 62°F.".to_string()),
            pending_tool_calls: None,
            turns: vec![TurnTrace {
                assistant_content: None,
                tool_calls: vec![ToolTrace {
                    tool_call: sample_tool_call(),
                    result: sample_tool_result(),
                    duration_ms: 203,
                }],
            }],
            state_snapshot: None,
        };
        let proto: proto::RunResultProto = result.clone().into();
        let back: RunResult = proto.into();
        assert_eq!(result.content, back.content);
        assert_eq!(result.turns.len(), back.turns.len());
        assert_eq!(
            result.turns[0].tool_calls[0].duration_ms,
            back.turns[0].tool_calls[0].duration_ms
        );
    }

    #[test]
    fn approval_mode_round_trip() {
        use vlinder_core::domain::ApprovalMode;
        let modes = vec![ApprovalMode::Autonomous, ApprovalMode::RequireApproval];
        for m in modes {
            let proto: proto::ApprovalModeProto = m.into();
            let back: ApprovalMode = proto.into();
            assert_eq!(m, back);
        }
    }
}
