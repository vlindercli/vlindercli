use serde::{Deserialize, Serialize};

use super::{ToolCall, TurnTrace};

/// The structured result of running an agent to completion.
///
/// Replaces the flat `String` formerly returned by `Harness::run_agent`.
/// Carries the final answer, tool trace history, and a state snapshot for
/// time-travel / forking.
///
/// Invariant: exactly one of `content` and `pending_tool_calls` is `Some`
/// on a successful return. Both are `None` only on error paths (never
/// returned from the harness — errors use `Err`).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunResult {
    /// The agent's final answer. `Some` when the loop terminated normally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// Tool calls awaiting human approval (HITL slot). Always `None` today
    /// (autonomous dispatch); reserved for a future HITL commit.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending_tool_calls: Option<Vec<ToolCall>>,
    /// Chronological history of completed turns.
    pub turns: Vec<TurnTrace>,
    /// Serialized agent state for time-travel / forking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_snapshot: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ToolCall, ToolCallId, ToolResult, ToolTrace, TurnTrace};
    use serde_json::json;

    fn sample_result() -> RunResult {
        RunResult {
            content: Some("The weather is 62°F.".to_string()),
            pending_tool_calls: None,
            turns: vec![TurnTrace {
                assistant_content: None,
                tool_calls: vec![ToolTrace {
                    tool_call: ToolCall {
                        id: ToolCallId::from("call-1".to_string()),
                        name: "get_weather".to_string(),
                        arguments: json!({"location": "SF"}),
                    },
                    result: ToolResult {
                        tool_call_id: ToolCallId::from("call-1".to_string()),
                        content: b"62F".to_vec(),
                    },
                    duration_ms: 203,
                }],
            }],
            state_snapshot: None,
        }
    }

    #[test]
    fn run_result_serde_round_trip() {
        let r = sample_result();
        let json = serde_json::to_string(&r).unwrap();
        let back: RunResult = serde_json::from_str(&json).unwrap();
        assert_eq!(r, back);
    }

    #[test]
    fn run_result_json_shape() {
        let r = sample_result();
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();

        assert_eq!(v["content"], "The weather is 62°F.");
        assert!(
            v.get("pending_tool_calls").is_none(),
            "None fields must be omitted"
        );
        assert!(
            v.get("state_snapshot").is_none(),
            "None fields must be omitted"
        );
        assert_eq!(v["turns"][0]["tool_calls"][0]["duration_ms"], 203);
    }

    #[test]
    fn run_result_pending_tool_calls_serialized_when_some() {
        let r = RunResult {
            content: None,
            pending_tool_calls: Some(vec![ToolCall {
                id: ToolCallId::from("call-2".to_string()),
                name: "approve_something".to_string(),
                arguments: json!({}),
            }]),
            turns: vec![],
            state_snapshot: None,
        };
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert!(v.get("pending_tool_calls").is_some());
        assert!(v.get("content").is_none());
    }
}
