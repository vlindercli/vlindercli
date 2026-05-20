use serde::{Deserialize, Serialize};

use super::{ToolCall, ToolResult};

/// One dispatched tool call with its paired result and wall-clock timing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolTrace {
    pub tool_call: ToolCall,
    pub result: ToolResult,
    /// Wall-clock time the harness waited for this single dispatch, in ms.
    /// For `delegate_agent`, includes the recursive sub-agent's full runtime.
    pub duration_ms: u64,
}

/// All tool activity from a single model turn.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnTrace {
    /// Intermediate model text emitted alongside this turn's `tool_calls`.
    /// Often `None` — `OpenAI` completions typically emit `content: null`
    /// alongside `tool_calls`; Claude routinely populates it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub assistant_content: Option<String>,
    /// Tools dispatched in this turn, in call order.
    pub tool_calls: Vec<ToolTrace>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{ToolCall, ToolCallId, ToolResult};
    use serde_json::json;

    fn sample_trace() -> ToolTrace {
        ToolTrace {
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
        }
    }

    #[test]
    fn tool_trace_serde_round_trip() {
        let trace = sample_trace();
        let json = serde_json::to_string(&trace).unwrap();
        let back: ToolTrace = serde_json::from_str(&json).unwrap();
        assert_eq!(trace, back);
        assert!(json.contains("203"));
        assert!(json.contains("get_weather"));
    }

    #[test]
    fn turn_trace_round_trip_no_content() {
        let turn = TurnTrace {
            assistant_content: None,
            tool_calls: vec![sample_trace()],
        };
        let json = serde_json::to_string(&turn).unwrap();
        let back: TurnTrace = serde_json::from_str(&json).unwrap();
        assert_eq!(turn, back);
        assert!(
            !json.contains("assistant_content"),
            "None fields must be omitted"
        );
    }

    #[test]
    fn turn_trace_round_trip_with_content() {
        let turn = TurnTrace {
            assistant_content: Some("Let me check the weather.".to_string()),
            tool_calls: vec![sample_trace()],
        };
        let json = serde_json::to_string(&turn).unwrap();
        let back: TurnTrace = serde_json::from_str(&json).unwrap();
        assert_eq!(turn, back);
        assert!(json.contains("Let me check the weather."));
    }
}
