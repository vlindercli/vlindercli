//! Harness execution events — the window onto agent execution.
//!
//! These events are emitted by the harness and consumed by the CLI (TUI),
//! the `OpenAI` API server, or any other observer. The event stream mirrors
//! the agent's execution: turns, tool calls, approval gates, and the final
//! result.
//!
//! `HarnessEvent` deliberately does NOT implement `Serialize`/`Deserialize`
//! because `ToolCallApprovalRequired` carries a `oneshot::Sender` that cannot
//! cross serialization boundaries. The gRPC bridge translates between
//! `HarnessEvent` and the proto `HarnessEventProto` which uses an
//! `approval_id` string instead.

use tokio::sync::oneshot;

use super::{BranchId, ResourceId, RunResult, SessionId, ToolCall, ToolTrace, TurnTrace};

#[derive(Clone, Debug)]
pub enum ApprovalDecision {
    Approve,
    Reject { reason: String },
}

pub enum HarnessEvent {
    /// Fired once when `run_agent` or `run_agent_with_messages` begins.
    RunStarted {
        agent_id: ResourceId,
        session_id: SessionId,
        timeline: BranchId,
    },
    /// Fired at the top of each iteration of the multi-turn loop.
    TurnStarted { turn_index: u32 },
    /// Fired after the model returns its text alongside or without tool calls.
    AssistantContent { turn_index: u32, text: String },
    /// Fired when a tool call needs human approval before dispatching.
    /// The consumer must send a decision via the oneshot channel.
    ToolCallApprovalRequired {
        tool_call: ToolCall,
        decision: oneshot::Sender<ApprovalDecision>,
    },
    /// Fired just before a tool call is dispatched.
    ToolCallStarted { tool_call: ToolCall },
    /// Fired when a tool call completes (success or failure).
    ToolCallCompleted { trace: ToolTrace },
    /// Fired after all tool calls in a turn complete.
    TurnCompleted { turn_index: u32, turn: TurnTrace },
    /// Fired when execution finishes — terminal event.
    RunCompleted { result: RunResult },
}

// Manual Clone is not possible because ToolCallApprovalRequired contains
// oneshot::Sender. However, the Debug and Clone derive on the outer enum
// will fail. Let's implement Debug and Clone manually.

impl Clone for HarnessEvent {
    fn clone(&self) -> Self {
        match self {
            Self::RunStarted {
                agent_id,
                session_id,
                timeline,
            } => Self::RunStarted {
                agent_id: agent_id.clone(),
                session_id: session_id.clone(),
                timeline: *timeline,
            },
            Self::TurnStarted { turn_index } => Self::TurnStarted {
                turn_index: *turn_index,
            },
            Self::AssistantContent { turn_index, text } => Self::AssistantContent {
                turn_index: *turn_index,
                text: text.clone(),
            },
            Self::ToolCallApprovalRequired { .. } => {
                // Cannot clone a oneshot::Sender — this variant is never
                // cloned in practice (the emitter sends it exactly once).
                // Panic on accidental clone.
                panic!("HarnessEvent::ToolCallApprovalRequired cannot be cloned (contains oneshot::Sender)")
            }
            Self::ToolCallStarted { tool_call } => Self::ToolCallStarted {
                tool_call: tool_call.clone(),
            },
            Self::ToolCallCompleted { trace } => Self::ToolCallCompleted {
                trace: trace.clone(),
            },
            Self::TurnCompleted { turn_index, turn } => Self::TurnCompleted {
                turn_index: *turn_index,
                turn: turn.clone(),
            },
            Self::RunCompleted { result } => Self::RunCompleted {
                result: result.clone(),
            },
        }
    }
}

impl std::fmt::Debug for HarnessEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RunStarted {
                agent_id,
                session_id,
                timeline,
            } => f
                .debug_struct("RunStarted")
                .field("agent_id", agent_id)
                .field("session_id", session_id)
                .field("timeline", timeline)
                .finish(),
            Self::TurnStarted { turn_index } => f
                .debug_struct("TurnStarted")
                .field("turn_index", turn_index)
                .finish(),
            Self::AssistantContent { turn_index, text } => f
                .debug_struct("AssistantContent")
                .field("turn_index", turn_index)
                .field("text", &text.chars().take(40).collect::<String>())
                .finish(),
            Self::ToolCallApprovalRequired { tool_call, .. } => f
                .debug_struct("ToolCallApprovalRequired")
                .field("tool_call", tool_call)
                .finish(),
            Self::ToolCallStarted { tool_call } => f
                .debug_struct("ToolCallStarted")
                .field("tool_call", tool_call)
                .finish(),
            Self::ToolCallCompleted { trace } => f
                .debug_struct("ToolCallCompleted")
                .field("trace", trace)
                .finish(),
            Self::TurnCompleted { turn_index, turn } => f
                .debug_struct("TurnCompleted")
                .field("turn_index", turn_index)
                .field("turn", turn)
                .finish(),
            Self::RunCompleted { result } => f
                .debug_struct("RunCompleted")
                .field("result", result)
                .finish(),
        }
    }
}
