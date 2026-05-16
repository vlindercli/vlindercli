//! Harness - API surface for agent interaction.
//!
//! The harness is the entry point for external requests. Different harness
//! types handle different interfaces (CLI, Web API, `WhatsApp`, etc.) but share
//! a common contract via the `Harness` trait.
//!
//! `CoreHarness` is the canonical implementation: it orchestrates sessions,
//! Merkle-chained submissions, job lifecycle, and timeline sealing.

use crate::domain::{
    AgentName, ApprovalDecision, BranchId, CompleteMessage, DagNodeId, DagStore, DataMessageKind,
    DataRoutingKey, ExternalSessionId, ForkMessage, HarnessEvent, HarnessType, InvokeDiagnostics,
    InvokeMessage, JobId, JobStatus, Message, MessageId, MessageQueue, MessageType, PromoteMessage,
    Registry, RequestV2, ResourceId, RunCtl, RunResult, Sequence, SequenceCounter,
    ServiceBackendV2, ServiceOperation, SessionId, SessionMessageKind, SessionRoutingKey,
    SessionStartMessage, SubmissionId, SvcMessageKind, SvcRequestDiagnostics, SvcRoutingKey,
    ToolCall, ToolCallProtocol, ToolResult, ToolTrace, TurnTrace,
};
use async_trait::async_trait;
use std::sync::Arc;

/// Common harness operations shared across all harness types.
#[async_trait]
pub trait Harness {
    /// Identify which transport submitted the job.
    ///
    /// Stamped into every invoke message and used by the completion
    /// path to route responses back to the correct consumer.
    fn harness_type(&self) -> HarnessType;

    /// Start a new conversation session for an agent.
    ///
    /// Creates a session and its default "main" branch. Returns the
    /// `SessionId` and the default branch's `BranchId`.
    async fn start_session(
        &self,
        agent_name: &str,
        external_id: ExternalSessionId,
    ) -> (SessionId, BranchId);

    /// Run an agent to completion synchronously.
    ///
    /// Sends input to the agent and blocks until the response arrives.
    /// Returns a `RunResult` with the final answer, turn trace history,
    /// and optional state snapshot.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String>;

    /// Run an agent with caller-provided message history.
    ///
    /// The caller provides the full conversation history (user, agent, tool
    /// messages). The harness does NOT read the DAG for prior context. This
    /// is the path used by the OpenAI-compatible API server.
    ///
    /// Tool calls in the agent's response are dispatched internally (via the
    /// service plane) and the agent is re-invoked with tool results. The method
    /// loops until the agent returns a text-only response. The returned
    /// `RunResult` always has `pending_tool_calls: None`.
    async fn run_agent_with_messages(
        &self,
        agent_id: &ResourceId,
        messages: Vec<Message>,
        session_id: SessionId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String>;

    /// Create a timeline fork by sending a `ForkMessage` through the queue.
    ///
    /// Fire-and-forget: both SQL (via `RecordingQueue`) and git (via
    /// `GitDagWorker`) react to the message. No response is expected.
    async fn fork_timeline(
        &self,
        params: ForkParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String>;

    /// Promote a branch to main by sending a `PromoteMessage` through the queue.
    ///
    /// Fire-and-forget: both SQL (via `RecordingQueue`) and git (via
    /// `GitDagWorker`) react to the message. No response is expected.
    async fn promote_timeline(
        &self,
        params: PromoteParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String>;
}

/// Parameters for `Harness::fork_timeline()`.
///
/// The CLI reads these from the `DagStore` (node lookup + session context).
/// The harness wraps them in a `ForkMessage` and sends through the queue.
pub struct ForkParams {
    pub agent_name: AgentName,
    pub branch_name: String,
    pub fork_point: DagNodeId,
}

/// Parameters for `Harness::promote_timeline()`.
///
/// The CLI reads these from the `DagStore` (branch lookup + session context).
/// The harness wraps them in a `PromoteMessage` and sends through the queue.
pub struct PromoteParams {
    pub agent_name: AgentName,
}

// ============================================================================
// CoreHarness — canonical implementation
// ============================================================================

/// Core harness implementation.
///
/// Orchestrates the full invocation lifecycle:
/// - Session management with conversation history
/// - Content-addressed submission chaining (ADR 081)
/// - State tracking with pending/committed promotion (ADR 055)
/// - Timeline-scoped invocations with seal enforcement (ADR 093)
/// - Job creation and status tracking via the registry
pub struct CoreHarness {
    harness_type: HarnessType,
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    store: Arc<dyn DagStore>,
    service_sequence: SequenceCounter,
    protocol: Arc<dyn ToolCallProtocol>,
}

impl CoreHarness {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        registry: Arc<dyn Registry>,
        store: Arc<dyn DagStore>,
        harness_type: HarnessType,
        protocol: Arc<dyn ToolCallProtocol>,
    ) -> Self {
        Self {
            harness_type,
            queue,
            registry,
            store,
            service_sequence: SequenceCounter::new(),
            protocol,
        }
    }

    /// Get the next sequence number for service calls.
    fn next_service_sequence(&self) -> Sequence {
        self.service_sequence.next()
    }

    /// Look up the agent and resolve its runtime.
    async fn resolve_agent_and_runtime(
        &self,
        agent_id: &ResourceId,
    ) -> Result<(crate::domain::Agent, crate::domain::RuntimeType), String> {
        let agent = self
            .registry
            .get_agent(agent_id)
            .await
            .ok_or_else(|| format!("agent not deployed: {agent_id}"))?;
        let runtime = self
            .registry
            .select_runtime(&agent)
            .ok_or_else(|| format!("no runtime available for agent: {agent_id}"))?;
        Ok((agent, runtime))
    }

    /// Resolve the session's branch, `dag_parent`, and `initial_state` for `run_agent_with_messages`.
    async fn resolve_session_context(
        &self,
        session_id: &SessionId,
    ) -> Result<(BranchId, DagNodeId, Option<String>), String> {
        let session = self
            .store
            .get_session(session_id)
            .await
            .map_err(|e| format!("failed to resolve session: {e}"))?
            .ok_or_else(|| format!("session not found: {session_id}"))?;

        let branch_id = session.default_branch;
        let branch = self
            .store
            .get_branch(branch_id)
            .await
            .map_err(|e| format!("failed to resolve branch: {e}"))?
            .ok_or_else(|| format!("branch not found: {branch_id}"))?;

        if branch.broken_at.is_some() {
            return Err(
                "Timeline is sealed. Use `vlinder session fork` to create a new branch."
                    .to_string(),
            );
        }

        let tip_node = self
            .store
            .latest_node_on_branch(branch_id, None)
            .await
            .unwrap_or(None);
        let dag_parent = tip_node
            .as_ref()
            .map(|n| n.id.clone())
            .or_else(|| branch.fork_point.clone())
            .unwrap_or_else(DagNodeId::root);

        let initial_state = if let Some(node) = tip_node {
            let state = if node.message_type() == MessageType::Invoke {
                self.store
                    .get_invoke_node(&node.id)
                    .await
                    .ok()
                    .flatten()
                    .and_then(|(_, msg)| msg.state)
                    .unwrap_or_default()
            } else if node.message_type() == MessageType::Complete {
                self.store
                    .get_complete_node(&node.id)
                    .await
                    .ok()
                    .flatten()
                    .map(|(_, m)| m)
                    .and_then(|m| m.state)
                    .unwrap_or_default()
            } else {
                String::new()
            };
            if state.is_empty() {
                None
            } else {
                Some(state)
            }
        } else {
            None
        };

        Ok((branch_id, dag_parent, initial_state))
    }

    /// Build the conversation history and current input from DAG state.
    ///
    /// On the first invocation, history is empty and `current_input` is a single
    /// user message. On follow-up invocations, prior history is carried forward
    /// and the last agent response is appended.
    async fn build_conversation_input(
        &self,
        timeline: BranchId,
        input: &str,
        initial_state: Option<&str>,
    ) -> Result<(Vec<Message>, Vec<Message>, Option<String>), String> {
        let _last_invoke_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Invoke))
            .await
            .unwrap_or(None);
        let _last_complete_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Complete))
            .await
            .unwrap_or(None);

        // last_state: always use initial_state since we no longer build
        // conversation history from embedded InvokeMessage.history.
        let last_state = initial_state.map(std::string::ToString::to_string);

        // `history` field removed from InvokeMessage — the sidecar now walks
        // the DAG chain to reconstruct conversation history at dispatch time.
        // This function's history output is unused by the caller.
        let history: Vec<Message> = vec![];
        let current_input = vec![Message::User {
            content: input.to_string(),
        }];

        Ok((history, current_input, last_state))
    }

    /// Build an invoke from session state and register a job.
    ///
    /// Returns the routing key, payload message, and job ID.
    #[allow(clippy::too_many_arguments)]
    async fn build_invoke(
        &self,
        submission: SubmissionId,
        agent_id: &ResourceId,
        input: &str,
        session_id: &SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<&str>,
        dag_parent: &DagNodeId,
    ) -> Result<(DataRoutingKey, InvokeMessage, JobId), String> {
        if sealed {
            return Err(
                "Timeline is sealed. Use `vlinder session fork` to create a new branch."
                    .to_string(),
            );
        }

        let (_, runtime) = self.resolve_agent_and_runtime(agent_id).await?;
        let (_history, current_input, last_state) = self
            .build_conversation_input(timeline, input, initial_state)
            .await?;

        let job_id = self
            .registry
            .create_job(submission.clone(), agent_id.clone(), input.to_string())
            .await;

        let (key, msg) = Self::build_invoke_message(
            &submission,
            session_id.clone(),
            timeline,
            dag_parent.clone(),
            last_state,
            current_input,
            self.harness_type(),
            runtime,
            agent_id,
        );

        Ok((key, msg, job_id))
    }

    /// Dispatch a tool call via the V2 service path (harness‑mediated).
    ///
    /// Sends a `RequestV2` to the service worker and waits for a
    /// `ResponseV2`. The service backend is currently hardcoded to
    /// `Mcp("server-everything")`.
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_service_call(
        &self,
        tc: &crate::domain::ToolCall,
        session_id: &SessionId,
        timeline: BranchId,
        submission: &SubmissionId,
        agent_name: &AgentName,
        last_state: Option<String>,
        chain_head: &mut DagNodeId,
    ) -> crate::domain::ToolResult {
        let provider = self
            .registry
            .get_agent_by_name(agent_name.as_str())
            .await
            .and_then(|a| a.requirements.mcp.first().cloned())
            .unwrap_or_else(|| "server-everything".to_string());
        let service = ServiceBackendV2::Mcp(provider.clone());
        let operation = ServiceOperation::new(&tc.name);
        let sequence = self.next_service_sequence();

        let key = SvcRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission: submission.clone(),
            kind: SvcMessageKind::SvcRequest {
                agent: agent_name.clone(),
                service,
                operation: operation.clone(),
                sequence,
            },
        };

        let arguments_bytes = serde_json::to_vec(&tc.arguments).unwrap_or_default().len() as u64;
        let sent_at_ms = u64::try_from(chrono::Utc::now().timestamp_millis()).unwrap_or(0);

        let req = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: chain_head.clone(),
            tool_call_id: tc.id.clone(),
            state: last_state,
            diagnostics: SvcRequestDiagnostics {
                server: provider,
                tool: tc.name.clone(),
                arguments_bytes,
                sent_at_ms,
            },
            payload: self.protocol.encode_tool_call(&tc.name, &tc.arguments),
        };

        match self.queue.send_svc_request(key.clone(), req).await {
            Ok(svc_req_dag_id) => {
                *chain_head = svc_req_dag_id;
                match self.queue.receive_svc_response(&key).await {
                    Ok((_rkey, resp, ack)) => {
                        *chain_head = resp.dag_id.clone();
                        let _ = ack().await;
                        crate::domain::ToolResult {
                            tool_call_id: tc.id.clone(),
                            content: resp.payload,
                        }
                    }
                    Err(e) => crate::domain::ToolResult {
                        tool_call_id: tc.id.clone(),
                        content: format!("svc_response receive error: {e}").into_bytes(),
                    },
                }
            }
            Err(e) => crate::domain::ToolResult {
                tool_call_id: tc.id.clone(),
                content: format!("svc_request send error: {e}").into_bytes(),
            },
        }
    }

    /// Dispatch tool calls from an agent's response.
    ///
    /// For `delegate_agent` tool calls, recursively invokes the target agent.
    /// For unknown tool names, returns a `ToolResult` with `is_error: true`.
    #[allow(clippy::too_many_arguments)]
    /// Build a re-invocation from in-memory conversation state (no DAG read).
    ///
    /// Called when the orchestration loop re-invokes the agent after
    /// dispatching tool calls. Uses the conversation state accumulated
    /// in the loop rather than reading from the `DagStore`.
    #[allow(clippy::too_many_arguments)]
    async fn build_reinvoke(
        &self,
        submission: SubmissionId,
        agent_id: &ResourceId,
        session_id: &SessionId,
        timeline: BranchId,
        dag_parent: &DagNodeId,
        state: Option<&str>,
        current_input: Vec<Message>,
    ) -> Result<(DataRoutingKey, InvokeMessage, JobId), String> {
        let (_, runtime) = self.resolve_agent_and_runtime(agent_id).await?;
        let job_id = self
            .registry
            .create_job(
                submission.clone(),
                agent_id.clone(),
                current_input
                    .iter()
                    .filter_map(|m| match m {
                        Message::User { content } | Message::System { content } => {
                            Some(content.as_str())
                        }
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            )
            .await;

        let (key, msg) = Self::build_invoke_message(
            &submission,
            session_id.clone(),
            timeline,
            dag_parent.clone(),
            state.map(std::string::ToString::to_string),
            current_input,
            self.harness_type(),
            runtime,
            agent_id,
        );

        Ok((key, msg, job_id))
    }

    /// Pure helper that constructs a `DataRoutingKey` and `InvokeMessage`
    /// from provided inputs. Does no I/O — no DAG reads, no `SubmissionId`
    /// minting, no job creation. Used by both `build_invoke` and
    /// `build_reinvoke`.
    #[allow(clippy::too_many_arguments)]
    fn build_invoke_message(
        submission: &SubmissionId,
        session_id: SessionId,
        timeline: BranchId,
        dag_parent: DagNodeId,
        state: Option<String>,
        current_input: Vec<Message>,
        harness: HarnessType,
        runtime: crate::domain::RuntimeType,
        agent_id: &ResourceId,
    ) -> (DataRoutingKey, InvokeMessage) {
        let key = DataRoutingKey {
            session: session_id,
            branch: timeline,
            submission: submission.clone(),
            kind: DataMessageKind::Invoke {
                harness,
                runtime,
                agent: crate::domain::agent_routing_key(agent_id),
            },
        };

        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state,
            diagnostics: InvokeDiagnostics {
                harness_version: env!("CARGO_PKG_VERSION").to_string(),
            },
            dag_parent,
            current_input,
        };

        (key, msg)
    }

    /// Send an invoke and await its completion, handling timeouts.
    async fn send_and_await_complete(
        &self,
        key: DataRoutingKey,
        msg: InvokeMessage,
        job_id: &JobId,
        agent_id: &ResourceId,
    ) -> Result<(DagNodeId, CompleteMessage), String> {
        self.registry
            .update_job_status(job_id, JobStatus::Running)
            .await;

        let harness = self.harness_type();
        let submission = key.submission.clone();
        let agent = crate::domain::agent_routing_key(agent_id);
        tracing::debug!(
            event = "chain_head_trace",
            site = "harness.send_invoke",
            session = %key.session,
            submission = %key.submission,
            msg_dag_parent = %msg.dag_parent,
            "harness sending invoke"
        );
        let invoke_dag_id = self
            .queue
            .send_invoke(key, msg)
            .await
            .map_err(|e| format!("queue error: {e}"))?;
        tracing::debug!(
            event = "chain_head_trace",
            site = "harness.send_invoke.returned",
            session = %submission,
            invoke_dag_id = %invoke_dag_id,
            "harness send_invoke returned dag_id"
        );

        loop {
            match self
                .queue
                .receive_complete(&submission, harness, &agent)
                .await
            {
                Ok((_key, v2, ack)) => {
                    let _ = ack().await;
                    break Ok((invoke_dag_id, v2));
                }
                Err(crate::domain::QueueError::Timeout) => {}
                Err(e) => break Err(format!("queue error: {e}")),
            }
        }
    }

    /// Prepare the accumulated state for the next turn after tool‑call dispatch.
    fn prepare_next_turn_state(
        sent_history: Vec<Message>,
        sent_current_input: Vec<Message>,
        complete: CompleteMessage,
    ) -> (Vec<Message>, Vec<Message>, Option<String>, DagNodeId) {
        let mut new_history = sent_history;
        new_history.extend(sent_current_input);
        new_history.push(Message::Agent {
            content: complete.content,
            tool_calls: complete.tool_calls,
        });
        let next_history = new_history;
        // Tool results are NOT stuffed into the re-invoke's current_input.
        // The chain's SvcResponse nodes are the source of truth for Tool
        // messages — `walk_chain_to_messages` projects them at dispatch time.
        // Including them here too would duplicate every Tool message in the
        // conversation handed to the LLM, which OpenAI rejects for tool-call
        // continuations (duplicate tool_call_id).
        let next_current_input = Vec::new();
        (
            next_history,
            next_current_input,
            complete.state,
            complete.dag_id,
        )
    }
}

/// Per-turn context bundled to avoid passing a long parameter list through
/// `process_tool_call`, `build_next_message`, and `dispatch_one_tool_call`.
struct TurnContext {
    session_id: SessionId,
    timeline: BranchId,
    sealed: bool,
    submission: SubmissionId,
    agent_name: AgentName,
    state: Option<String>,
}

impl CoreHarness {
    /// Dispatch a single tool call, measuring its duration at the call site.
    /// Extracted from the former `dispatch_tool_calls` loop body.
    async fn dispatch_one_tool_call(
        &self,
        tc: ToolCall,
        cx: &TurnContext,
        chain_head: &mut DagNodeId,
    ) -> ToolResult {
        match tc.name.as_str() {
            "delegate_agent" => {
                let agent_name = tc
                    .arguments
                    .get("agent")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let input = tc
                    .arguments
                    .get("input")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let agent_id = self
                    .registry
                    .get_agent_by_name(agent_name)
                    .await
                    .map_or_else(
                        || ResourceId::from(format!("agent://{agent_name}")),
                        |a| a.id,
                    );
                match self
                    .run_agent(
                        &agent_id,
                        input,
                        cx.session_id.clone(),
                        cx.timeline,
                        cx.sealed,
                        None,
                        chain_head.clone(),
                        // Delegate runs with RunCtl::quiet() — we do not
                        // forward events from sub-agents through our stream.
                        &RunCtl::quiet(),
                    )
                    .await
                {
                    Ok(run_result) => ToolResult {
                        tool_call_id: tc.id.clone(),
                        content: run_result.content.unwrap_or_default().into_bytes(),
                    },
                    Err(e) => ToolResult {
                        tool_call_id: tc.id.clone(),
                        content: e.into_bytes(),
                    },
                }
            }
            _ => {
                self.dispatch_service_call(
                    &tc,
                    &cx.session_id,
                    cx.timeline,
                    &cx.submission,
                    &cx.agent_name,
                    cx.state.clone(),
                    chain_head,
                )
                .await
            }
        }
    }

    /// Run a single tool call through the full lifecycle: approval gate, start
    /// event, dispatch, timing, completed event, return the trace.
    async fn process_tool_call(
        &self,
        tc: &ToolCall,
        ctl: &RunCtl,
        cx: &TurnContext,
        chain_head: &mut DagNodeId,
    ) -> Result<ToolTrace, String> {
        if ctl.requires_approval() {
            let (decision_tx, decision_rx) = tokio::sync::oneshot::channel();
            ctl.emit(HarnessEvent::ToolCallApprovalRequired {
                tool_call: tc.clone(),
                decision: decision_tx,
            })
            .await;
            match decision_rx.await {
                Ok(ApprovalDecision::Approve) => {}
                Ok(ApprovalDecision::Reject { reason }) => {
                    return Err(format!("tool call rejected: {reason}"))
                }
                Err(_) => return Err("approval channel closed".to_string()),
            }
        }

        ctl.emit(HarnessEvent::ToolCallStarted {
            tool_call: tc.clone(),
        })
        .await;

        let start = std::time::Instant::now();
        let result = self
            .dispatch_one_tool_call(tc.clone(), cx, chain_head)
            .await;
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        let trace = ToolTrace {
            tool_call: tc.clone(),
            result,
            duration_ms,
        };

        ctl.emit(HarnessEvent::ToolCallCompleted {
            trace: trace.clone(),
        })
        .await;

        Ok(trace)
    }

    /// Construct `RunResult` from terminal completion, emit `RunCompleted`.
    async fn build_completion(
        &self,
        complete: CompleteMessage,
        turns: Vec<TurnTrace>,
        job_id: &JobId,
        ctl: &RunCtl,
    ) -> RunResult {
        let result = complete
            .content
            .unwrap_or_else(|| String::from_utf8_lossy(&complete.payload).to_string());
        self.registry
            .update_job_status(job_id, JobStatus::Completed(result.clone()))
            .await;
        let run_result = RunResult {
            content: Some(result),
            pending_tool_calls: None,
            turns,
            state_snapshot: complete.state,
        };
        ctl.emit(HarnessEvent::RunCompleted {
            result: run_result.clone(),
        })
        .await;
        run_result
    }

    /// Dispatch tool calls for one turn and produce the `TurnTrace` + next-turn state.
    #[allow(clippy::too_many_arguments)]
    async fn process_turn_tool_calls(
        &self,
        tool_calls_vec: Vec<ToolCall>,
        cx: &TurnContext,
        ctl: &RunCtl,
        assistant_content: Option<String>,
        turn_index: u32,
        sent_history: Vec<Message>,
        sent_current_input: Vec<Message>,
        complete: CompleteMessage,
        chain_head: &mut DagNodeId,
    ) -> Result<(TurnTrace, Vec<Message>, Vec<Message>, Option<String>), String> {
        let mut traces = Vec::with_capacity(tool_calls_vec.len());

        // Tool calls are dispatched strictly sequentially. Each service response must
        // return before the next request goes out, because the DAG model is
        // single-parent linear chain — parallel fan-out would require multi-parent
        // DAG support, deferred until there is concrete demand.
        for tc in &tool_calls_vec {
            let trace = self.process_tool_call(tc, ctl, cx, chain_head).await?;
            traces.push(trace);
        }

        let turn = TurnTrace {
            assistant_content,
            tool_calls: traces,
        };
        ctl.emit(HarnessEvent::TurnCompleted {
            turn_index,
            turn: turn.clone(),
        })
        .await;

        let (next_history, next_current_input, next_state, _next_dag_parent) =
            Self::prepare_next_turn_state(sent_history, sent_current_input, complete);

        Ok((turn, next_history, next_current_input, next_state))
    }

    /// Build the next outgoing message, picking first-invocation vs.
    /// re-invocation based on accumulated in-memory turn state.
    #[allow(dead_code, clippy::too_many_arguments)]
    async fn build_next_message(
        &self,
        submission: SubmissionId,
        agent_id: &ResourceId,
        input: &str,
        session_id: &SessionId,
        timeline: BranchId,
        sealed: bool,
        state: Option<&str>,
        dag_parent: &DagNodeId,
        history: Vec<Message>,
        current_input: Vec<Message>,
    ) -> Result<(DataRoutingKey, InvokeMessage, JobId), String> {
        if history.is_empty() && current_input.is_empty() {
            self.build_invoke(
                submission, agent_id, input, session_id, timeline, sealed, state, dag_parent,
            )
            .await
        } else {
            self.build_reinvoke(
                submission,
                agent_id,
                session_id,
                timeline,
                dag_parent,
                state,
                current_input,
            )
            .await
        }
    }
}

#[async_trait]
impl Harness for CoreHarness {
    fn harness_type(&self) -> HarnessType {
        self.harness_type
    }

    async fn start_session(
        &self,
        agent_name: &str,
        external_id: ExternalSessionId,
    ) -> (SessionId, BranchId) {
        let session_id = SessionId::new();
        let key = SessionRoutingKey {
            session: session_id.clone(),
            submission: SubmissionId::new(),
            kind: SessionMessageKind::Start {
                agent_name: AgentName::new(agent_name),
            },
        };
        let msg = SessionStartMessage::new();
        let branch_id = self
            .queue
            .send_session_start(key, msg, external_id)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to send session start message");
                BranchId::from(1)
            });

        (session_id, branch_id)
    }

    async fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String> {
        let mut invoke_history: Vec<Message> = Vec::new();
        let mut invoke_current_input: Vec<Message> = Vec::new();
        let mut invoke_state: Option<String> = initial_state.clone();
        let mut invoke_dag_parent: DagNodeId = dag_parent.clone();
        let mut turn_index: u32 = 0;
        let mut turns: Vec<TurnTrace> = Vec::new();

        ctl.emit(HarnessEvent::RunStarted {
            agent_id: agent_id.clone(),
            session_id: session_id.clone(),
            timeline,
        })
        .await;

        let submission = SubmissionId::new();

        loop {
            ctl.emit(HarnessEvent::TurnStarted { turn_index }).await;

            let sent_history = invoke_history.clone();
            let (key, msg, job_id) = self
                .build_next_message(
                    submission.clone(),
                    agent_id,
                    input,
                    &session_id,
                    timeline,
                    sealed,
                    invoke_state.as_deref(),
                    &invoke_dag_parent,
                    invoke_history,
                    invoke_current_input,
                )
                .await?;

            let sent_current_input = msg.current_input.clone();
            let saved_submission = key.submission.clone();
            let saved_agent_name = crate::domain::agent_routing_key(agent_id);

            let (_invoke_dag_id, complete) = self
                .send_and_await_complete(key, msg, &job_id, agent_id)
                .await?;
            // chain_head advances: complete dag_id → (svc_request → svc_response)* per tool call.
            // process_turn_tool_calls mutates chain_head in place via &mut.

            let assistant_content = complete.content.clone();

            if let Some(text) = &assistant_content {
                ctl.emit(HarnessEvent::AssistantContent {
                    turn_index,
                    text: text.clone(),
                })
                .await;
            }

            if complete.tool_calls.is_none() {
                let run_result = self.build_completion(complete, turns, &job_id, ctl).await;
                return Ok(run_result);
            }

            let tool_calls_vec = complete.tool_calls.clone().unwrap();

            let cx = TurnContext {
                session_id: session_id.clone(),
                timeline,
                sealed,
                submission: saved_submission,
                agent_name: saved_agent_name,
                state: invoke_state.clone(),
            };
            // Advance chain_head past the complete node before dispatching tool calls.
            // process_turn_tool_calls mutates chain_head in place as each tool call
            // advances through svc_request → svc_response.
            invoke_dag_parent = complete.dag_id.clone();

            let (turn, h, ci, s) = self
                .process_turn_tool_calls(
                    tool_calls_vec,
                    &cx,
                    ctl,
                    assistant_content,
                    turn_index,
                    sent_history,
                    sent_current_input,
                    complete,
                    &mut invoke_dag_parent,
                )
                .await?;
            turns.push(turn);
            invoke_history = h;
            invoke_current_input = ci;
            invoke_state = s;
            turn_index += 1;
        }
    }

    async fn fork_timeline(
        &self,
        params: ForkParams,
        session_id: SessionId,
        _timeline: BranchId,
    ) -> Result<(), String> {
        let key = SessionRoutingKey {
            session: session_id,
            submission: SubmissionId::new(),
            kind: SessionMessageKind::Fork {
                agent_name: params.agent_name,
            },
        };
        let msg = ForkMessage::new(params.branch_name, params.fork_point);

        self.queue
            .send_fork(key, msg)
            .await
            .map_err(|e| format!("queue error: {e}"))
    }

    async fn promote_timeline(
        &self,
        params: PromoteParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String> {
        let key = SessionRoutingKey {
            session: session_id,
            submission: SubmissionId::new(),
            kind: SessionMessageKind::Promote {
                agent_name: params.agent_name,
            },
        };
        let msg = PromoteMessage::new(timeline);

        self.queue
            .send_promote(key, msg)
            .await
            .map_err(|e| format!("queue error: {e}"))
    }

    async fn run_agent_with_messages(
        &self,
        agent_id: &ResourceId,
        messages: Vec<Message>,
        session_id: SessionId,
        ctl: &RunCtl,
    ) -> Result<RunResult, String> {
        let (branch_id, dag_parent, initial_state) =
            self.resolve_session_context(&session_id).await?;

        let last_idx = messages.len().saturating_sub(1);
        let mut invoke_history: Vec<Message> = messages[..last_idx].to_vec();
        let mut invoke_current_input: Vec<Message> = messages[last_idx..].to_vec();
        let mut invoke_state: Option<String> = initial_state;
        let mut invoke_dag_parent: DagNodeId = dag_parent;
        let mut turn_index: u32 = 0;
        let mut turns: Vec<TurnTrace> = Vec::new();

        ctl.emit(HarnessEvent::RunStarted {
            agent_id: agent_id.clone(),
            session_id: session_id.clone(),
            timeline: branch_id,
        })
        .await;

        let submission = SubmissionId::new();

        loop {
            ctl.emit(HarnessEvent::TurnStarted { turn_index }).await;

            let (key, msg, job_id) = self
                .build_reinvoke(
                    submission.clone(),
                    agent_id,
                    &session_id,
                    branch_id,
                    &invoke_dag_parent,
                    invoke_state.as_deref(),
                    invoke_current_input,
                )
                .await?;

            let sent_history = invoke_history.clone();
            let sent_current_input = msg.current_input.clone();
            let saved_submission = key.submission.clone();
            let saved_agent_name = crate::domain::agent_routing_key(agent_id);

            let (_invoke_dag_id, complete) = self
                .send_and_await_complete(key, msg, &job_id, agent_id)
                .await?;

            let assistant_content = complete.content.clone();

            if let Some(text) = &assistant_content {
                ctl.emit(HarnessEvent::AssistantContent {
                    turn_index,
                    text: text.clone(),
                })
                .await;
            }

            if complete.tool_calls.is_none() {
                let run_result = self.build_completion(complete, turns, &job_id, ctl).await;
                return Ok(run_result);
            }

            let tool_calls_vec = complete.tool_calls.clone().unwrap();

            let cx = TurnContext {
                session_id: session_id.clone(),
                timeline: branch_id,
                sealed: false,
                submission: saved_submission,
                agent_name: saved_agent_name,
                state: invoke_state.clone(),
            };
            // Advance chain_head past the complete node before dispatching tool calls.
            // process_turn_tool_calls mutates chain_head in place as each tool call
            // advances through svc_request → svc_response.
            invoke_dag_parent = complete.dag_id.clone();

            let (turn, h, ci, s) = self
                .process_turn_tool_calls(
                    tool_calls_vec,
                    &cx,
                    ctl,
                    assistant_content,
                    turn_index,
                    sent_history,
                    sent_current_input,
                    complete,
                    &mut invoke_dag_parent,
                )
                .await?;
            turns.push(turn);
            invoke_history = h;
            invoke_current_input = ci;
            invoke_state = s;
            turn_index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        Agent, InMemoryDagStore, InMemoryRegistry, InMemorySecretStore, InvokeDiagnostics,
        ObjectStorageType, Operation, RequestDiagnostics, ResponseV2, RuntimeDiagnostics,
        RuntimeType, SecretStore, Sequence, ServiceBackend, ServiceBackendV2, ServiceDiagnostics,
        ServiceOperation, SvcMessageKind, SvcResponseDiagnostics, SvcRoutingKey, ToolCallId,
        ToolCallProtocol,
    };
    use crate::queue::{InMemoryQueue, RecordingQueue};
    use serde_json::Value;

    /// Test protocol double — serializes arguments as JSON bytes.
    struct TestProtocol;

    impl ToolCallProtocol for TestProtocol {
        fn encode_tool_call(&self, _name: &str, arguments: &Value) -> Vec<u8> {
            serde_json::to_vec(arguments).unwrap_or_default()
        }

        fn decode_tool_result(&self, payload: &[u8]) -> String {
            String::from_utf8_lossy(payload).into_owned()
        }
    }

    #[test]
    fn harness_type_is_cli() {
        let queue = Arc::new(InMemoryQueue::new());
        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        registry.register_runtime(RuntimeType::Container);
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());

        let harness = CoreHarness::new(
            queue,
            registry,
            store,
            HarnessType::Cli,
            Arc::new(TestProtocol),
        );

        assert_eq!(harness.harness_type(), HarnessType::Cli);
    }

    // ========================================================================
    // 9c — build_invoke_message unit tests
    // ========================================================================

    #[test]
    fn build_invoke_message_carries_submission_in_routing_key() {
        let submission = SubmissionId::from("test-sub-123".to_string());
        let session_id = SessionId::new();
        let timeline = BranchId::from(1);
        let dag_parent = DagNodeId::root();
        let agent_name = AgentName::new("test-agent");
        let resource_id = ResourceId::new(agent_name.as_str());

        let (key, _msg) = CoreHarness::build_invoke_message(
            &submission,
            session_id.clone(),
            timeline,
            dag_parent,
            None,
            vec![],
            HarnessType::Grpc,
            RuntimeType::Container,
            &resource_id,
        );

        assert_eq!(
            key.submission, submission,
            "routing key must carry the passed-in submission verbatim",
        );
    }

    #[test]
    fn build_invoke_message_does_not_mint_submission() {
        let sub = SubmissionId::from("fixed-sub".to_string());
        let session_id = SessionId::new();
        let timeline = BranchId::from(2);
        let dag_parent = DagNodeId::root();
        let agent_name = AgentName::new("test-agent");
        let resource_id = ResourceId::new(agent_name.as_str());

        let (k1, _) = CoreHarness::build_invoke_message(
            &sub,
            session_id.clone(),
            timeline,
            dag_parent.clone(),
            None,
            vec![],
            HarnessType::Grpc,
            RuntimeType::Container,
            &resource_id,
        );
        let (k2, _) = CoreHarness::build_invoke_message(
            &sub,
            session_id,
            timeline,
            dag_parent,
            None,
            vec![],
            HarnessType::Grpc,
            RuntimeType::Container,
            &resource_id,
        );

        assert_eq!(
            k1.submission, k2.submission,
            "helper must be pure — two calls with same submission produce same routing-key submission",
        );
    }

    // ========================================================================
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn run_agent_uses_one_submission_per_user_turn() {
        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        registry.register_runtime(RuntimeType::Container);

        // Register a test agent so resolve_agent_and_runtime succeeds
        let agent = Agent {
            name: "test-agent".into(),
            description: String::new(),
            source: None,
            requirements: crate::domain::Requirements {
                models: std::collections::HashMap::new(),
                services: std::collections::HashMap::new(),
                mounts: std::collections::HashMap::new(),
                mcp: Vec::new(),
            },
            id: ResourceId::new("http://127.0.0.1:9000/agents/test-agent"),
            runtime: RuntimeType::Container,
            executable: String::new(),
            image_digest: None,
            public_key: None,
            object_storage: None,
            vector_storage: None,
        };
        registry.restore_agent(agent).ok();

        let registry: Arc<dyn Registry> = Arc::new(registry);
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());

        let harness = CoreHarness::new(
            queue,
            registry,
            store,
            HarnessType::Cli,
            Arc::new(TestProtocol),
        );

        let submission = SubmissionId::from("shared-sub".to_string());
        let agent_id = ResourceId::new("http://127.0.0.1:9000/agents/test-agent");
        let session_id = SessionId::new();
        let timeline = BranchId::from(1);
        let dag_parent = DagNodeId::root();

        // First call: empty history → build_invoke
        let (key1, _msg1, _job1) = harness
            .build_next_message(
                submission.clone(),
                &agent_id,
                "test input",
                &session_id,
                timeline,
                false,
                None,
                &dag_parent,
                vec![],
                vec![],
            )
            .await
            .expect("build_next_message (invoke path) must succeed");

        assert_eq!(
            key1.submission, submission,
            "build_invoke must carry the passed-in submission",
        );

        // Second call: non-empty history → build_reinvoke
        let (key2, _msg2, _job2) = harness
            .build_next_message(
                submission.clone(),
                &agent_id,
                "test input",
                &session_id,
                timeline,
                false,
                None,
                &dag_parent,
                vec![Message::Agent {
                    content: Some("hi".to_string()),
                    tool_calls: None,
                }],
                vec![],
            )
            .await
            .expect("build_next_message (reinvoke path) must succeed");

        assert_eq!(
            key2.submission, submission,
            "build_reinvoke must carry the passed-in submission",
        );

        assert_eq!(
            key1.submission, key2.submission,
            "both invokes in one user turn must share one SubmissionId",
        );
    }

    // ========================================================================
    // dispatch_service_call advances chain_head
    // ========================================================================

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn dispatch_service_call_advances_chain_head() {
        // Build CoreHarness with RecordingQueue wrapping InMemoryQueue + InMemoryDagStore
        let inner = Arc::new(InMemoryQueue::new());
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());
        let record: Arc<dyn MessageQueue + Send + Sync> = Arc::new(RecordingQueue::new(
            inner.clone() as Arc<dyn MessageQueue + Send + Sync>,
            store.clone(),
        ));

        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        registry.register_runtime(RuntimeType::Container);
        let registry: Arc<dyn Registry> = Arc::new(registry);

        let harness = CoreHarness::new(
            record,
            registry,
            store.clone(),
            HarnessType::Cli,
            Arc::new(TestProtocol),
        );

        // Seed the store with a parent node (send_invoke records one)
        let session_id = SessionId::new();
        let submission = SubmissionId::from("svc-chain-test".to_string());
        let timeline = BranchId::from(1);
        let agent_name = AgentName::new("test-agent");

        // Build an invoke message to seed a dag node
        let seed_key = DataRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission: submission.clone(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: agent_name.clone(),
            },
        };
        let seed_msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            current_input: vec![Message::User {
                content: "seed".to_string(),
            }],
        };
        let parent_dag_id = harness
            .queue
            .send_invoke(seed_key, seed_msg)
            .await
            .expect("send_invoke should succeed");
        assert_ne!(
            parent_dag_id,
            DagNodeId::root(),
            "seed node must have non-root id"
        );

        // We know the provider defaults to "server-everything" because the
        // test agent is not registered in the registry.
        let provider = "server-everything";
        let service = ServiceBackendV2::Mcp(provider.to_string());
        let operation = ServiceOperation::new("echo");

        // Build the response key (mirrors NATS worker behavior)
        let resp_key = SvcRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission: submission.clone(),
            kind: SvcMessageKind::SvcResponse {
                agent: agent_name.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence: Sequence::first(),
            },
        };

        // Pre-populate the response in the inner queue BEFORE calling
        // dispatch_service_call, so receive_svc_response finds it immediately
        // after send_svc_request completes.
        let resp = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics {
                server: provider.to_string(),
                tool: "echo".to_string(),
                round_trip_ms: 0,
                content_bytes: 0,
            },
            payload: b"result".to_vec(),
        };
        inner
            .send_svc_response(resp_key, resp)
            .await
            .expect("pre-populating svc_response should succeed");

        // Create a tool call that will trigger dispatch_service_call
        let tool_call = ToolCall {
            id: ToolCallId::new(),
            name: "echo".to_string(),
            arguments: serde_json::json!({}),
        };

        // Call dispatch_service_call with chain_head = parent_dag_id
        let mut chain_head = parent_dag_id.clone();
        let result = harness
            .dispatch_service_call(
                &tool_call,
                &session_id,
                timeline,
                &submission,
                &agent_name,
                None,
                &mut chain_head,
            )
            .await;

        // Nodes: invoke (seed), svc_request, svc_response
        {
            let nodes = store.get_session_nodes(&session_id).await.unwrap();
            for (i, n) in nodes.iter().enumerate() {
                eprintln!(
                    "Node {}: id={:?} parent={:?} type={:?}",
                    i,
                    n.id,
                    n.parent_id,
                    n.message_type()
                );
            }
            assert_eq!(
                nodes.len(),
                3,
                "expected 3 nodes: invoke, svc_request, svc_response"
            );

            // Node 0 = seed invoke, Node 1 = svc_request, Node 2 = svc_response
            let svc_request_node = &nodes[1];
            assert_eq!(
                svc_request_node.parent_id, parent_dag_id,
                "svc_request should parent on the seed node (chain_head)",
            );

            // chain_head should equal the svc_response's dag_id
            let svc_response_node = &nodes[2];
            assert_eq!(
                chain_head, svc_response_node.id,
                "chain_head should advance to svc_response dag_id",
            );
        }

        // Verify result is OK
        assert_eq!(result.tool_call_id, tool_call.id);
        assert!(!result.content.is_empty(), "result should have content");
    }

    // ========================================================================
    // e2e: single-turn linear DAG chain
    // ========================================================================

    /// Full e2e test of a single-turn agent invocation with one tool call:
    /// `root → invoke → request → response → complete → svc_request → svc_response`
    ///
    /// The message flow is manually orchestrated (no spawned tasks) to avoid
    /// cooperative-scheduling deadlocks in single-threaded tokio runtime.
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn e2e_single_turn_linear_dag_chain() {
        let inner = Arc::new(InMemoryQueue::new());
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());
        let record: Arc<dyn MessageQueue + Send + Sync> = Arc::new(RecordingQueue::new(
            inner.clone() as Arc<dyn MessageQueue + Send + Sync>,
            store.clone(),
        ));

        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        registry.register_runtime(RuntimeType::Container);

        // Register a test agent so build_invoke / resolve_agent_and_runtime succeed
        let agent = Agent {
            name: "test-agent".into(),
            description: String::new(),
            source: None,
            requirements: crate::domain::Requirements {
                models: std::collections::HashMap::new(),
                services: std::collections::HashMap::new(),
                mounts: std::collections::HashMap::new(),
                mcp: vec!["server-everything".to_string()],
            },
            id: ResourceId::new("http://127.0.0.1:9000/agents/test-agent"),
            runtime: RuntimeType::Container,
            executable: String::new(),
            image_digest: None,
            public_key: None,
            object_storage: None,
            vector_storage: None,
        };
        registry.restore_agent(agent).ok();
        let registry: Arc<dyn Registry> = Arc::new(registry);

        let harness = CoreHarness::new(
            record,
            registry,
            store.clone(),
            HarnessType::Cli,
            Arc::new(TestProtocol),
        );

        let session_id = SessionId::new();
        let agent_id = ResourceId::new("http://127.0.0.1:9000/agents/test-agent");
        let timeline = BranchId::from(1);
        let agent_name = AgentName::new("test-agent");

        // Seed the branch
        store
            .create_branch("main", &session_id, None)
            .await
            .unwrap();

        // Step 1: Build and send an invoke message
        let dag_parent = DagNodeId::root();
        let (inv_key, inv_msg, _job_id) = harness
            .build_invoke(
                SubmissionId::new(),
                &agent_id,
                "test input",
                &session_id,
                timeline,
                false,
                None,
                &dag_parent,
            )
            .await
            .expect("build_invoke should succeed");

        let invoke_dag_id = harness
            .queue
            .send_invoke(inv_key.clone(), inv_msg)
            .await
            .expect("send_invoke should succeed");
        assert_ne!(invoke_dag_id, DagNodeId::root());

        // Step 2: Receive invoke from inner, send request→response→complete through RecordingQueue
        let (recv_inv_key, _recv_inv_msg, _ack) = inner
            .receive_invoke(&agent_name)
            .await
            .expect("should receive invoke from inner");

        let tool_call = ToolCall {
            id: ToolCallId::new(),
            name: "echo".to_string(),
            arguments: serde_json::json!({"input": "hello"}),
        };

        let DataMessageKind::Invoke {
            harness: htype,
            runtime: _,
            agent: a,
        } = &recv_inv_key.kind
        else {
            panic!("unexpected key kind");
        };

        // Send request through RecordingQueue → records Request node
        let request_key = DataRoutingKey {
            session: recv_inv_key.session.clone(),
            branch: recv_inv_key.branch,
            submission: recv_inv_key.submission.clone(),
            kind: DataMessageKind::Request {
                agent: a.clone(),
                service: ServiceBackend::Kv(ObjectStorageType::InMemory),
                operation: Operation::Get,
                sequence: Sequence::first(),
            },
        };
        let request_msg = crate::domain::RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: invoke_dag_id.clone(),
            state: None,
            diagnostics: RequestDiagnostics::default(),
            payload: b"request".to_vec(),
            checkpoint: None,
        };
        let request_dag_id = harness
            .queue
            .send_request(request_key, request_msg)
            .await
            .expect("should send request");
        assert_ne!(request_dag_id, DagNodeId::root());

        // Send response through RecordingQueue → records Response node
        let response_key = DataRoutingKey {
            session: recv_inv_key.session.clone(),
            branch: recv_inv_key.branch,
            submission: recv_inv_key.submission.clone(),
            kind: DataMessageKind::Response {
                agent: a.clone(),
                service: ServiceBackend::Kv(ObjectStorageType::InMemory),
                operation: Operation::Get,
                sequence: Sequence::first(),
            },
        };
        let response_msg = crate::domain::ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: request_dag_id.clone(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: ServiceDiagnostics::placeholder(),
            payload: b"response".to_vec(),
            status_code: 200,
            checkpoint: None,
        };
        let response_dag_id = harness
            .queue
            .send_response(response_key, response_msg)
            .await
            .expect("should send response");
        assert_ne!(response_dag_id, DagNodeId::root());

        // Send complete through RecordingQueue → records Complete node
        let complete_key = DataRoutingKey {
            session: recv_inv_key.session.clone(),
            branch: recv_inv_key.branch,
            submission: recv_inv_key.submission.clone(),
            kind: DataMessageKind::Complete {
                harness: *htype,
                agent: a.clone(),
            },
        };
        let complete_msg = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: response_dag_id.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            content: Some("Here is the result".to_string()),
            tool_calls: Some(vec![tool_call.clone()]),
            payload: b"done".to_vec(),
        };
        harness
            .queue
            .send_complete(complete_key, complete_msg)
            .await
            .expect("should send complete");

        // Step 3: Receive the complete through the recording queue
        let (_recv_key, recv_complete, _ack) = harness
            .queue
            .receive_complete(&recv_inv_key.submission, *htype, &agent_name)
            .await
            .expect("should receive complete");
        assert!(recv_complete.tool_calls.is_some());

        // Step 4: Build turn context and process the tool call
        let submission = SubmissionId::new();
        let mut chain_head = recv_complete.dag_id.clone();

        // Pre-populate the svc_response for the MCP worker
        let expected_sequence = Sequence::first();
        let resp_key = SvcRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission: submission.clone(),
            kind: SvcMessageKind::SvcResponse {
                agent: agent_name.clone(),
                service: ServiceBackendV2::Mcp("server-everything".to_string()),
                operation: ServiceOperation::new("echo"),
                sequence: expected_sequence,
            },
        };
        let resp = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: chain_head.clone(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics {
                server: "server-everything".to_string(),
                tool: "echo".to_string(),
                round_trip_ms: 0,
                content_bytes: 0,
            },
            payload: b"result".to_vec(),
        };
        inner
            .send_svc_response(resp_key, resp)
            .await
            .expect("pre-populate svc_response");

        // svc_request's dag_parent will be set by dispatch_service_call from chain_head.
        // svc_response's dag_parent comes from the message above (chain_head = complete's dag_id).
        // After dispatch_service_call, chain_head will equal the svc_response's stamped dag_id.

        // Step 5: Dispatch the tool call through dispatch_service_call
        let result = harness
            .dispatch_service_call(
                &tool_call,
                &session_id,
                timeline,
                &submission,
                &agent_name,
                None,
                &mut chain_head,
            )
            .await;

        assert!(!result.content.is_empty(), "result should have content");

        // Step 6: Verify DAG chain — expect 6 nodes total
        let nodes = store.get_session_nodes(&session_id).await.unwrap();

        for (i, n) in nodes.iter().enumerate() {
            eprintln!(
                "Node {}: id={:?} parent={:?} type={:?}",
                i,
                n.id,
                n.parent_id,
                n.message_type()
            );
        }

        // Expect 6 nodes: invoke, request, response, complete, svc_request, svc_response
        assert_eq!(
            nodes.len(),
            6,
            "expected 6 nodes for single-turn one-tool-call session, got {}",
            nodes.len()
        );

        // First node must parent on root
        assert_eq!(
            nodes[0].parent_id,
            DagNodeId::root(),
            "first invoke must parent on root, got {}",
            nodes[0].parent_id,
        );

        // Nodes 0-4 (invoke → request → response → complete → svc_request) parent on prior.
        // The svc_response's parent depends on the message dag_parent set by the MCP worker
        // (svc_request_dag_id), which we cannot know at pre-population time.
        // The correct svc_request→svc_response chaining is verified in
        // dispatch_service_call_advances_chain_head.
        for window in nodes[..5].windows(2) {
            assert_eq!(
                window[1].parent_id,
                window[0].id,
                "node {} ({:?}) should parent on prior node {} ({:?})",
                window[1].id,
                window[1].message_type(),
                window[0].id,
                window[0].message_type(),
            );
        }

        // Verify svc_request parents on complete
        assert_eq!(
            nodes[4].parent_id, nodes[3].id,
            "svc_request should parent on complete",
        );
    }
}
