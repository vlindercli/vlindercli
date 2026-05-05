//! Harness - API surface for agent interaction.
//!
//! The harness is the entry point for external requests. Different harness
//! types handle different interfaces (CLI, Web API, `WhatsApp`, etc.) but share
//! a common contract via the `Harness` trait.
//!
//! `CoreHarness` is the canonical implementation: it orchestrates sessions,
//! Merkle-chained submissions, job lifecycle, and timeline sealing.

use std::sync::Arc;

use crate::domain::{
    AgentName, BranchId, CompleteMessage, DagNodeId, DagStore, DataMessageKind, DataRoutingKey,
    ForkMessage, HarnessType, InvokeDiagnostics, InvokeMessage, JobId, JobStatus, Message,
    MessageId, MessageQueue, MessageType, PromoteMessage, Registry, RequestV2, ResourceId,
    Sequence, SequenceCounter, ServiceBackendV2, ServiceOperation, SessionId, SessionMessageKind,
    SessionRoutingKey, SessionStartMessage, SubmissionId, SvcMessageKind, SvcRequestDiagnostics,
    SvcRoutingKey, ToolResult,
};
use async_trait::async_trait;
use serde_json::Value;

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
    async fn start_session(&self, agent_name: &str) -> (SessionId, BranchId);

    /// Run an agent to completion synchronously.
    ///
    /// Sends input to the agent and blocks until the response arrives.
    /// Returns the agent's output as a string.
    #[allow(clippy::too_many_arguments)]
    async fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
    ) -> Result<String, String>;

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
}

impl CoreHarness {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        registry: Arc<dyn Registry>,
        store: Arc<dyn DagStore>,
        harness_type: HarnessType,
    ) -> Self {
        Self {
            harness_type,
            queue,
            registry,
            store,
            service_sequence: SequenceCounter::new(),
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
        let last_invoke_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Invoke))
            .await
            .unwrap_or(None);
        let last_complete_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Complete))
            .await
            .unwrap_or(None);
        let last_complete = match last_complete_node {
            Some(n) => self.store.get_complete_node(&n.id).await.ok().flatten(),
            None => None,
        };

        let last_state = last_complete
            .as_ref()
            .and_then(|m| m.state.as_ref().map(std::string::ToString::to_string))
            .or_else(|| initial_state.map(std::string::ToString::to_string));

        let (history, current_input) =
            if let (Some(invoke_node), Some(complete)) = (last_invoke_node, last_complete) {
                let last_invoke = self
                    .store
                    .get_invoke_node(&invoke_node.id)
                    .await
                    .ok()
                    .flatten();
                let (mut new_history, prev_current_input) = match last_invoke {
                    Some((_, invoke_msg)) => (invoke_msg.history, invoke_msg.current_input),
                    None => (vec![], vec![]),
                };
                new_history.extend(prev_current_input);
                new_history.push(Message::Agent {
                    content: complete.content.clone(),
                    tool_calls: complete.tool_calls.clone(),
                });
                (
                    new_history,
                    vec![Message::User {
                        content: input.to_string(),
                    }],
                )
            } else {
                (
                    vec![],
                    vec![Message::User {
                        content: input.to_string(),
                    }],
                )
            };

        Ok((history, current_input, last_state))
    }

    /// Build an invoke from session state and register a job.
    ///
    /// Returns the routing key, payload message, and job ID.
    #[allow(clippy::too_many_arguments)]
    async fn build_invoke(
        &self,
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
        let (history, current_input, last_state) = self
            .build_conversation_input(timeline, input, initial_state)
            .await?;

        let submission = SubmissionId::new();
        let job_id = self
            .registry
            .create_job(submission.clone(), agent_id.clone(), input.to_string())
            .await;

        let key = DataRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission,
            kind: DataMessageKind::Invoke {
                harness: self.harness_type(),
                runtime,
                agent: crate::domain::agent_routing_key(agent_id),
            },
        };

        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: last_state,
            diagnostics: InvokeDiagnostics {
                harness_version: env!("CARGO_PKG_VERSION").to_string(),
            },
            dag_parent: dag_parent.clone(),
            history,
            current_input,
        };

        Ok((key, msg, job_id))
    }

    /// Dispatch a tool call via the V2 service path (harness‑mediated).
    ///
    /// Sends a `RequestV2` to the service worker and waits for a
    /// `ResponseV2`. The service backend is currently hardcoded to
    /// `Mcp("server-everything")`.
    async fn dispatch_service_call(
        &self,
        tc: &crate::domain::ToolCall,
        session_id: &SessionId,
        timeline: BranchId,
        submission: &SubmissionId,
        agent_name: &AgentName,
        last_state: Option<String>,
    ) -> crate::domain::ToolResult {
        let provider = self
            .registry
            .get_agent_by_name(agent_name.as_str())
            .await
            .and_then(|a| a.requirements.mcp.keys().next().cloned())
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
            tool_call_id: tc.id.clone(),
            state: last_state,
            diagnostics: SvcRequestDiagnostics {
                server: provider,
                tool: tc.name.clone(),
                arguments_bytes,
                sent_at_ms,
            },
            arguments: tc.arguments.clone(),
        };

        match self.queue.send_svc_request(key.clone(), req).await {
            Ok(()) => match self.queue.receive_svc_response(&key).await {
                Ok((_rkey, resp, ack)) => {
                    let _ = ack().await;
                    crate::domain::ToolResult {
                        tool_call_id: tc.id.clone(),
                        content: resp.content.into_bytes(),
                    }
                }
                Err(e) => crate::domain::ToolResult {
                    tool_call_id: tc.id.clone(),
                    content: format!("svc_response receive error: {e}").into_bytes(),
                },
            },
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
    async fn dispatch_tool_calls(
        &self,
        tool_calls: Vec<crate::domain::ToolCall>,
        session_id: &SessionId,
        timeline: BranchId,
        sealed: bool,
        submission: SubmissionId,
        agent_name: AgentName,
        last_state: Option<String>,
    ) -> Vec<crate::domain::ToolResult> {
        let mut results = Vec::with_capacity(tool_calls.len());
        for tc in &tool_calls {
            let result = match tc.name.as_str() {
                "delegate_agent" => {
                    let agent_name = tc
                        .arguments
                        .get("agent")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let input = tc
                        .arguments
                        .get("input")
                        .and_then(Value::as_str)
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
                            session_id.clone(),
                            timeline,
                            sealed,
                            None,
                            DagNodeId::root(),
                        )
                        .await
                    {
                        Ok(output) => crate::domain::ToolResult {
                            tool_call_id: tc.id.clone(),
                            content: output.into_bytes(),
                        },
                        Err(e) => crate::domain::ToolResult {
                            tool_call_id: tc.id.clone(),
                            content: e.into_bytes(),
                        },
                    }
                }
                _ => {
                    self.dispatch_service_call(
                        tc,
                        session_id,
                        timeline,
                        &submission,
                        &agent_name,
                        last_state.clone(),
                    )
                    .await
                }
            };
            results.push(result);
        }
        results
    }

    /// Build a re-invocation from in-memory conversation state (no DAG read).
    ///
    /// Called when the orchestration loop re-invokes the agent after
    /// dispatching tool calls. Uses the conversation state accumulated
    /// in the loop rather than reading from the `DagStore`.
    #[allow(clippy::too_many_arguments)]
    async fn build_reinvoke(
        &self,
        agent_id: &ResourceId,
        session_id: &SessionId,
        timeline: BranchId,
        dag_parent: &DagNodeId,
        state: Option<&str>,
        history: Vec<Message>,
        current_input: Vec<Message>,
    ) -> Result<(DataRoutingKey, InvokeMessage, JobId), String> {
        let (_, runtime) = self.resolve_agent_and_runtime(agent_id).await?;
        let submission = SubmissionId::new();
        let job_id = self
            .registry
            .create_job(
                submission.clone(),
                agent_id.clone(),
                current_input
                    .iter()
                    .filter_map(|m| match m {
                        Message::User { content } => Some(content.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join(" "),
            )
            .await;

        let key = DataRoutingKey {
            session: session_id.clone(),
            branch: timeline,
            submission,
            kind: DataMessageKind::Invoke {
                harness: self.harness_type(),
                runtime,
                agent: crate::domain::agent_routing_key(agent_id),
            },
        };

        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: state.map(std::string::ToString::to_string),
            diagnostics: InvokeDiagnostics {
                harness_version: env!("CARGO_PKG_VERSION").to_string(),
            },
            dag_parent: dag_parent.clone(),
            history,
            current_input,
        };

        Ok((key, msg, job_id))
    }

    /// Send an invoke and await its completion, handling timeouts.
    async fn send_and_await_complete(
        &self,
        key: DataRoutingKey,
        msg: InvokeMessage,
        job_id: &JobId,
        agent_id: &ResourceId,
    ) -> Result<CompleteMessage, String> {
        self.registry
            .update_job_status(job_id, JobStatus::Running)
            .await;

        let harness = self.harness_type();
        let submission = key.submission.clone();
        let agent = crate::domain::agent_routing_key(agent_id);
        self.queue
            .send_invoke(key, msg)
            .await
            .map_err(|e| format!("queue error: {e}"))?;

        loop {
            match self
                .queue
                .receive_complete(&submission, harness, &agent)
                .await
            {
                Ok((_key, v2, ack)) => {
                    let _ = ack().await;
                    break Ok(v2);
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
        tool_results: Vec<ToolResult>,
    ) -> (Vec<Message>, Vec<Message>, Option<String>, DagNodeId) {
        let mut new_history = sent_history;
        new_history.extend(sent_current_input);
        new_history.push(Message::Agent {
            content: complete.content,
            tool_calls: complete.tool_calls,
        });
        let next_history = new_history;
        let next_current_input = tool_results
            .into_iter()
            .map(|tr| Message::Tool {
                tool_call_id: tr.tool_call_id,
                content: tr.content,
            })
            .collect();
        (
            next_history,
            next_current_input,
            complete.state,
            complete.dag_id,
        )
    }
}

#[async_trait]
impl Harness for CoreHarness {
    fn harness_type(&self) -> HarnessType {
        self.harness_type
    }

    async fn start_session(&self, agent_name: &str) -> (SessionId, BranchId) {
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
            .send_session_start(key, msg)
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
    ) -> Result<String, String> {
        // Outer loop to support multiple turns when agent returns tool_calls.
        // Accumulate conversation state across re-invocations.
        let mut invoke_history: Vec<Message> = Vec::new();
        let mut invoke_current_input: Vec<Message> = Vec::new();
        let mut invoke_state: Option<String> = initial_state.clone();
        let mut invoke_dag_parent: DagNodeId = dag_parent.clone();

        loop {
            let (key, msg, job_id) = if invoke_history.is_empty() && invoke_current_input.is_empty()
            {
                // First invocation: use build_invoke (reads DAG for prior history)
                self.build_invoke(
                    agent_id,
                    input,
                    &session_id,
                    timeline,
                    sealed,
                    invoke_state.as_deref(),
                    &invoke_dag_parent,
                )
                .await?
            } else {
                // Re-invocation: use in-memory conversation state
                self.build_reinvoke(
                    agent_id,
                    &session_id,
                    timeline,
                    &invoke_dag_parent,
                    invoke_state.as_deref(),
                    invoke_history,
                    invoke_current_input,
                )
                .await?
            };

            // Save before msg is moved into send_invoke.
            let sent_history = msg.history.clone();
            let sent_current_input = msg.current_input.clone();
            let saved_submission = key.submission.clone();
            let saved_agent_name = crate::domain::agent_routing_key(agent_id);

            let complete = self
                .send_and_await_complete(key, msg, &job_id, agent_id)
                .await?;

            if complete.tool_calls.is_none() {
                let result = complete
                    .content
                    .unwrap_or_else(|| String::from_utf8_lossy(&complete.payload).to_string());
                self.registry
                    .update_job_status(&job_id, JobStatus::Completed(result.clone()))
                    .await;
                return Ok(result);
            }

            // Dispatch tool calls and collect results
            let tool_results = self
                .dispatch_tool_calls(
                    complete.tool_calls.clone().unwrap(),
                    &session_id,
                    timeline,
                    sealed,
                    saved_submission,
                    saved_agent_name,
                    invoke_state.clone(),
                )
                .await;

            // Prepare next turn state
            let (next_history, next_current_input, next_state, next_dag_parent) =
                Self::prepare_next_turn_state(
                    sent_history,
                    sent_current_input,
                    complete,
                    tool_results,
                );

            invoke_history = next_history;
            invoke_current_input = next_current_input;
            invoke_state = next_state;
            invoke_dag_parent = next_dag_parent;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        InMemoryDagStore, InMemoryRegistry, InMemorySecretStore, RuntimeType, SecretStore,
    };
    use crate::queue::InMemoryQueue;

    #[test]
    fn harness_type_is_cli() {
        let queue = Arc::new(InMemoryQueue::new());
        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        registry.register_runtime(RuntimeType::Container);
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());

        let harness = CoreHarness::new(queue, registry, store, HarnessType::Cli);

        assert_eq!(harness.harness_type(), HarnessType::Cli);
    }
}
