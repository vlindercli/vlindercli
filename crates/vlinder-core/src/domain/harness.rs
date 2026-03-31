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
    AgentName, BranchId, DagNodeId, DagStore, DataMessageKind, DataRoutingKey, ForkMessage,
    HarnessType, InvokeDiagnostics, InvokeMessage, JobId, JobStatus, MessageId, MessageQueue,
    MessageType, PromoteMessage, Registry, ResourceId, SessionId, SessionMessageKind,
    SessionRoutingKey, SessionStartMessage, SubmissionId,
};

/// Common harness operations shared across all harness types.
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
    fn start_session(&self, agent_name: &str) -> (SessionId, BranchId);

    /// Run an agent to completion synchronously.
    ///
    /// Sends input to the agent and blocks until the response arrives.
    /// Returns the agent's output as a string.
    #[allow(clippy::too_many_arguments)]
    fn run_agent(
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
    fn fork_timeline(
        &self,
        params: ForkParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String>;

    /// Promote a branch to main by sending a `PromoteMessage` through the queue.
    ///
    /// Fire-and-forget: both SQL (via `RecordingQueue`) and git (via
    /// `GitDagWorker`) react to the message. No response is expected.
    fn promote_timeline(
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

/// Build an enriched payload from DAG-derived history.
///
/// Each invoke payload already contains the full conversation history up to
/// that point, so we only need the last invoke + last complete to reconstruct.
fn build_payload(
    last_invoke_payload: Option<&str>,
    last_complete_payload: Option<&str>,
    current_input: &str,
) -> String {
    match (last_invoke_payload, last_complete_payload) {
        (Some(invoke), Some(complete)) => {
            format!("{invoke}\nAgent: {complete}\nUser: {current_input}")
        }
        _ => format!("User: {current_input}"),
    }
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
        }
    }

    /// Build an invoke from session state and register a job.
    ///
    /// Returns the routing key, payload message, and job ID.
    #[allow(clippy::too_many_arguments)]
    fn build_invoke(
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

        let agent = self
            .registry
            .get_agent(agent_id)
            .ok_or_else(|| format!("agent not deployed: {agent_id}"))?;
        let runtime = self
            .registry
            .select_runtime(&agent)
            .ok_or_else(|| format!("no runtime available for agent: {agent_id}"))?;

        let last_invoke_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Invoke))
            .unwrap_or(None);
        let last_invoke_payload = last_invoke_node.as_ref().and_then(|n| {
            self.store
                .get_invoke_node(&n.id)
                .ok()
                .flatten()
                .map(|(_, msg)| String::from_utf8_lossy(&msg.payload).to_string())
        });
        let last_complete_node = self
            .store
            .latest_node_on_branch(timeline, Some(MessageType::Complete))
            .unwrap_or(None);
        let last_complete = last_complete_node
            .as_ref()
            .and_then(|n| self.store.get_complete_node(&n.id).ok().flatten());
        let last_complete_payload = last_complete
            .as_ref()
            .map(|m| String::from_utf8_lossy(&m.payload).to_string());
        let enriched_payload = build_payload(
            last_invoke_payload.as_deref(),
            last_complete_payload.as_deref(),
            input,
        );
        let submission = SubmissionId::new();
        let last_state = last_complete
            .as_ref()
            .and_then(|m| m.state.as_ref().map(std::string::ToString::to_string))
            .or_else(|| initial_state.map(std::string::ToString::to_string));

        let job_id =
            self.registry
                .create_job(submission.clone(), agent_id.clone(), input.to_string());

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
            payload: enriched_payload.as_bytes().to_vec(),
        };

        Ok((key, msg, job_id))
    }
}

impl Harness for CoreHarness {
    fn harness_type(&self) -> HarnessType {
        self.harness_type
    }

    fn start_session(&self, agent_name: &str) -> (SessionId, BranchId) {
        let session_id = SessionId::new();
        let key = SessionRoutingKey {
            session: session_id.clone(),
            submission: SubmissionId::new(),
            kind: SessionMessageKind::Start {
                agent_name: AgentName::new(agent_name),
            },
        };
        let msg = SessionStartMessage::new();
        let branch_id = self.queue.send_session_start(key, msg).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to send session start message");
            BranchId::from(1)
        });

        (session_id, branch_id)
    }

    fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
    ) -> Result<String, String> {
        let (key, msg, job_id) = self.build_invoke(
            agent_id,
            input,
            &session_id,
            timeline,
            sealed,
            initial_state.as_deref(),
            &dag_parent,
        )?;
        self.registry.update_job_status(&job_id, JobStatus::Running);

        let harness = self.harness_type();
        let submission = key.submission.clone();
        let agent = crate::domain::agent_routing_key(agent_id);
        self.queue
            .send_invoke(key, msg)
            .map_err(|e| format!("queue error: {e}"))?;

        let result = loop {
            match self.queue.receive_complete(&submission, harness, &agent) {
                Ok((_key, v2, ack)) => {
                    let _ = ack();
                    break String::from_utf8_lossy(&v2.payload).to_string();
                }
                Err(crate::domain::QueueError::Timeout) => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(e) => return Err(format!("queue error: {e}")),
            }
        };

        self.registry
            .update_job_status(&job_id, JobStatus::Completed(result.clone()));
        Ok(result)
    }

    fn fork_timeline(
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
            .map_err(|e| format!("queue error: {e}"))
    }

    fn promote_timeline(
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
