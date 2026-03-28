//! DAG domain types — content-addressed Merkle DAG for runtime tracing (ADR 067).
//!
//! The platform observes every NATS message and writes one DAG node per message.
//! No pairing, no buffering — each message is independently meaningful.
//!
//! Each `DagNode` captures one message:
//! - `hash`: SHA-256(payload || `parent_hash` || `message_type` || diagnostics) — Merkle chain
//! - `parent_hash`: previous node in the same session (empty string for root)
//! - `message_type`: Invoke, Request, Response, Complete, or Delegate
//! - `from` / `to`: sender and receiver (parsed from NATS subject)
//! - `session_id`: groups nodes into a linear chain per session
//! - `submission_id`: groups all messages for one user request
//! - `payload`: raw message payload
//! - `created_at`: ISO-8601 timestamp

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};

use super::message::identity::{Instance, StateHash};
use super::session::Session;

/// Snapshot of all store states at a point in the DAG (ADR 116).
///
/// Maps service instance names to their per-store state hashes.
/// `BTreeMap` guarantees deterministic ordering for content addressing.
/// Always present on every `DagNode` — inherited from parent if unchanged.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Snapshot(pub BTreeMap<Instance, StateHash>);

impl Snapshot {
    /// Empty snapshot — no stores, no state. Used for root nodes.
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    /// Return a new snapshot with one store's state updated.
    #[must_use]
    pub fn with_state(&self, instance: Instance, hash: StateHash) -> Self {
        let mut map = self.0.clone();
        map.insert(instance, hash);
        Self(map)
    }
}

/// Operational plane — determines routing, retention, and access control (ADR 121).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plane {
    /// Agent execution: invoke, request, response, complete.
    Data,
    /// Compensating transactions: fork, promote.
    Session,
}

/// The message types in the Vlinder protocol (ADR 044).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Invoke,
    Request,
    Response,
    Complete,
    Delegate,
    Fork,
    Promote,
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageType::Invoke => "invoke",
            MessageType::Request => "request",
            MessageType::Response => "response",
            MessageType::Complete => "complete",
            MessageType::Delegate => "delegate",
            MessageType::Fork => "fork",
            MessageType::Promote => "promote",
        }
    }

    pub fn plane(&self) -> Plane {
        match self {
            MessageType::Invoke
            | MessageType::Request
            | MessageType::Response
            | MessageType::Complete
            | MessageType::Delegate => Plane::Data,
            MessageType::Fork | MessageType::Promote => Plane::Session,
        }
    }
}

impl std::str::FromStr for MessageType {
    type Err = String;

    /// Parse from string. Accepts both canonical names and NATS subject
    /// abbreviations (`req` -> Request, `res` -> Response).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "invoke" => Ok(MessageType::Invoke),
            "req" | "request" => Ok(MessageType::Request),
            "res" | "response" => Ok(MessageType::Response),
            "complete" => Ok(MessageType::Complete),
            "delegate" => Ok(MessageType::Delegate),
            "fork" => Ok(MessageType::Fork),
            "promote" => Ok(MessageType::Promote),
            _ => Err(format!("unknown message type: {s}")),
        }
    }
}

/// A single node in the runtime-captured DAG.
///
/// Wraps the full `ObservableMessage` with DAG metadata:
/// content-addressed ID, Merkle parent pointer, insertion timestamp,
/// and the full storage snapshot at this point (ADR 116).
#[derive(Debug, Clone, PartialEq)]
pub struct DagNode {
    pub id: super::DagNodeId,
    pub parent_id: super::DagNodeId,
    pub created_at: DateTime<Utc>,
    pub state: Snapshot,
    pub msg_type: MessageType,
    pub session: super::SessionId,
    pub submission: super::SubmissionId,
    pub branch: super::BranchId,
    pub protocol_version: String,
    /// Legacy message format. `None` for typed-table rows (invoke).
    pub message: Option<super::SessionPlane>,
}

impl DagNode {
    pub fn message_type(&self) -> MessageType {
        self.msg_type
    }
    pub fn session_id(&self) -> &super::SessionId {
        &self.session
    }
    pub fn submission_id(&self) -> &super::SubmissionId {
        &self.submission
    }
    pub fn payload(&self) -> &[u8] {
        self.message.as_ref().map_or(&[], |m| m.payload())
    }
    pub fn protocol_version(&self) -> &str {
        &self.protocol_version
    }
    pub fn branch_id(&self) -> &super::BranchId {
        &self.branch
    }
    pub fn message_state(&self) -> Option<&str> {
        self.message.as_ref().and_then(|m| m.state())
    }
}

/// Compute the content-addressed hash for a DAG node.
///
/// `sha256(payload || parent_hash || message_type || diagnostics || session_id)`
/// — the parent hash makes this a Merkle chain: changing any ancestor
/// invalidates all descendants. The `session_id` ensures that identical
/// messages in different sessions produce different hashes (prevents
/// INSERT OR IGNORE from silently dropping cross-session duplicates).
pub fn hash_dag_node(
    payload: &[u8],
    parent_id: &super::DagNodeId,
    message_type: &MessageType,
    diagnostics: &[u8],
    session_id: &super::SessionId,
) -> super::DagNodeId {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.update(parent_id.as_str().as_bytes());
    hasher.update(message_type.as_str().as_bytes());
    hasher.update(diagnostics);
    hasher.update(session_id.as_str().as_bytes());
    super::DagNodeId::from(format!("{:x}", hasher.finalize()))
}

/// Summary of a session for the viewer index page.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionSummary {
    pub session_id: super::SessionId,
    pub agent_name: String,
    pub started_at: DateTime<Utc>,
    pub message_count: usize,
    pub is_open: bool,
}

/// A branch within a session — represents a named line of execution.
///
/// Branch 1 is always the canonical (default) branch. Fork creates a
/// new branch from a point on an existing one.
#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    pub id: super::BranchId,
    pub name: String,
    pub session_id: super::SessionId,
    pub fork_point: Option<super::DagNodeId>,
    pub head: Option<super::DagNodeId>,
    pub broken_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// A worker that persists observable messages to a DAG structure.
///
/// Git is one implementation (`GitDagWorker`). Any backend that can
/// preserve the chronological message stream would implement this.
pub trait DagWorker: Send {
    /// Persist a single observable message (legacy path).
    fn on_observable_message(&mut self, msg: &super::SessionPlane, created_at: DateTime<Utc>);

    /// Persist an invoke message (data-plane path, ADR 121).
    fn on_invoke(
        &mut self,
        _key: &super::DataRoutingKey,
        _msg: &super::InvokeMessage,
        _created_at: DateTime<Utc>,
    ) {
        // Default no-op — implementations opt in.
    }

    /// Persist a request message (data-plane path, ADR 121).
    fn on_request(
        &mut self,
        _key: &super::DataRoutingKey,
        _msg: &super::RequestMessage,
        _created_at: DateTime<Utc>,
    ) {
    }

    /// Persist a response message (data-plane path, ADR 121).
    fn on_response(
        &mut self,
        _key: &super::DataRoutingKey,
        _msg: &super::ResponseMessage,
        _created_at: DateTime<Utc>,
    ) {
    }

    /// Persist a complete message (data-plane path, ADR 121).
    fn on_complete(
        &mut self,
        _key: &super::DataRoutingKey,
        _msg: &super::CompleteMessage,
        _created_at: DateTime<Utc>,
    ) {
        // Default no-op — implementations opt in.
    }

    /// Persist a fork message (session-plane path).
    fn on_fork(
        &mut self,
        _key: &super::SessionRoutingKey,
        _msg: &super::ForkMessageV2,
        _created_at: DateTime<Utc>,
    ) {
    }

    /// Persist a promote message (session-plane path).
    fn on_promote(
        &mut self,
        _key: &super::SessionRoutingKey,
        _msg: &super::PromoteMessageV2,
        _created_at: DateTime<Utc>,
    ) {
    }
}

/// Persistence layer for DAG nodes.
pub trait DagStore: Send + Sync {
    /// Insert a node. Idempotent (content-addressed, INSERT OR IGNORE).
    fn insert_node(&self, node: &DagNode) -> Result<(), String>;

    /// Insert a typed invoke node. Writes to `dag_nodes` + `invoke_nodes`.
    fn insert_invoke_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        key: &super::DataRoutingKey,
        msg: &super::InvokeMessage,
    ) -> Result<(), String> {
        let _ = (dag_id, parent_id, created_at, state, key, msg);
        Err("insert_invoke_node not implemented".to_string())
    }

    /// Insert a typed complete node. Writes to `dag_nodes` + `complete_nodes`.
    #[allow(clippy::too_many_arguments)]
    fn insert_complete_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        agent: &super::AgentName,
        harness: super::HarnessType,
        msg: &super::CompleteMessage,
    ) -> Result<(), String> {
        let _ = (
            dag_id, parent_id, created_at, state, session, submission, branch, agent, harness, msg,
        );
        Err("insert_complete_node not implemented".to_string())
    }

    /// Insert a typed request node. Writes to `dag_nodes` + `request_nodes`.
    #[allow(clippy::too_many_arguments)]
    fn insert_request_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        agent: &super::AgentName,
        service: super::ServiceBackend,
        operation: super::Operation,
        sequence: super::Sequence,
        msg: &super::RequestMessage,
    ) -> Result<(), String> {
        let _ = (
            dag_id, parent_id, created_at, state, session, submission, branch, agent, service,
            operation, sequence, msg,
        );
        Err("insert_request_node not implemented".to_string())
    }

    /// Insert a typed response node. Writes to `dag_nodes` + `response_nodes`.
    #[allow(clippy::too_many_arguments)]
    fn insert_response_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        agent: &super::AgentName,
        service: super::ServiceBackend,
        operation: super::Operation,
        sequence: super::Sequence,
        msg: &super::ResponseMessage,
    ) -> Result<(), String> {
        let _ = (
            dag_id, parent_id, created_at, state, session, submission, branch, agent, service,
            operation, sequence, msg,
        );
        Err("insert_response_node not implemented".to_string())
    }

    /// Insert a typed fork node. Writes to `dag_nodes` + `fork_nodes`.
    fn insert_fork_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        key: &super::SessionRoutingKey,
        msg: &super::ForkMessageV2,
    ) -> Result<(), String> {
        let _ = (dag_id, parent_id, created_at, state, key, msg);
        Err("insert_fork_node not implemented".to_string())
    }

    /// Insert a typed promote node. Writes to `dag_nodes` + `promote_nodes`.
    fn insert_promote_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        key: &super::SessionRoutingKey,
        msg: &super::PromoteMessageV2,
    ) -> Result<(), String> {
        let _ = (dag_id, parent_id, created_at, state, key, msg);
        Err("insert_promote_node not implemented".to_string())
    }

    /// Retrieve a node by its content-addressed ID.
    fn get_node(&self, id: &super::DagNodeId) -> Result<Option<DagNode>, String>;

    /// Retrieve a node by ID prefix. Returns an error if ambiguous.
    fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
        // Default: try exact match first, then scan is left to implementors.
        self.get_node(&super::DagNodeId::from(prefix.to_string()))
    }

    /// Get all nodes in a session, ordered by `created_at`.
    fn get_session_nodes(&self, session_id: &super::SessionId) -> Result<Vec<DagNode>, String>;

    /// Get all children of a given parent ID.
    fn get_children(&self, parent_id: &super::DagNodeId) -> Result<Vec<DagNode>, String>;

    // -------------------------------------------------------------------------
    // Branch methods
    // -------------------------------------------------------------------------

    /// Create a new branch. Returns the auto-generated ID.
    fn create_branch(
        &self,
        name: &str,
        session_id: &super::SessionId,
        fork_point: Option<&super::DagNodeId>,
    ) -> Result<super::BranchId, String>;

    /// Look up a branch by its name.
    fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String>;

    /// Look up a branch by its integer ID.
    fn get_branch(&self, id: super::BranchId) -> Result<Option<Branch>, String>;

    /// List all sessions with summary information.
    fn list_sessions(&self) -> Result<Vec<SessionSummary>, String>;

    /// Get all nodes for a submission (a single turn), ordered by `created_at`.
    fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String>;

    /// Retrieve typed invoke data by DAG node hash.
    ///
    /// Returns the routing key and invoke message from the `invoke_nodes` table.
    /// Returns `None` if the hash doesn't exist or isn't an invoke node.
    fn get_invoke_node(
        &self,
        dag_hash: &super::DagNodeId,
    ) -> Result<Option<(super::DataRoutingKey, super::InvokeMessage)>, String> {
        let _ = dag_hash;
        Ok(None) // Default no-op for non-SQL implementations
    }

    /// Retrieve typed complete data by DAG node hash.
    fn get_complete_node(
        &self,
        dag_hash: &super::DagNodeId,
    ) -> Result<Option<super::CompleteMessage>, String> {
        let _ = dag_hash;
        Ok(None)
    }

    /// Retrieve typed request data by DAG node hash.
    fn get_request_node(
        &self,
        dag_hash: &super::DagNodeId,
    ) -> Result<Option<super::RequestMessage>, String> {
        let _ = dag_hash;
        Ok(None)
    }

    /// Retrieve typed response data by DAG node hash.
    fn get_response_node(
        &self,
        dag_hash: &super::DagNodeId,
    ) -> Result<Option<super::ResponseMessage>, String> {
        let _ = dag_hash;
        Ok(None)
    }

    /// Get all branches for a session.
    fn get_branches_for_session(
        &self,
        session_id: &super::SessionId,
    ) -> Result<Vec<Branch>, String>;

    /// Get the most recent `DagNode` on a branch, optionally filtered by message type.
    fn latest_node_on_branch(
        &self,
        branch_id: super::BranchId,
        message_type: Option<MessageType>,
    ) -> Result<Option<DagNode>, String>;

    /// Rename a branch.
    fn rename_branch(&self, id: super::BranchId, new_name: &str) -> Result<(), String>;

    /// Mark a branch as broken (sealed). Sets `broken_at` to the given timestamp.
    fn seal_branch(&self, id: super::BranchId, broken_at: DateTime<Utc>) -> Result<(), String>;

    /// Update a session's default branch.
    fn update_session_default_branch(
        &self,
        session_id: &super::SessionId,
        branch_id: super::BranchId,
    ) -> Result<(), String>;

    // -------------------------------------------------------------------------
    // Session CRUD
    // -------------------------------------------------------------------------

    /// Persist a new session. Idempotent (ignores if ID already exists).
    fn create_session(&self, session: &Session) -> Result<(), String>;

    /// Look up a session by its ID.
    fn get_session(&self, session_id: &super::SessionId) -> Result<Option<Session>, String>;

    /// Look up a session by its friendly name.
    fn get_session_by_name(&self, name: &str) -> Result<Option<Session>, String>;
}

// ============================================================================
// In-Memory Implementation (for testing)
// ============================================================================

/// In-memory `DagStore` for unit tests that don't need `SQLite`.
pub struct InMemoryDagStore {
    nodes: std::sync::Mutex<Vec<DagNode>>,
    branches: std::sync::Mutex<Vec<Branch>>,
    sessions: std::sync::Mutex<Vec<Session>>,
}

impl InMemoryDagStore {
    pub fn new() -> Self {
        Self {
            nodes: std::sync::Mutex::new(Vec::new()),
            branches: std::sync::Mutex::new(Vec::new()),
            sessions: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl Default for InMemoryDagStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DagStore for InMemoryDagStore {
    fn insert_node(&self, node: &DagNode) -> Result<(), String> {
        let mut nodes = self.nodes.lock().unwrap();
        // Idempotent: skip if ID already exists
        if !nodes.iter().any(|n| n.id == node.id) {
            nodes.push(node.clone());
        }
        Ok(())
    }

    fn insert_invoke_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        key: &super::DataRoutingKey,
        _msg: &super::InvokeMessage,
    ) -> Result<(), String> {
        let node = DagNode {
            id: dag_id.clone(),
            parent_id: parent_id.clone(),
            created_at,
            state: state.clone(),
            msg_type: MessageType::Invoke,
            session: key.session.clone(),
            submission: key.submission.clone(),
            branch: key.branch,
            protocol_version: "v1".to_string(),
            message: None,
        };
        self.insert_node(&node)
    }

    fn insert_complete_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        _agent: &super::AgentName,
        _harness: super::HarnessType,
        _msg: &super::CompleteMessage,
    ) -> Result<(), String> {
        let node = DagNode {
            id: dag_id.clone(),
            parent_id: parent_id.clone(),
            created_at,
            state: state.clone(),
            msg_type: MessageType::Complete,
            session: session.clone(),
            submission: submission.clone(),
            branch,
            protocol_version: "v1".to_string(),
            message: None,
        };
        self.insert_node(&node)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_request_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        _agent: &super::AgentName,
        _service: super::ServiceBackend,
        _operation: super::Operation,
        _sequence: super::Sequence,
        _msg: &super::RequestMessage,
    ) -> Result<(), String> {
        let node = DagNode {
            id: dag_id.clone(),
            parent_id: parent_id.clone(),
            created_at,
            state: state.clone(),
            msg_type: MessageType::Request,
            session: session.clone(),
            submission: submission.clone(),
            branch,
            protocol_version: "v1".to_string(),
            message: None,
        };
        self.insert_node(&node)
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_response_node(
        &self,
        dag_id: &super::DagNodeId,
        parent_id: &super::DagNodeId,
        created_at: DateTime<Utc>,
        state: &Snapshot,
        session: &super::SessionId,
        submission: &super::SubmissionId,
        branch: super::BranchId,
        _agent: &super::AgentName,
        _service: super::ServiceBackend,
        _operation: super::Operation,
        _sequence: super::Sequence,
        _msg: &super::ResponseMessage,
    ) -> Result<(), String> {
        let node = DagNode {
            id: dag_id.clone(),
            parent_id: parent_id.clone(),
            created_at,
            state: state.clone(),
            msg_type: MessageType::Response,
            session: session.clone(),
            submission: submission.clone(),
            branch,
            protocol_version: "v1".to_string(),
            message: None,
        };
        self.insert_node(&node)
    }

    fn get_complete_node(
        &self,
        _dag_hash: &super::DagNodeId,
    ) -> Result<Option<super::CompleteMessage>, String> {
        Ok(None)
    }

    fn get_node(&self, id: &super::DagNodeId) -> Result<Option<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes.iter().find(|n| n.id == *id).cloned())
    }

    fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        let matches: Vec<_> = nodes
            .iter()
            .filter(|n| n.id.as_str().starts_with(prefix))
            .collect();
        match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches[0].clone())),
            n => Err(format!("ambiguous ID prefix '{prefix}': {n} matches")),
        }
    }

    fn get_session_nodes(&self, session_id: &super::SessionId) -> Result<Vec<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        let mut result: Vec<DagNode> = nodes
            .iter()
            .filter(|n| *n.session_id() == *session_id)
            .cloned()
            .collect();
        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(result)
    }

    fn get_children(&self, parent_id: &super::DagNodeId) -> Result<Vec<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes
            .iter()
            .filter(|n| n.parent_id == *parent_id)
            .cloned()
            .collect())
    }

    fn create_branch(
        &self,
        name: &str,
        session_id: &super::SessionId,
        fork_point: Option<&super::DagNodeId>,
    ) -> Result<super::BranchId, String> {
        let mut branches = self.branches.lock().unwrap();
        let id = super::BranchId::from(i64::try_from(branches.len()).unwrap_or(i64::MAX) + 1);
        branches.push(Branch {
            id,
            name: name.to_string(),
            session_id: session_id.clone(),
            fork_point: fork_point.cloned(),
            head: None,
            broken_at: None,
            created_at: Utc::now(),
        });
        Ok(id)
    }

    fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String> {
        let branches = self.branches.lock().unwrap();
        Ok(branches.iter().find(|b| b.name == name).cloned())
    }

    fn get_branch(&self, id: super::BranchId) -> Result<Option<Branch>, String> {
        let branches = self.branches.lock().unwrap();
        Ok(branches.iter().find(|b| b.id == id).cloned())
    }

    fn list_sessions(&self) -> Result<Vec<SessionSummary>, String> {
        let nodes = self.nodes.lock().unwrap();
        let mut sessions: std::collections::HashMap<super::SessionId, Vec<&DagNode>> =
            std::collections::HashMap::new();
        for node in nodes.iter() {
            sessions
                .entry(node.session_id().clone())
                .or_default()
                .push(node);
        }

        let mut summaries: Vec<SessionSummary> = sessions
            .into_iter()
            .map(|(session_id, mut nodes)| {
                nodes.sort_by(|a, b| a.created_at.cmp(&b.created_at));
                // Prefer Invoke for agent name (v2 path), fall back to first
                // Complete or any node (v1 sessions may not have an Invoke node).
                let invoke_node = nodes
                    .iter()
                    .find(|n| n.message_type() == MessageType::Invoke);
                let agent_name = if let Some(n) = invoke_node {
                    n.message
                        .as_ref()
                        .map(|m| m.sender_receiver().1)
                        .unwrap_or_default()
                } else {
                    // No invoke node — extract agent from the first Complete
                    nodes
                        .iter()
                        .find(|n| n.message_type() == MessageType::Complete)
                        .and_then(|n| n.message.as_ref())
                        .map(|m| m.sender_receiver().0)
                        .unwrap_or_default()
                };
                let started_at = nodes.first().map(|n| n.created_at).unwrap_or_default();
                let message_count = nodes
                    .iter()
                    .filter(|n| {
                        n.message_type() == MessageType::Invoke
                            || n.message_type() == MessageType::Complete
                    })
                    .count();
                let is_open = nodes
                    .last()
                    .is_some_and(|n| n.message_type() != MessageType::Complete);
                SessionSummary {
                    session_id,
                    agent_name,
                    started_at,
                    message_count,
                    is_open,
                }
            })
            .collect();

        summaries.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        Ok(summaries)
    }

    fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        let mut result: Vec<DagNode> = nodes
            .iter()
            .filter(|n| n.submission_id().as_str() == submission_id)
            .cloned()
            .collect();
        result.sort_by(|a, b| a.created_at.cmp(&b.created_at));
        Ok(result)
    }

    fn get_branches_for_session(
        &self,
        session_id: &super::SessionId,
    ) -> Result<Vec<Branch>, String> {
        let branches = self.branches.lock().unwrap();
        Ok(branches
            .iter()
            .filter(|b| b.session_id == *session_id)
            .cloned()
            .collect())
    }

    fn latest_node_on_branch(
        &self,
        branch_id: super::BranchId,
        message_type: Option<MessageType>,
    ) -> Result<Option<DagNode>, String> {
        let nodes = self.nodes.lock().unwrap();
        Ok(nodes
            .iter()
            .rev()
            .filter(|n| *n.branch_id() == branch_id)
            .find(|n| message_type.is_none_or(|mt| n.message_type() == mt))
            .cloned())
    }

    fn rename_branch(&self, id: super::BranchId, new_name: &str) -> Result<(), String> {
        let mut branches = self.branches.lock().unwrap();
        let branch = branches
            .iter_mut()
            .find(|b| b.id == id)
            .ok_or_else(|| format!("branch {id} not found"))?;
        branch.name = new_name.to_string();
        Ok(())
    }

    fn seal_branch(&self, id: super::BranchId, broken_at: DateTime<Utc>) -> Result<(), String> {
        let mut branches = self.branches.lock().unwrap();
        let branch = branches
            .iter_mut()
            .find(|b| b.id == id)
            .ok_or_else(|| format!("branch {id} not found"))?;
        branch.broken_at = Some(broken_at);
        Ok(())
    }

    fn update_session_default_branch(
        &self,
        session_id: &super::SessionId,
        branch_id: super::BranchId,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        let session = sessions
            .iter_mut()
            .find(|s| s.id == *session_id)
            .ok_or_else(|| format!("session {session_id} not found"))?;
        session.default_branch = branch_id;
        Ok(())
    }

    fn create_session(&self, session: &Session) -> Result<(), String> {
        let mut sessions = self.sessions.lock().unwrap();
        if sessions
            .iter()
            .any(|s| s.id.as_str() == session.id.as_str())
        {
            return Ok(());
        }
        sessions.push(session.clone());
        Ok(())
    }

    fn get_session(&self, session_id: &super::SessionId) -> Result<Option<Session>, String> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions.iter().find(|s| s.id == *session_id).cloned())
    }

    fn get_session_by_name(&self, name: &str) -> Result<Option<Session>, String> {
        let sessions = self.sessions.lock().unwrap();
        Ok(sessions.iter().find(|s| s.name == name).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    // --- MessageType tests ---

    #[test]
    fn message_type_round_trips() {
        for mt in [
            MessageType::Invoke,
            MessageType::Request,
            MessageType::Response,
            MessageType::Complete,
            MessageType::Delegate,
            MessageType::Fork,
            MessageType::Promote,
        ] {
            assert_eq!(MessageType::from_str(mt.as_str()), Ok(mt));
        }
    }

    #[test]
    fn message_type_from_unknown_returns_none() {
        assert!(MessageType::from_str("unknown").is_err());
    }

    // --- hash_dag_node tests ---

    use crate::domain::{DagNodeId, SessionId};

    fn did(s: &str) -> DagNodeId {
        DagNodeId::from(s.to_string())
    }

    fn sid(s: &str) -> SessionId {
        SessionId::try_from(s.to_string()).unwrap()
    }

    // Valid UUIDs for testing
    const SES_1: &str = "d4761d76-dee4-4ebf-9df4-43b52efa4f78";
    const SES_2: &str = "e2660cff-33d6-4428-acca-2d297dcc1cad";

    #[test]
    fn hash_is_valid_sha256() {
        let hash = hash_dag_node(b"hello", &did(""), &MessageType::Invoke, b"", &sid(SES_1));
        assert_eq!(hash.as_str().len(), 64);
        assert!(hash.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_changes_with_parent() {
        let h1 = hash_dag_node(b"payload", &did(""), &MessageType::Invoke, b"", &sid(SES_1));
        let h2 = hash_dag_node(
            b"payload",
            &did("parent-abc"),
            &MessageType::Invoke,
            b"",
            &sid(SES_1),
        );
        assert_ne!(h1, h2, "Merkle property: different parent → different hash");
    }

    #[test]
    fn hash_changes_with_payload() {
        let h1 = hash_dag_node(
            b"payload-a",
            &did(""),
            &MessageType::Invoke,
            b"",
            &sid(SES_1),
        );
        let h2 = hash_dag_node(
            b"payload-b",
            &did(""),
            &MessageType::Invoke,
            b"",
            &sid(SES_1),
        );
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_changes_with_message_type() {
        let h1 = hash_dag_node(b"payload", &did(""), &MessageType::Invoke, b"", &sid(SES_1));
        let h2 = hash_dag_node(
            b"payload",
            &did(""),
            &MessageType::Complete,
            b"",
            &sid(SES_1),
        );
        assert_ne!(h1, h2, "same payload, different type → different hash");
    }

    #[test]
    fn hash_changes_with_diagnostics() {
        let h1 = hash_dag_node(b"payload", &did(""), &MessageType::Invoke, b"", &sid(SES_1));
        let h2 = hash_dag_node(
            b"payload",
            &did(""),
            &MessageType::Invoke,
            b"{\"duration_ms\":100}",
            &sid(SES_1),
        );
        assert_ne!(h1, h2, "different diagnostics → different hash");
    }

    #[test]
    fn hash_changes_with_session_id() {
        let h1 = hash_dag_node(b"payload", &did(""), &MessageType::Invoke, b"", &sid(SES_1));
        let h2 = hash_dag_node(b"payload", &did(""), &MessageType::Invoke, b"", &sid(SES_2));
        assert_ne!(
            h1, h2,
            "same payload in different sessions → different hash"
        );
    }

    #[test]
    fn hash_is_deterministic() {
        let h1 = hash_dag_node(
            b"same",
            &did("p"),
            &MessageType::Request,
            b"diag",
            &sid(SES_1),
        );
        let h2 = hash_dag_node(
            b"same",
            &did("p"),
            &MessageType::Request,
            b"diag",
            &sid(SES_1),
        );
        assert_eq!(h1, h2);
    }
}
