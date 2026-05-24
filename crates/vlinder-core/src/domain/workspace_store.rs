//! Versioned workspace storage trait (ADR 133).
//!
//! Substrate-independent contract for per-(agent, session, branch) workspaces
//! with content-derived state pointers. The trait speaks vlinder domain types
//! and returns opaque state pointers — the queue protocol treats them as
//! strings, only the impl parses them.
//!
//! The contract `snapshot` must satisfy: identical workspace content always
//! produces the same returned state pointer, including in the no-write case.
//! This is what makes time-travel and fork-from-any-point work consistently.

use std::fmt;
use std::path::PathBuf;

use async_trait::async_trait;

use crate::domain::{AgentName, BranchId, DagNodeId, SessionId};

/// A versioned workspace storage substrate.
///
/// Implementations must be thread-safe. The trait is a critical-system trait —
/// every method must be implemented, no defaults, and the contract is pinned
/// by integration tests against each production impl.
#[async_trait]
pub trait WorkspaceStore: Send + Sync {
    /// Ensure a writable workspace exists for (agent, session, branch).
    /// If `parent_state` is `Some`, materialize from that state pointer
    /// (typically a snapshot identifier from a previous `snapshot()` call).
    /// Returns a filesystem path the runtime can bind-mount into a container.
    async fn ensure_workspace(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
        parent_state: Option<&str>,
    ) -> Result<PathBuf, WorkspaceError>;

    /// Snapshot the current workspace state. Returns a content-derived state
    /// pointer. Identical content produces the same return value, including
    /// in the no-write case (dedup is automatic).
    ///
    /// `dag_node_id` is the identifier of the message DAG node that produced
    /// this snapshot, used for traceability between the message DAG and the
    /// storage DAG.
    async fn snapshot(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
        dag_node_id: &DagNodeId,
    ) -> Result<String, WorkspaceError>;

    /// Destroy the live workspace for (agent, session, branch). Per-impl
    /// semantics decide what happens to historical snapshots; ZFS implements
    /// this as `zfs destroy -r`, which removes the dataset and its snapshots.
    async fn destroy_workspace(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
    ) -> Result<(), WorkspaceError>;
}

/// Errors a `WorkspaceStore` impl may return.
#[derive(Debug)]
pub enum WorkspaceError {
    /// The requested workspace or snapshot does not exist.
    NotFound(String),
    /// The underlying storage operation failed.
    OperationFailed(String),
    /// The state pointer could not be parsed by this impl.
    InvalidStatePointer(String),
}

impl fmt::Display for WorkspaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkspaceError::NotFound(name) => write!(f, "workspace not found: {name}"),
            WorkspaceError::OperationFailed(msg) => write!(f, "operation failed: {msg}"),
            WorkspaceError::InvalidStatePointer(s) => {
                write!(f, "invalid state pointer: {s}")
            }
        }
    }
}

impl std::error::Error for WorkspaceError {}
