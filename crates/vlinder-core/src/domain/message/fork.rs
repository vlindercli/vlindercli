//! `ForkMessage`: CLI → Platform (create a timeline fork).
//!
//! A control plane message that creates a new timeline branch in the DAG.
//! Both projections (SQL `DagStore` and git repo) react to this message:
//! - SQL: creates a Timeline row with `parent_id` and `fork_point`
//! - Git: creates a branch and updates timeline index files
//!
//! Unlike service messages, `ForkMessage` carries no payload — the fork point
//! hash and branch name are all that's needed to define the topology change.

use crate::domain::AgentName;

use super::identity::{BranchId, DagNodeId, MessageId, SessionId, SubmissionId};
use super::PROTOCOL_VERSION;

/// Fork message: CLI → Platform
///
/// Creates a new timeline branch from a point in the DAG. The fork point
/// is a canonical `DagNode` hash in the source session. The new timeline
/// inherits the session's history up to the fork point.
///
/// This is a control plane message — there is no reply. The CLI confirms
/// success by querying the `DagStore` after the message is processed.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForkMessage {
    pub id: MessageId,
    pub protocol_version: String,
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub session: SessionId,
    /// Agent that owns the session being forked (needed for git tree path).
    pub agent_name: AgentName,
    /// Branch name for the new timeline (e.g., "repair-infer-3").
    pub branch_name: String,
    /// The `DagNode` to fork from.
    pub fork_point: DagNodeId,
}

impl ForkMessage {
    pub fn new(
        branch: BranchId,
        submission: SubmissionId,
        session: SessionId,
        agent_name: AgentName,
        branch_name: String,
        fork_point: DagNodeId,
    ) -> Self {
        Self {
            id: MessageId::new(),
            protocol_version: PROTOCOL_VERSION.to_string(),
            branch,
            submission,
            session,
            agent_name,
            branch_name,
            fork_point,
        }
    }
}

/// Fork payload (v2) — routing lives on `SessionRoutingKey`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForkMessageV2 {
    pub id: MessageId,
    pub branch_name: String,
    pub fork_point: DagNodeId,
}

impl ForkMessageV2 {
    pub fn new(branch_name: String, fork_point: DagNodeId) -> Self {
        Self {
            id: MessageId::new(),
            branch_name,
            fork_point,
        }
    }
}
