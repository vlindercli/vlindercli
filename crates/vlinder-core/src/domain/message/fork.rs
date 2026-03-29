//! `ForkMessageV`: CLI → Platform (create a timeline fork).
//!
//! A control plane message that creates a new timeline branch in the DAG.
//! Both projections (SQL `DagStore` and git repo) react to this message:
//! - SQL: creates a Timeline row with `parent_id` and `fork_point`
//! - Git: creates a branch and updates timeline index files
//!
//! Unlike service messages, `ForkMessageV` carries no payload — the fork point
//! hash and branch name are all that's needed to define the topology change.

use super::identity::{DagNodeId, MessageId};

/// Fork payload — routing lives on `SessionRoutingKey`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForkMessage {
    pub id: MessageId,
    pub branch_name: String,
    pub fork_point: DagNodeId,
}

impl ForkMessage {
    pub fn new(branch_name: String, fork_point: DagNodeId) -> Self {
        Self {
            id: MessageId::new(),
            branch_name,
            fork_point,
        }
    }
}
