//! `PromoteMessage`: CLI → Platform (promote a branch to main).
//!
//! A control plane message that makes a branch the canonical "main"
//! branch for its session. Both projections (SQL `DagStore` and git repo) react:
//! - SQL: renames old main to `broken-{date}`, sets `broken_at`;
//!   renames promoted branch to "main"
//! - Git: updates refs accordingly

use super::identity::{BranchId, MessageId};

/// Promote payload — routing lives on `SessionRoutingKey`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PromoteMessage {
    pub id: MessageId,
    pub branch_id: BranchId,
}

impl PromoteMessage {
    pub fn new(branch_id: BranchId) -> Self {
        Self {
            id: MessageId::new(),
            branch_id,
        }
    }
}
