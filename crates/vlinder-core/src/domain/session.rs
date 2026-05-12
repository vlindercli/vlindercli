//! Session — conversation state for multi-turn interactions (ADR 054).
//!
//! A session groups multiple submissions into a conversation. History is
//! derived from the DAG — the session itself only tracks identity.

use serde::{Deserialize, Serialize};

use chrono::{DateTime, Utc};

use super::{BranchId, ExternalSessionId, SessionId};

/// A conversation session — container for branches.
///
/// A session groups branches into a single conversation. The
/// `default_branch` points to the canonical branch (typically "main").
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Session {
    /// Session identifier (UUID).
    pub id: SessionId,
    /// External user-supplied identifier.
    pub external_id: ExternalSessionId,
    /// Human-friendly name (petname, unique).
    pub name: String,
    /// Agent name this session is with.
    pub agent: String,
    /// The canonical branch for this session.
    pub default_branch: BranchId,
    /// When this session was created.
    pub created_at: DateTime<Utc>,
}

impl Session {
    /// Create a new session with a generated petname.
    pub fn new(
        id: SessionId,
        external_id: ExternalSessionId,
        agent: impl Into<String>,
        default_branch: BranchId,
    ) -> Self {
        let name = id.petname();
        let agent = agent.into();
        Self {
            id,
            external_id,
            name,
            agent,
            default_branch,
            created_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_session() {
        let external_id = ExternalSessionId::new("ticket-123").unwrap();
        let session = Session::new(
            SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap(),
            external_id.clone(),
            "pensieve",
            BranchId::from(1),
        );

        assert_eq!(session.agent, "pensieve");
        assert_eq!(session.external_id, external_id);
        assert_eq!(session.id.as_str(), "d4761d76-dee4-4ebf-9df4-43b52efa4f78");
        assert_eq!(session.default_branch, BranchId::from(1));
    }
}
