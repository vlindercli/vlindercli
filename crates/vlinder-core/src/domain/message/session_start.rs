//! `SessionStartMessageV2`: CLI → Platform (create a conversation session).
//!
//! A control plane message that creates a new session in the `DagStore`.
//! The `RecordingQueue` persists the session row when it processes this
//! message, satisfying the FK constraint before any `dag_nodes` are inserted.
//!
//! This is fire-and-forget — there is no reply.

use super::identity::MessageId;

/// Session start payload (v2) — routing lives on `SessionRoutingKey`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SessionStartMessageV2 {
    pub id: MessageId,
}

impl Default for SessionStartMessageV2 {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStartMessageV2 {
    pub fn new() -> Self {
        Self {
            id: MessageId::new(),
        }
    }
}
