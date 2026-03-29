//! `SessionStartMessage`: CLI → Platform (create a conversation session).
//!
//! A control plane message that creates a new session in the `DagStore`.
//! The `RecordingQueue` persists the session row when it processes this
//! message, satisfying the FK constraint before any `dag_nodes` are inserted.
//!
//! This is fire-and-forget — there is no reply.

use super::identity::MessageId;

/// Session start payload — routing lives on `SessionRoutingKey`.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SessionStartMessage {
    pub id: MessageId,
}

impl Default for SessionStartMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionStartMessage {
    pub fn new() -> Self {
        Self {
            id: MessageId::new(),
        }
    }
}
