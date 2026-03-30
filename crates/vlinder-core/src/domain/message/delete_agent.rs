//! `DeleteAgentMessage`: CLI → Platform (remove an agent).
//!
//! Self-sufficient payload — carries all context needed to act on the message
//! without inspecting the routing key or NATS subject.

use super::identity::MessageId;
use crate::domain::AgentName;

/// Delete agent payload.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeleteAgentMessage {
    pub id: MessageId,
    pub agent: AgentName,
}

impl DeleteAgentMessage {
    pub fn new(agent: AgentName) -> Self {
        Self {
            id: MessageId::new(),
            agent,
        }
    }
}
