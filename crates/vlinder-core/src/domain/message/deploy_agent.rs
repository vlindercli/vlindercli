//! `DeployAgentMessage`: CLI → Platform (register and deploy an agent).
//!
//! Self-sufficient payload — carries all context needed to act on the message
//! without inspecting the routing key or NATS subject.

use super::identity::MessageId;
use crate::domain::AgentManifest;

/// Deploy agent payload.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DeployAgentMessage {
    pub id: MessageId,
    pub manifest: AgentManifest,
}

impl DeployAgentMessage {
    pub fn new(manifest: AgentManifest) -> Self {
        Self {
            id: MessageId::new(),
            manifest,
        }
    }
}
