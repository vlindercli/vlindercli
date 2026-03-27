//! `ObservableMessage` — unified enum wrapping all typed messages.
//!
//! `ObservableMessageHeaders` carries all message metadata without the payload.
//! Transports deserialize their wire format into headers; the domain assembles
//! the final `ObservableMessage` by attaching the payload via `assemble()`.

use super::super::dag::MessageType;
use super::super::routing_key::{RoutingKey, RoutingKind};
use super::fork::ForkMessage;
use super::identity::{BranchId, DagNodeId, MessageId, SessionId, SubmissionId};
use super::promote::PromoteMessage;

/// Unified message type wrapping all typed messages.
///
/// Used for polymorphic handling when the specific message type isn't known
/// at compile time (e.g., receiving from a queue).
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ObservableMessage {
    Fork(ForkMessage),
    Promote(PromoteMessage),
}

/// Message metadata without payload.
///
/// Transports produce this by deserializing their wire format (NATS headers,
/// SQS attributes, etc.) into typed domain fields. The domain then assembles
/// the final `ObservableMessage` by attaching the payload.
///
/// Common fields (`id`, `protocol_version`, `state`) live on the struct.
/// Routing dimensions (session, branch, submission + variant-specific)
/// live on the `RoutingKey`. Variant-specific extras (diagnostics,
/// checkpoint, etc.) live on `MessageDetails`.
#[derive(Debug)]
pub struct ObservableMessageHeaders {
    pub id: MessageId,
    pub protocol_version: String,
    pub state: Option<String>,
    pub routing_key: RoutingKey,
    pub details: MessageDetails,
}

/// Variant-specific message metadata not carried by the routing key.
#[derive(Debug)]
pub enum MessageDetails {
    Fork {
        branch_name: String,
        fork_point: DagNodeId,
    },
    Promote,
}

impl ObservableMessageHeaders {
    /// Assemble a full `ObservableMessage` by attaching a payload.
    pub fn assemble(self, _payload: Vec<u8>) -> ObservableMessage {
        let id = self.id;
        let protocol_version = self.protocol_version;
        let session = self.routing_key.session;
        let branch = self.routing_key.branch;
        let submission = self.routing_key.submission;

        match (self.routing_key.kind, self.details) {
            (
                RoutingKind::Fork { agent_name },
                MessageDetails::Fork {
                    branch_name,
                    fork_point,
                },
            ) => ObservableMessage::Fork(ForkMessage {
                id,
                protocol_version,
                branch,
                submission,
                session,
                agent_name,
                branch_name,
                fork_point,
            }),
            (RoutingKind::Promote { agent_name }, MessageDetails::Promote) => {
                ObservableMessage::Promote(PromoteMessage {
                    id,
                    protocol_version,
                    branch,
                    submission,
                    session,
                    agent_name,
                })
            }
            _ => unreachable!("routing key kind and message details must match"),
        }
    }
}

impl ObservableMessage {
    pub fn message_type(&self) -> MessageType {
        match self {
            ObservableMessage::Fork(_) => MessageType::Fork,
            ObservableMessage::Promote(_) => MessageType::Promote,
        }
    }

    pub fn protocol_version(&self) -> &str {
        match self {
            ObservableMessage::Fork(m) => &m.protocol_version,
            ObservableMessage::Promote(m) => &m.protocol_version,
        }
    }

    pub fn id(&self) -> &MessageId {
        match self {
            ObservableMessage::Fork(m) => &m.id,
            ObservableMessage::Promote(m) => &m.id,
        }
    }

    pub fn submission(&self) -> &SubmissionId {
        match self {
            ObservableMessage::Fork(m) => &m.submission,
            ObservableMessage::Promote(m) => &m.submission,
        }
    }

    pub fn branch(&self) -> &BranchId {
        match self {
            ObservableMessage::Fork(m) => &m.branch,
            ObservableMessage::Promote(m) => &m.branch,
        }
    }

    pub fn session(&self) -> &SessionId {
        match self {
            ObservableMessage::Fork(m) => &m.session,
            ObservableMessage::Promote(m) => &m.session,
        }
    }

    /// Get the payload as raw bytes.
    pub fn payload(&self) -> &[u8] {
        match self {
            ObservableMessage::Fork(_) | ObservableMessage::Promote(_) => &[],
        }
    }

    /// Restore payload from a separate storage column.
    ///
    /// Needed because payload is `#[serde(skip)]` on some message types,
    /// so it's lost during JSON round-trip through `message_blob`.
    pub fn set_payload(&mut self, _payload: Vec<u8>) {
        match self {
            ObservableMessage::Fork(_) | ObservableMessage::Promote(_) => {}
        }
    }

    /// Extract (sender, receiver) routing pair.
    pub fn sender_receiver(&self) -> (String, String) {
        match self {
            ObservableMessage::Fork(m) => ("platform".to_string(), m.agent_name.to_string()),
            ObservableMessage::Promote(m) => ("platform".to_string(), m.agent_name.to_string()),
        }
    }

    /// State hash (ADR 055).
    pub fn state(&self) -> Option<&str> {
        match self {
            ObservableMessage::Fork(_) | ObservableMessage::Promote(_) => None,
        }
    }

    /// Checkpoint handler name (ADR 111).
    pub fn checkpoint(&self) -> Option<&str> {
        None
    }

    /// Operation name (ADR 113).
    pub fn operation(&self) -> Option<&str> {
        None
    }

    /// Serialize diagnostics to JSON bytes.
    pub fn diagnostics_json(&self) -> Vec<u8> {
        Vec::new()
    }

    /// Raw stderr from container execution.
    pub fn stderr(&self) -> &[u8] {
        &[]
    }
}

impl From<ForkMessage> for ObservableMessage {
    fn from(msg: ForkMessage) -> Self {
        ObservableMessage::Fork(msg)
    }
}

impl From<PromoteMessage> for ObservableMessage {
    fn from(msg: PromoteMessage) -> Self {
        ObservableMessage::Promote(msg)
    }
}
