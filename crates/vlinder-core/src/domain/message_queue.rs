//! Message queue trait definition (ADR 044).
//!
//! Typed message methods for send and receive:
//! - `send_invoke()` / `receive_invoke()`: Harness → Runtime (ADR 121)
//! - `send_request()` / `receive_request()`: Runtime → Service
//! - `send_response()` / `receive_response()`: Service → Runtime
//! - `send_complete()` / `receive_complete()`: Runtime → Harness
//!
//! Each receive method returns a tuple of (`TypedMessage`, `AckFn`) where
//! `AckFn` acknowledges successful processing.

use super::{
    AgentName, CompleteMessage, DataMessageKind, DataRoutingKey, DeleteAgentMessage,
    DeployAgentMessage, ForkMessage, HarnessType, InfraRoutingKey, InvokeMessage, Operation,
    PromoteMessage, RequestMessage, ResourceId, ResponseMessage, Sequence, ServiceBackend,
    SessionRoutingKey, SessionStartMessage, SubmissionId,
};
use std::fmt;

/// One-shot closure that acknowledges a received message was processed.
pub type Acknowledgement = Box<dyn FnOnce() -> Result<(), QueueError> + Send>;

// --- MessageQueue Trait ---

/// A message queue for sending and receiving typed messages (ADR 044).
pub trait MessageQueue {
    // -------------------------------------------------------------------------
    // Invoke (ADR 121 — data plane)
    // -------------------------------------------------------------------------

    /// Send an invoke on the data plane (ADR 121).
    ///
    /// Routing key and payload are separate: the key goes into the subject,
    /// the payload goes into the NATS message body.
    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError>;

    /// Receive an invoke from the data plane (ADR 121).
    ///
    /// Returns the routing key, payload, and acknowledgement.
    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError>;

    // -------------------------------------------------------------------------
    // Complete (ADR 121 — data plane)
    // -------------------------------------------------------------------------

    /// Send a complete on the data plane (ADR 121).
    fn send_complete(&self, _key: DataRoutingKey, _msg: CompleteMessage) -> Result<(), QueueError> {
        Err(QueueError::SendFailed(
            "send_complete not implemented".into(),
        ))
    }

    /// Receive a complete from the data plane (ADR 121).
    fn receive_complete(
        &self,
        _submission: &SubmissionId,
        _harness: HarnessType,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    // -------------------------------------------------------------------------
    // Request (ADR 121 — data plane)
    // -------------------------------------------------------------------------

    /// Send a request on the data plane (ADR 121).
    fn send_request(&self, _key: DataRoutingKey, _msg: RequestMessage) -> Result<(), QueueError> {
        Err(QueueError::SendFailed(
            "send_request not implemented".into(),
        ))
    }

    /// Receive a request from the data plane (ADR 121).
    fn receive_request(
        &self,
        _service: ServiceBackend,
        _operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    // -------------------------------------------------------------------------
    // Response (ADR 121 — data plane)
    // -------------------------------------------------------------------------

    /// Send a response on the data plane (ADR 121).
    fn send_response(&self, _key: DataRoutingKey, _msg: ResponseMessage) -> Result<(), QueueError> {
        Err(QueueError::SendFailed(
            "send_response not implemented".into(),
        ))
    }

    /// Receive a response from the data plane (ADR 121).
    fn receive_response(
        &self,
        _submission: &SubmissionId,
        _service: ServiceBackend,
        _operation: Operation,
        _sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    // -------------------------------------------------------------------------
    // Session plane
    // -------------------------------------------------------------------------

    /// Send a fork on the session plane.
    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError>;

    /// Receive a fork from the session plane.
    fn receive_fork(
        &self,
    ) -> Result<(SessionRoutingKey, ForkMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    /// Send a promote on the session plane.
    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError>;

    /// Receive a promote from the session plane.
    fn receive_promote(
        &self,
    ) -> Result<(SessionRoutingKey, PromoteMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    /// Start a session on the session plane.
    fn send_session_start(
        &self,
        key: SessionRoutingKey,
        msg: SessionStartMessage,
    ) -> Result<super::BranchId, QueueError>;

    // -------------------------------------------------------------------------
    // Infra plane (ADR 121)
    // -------------------------------------------------------------------------

    /// Enqueue an agent deploy on the infra plane.
    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError>;

    /// Receive an agent deploy from the infra plane.
    fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    /// Enqueue an agent delete on the infra plane.
    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError>;

    /// Receive an agent delete from the infra plane.
    fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout)
    }

    // -------------------------------------------------------------------------
    // Request-reply facades (ADR 092)
    // -------------------------------------------------------------------------

    /// Send a service request and block until the response arrives.
    ///
    /// Send a service request on the data plane and block until the response arrives.
    ///
    /// Data-plane variant of `call_service`. Routing key carries session/branch/submission/
    /// service/operation/sequence; payload is `RequestMessageV2`.
    fn call_service(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
    ) -> Result<ResponseMessage, QueueError> {
        let DataMessageKind::Request {
            service,
            operation,
            sequence,
            ..
        } = key.kind
        else {
            return Err(QueueError::SendFailed(
                "call_service_v2 requires a Request routing key".into(),
            ));
        };
        let submission = key.submission.clone();
        send_and_wait(
            || self.send_request(key, msg),
            || {
                self.receive_response(&submission, service, operation, sequence)
                    .map(|(_key, msg, ack)| (msg, ack))
            },
        )
    }
}

// --- Request-reply internals ---

/// Send a message and poll until the correlated reply arrives (ADR 092).
///
/// Single implementation behind `call_service()`.
fn send_and_wait<T>(
    send: impl FnOnce() -> Result<(), QueueError>,
    receive: impl Fn() -> Result<(T, Acknowledgement), QueueError>,
) -> Result<T, QueueError> {
    send()?;
    loop {
        match receive() {
            Ok((reply, ack)) => {
                let _ = ack();
                return Ok(reply);
            }
            Err(QueueError::Timeout) => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(e) => return Err(e),
        }
    }
}

// --- Errors ---

#[derive(Debug)]
pub enum QueueError {
    /// Failed to send message
    SendFailed(String),
    /// Failed to receive message
    ReceiveFailed(String),
    /// Receive timed out without a message (not an error, just no work)
    Timeout,
}

impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueError::SendFailed(msg) => write!(f, "send failed: {msg}"),
            QueueError::ReceiveFailed(msg) => write!(f, "receive failed: {msg}"),
            QueueError::Timeout => write!(f, "receive timed out"),
        }
    }
}

impl std::error::Error for QueueError {}

// --- Routing ---

/// Extract the agent name from a registry-assigned `ResourceId`.
///
/// Registry IDs have the format `<registry>/agents/<name>`.
/// The last path component is the agent name, used as the NATS subject token.
pub fn agent_routing_key(agent_id: &ResourceId) -> AgentName {
    let name = if let Some(path) = agent_id.path() {
        path.rsplit('/')
            .next()
            .filter(|n| !n.is_empty())
            .unwrap_or(agent_id.as_str())
    } else {
        agent_id.as_str()
    };
    AgentName::new(name)
}
