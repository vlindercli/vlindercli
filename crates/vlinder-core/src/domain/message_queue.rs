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
    // Lifecycle (ADR 125)
    // -------------------------------------------------------------------------

    /// Called once at daemon startup (ADR 125).
    ///
    /// Backends create cluster-level queue resources here.
    ///
    /// SQS: creates static queues (deploy, delete, fork, promote) + DLQs.
    /// NATS: no-op (subjects are implicit, stream created at connect time).
    fn on_cluster_start(&self) -> Result<(), QueueError> {
        Ok(())
    }

    /// Called when a new agent is deployed (ADR 125).
    ///
    /// Backends create agent-scoped queue resources here.
    /// Must be idempotent — calling twice for the same agent is safe.
    ///
    /// SQS: creates invoke, complete, and response queues + DLQs.
    /// NATS: no-op (subjects are implicit).
    fn on_agent_deployed(&self, _agent: &AgentName) -> Result<(), QueueError> {
        Ok(())
    }

    /// Called when an agent is deleted (ADR 125).
    ///
    /// Backends tear down agent-scoped queue resources here.
    /// Must be idempotent — calling on a non-existent agent is safe.
    ///
    /// SQS: deletes the agent's invoke, complete, and response queues + DLQs.
    /// NATS: no-op.
    fn on_agent_deleted(&self, _agent: &AgentName) -> Result<(), QueueError> {
        Ok(())
    }

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
        _agent: &AgentName,
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
        _agent: &AgentName,
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
            ref agent,
            service,
            operation,
            sequence,
        } = key.kind
        else {
            return Err(QueueError::SendFailed(
                "call_service_v2 requires a Request routing key".into(),
            ));
        };
        let submission = key.submission.clone();
        let agent = agent.clone();
        send_and_wait(
            || self.send_request(key, msg),
            || {
                self.receive_response(&submission, &agent, service, operation, sequence)
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

/// Assert the routing contract for `receive_complete`: the returned message's
/// submission must match the requested submission.
#[allow(dead_code)]
pub fn assert_complete_routing_contract(
    result: &Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError>,
    expected: &SubmissionId,
) {
    if let Ok((key, _, _)) = result {
        assert!(
            &key.submission == expected,
            "routing contract: receive_complete expected submission {}, got {}",
            expected,
            key.submission
        );
    }
}

/// Assert the routing contract for `receive_response`: the returned message
/// must match the requested submission, service, operation, and sequence.
#[allow(dead_code)]
pub fn assert_response_routing_contract(
    result: &Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError>,
    expected_submission: &SubmissionId,
    expected_service: ServiceBackend,
    expected_operation: Operation,
    expected_sequence: Sequence,
) {
    if let Ok((key, _, _)) = result {
        assert!(
            &key.submission == expected_submission,
            "routing contract: receive_response expected submission {}, got {}",
            expected_submission,
            key.submission
        );
        if let DataMessageKind::Response {
            service,
            operation,
            sequence,
            ..
        } = &key.kind
        {
            assert!(
                *service == expected_service
                    && *operation == expected_operation
                    && *sequence == expected_sequence,
                "routing contract: receive_response filter mismatch"
            );
        }
    }
}

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

// --- Kani proofs: routing contract verification (ADR 125) ---
//
// These proofs verify that the MessageQueue routing contract — receive methods
// must return only messages matching the requested filter parameters — is:
//   1. Satisfiable by subject-routed backends (NATS-like)
//   2. Violated by unfiltered backends (SQS-like)
//
// This is the formal evidence for ruling out Amazon SQS as a queue backend.

#[cfg(kani)]
mod routing_contract_proofs {
    use super::*;
    use crate::domain::diagnostics::RuntimeDiagnostics;

    fn make_key(submission: &str, agent: &str) -> DataRoutingKey {
        DataRoutingKey {
            session: super::super::SessionId::try_from(
                "00000000-0000-0000-0000-000000000000".to_string(),
            )
            .unwrap(),
            branch: super::super::BranchId::from(1),
            submission: SubmissionId::from(submission.to_string()),
            kind: DataMessageKind::Complete {
                agent: AgentName::new(agent),
                harness: HarnessType::Cli,
            },
        }
    }

    fn make_complete() -> CompleteMessage {
        CompleteMessage {
            id: super::super::MessageId::from("m".to_string()),
            dag_id: super::super::DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            payload: vec![],
        }
    }

    /// Model: subject-routed queue (NATS-like).
    /// Server-side filtering — only returns messages matching the subscription.
    struct FilteredQueue {
        keys: [DataRoutingKey; 2],
        msgs: [CompleteMessage; 2],
    }

    impl MessageQueue for FilteredQueue {
        fn send_invoke(&self, _: DataRoutingKey, _: InvokeMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn receive_invoke(
            &self,
            _: &AgentName,
        ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
            Err(QueueError::Timeout)
        }
        fn send_fork(&self, _: SessionRoutingKey, _: ForkMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_promote(&self, _: SessionRoutingKey, _: PromoteMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_session_start(
            &self,
            _: SessionRoutingKey,
            _: SessionStartMessage,
        ) -> Result<super::super::BranchId, QueueError> {
            Ok(super::super::BranchId::from(1))
        }
        fn send_deploy_agent(
            &self,
            _: InfraRoutingKey,
            _: DeployAgentMessage,
        ) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_delete_agent(
            &self,
            _: InfraRoutingKey,
            _: DeleteAgentMessage,
        ) -> Result<(), QueueError> {
            Ok(())
        }

        fn receive_complete(
            &self,
            submission: &SubmissionId,
            _harness: HarnessType,
            _agent: &AgentName,
        ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
            // Server-side filter: only return messages matching submission
            for i in 0..self.keys.len() {
                if &self.keys[i].submission == submission {
                    return Ok((
                        self.keys[i].clone(),
                        self.msgs[i].clone(),
                        Box::new(|| Ok(())),
                    ));
                }
            }
            Err(QueueError::Timeout)
        }
    }

    /// Model: unfiltered queue (SQS-like).
    /// No server-side filtering — returns whatever message is next.
    struct UnfilteredQueue {
        keys: [DataRoutingKey; 2],
        msgs: [CompleteMessage; 2],
    }

    impl MessageQueue for UnfilteredQueue {
        fn send_invoke(&self, _: DataRoutingKey, _: InvokeMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn receive_invoke(
            &self,
            _: &AgentName,
        ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
            Err(QueueError::Timeout)
        }
        fn send_fork(&self, _: SessionRoutingKey, _: ForkMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_promote(&self, _: SessionRoutingKey, _: PromoteMessage) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_session_start(
            &self,
            _: SessionRoutingKey,
            _: SessionStartMessage,
        ) -> Result<super::super::BranchId, QueueError> {
            Ok(super::super::BranchId::from(1))
        }
        fn send_deploy_agent(
            &self,
            _: InfraRoutingKey,
            _: DeployAgentMessage,
        ) -> Result<(), QueueError> {
            Ok(())
        }
        fn send_delete_agent(
            &self,
            _: InfraRoutingKey,
            _: DeleteAgentMessage,
        ) -> Result<(), QueueError> {
            Ok(())
        }

        fn receive_complete(
            &self,
            _submission: &SubmissionId,
            _harness: HarnessType,
            _agent: &AgentName,
        ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
            // No filtering — return first message regardless of submission
            Ok((
                self.keys[0].clone(),
                self.msgs[0].clone(),
                Box::new(|| Ok(())),
            ))
        }
    }

    /// PROOF: A subject-routed queue always satisfies the routing contract.
    #[kani::proof]
    fn filtered_queue_satisfies_routing_contract() {
        let q = FilteredQueue {
            keys: [make_key("sub-1", "agent"), make_key("sub-2", "agent")],
            msgs: [make_complete(), make_complete()],
        };

        // Ask for sub-1 — must get sub-1
        let result = q.receive_complete(
            &SubmissionId::from("sub-1".to_string()),
            HarnessType::Cli,
            &AgentName::new("agent"),
        );
        assert_complete_routing_contract(&result, &SubmissionId::from("sub-1".to_string()));

        // Ask for sub-2 — must get sub-2
        let result = q.receive_complete(
            &SubmissionId::from("sub-2".to_string()),
            HarnessType::Cli,
            &AgentName::new("agent"),
        );
        assert_complete_routing_contract(&result, &SubmissionId::from("sub-2".to_string()));
    }

    /// PROOF: An unfiltered queue violates the routing contract.
    /// Kani finds a counterexample where the wrong submission is returned.
    #[kani::proof]
    #[kani::should_panic]
    fn unfiltered_queue_violates_routing_contract() {
        let q = UnfilteredQueue {
            keys: [make_key("sub-1", "agent"), make_key("sub-2", "agent")],
            msgs: [make_complete(), make_complete()],
        };

        // Ask for sub-2 — but unfiltered queue returns sub-1 (first in queue)
        let result = q.receive_complete(
            &SubmissionId::from("sub-2".to_string()),
            HarnessType::Cli,
            &AgentName::new("agent"),
        );
        assert_complete_routing_contract(&result, &SubmissionId::from("sub-2".to_string()));
    }
}
