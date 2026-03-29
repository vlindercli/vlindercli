//! In-memory queue implementation.

#[cfg(test)]
use crate::domain::InvokeDiagnostics;
use crate::domain::{
    Acknowledgement, AgentName, BranchId, CompleteMessage, DataMessageKind, DataRoutingKey,
    ForkMessage, HarnessType, InvokeMessage, MessageQueue, Operation, PromoteMessage, QueueError,
    RequestMessage, ResponseMessage, Sequence, ServiceBackend, SessionRoutingKey,
    SessionStartMessage, SubmissionId,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// In-memory message queue for single-process use.
///
/// Each message type has its own typed queue keyed by `DataRoutingKey`.
/// No wrapper enum — the routing key discriminates the type via `DataMessageKind`.
///
/// ACK/NACK operations are no-ops since messages are removed from the queue
/// immediately on receive (no durability or redelivery support).
pub struct InMemoryQueue {
    invokes: Arc<Mutex<HashMap<DataRoutingKey, VecDeque<InvokeMessage>>>>,
    completes: Arc<Mutex<HashMap<DataRoutingKey, VecDeque<CompleteMessage>>>>,
    requests: Arc<Mutex<HashMap<DataRoutingKey, VecDeque<RequestMessage>>>>,
    responses: Arc<Mutex<HashMap<DataRoutingKey, VecDeque<ResponseMessage>>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            invokes: Arc::new(Mutex::new(HashMap::new())),
            completes: Arc::new(Mutex::new(HashMap::new())),
            requests: Arc::new(Mutex::new(HashMap::new())),
            responses: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for InMemoryQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageQueue for InMemoryQueue {
    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let mut q = self
            .invokes
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        q.entry(key).or_default().push_back(msg);
        Ok(())
    }

    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let mut q = self
            .invokes
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in q.iter_mut() {
            let DataMessageKind::Invoke { agent: a, .. } = &key.kind else {
                continue;
            };
            if a == agent {
                if let Some(msg) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let mut q = self
            .completes
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        q.entry(key).or_default().push_back(msg);
        Ok(())
    }

    fn receive_complete(
        &self,
        submission: &SubmissionId,
        _harness: HarnessType,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let mut q = self
            .completes
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in q.iter_mut() {
            if key.submission != *submission {
                continue;
            }
            let DataMessageKind::Complete { .. } = &key.kind else {
                continue;
            };
            if let Some(msg) = queue.pop_front() {
                let key = key.clone();
                return Ok((key, msg, Box::new(|| Ok(()))));
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        let mut q = self
            .requests
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        q.entry(key).or_default().push_back(msg);
        Ok(())
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let mut q = self
            .requests
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in q.iter_mut() {
            let DataMessageKind::Request {
                service: s,
                operation: o,
                ..
            } = &key.kind
            else {
                continue;
            };
            if *s == service && *o == operation {
                if let Some(msg) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        let mut q = self
            .responses
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        q.entry(key).or_default().push_back(msg);
        Ok(())
    }

    fn receive_response(
        &self,
        submission: &SubmissionId,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let mut q = self
            .responses
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in q.iter_mut() {
            if key.submission != *submission {
                continue;
            }
            let DataMessageKind::Response {
                service: s,
                operation: o,
                sequence: sq,
                ..
            } = &key.kind
            else {
                continue;
            };
            if *s == service && *o == operation && *sq == sequence {
                if let Some(msg) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_fork(&self, _key: SessionRoutingKey, _msg: ForkMessage) -> Result<(), QueueError> {
        // Fire-and-forget — no consumer subscribes.
        Ok(())
    }

    fn send_promote(
        &self,
        _key: SessionRoutingKey,
        _msg: PromoteMessage,
    ) -> Result<(), QueueError> {
        // Fire-and-forget — no consumer subscribes.
        Ok(())
    }

    fn send_session_start(
        &self,
        _key: SessionRoutingKey,
        _msg: SessionStartMessage,
    ) -> Result<BranchId, QueueError> {
        // InMemoryQueue doesn't have a store — return a placeholder.
        // RecordingQueue wraps this and returns the real branch ID.
        Ok(BranchId::from(1))
    }
}

// ============================================================================
// Internal helpers
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::RuntimeType;
    use crate::domain::{BranchId, DagNodeId, MessageId, SessionId, SubmissionId};

    fn test_agent_id() -> AgentName {
        AgentName::new("echo-agent")
    }

    fn test_submission() -> SubmissionId {
        SubmissionId::from("sub-test-123".to_string())
    }

    // ========================================================================
    // Typed receive tests
    // ========================================================================

    #[test]
    fn receive_invoke_returns_typed_message() {
        let queue = InMemoryQueue::new();

        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: test_submission(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: test_agent_id(),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::from("msg-invoke-1".to_string()),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: String::new(),
            },
            dag_parent: DagNodeId::root(),
            payload: b"hello".to_vec(),
        };
        let original_id = msg.id.clone();

        queue.send_invoke(key, msg).unwrap();

        // Receive typed message
        let (recv_key, received, ack) = queue.receive_invoke(&test_agent_id()).unwrap();

        assert_eq!(received.id, original_id);
        assert!(matches!(
            recv_key.kind,
            DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                ..
            }
        ));
        assert_eq!(received.payload, b"hello");

        ack().unwrap();
    }

    #[test]
    fn receive_invoke_preserves_all_dimensions() {
        let queue = InMemoryQueue::new();

        let submission = test_submission();
        let agent_id = test_agent_id();

        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: submission.clone(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Web,
                runtime: RuntimeType::Container,
                agent: agent_id.clone(),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::from("msg-invoke-2".to_string()),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: String::new(),
            },
            dag_parent: DagNodeId::root(),
            payload: b"input".to_vec(),
        };

        queue.send_invoke(key, msg).unwrap();

        let (recv_key, _, _) = queue.receive_invoke(&test_agent_id()).unwrap();

        // All dimensions preserved for reply construction
        assert_eq!(recv_key.submission, submission);
        assert!(matches!(
            recv_key.kind,
            DataMessageKind::Invoke { ref agent, harness: HarnessType::Web, .. } if *agent == agent_id
        ));
    }

    // ========================================================================
    // Invoke tests (ADR 121 — data plane)
    // ========================================================================

    fn test_data_routing_key() -> DataRoutingKey {
        DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: test_submission(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: test_agent_id(),
            },
        }
    }

    fn test_invoke() -> InvokeMessage {
        InvokeMessage {
            id: crate::domain::MessageId::from("msg-1".to_string()),
            dag_id: crate::domain::DagNodeId::root(),
            state: Some("state-abc".to_string()),
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: crate::domain::DagNodeId::root(),
            payload: b"hello".to_vec(),
        }
    }

    #[test]
    fn send_and_receive_invoke() {
        let queue = InMemoryQueue::new();
        let key = test_data_routing_key();
        let msg = test_invoke();

        queue.send_invoke(key, msg.clone()).unwrap();

        let (recv_key, recv_msg, ack) = queue.receive_invoke(&test_agent_id()).unwrap();

        assert_eq!(recv_msg, msg);
        assert_eq!(recv_key.submission, test_submission());
        ack().unwrap();
    }

    #[test]
    fn receive_invoke_times_out_for_wrong_agent() {
        let queue = InMemoryQueue::new();
        let key = test_data_routing_key();
        let msg = test_invoke();

        queue.send_invoke(key, msg).unwrap();

        let result = queue.receive_invoke(&AgentName::new("other-agent"));
        assert!(matches!(result, Err(QueueError::Timeout)));
    }

    #[test]
    fn multiple_invoke_messages_delivered_in_order() {
        let queue = InMemoryQueue::new();

        let key = test_data_routing_key();
        let msg1 = InvokeMessage {
            id: crate::domain::MessageId::from("msg-first".to_string()),
            dag_id: crate::domain::DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: String::new(),
            },
            dag_parent: crate::domain::DagNodeId::root(),
            payload: b"first".to_vec(),
        };
        let msg2 = InvokeMessage {
            id: crate::domain::MessageId::from("msg-second".to_string()),
            dag_id: crate::domain::DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: String::new(),
            },
            dag_parent: crate::domain::DagNodeId::root(),
            payload: b"second".to_vec(),
        };

        queue.send_invoke(key.clone(), msg1.clone()).unwrap();
        queue.send_invoke(key, msg2.clone()).unwrap();

        let (_, recv1, _) = queue.receive_invoke(&test_agent_id()).unwrap();
        assert_eq!(recv1, msg1);

        let (_, recv2, _) = queue.receive_invoke(&test_agent_id()).unwrap();
        assert_eq!(recv2, msg2);
    }
}
