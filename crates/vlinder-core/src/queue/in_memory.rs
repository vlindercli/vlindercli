//! In-memory queue implementation.

#[cfg(test)]
use crate::domain::InvokeDiagnostics;
use crate::domain::{
    Acknowledgement, AgentName, CompleteMessage, DataMessageKind, DataPlane, DataRoutingKey,
    ForkMessage, HarnessType, InvokeMessage, MessageQueue, ObservableMessage, Operation,
    QueueError, RepairMessage, RequestMessage, ResponseMessage, RoutingKey, RoutingKind, Sequence,
    ServiceBackend, SubmissionId,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

/// In-memory message queue for single-process use.
///
/// Messages are keyed by `RoutingKey` (ADR 096) — collision-freedom is
/// structural, not dependent on string formatting.
///
/// ACK/NACK operations are no-ops since messages are removed from the queue
/// immediately on receive (no durability or redelivery support).
pub struct InMemoryQueue {
    /// Data-plane messages keyed by `DataRoutingKey` (ADR 121).
    data_queues: Arc<Mutex<HashMap<DataRoutingKey, VecDeque<DataPlane>>>>,
    /// Legacy messages keyed by `RoutingKey` (ADR 096 §5).
    pub(crate) typed_queues: Arc<Mutex<HashMap<RoutingKey, VecDeque<ObservableMessage>>>>,
}

impl InMemoryQueue {
    pub fn new() -> Self {
        Self {
            data_queues: Arc::new(Mutex::new(HashMap::new())),
            typed_queues: Arc::new(Mutex::new(HashMap::new())),
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
        let v2 = DataPlane::Invoke {
            key: key.clone(),
            msg,
        };
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        data.entry(key).or_default().push_back(v2);
        Ok(())
    }

    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in data.iter_mut() {
            let DataMessageKind::Invoke { agent: a, .. } = &key.kind else {
                continue;
            };
            if a == agent {
                if let Some(DataPlane::Invoke { msg, .. }) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let v2 = DataPlane::Complete {
            key: key.clone(),
            msg,
        };
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        data.entry(key).or_default().push_back(v2);
        Ok(())
    }

    fn receive_complete(
        &self,
        submission: &SubmissionId,
        _harness: HarnessType,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in data.iter_mut() {
            if key.submission != *submission {
                continue;
            }
            let DataMessageKind::Complete { .. } = &key.kind else {
                continue;
            };
            if let Some(DataPlane::Complete { msg, .. }) = queue.pop_front() {
                let key = key.clone();
                return Ok((key, msg, Box::new(|| Ok(()))));
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        let v2 = DataPlane::Request {
            key: key.clone(),
            msg,
        };
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        data.entry(key).or_default().push_back(v2);
        Ok(())
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in data.iter_mut() {
            let DataMessageKind::Request {
                service: s,
                operation: o,
                ..
            } = &key.kind
            else {
                continue;
            };
            if *s == service && *o == operation {
                if let Some(DataPlane::Request { msg, .. }) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        let v2 = DataPlane::Response {
            key: key.clone(),
            msg,
        };
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::SendFailed(format!("lock poisoned: {e}")))?;
        data.entry(key).or_default().push_back(v2);
        Ok(())
    }

    fn receive_response(
        &self,
        submission: &SubmissionId,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let mut data = self
            .data_queues
            .lock()
            .map_err(|e| QueueError::ReceiveFailed(format!("lock poisoned: {e}")))?;

        for (key, queue) in data.iter_mut() {
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
                if let Some(DataPlane::Response { msg, .. }) = queue.pop_front() {
                    let key = key.clone();
                    return Ok((key, msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_repair(&self, msg: RepairMessage) -> Result<(), QueueError> {
        let key = msg.routing_key();
        let mut typed = self.typed_queues.lock().unwrap();
        typed
            .entry(key)
            .or_default()
            .push_back(ObservableMessage::Repair(msg));
        Ok(())
    }

    fn receive_repair(
        &self,
        agent: &AgentName,
    ) -> Result<(RepairMessage, Acknowledgement), QueueError> {
        let mut typed = self.typed_queues.lock().unwrap();

        for (key, queue) in typed.iter_mut() {
            let matches = match key {
                RoutingKey {
                    kind: RoutingKind::Repair { agent: ref a, .. },
                    ..
                } => a == agent,
                _ => false,
            };
            if matches {
                if let Some(ObservableMessage::Repair(msg)) = queue.front() {
                    let msg = msg.clone();
                    queue.pop_front();
                    return Ok((msg, Box::new(|| Ok(()))));
                }
            }
        }

        Err(QueueError::Timeout)
    }

    fn send_fork(&self, _msg: ForkMessage) -> Result<(), QueueError> {
        // Fork is fire-and-forget — no consumer subscribes.
        // RecordingQueue intercepts and records the DagNode before this is called.
        Ok(())
    }

    fn send_promote(&self, _msg: crate::domain::PromoteMessage) -> Result<(), QueueError> {
        // Promote is fire-and-forget — no consumer subscribes.
        // RecordingQueue intercepts and records the DagNode before this is called.
        Ok(())
    }

    fn send_session_start(
        &self,
        _msg: crate::domain::SessionStartMessage,
    ) -> Result<crate::domain::BranchId, QueueError> {
        // InMemoryQueue doesn't have a store — return a placeholder.
        // RecordingQueue wraps this and returns the real branch ID.
        Ok(crate::domain::BranchId::from(1))
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
