//! `RecordingQueue` — transactional outbox for synchronous DAG recording.
//!
//! Wraps any `MessageQueue` and records a `DagNode` into a `DagStore`
//! before forwarding each send. This eliminates the race condition where
//! a query sees stale state because the async NATS consumer hasn't
//! processed the latest messages yet.
//!
//! Receive and routing methods delegate straight through.

use std::sync::Arc;

use chrono::Utc;

use crate::domain::{
    hash_dag_node, Acknowledgement, CompleteMessage, DagNodeId, DagStore, DataRoutingKey,
    ForkMessageV2, Instance, InvokeMessage, MessageQueue, MessageType, PromoteMessageV2,
    QueueError, SessionMessageKind, SessionRoutingKey, SessionStartMessageV2, Snapshot, StateHash,
    SubmissionId,
};

/// A `MessageQueue` decorator that synchronously records DAG nodes on send.
///
/// Every `send_*` call: clone → convert to `ObservableMessage` → build
/// `DagNode` with Merkle chaining → insert into `DagStore` → forward
/// original message to inner queue.
///
/// `DagStore` write failures are logged but don't block message sending.
///
/// Merkle chain parent resolution uses the timeline `head` pointer stored
/// in the database, ensuring that multiple `RecordingQueue` instances (sidecar
/// + daemon) share a single source of truth for chaining (issue #37).
pub struct RecordingQueue {
    inner: Arc<dyn MessageQueue + Send + Sync>,
    store: Arc<dyn DagStore>,
}

impl RecordingQueue {
    pub fn new(inner: Arc<dyn MessageQueue + Send + Sync>, store: Arc<dyn DagStore>) -> Self {
        Self { inner, store }
    }

    /// Record a DAG node for an invoke message. Returns the computed `DagNodeId`.
    fn record_invoke(&self, key: &DataRoutingKey, msg: &InvokeMessage) -> DagNodeId {
        let branch_id = key.branch;

        let dag_parent_override = if msg.dag_parent.is_empty() {
            None
        } else {
            Some(msg.dag_parent.clone())
        };

        let parent_node = dag_parent_override
            .and_then(|id| {
                self.store.get_node(&id).unwrap_or_else(|e| {
                    tracing::warn!(error = %e, "Failed to look up dag_parent node");
                    None
                })
            })
            .or_else(|| {
                self.store
                    .latest_node_on_branch(branch_id, None)
                    .unwrap_or_else(|e| {
                        tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                        None
                    })
            });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let id = hash_dag_node(
            &msg.payload,
            &parent_id,
            &MessageType::Invoke,
            &diagnostics_json,
            &key.session,
        );

        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        if let Err(e) = self
            .store
            .insert_invoke_node(&id, &parent_id, Utc::now(), &state, key, msg)
        {
            tracing::warn!(error = %id, "Failed to record invoke node: {e}");
        }

        id
    }

    /// Record a DAG node for a complete message (data-plane path).
    fn record_complete(&self, key: &DataRoutingKey, msg: &CompleteMessage) {
        let branch_id = key.branch;

        let parent_node = self
            .store
            .latest_node_on_branch(branch_id, None)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let id = hash_dag_node(
            &msg.payload,
            &parent_id,
            &MessageType::Complete,
            &diagnostics_json,
            &key.session,
        );

        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        let crate::domain::DataMessageKind::Complete { agent, harness } = &key.kind else {
            tracing::error!("record_complete called with non-Complete key");
            return;
        };

        if let Err(e) = self.store.insert_complete_node(
            &id,
            &parent_id,
            Utc::now(),
            &state,
            &key.session,
            &key.submission,
            key.branch,
            agent,
            *harness,
            msg,
        ) {
            tracing::warn!(error = %id, "Failed to record complete node: {e}");
        }
    }
    /// Record a DAG node for a request message (data-plane path).
    fn record_request(&self, key: &DataRoutingKey, msg: &crate::domain::RequestMessage) {
        let branch_id = key.branch;

        let parent_node = self
            .store
            .latest_node_on_branch(branch_id, None)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let id = hash_dag_node(
            &msg.payload,
            &parent_id,
            &MessageType::Request,
            &diagnostics_json,
            &key.session,
        );

        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        let crate::domain::DataMessageKind::Request {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("record_request called with non-Request key");
            return;
        };

        if let Err(e) = self.store.insert_request_node(
            &id,
            &parent_id,
            Utc::now(),
            &state,
            &key.session,
            &key.submission,
            key.branch,
            agent,
            *service,
            *operation,
            *sequence,
            msg,
        ) {
            tracing::warn!(error = %id, "Failed to record request node: {e}");
        }
    }

    /// Record a DAG node for a response message (data-plane path).
    fn record_response(&self, key: &DataRoutingKey, msg: &crate::domain::ResponseMessage) {
        let branch_id = key.branch;

        let parent_node = self
            .store
            .latest_node_on_branch(branch_id, None)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let id = hash_dag_node(
            &msg.payload,
            &parent_id,
            &MessageType::Response,
            &diagnostics_json,
            &key.session,
        );

        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        let crate::domain::DataMessageKind::Response {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("record_response called with non-Response key");
            return;
        };

        if let Err(e) = self.store.insert_response_node(
            &id,
            &parent_id,
            Utc::now(),
            &state,
            &key.session,
            &key.submission,
            key.branch,
            agent,
            *service,
            *operation,
            *sequence,
            msg,
        ) {
            tracing::warn!(error = %id, "Failed to record response node: {e}");
        }
    }
}

impl MessageQueue for RecordingQueue {
    // -------------------------------------------------------------------------
    // Send methods — record DAG node, then forward
    // -------------------------------------------------------------------------

    fn send_invoke(&self, key: DataRoutingKey, mut msg: InvokeMessage) -> Result<(), QueueError> {
        let dag_id = self.record_invoke(&key, &msg);
        msg.dag_id = dag_id;
        self.inner.send_invoke(key, msg)
    }

    fn receive_invoke(
        &self,
        agent: &crate::domain::AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        self.inner.receive_invoke(agent)
    }

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        // Record to typed table before forwarding
        self.record_complete(&key, &msg);
        self.inner.send_complete(key, msg)
    }

    fn send_request(
        &self,
        key: DataRoutingKey,
        msg: crate::domain::RequestMessage,
    ) -> Result<(), QueueError> {
        self.record_request(&key, &msg);
        self.inner.send_request(key, msg)
    }

    fn send_response(
        &self,
        key: DataRoutingKey,
        msg: crate::domain::ResponseMessage,
    ) -> Result<(), QueueError> {
        self.record_response(&key, &msg);
        self.inner.send_response(key, msg)
    }

    // -------------------------------------------------------------------------
    // Receive methods — delegate straight through
    // -------------------------------------------------------------------------

    fn receive_complete(
        &self,
        submission: &SubmissionId,
        harness: crate::domain::HarnessType,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        self.inner.receive_complete(submission, harness)
    }

    fn receive_request(
        &self,
        service: crate::domain::ServiceBackend,
        operation: crate::domain::Operation,
    ) -> Result<
        (
            DataRoutingKey,
            crate::domain::RequestMessage,
            Acknowledgement,
        ),
        QueueError,
    > {
        self.inner.receive_request(service, operation)
    }

    fn receive_response(
        &self,
        submission: &SubmissionId,
        service: crate::domain::ServiceBackend,
        operation: crate::domain::Operation,
        sequence: crate::domain::Sequence,
    ) -> Result<
        (
            DataRoutingKey,
            crate::domain::ResponseMessage,
            Acknowledgement,
        ),
        QueueError,
    > {
        self.inner
            .receive_response(submission, service, operation, sequence)
    }

    // -------------------------------------------------------------------------
    // Session plane
    // -------------------------------------------------------------------------

    fn send_fork_v2(&self, key: SessionRoutingKey, msg: ForkMessageV2) -> Result<(), QueueError> {
        self.record_fork(&key, &msg);

        // Create the branch row
        match self
            .store
            .create_branch(&msg.branch_name, &key.session, Some(&msg.fork_point))
        {
            Ok(id) => {
                tracing::info!(
                    branch_id = id.as_i64(),
                    branch = %msg.branch_name,
                    "Created branch on fork (v2)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    branch = %msg.branch_name,
                    "Failed to create branch on fork (v2)"
                );
            }
        }

        // Forward to inner (fire-and-forget)
        let _ = self.inner.send_fork_v2(key, msg);
        Ok(())
    }

    fn send_promote_v2(
        &self,
        key: SessionRoutingKey,
        msg: PromoteMessageV2,
    ) -> Result<(), QueueError> {
        self.record_promote(&key, &msg);

        // Promote: seal old main, rename promoted branch to "main"
        let branch_to_promote = self.store.get_branch(msg.branch_id).ok().flatten();

        let old_main = self
            .store
            .get_branch_by_name("main")
            .ok()
            .flatten()
            .filter(|b| b.session_id == key.session);

        if let Some(old) = old_main {
            let sealed_name = format!("broken-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
            if let Err(e) = self.store.seal_branch(old.id, chrono::Utc::now()) {
                tracing::warn!(error = %e, branch = old.id.as_i64(), "Failed to seal old main");
            }
            if let Err(e) = self.store.rename_branch(old.id, &sealed_name) {
                tracing::warn!(error = %e, branch = old.id.as_i64(), "Failed to rename old main");
            }
        }

        if let Some(promoted) = branch_to_promote {
            if let Err(e) = self.store.rename_branch(promoted.id, "main") {
                tracing::warn!(error = %e, "Failed to rename promoted branch to main");
            }
            if let Err(e) = self
                .store
                .update_session_default_branch(&key.session, promoted.id)
            {
                tracing::warn!(error = %e, "Failed to update session default branch");
            }
        }

        let _ = self.inner.send_promote_v2(key, msg);
        Ok(())
    }

    fn send_session_start_v2(
        &self,
        key: SessionRoutingKey,
        msg: SessionStartMessageV2,
    ) -> Result<crate::domain::BranchId, QueueError> {
        let SessionMessageKind::Start { agent_name } = &key.kind else {
            return Err(QueueError::SendFailed("expected Start kind".into()));
        };

        // Create session first (branches FK to sessions), then default branch.
        let placeholder_branch = crate::domain::BranchId::from(1);
        let session = crate::domain::Session::new(
            key.session.clone(),
            agent_name.as_str(),
            placeholder_branch,
        );
        if let Err(e) = self.store.create_session(&session) {
            tracing::warn!(error = %e, "Failed to persist session (v2)");
        }
        let default_branch = self
            .store
            .create_branch("main", &key.session, None)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to create default branch (v2)");
                placeholder_branch
            });
        if let Err(e) = self
            .store
            .update_session_default_branch(&key.session, default_branch)
        {
            tracing::warn!(error = %e, "Failed to update session default branch (v2)");
        }

        let _ = self.inner.send_session_start_v2(key, msg);
        Ok(default_branch)
    }
}

impl RecordingQueue {
    /// Record a fork DAG node.
    fn record_fork(&self, key: &SessionRoutingKey, msg: &ForkMessageV2) {
        let SessionMessageKind::Fork { .. } = &key.kind else {
            return;
        };

        // Fork's parent is the fork point itself
        let parent_node = self.store.get_node(&msg.fork_point).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to look up fork point node");
            None
        });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let id = hash_dag_node(&[], &parent_id, &MessageType::Fork, &[], &key.session);

        if let Err(e) =
            self.store
                .insert_fork_node(&id, &parent_id, Utc::now(), &parent_state, key, msg)
        {
            tracing::warn!(error = %id, "Failed to record fork node: {e}");
        }
    }

    /// Record a promote DAG node.
    fn record_promote(&self, key: &SessionRoutingKey, msg: &PromoteMessageV2) {
        // Promote's parent is the latest node on the branch being promoted
        let parent_node = self
            .store
            .latest_node_on_branch(msg.branch_id, None)
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to query latest node for promote");
                None
            });

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let id = hash_dag_node(&[], &parent_id, &MessageType::Promote, &[], &key.session);

        if let Err(e) =
            self.store
                .insert_promote_node(&id, &parent_id, Utc::now(), &parent_state, key, msg)
        {
            tracing::warn!(error = %id, "Failed to record promote node: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        AgentName, BranchId, DagNode, DataMessageKind, DataRoutingKey, HarnessType,
        InMemoryDagStore, InvokeDiagnostics, InvokeMessage, MessageId, MessageType,
        RuntimeDiagnostics, RuntimeType, SessionId, SubmissionId,
    };
    use crate::queue::InMemoryQueue;

    fn test_store() -> Arc<dyn DagStore> {
        let store = Arc::new(InMemoryDagStore::new());
        // Seed "main" branch (id=1) — mirrors production setup.
        store.create_branch("main", &test_session(), None).unwrap();
        store
    }

    fn test_queue(store: Arc<dyn DagStore>) -> RecordingQueue {
        let inner: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        RecordingQueue::new(inner, store)
    }

    fn test_session() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }

    fn test_submission() -> SubmissionId {
        SubmissionId::from("sub-test-001".to_string())
    }

    fn test_agent_id() -> AgentName {
        AgentName::new("echo")
    }

    fn test_invoke() -> (DataRoutingKey, InvokeMessage) {
        let key = DataRoutingKey {
            session: test_session(),
            branch: BranchId::from(1),
            submission: test_submission(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: test_agent_id(),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            payload: b"hello".to_vec(),
        };
        (key, msg)
    }

    fn test_complete() -> (DataRoutingKey, CompleteMessage) {
        let key = DataRoutingKey {
            session: test_session(),
            branch: BranchId::from(1),
            submission: test_submission(),
            kind: crate::domain::DataMessageKind::Complete {
                agent: test_agent_id(),
                harness: HarnessType::Cli,
            },
        };
        let msg = CompleteMessage {
            id: crate::domain::MessageId::new(),
            dag_id: crate::domain::DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            payload: b"done".to_vec(),
        };
        (key, msg)
    }

    #[test]
    fn send_invoke_records_dag_node() {
        let store = test_store();
        let queue = test_queue(Arc::clone(&store));

        let (key, msg) = test_invoke();
        let sid = key.session.clone();

        queue.send_invoke(key, msg).unwrap();

        let nodes = store.get_session_nodes(&sid).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].message_type(), MessageType::Invoke);
        assert_eq!(nodes[0].parent_id, DagNodeId::root());
    }

    #[test]
    fn send_complete_records_dag_node() {
        let store = test_store();
        let queue = test_queue(Arc::clone(&store));

        let (key, msg) = test_complete();
        let sid = key.session.clone();

        queue.send_complete(key, msg).unwrap();

        let nodes = store.get_session_nodes(&sid).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].message_type(), MessageType::Complete);
    }

    #[test]
    fn same_timeline_chains_across_sessions() {
        let store = test_store();
        let queue = test_queue(Arc::clone(&store));

        // Two sessions on the same timeline chain sequentially via the
        // shared timeline head pointer.
        let ses_aaa =
            SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();
        let ses_bbb =
            SessionId::try_from("7897b0a7-937b-4457-87c3-07c4cab30c55".to_string()).unwrap();

        let (mut key1, mut msg1) = test_invoke();
        key1.session = ses_aaa.clone();
        msg1.payload = b"hello-aaa".to_vec();

        let (mut key2, mut msg2) = test_invoke();
        key2.session = ses_bbb.clone();
        msg2.payload = b"hello-bbb".to_vec();

        queue.send_invoke(key1, msg1).unwrap();
        queue.send_invoke(key2, msg2).unwrap();

        let nodes1 = store.get_session_nodes(&ses_aaa).unwrap();
        let nodes2 = store.get_session_nodes(&ses_bbb).unwrap();

        assert_eq!(nodes1.len(), 1);
        assert_eq!(nodes2.len(), 1);
        // First invoke is root, second chains off first via timeline head
        assert_eq!(nodes1[0].parent_id, DagNodeId::root());
        assert_eq!(nodes2[0].parent_id, nodes1[0].id);
    }

    #[test]
    fn receive_methods_delegate_through() {
        let store = test_store();
        let inner = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(
            Arc::clone(&inner) as Arc<dyn MessageQueue + Send + Sync>,
            store,
        );

        // Send a message through the inner queue's trait method
        let (key, msg) = test_invoke();
        inner.send_invoke(key, msg).unwrap();

        // Receive through the recording queue — should delegate to inner
        let result = queue.receive_invoke(&test_agent_id());
        assert!(result.is_ok());
    }

    #[test]
    fn dag_store_error_does_not_block_send() {
        // Use a store that always fails on insert
        struct FailStore;
        impl DagStore for FailStore {
            fn insert_node(&self, _: &DagNode) -> Result<(), String> {
                Err("simulated failure".to_string())
            }
            fn get_node(&self, _: &crate::domain::DagNodeId) -> Result<Option<DagNode>, String> {
                Ok(None)
            }
            fn get_session_nodes(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            fn get_children(&self, _: &crate::domain::DagNodeId) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            fn create_branch(
                &self,
                _: &str,
                _: &crate::domain::SessionId,
                _: Option<&crate::domain::DagNodeId>,
            ) -> Result<crate::domain::BranchId, String> {
                Ok(crate::domain::BranchId::from(0))
            }
            fn get_branch_by_name(&self, _: &str) -> Result<Option<crate::domain::Branch>, String> {
                Ok(None)
            }
            fn get_branch(
                &self,
                _: crate::domain::BranchId,
            ) -> Result<Option<crate::domain::Branch>, String> {
                Ok(None)
            }
            fn list_sessions(&self) -> Result<Vec<crate::domain::SessionSummary>, String> {
                Ok(vec![])
            }
            fn get_nodes_by_submission(&self, _: &str) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            fn get_branches_for_session(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Vec<crate::domain::Branch>, String> {
                Ok(vec![])
            }
            fn latest_node_on_branch(
                &self,
                _: crate::domain::BranchId,
                _: Option<crate::domain::MessageType>,
            ) -> Result<Option<crate::domain::DagNode>, String> {
                Ok(None)
            }
            fn create_session(&self, _: &crate::domain::Session) -> Result<(), String> {
                Ok(())
            }
            fn get_session(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Option<crate::domain::Session>, String> {
                Ok(None)
            }
            fn rename_branch(&self, _: crate::domain::BranchId, _: &str) -> Result<(), String> {
                Ok(())
            }
            fn seal_branch(
                &self,
                _: crate::domain::BranchId,
                _: chrono::DateTime<chrono::Utc>,
            ) -> Result<(), String> {
                Ok(())
            }
            fn update_session_default_branch(
                &self,
                _: &crate::domain::SessionId,
                _: crate::domain::BranchId,
            ) -> Result<(), String> {
                Ok(())
            }
            fn get_session_by_name(
                &self,
                _: &str,
            ) -> Result<Option<crate::domain::Session>, String> {
                Ok(None)
            }
        }

        let store: Arc<dyn DagStore> = Arc::new(FailStore);
        let inner: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(inner, store);

        // Send should still succeed despite store failure
        let (key, msg) = test_invoke();
        let result = queue.send_invoke(key, msg);
        assert!(result.is_ok());
    }

    #[test]
    fn invoke_with_dag_parent_overrides_chain() {
        let store = test_store();
        let queue = test_queue(Arc::clone(&store));

        // Send a normal invoke first to populate the chain
        let (key1, msg1) = test_invoke();
        let sid = key1.session.clone();
        queue.send_invoke(key1, msg1).unwrap();

        let nodes = store.get_session_nodes(&sid).unwrap();
        assert_eq!(nodes.len(), 1);
        let first_id = nodes[0].id.clone();

        // Send a second invoke (same session) — normally chains off first
        let (key2, mut msg2) = test_invoke();
        msg2.payload = b"second".to_vec();
        queue.send_invoke(key2, msg2).unwrap();

        let nodes = store.get_session_nodes(&sid).unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].parent_id, first_id, "normal chaining");

        // Send a third invoke with explicit dag_parent pointing to first node,
        // bypassing the chain cache (which would point to second node)
        let (key3, mut msg3) = test_invoke();
        msg3.payload = b"forked".to_vec();
        msg3.dag_parent = first_id.clone();
        queue.send_invoke(key3, msg3).unwrap();

        let nodes = store.get_session_nodes(&sid).unwrap();
        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes[2].parent_id, first_id,
            "dag_parent should override chain cache"
        );
    }
}
