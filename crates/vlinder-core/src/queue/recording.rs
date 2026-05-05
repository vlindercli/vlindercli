//! `RecordingQueue` — transactional outbox for synchronous DAG recording.
//!
//! Wraps any `MessageQueue` and records a `DagNode` into a `DagStore`
//! before forwarding each send. This eliminates the race condition where
//! a query sees stale state because the async NATS consumer hasn't
//! processed the latest messages yet.
//!
//! Receive and routing methods delegate straight through.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;

use crate::domain::{
    hash_dag_node, Acknowledgement, CompleteMessage, DagNodeId, DagStore, DataMessageKind,
    DataRoutingKey, DeleteAgentMessage, DeployAgentMessage, ForkMessage, InfraRoutingKey, Instance,
    InvokeMessage, MessageQueue, MessageType, PromoteMessage, QueueError, SessionMessageKind,
    SessionRoutingKey, SessionStartMessage, Snapshot, StateHash, SubmissionId,
};

#[cfg(test)]
use crate::domain::{SvcRequestDiagnostics, SvcResponseDiagnostics};

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
    async fn record_invoke(&self, key: &DataRoutingKey, msg: &InvokeMessage) -> DagNodeId {
        let branch_id = key.branch;

        let dag_parent_override = if msg.dag_parent.is_empty() {
            None
        } else {
            Some(msg.dag_parent.clone())
        };

        let parent_node = match dag_parent_override {
            Some(id) => self.store.get_node(&id).await.unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to look up dag_parent node");
                None
            }),
            None => match self.store.latest_node_on_branch(branch_id, None).await {
                Ok(Some(node)) => Some(node),
                Ok(None) => None,
                Err(e) => {
                    tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                    None
                }
            },
        };

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let payload_json = serde_json::to_vec(msg).unwrap_or_default();
        let id = hash_dag_node(
            &payload_json,
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
            .await
        {
            let DataMessageKind::Invoke { agent, .. } = &key.kind else {
                unreachable!()
            };
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                error = %e,
                "failed to record invoke node — DAG chain may have a gap"
            );
        }

        id
    }

    /// Record a DAG node for a complete message (data-plane path).
    async fn record_complete(&self, key: &DataRoutingKey, msg: &CompleteMessage) {
        let branch_id = key.branch;

        let parent_node = match self.store.latest_node_on_branch(branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            }
        };

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

        if let Err(e) = self
            .store
            .insert_complete_node(
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
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                error = %e,
                "failed to record complete node — DAG chain may have a gap"
            );
        }
    }
    /// Record a DAG node for a request message (data-plane path).
    async fn record_request(&self, key: &DataRoutingKey, msg: &crate::domain::RequestMessage) {
        let branch_id = key.branch;

        let parent_node = match self.store.latest_node_on_branch(branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            }
        };

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

        if let Err(e) = self
            .store
            .insert_request_node(
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
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                service = %service.service_type(),
                operation = %operation.as_str(),
                error = %e,
                "failed to record request node — DAG chain may have a gap"
            );
        }
    }

    /// Record a DAG node for a response message (data-plane path).
    async fn record_response(&self, key: &DataRoutingKey, msg: &crate::domain::ResponseMessage) {
        let branch_id = key.branch;

        let parent_node = match self.store.latest_node_on_branch(branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, branch = branch_id.as_i64(), "Failed to query latest node on branch");
                None
            }
        };

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

        if let Err(e) = self
            .store
            .insert_response_node(
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
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                service = %service.service_type(),
                operation = %operation.as_str(),
                error = %e,
                "failed to record response node — DAG chain may have a gap"
            );
        }
    }

    /// Record a DAG node for a harness-mediated service request (V2 path).
    ///
    /// Hashes the full message, inherits KV state from the parent,
    /// stamps `dag_id` on the message, and returns the computed `DagNodeId`.
    async fn record_svc_request(
        &self,
        key: &crate::domain::SvcRoutingKey,
        msg: &mut crate::domain::RequestV2,
    ) -> DagNodeId {
        let branch_id = key.branch;

        let parent_node = match self.store.latest_node_on_branch(branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    branch = branch_id.as_i64(),
                    "Failed to query latest node on branch"
                );
                None
            }
        };

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        // Hash the full message (fixes gap 1: was hashing only arguments)
        let payload_json = serde_json::to_vec(msg).unwrap_or_default();
        let id = hash_dag_node(
            &payload_json,
            &parent_id,
            &MessageType::SvcRequest,
            &[],
            &key.session,
        );

        // State inheritance (fixes gap 2: was using parent_state directly)
        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        let crate::domain::SvcMessageKind::SvcRequest {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("record_svc_request called with non-SvcRequest key");
            return DagNodeId::root();
        };

        // Stamp dag_id on the message (fixes gap 3)
        msg.dag_id = id.clone();

        if let Err(e) = self
            .store
            .insert_svc_request_node(
                &id,
                &parent_id,
                Utc::now(),
                &state,
                &key.session,
                &key.submission,
                key.branch,
                agent,
                service.clone(),
                operation.clone(),
                *sequence,
                msg,
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                service = %service.service_type_str(),
                operation = %operation.as_str(),
                error = %e,
                "failed to record svc_request node — DAG chain may have a gap"
            );
        }

        id
    }

    /// Record a DAG node for a harness-mediated service response (V2 path).
    ///
    /// Hashes the full message, inherits KV state from the parent,
    /// stamps `dag_id` on the message, and returns the computed `DagNodeId`.
    async fn record_svc_response(
        &self,
        key: &crate::domain::SvcRoutingKey,
        msg: &mut crate::domain::ResponseV2,
    ) -> DagNodeId {
        let branch_id = key.branch;

        let parent_node = match self.store.latest_node_on_branch(branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    branch = branch_id.as_i64(),
                    "Failed to query latest node on branch"
                );
                None
            }
        };

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        // Hash the full message (fixes gap 1: was hashing only content bytes)
        let payload_json = serde_json::to_vec(msg).unwrap_or_default();
        let id = hash_dag_node(
            &payload_json,
            &parent_id,
            &MessageType::SvcResponse,
            &[],
            &key.session,
        );

        // State inheritance (fixes gap 2: was using parent_state directly)
        let state = match &msg.state {
            Some(s) if !s.is_empty() => {
                parent_state.with_state(Instance::from("kv"), StateHash::from(s.clone()))
            }
            _ => parent_state,
        };

        let crate::domain::SvcMessageKind::SvcResponse {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("record_svc_response called with non-SvcResponse key");
            return DagNodeId::root();
        };

        // Stamp dag_id on the message (fixes gap 3)
        msg.dag_id = id.clone();

        if let Err(e) = self
            .store
            .insert_svc_response_node(
                &id,
                &parent_id,
                Utc::now(),
                &state,
                &key.session,
                &key.submission,
                key.branch,
                agent,
                service.clone(),
                operation.clone(),
                *sequence,
                msg,
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                submission = %key.submission,
                agent = %agent,
                branch = key.branch.as_i64(),
                service = %service.service_type_str(),
                operation = %operation.as_str(),
                error = %e,
                "failed to record svc_response node — DAG chain may have a gap"
            );
        }

        id
    }
}

#[async_trait]
impl MessageQueue for RecordingQueue {
    // -------------------------------------------------------------------------
    // Lifecycle — delegate to inner
    // -------------------------------------------------------------------------

    async fn on_cluster_start(&self) -> Result<(), QueueError> {
        self.inner.on_cluster_start().await
    }

    async fn on_agent_deployed(&self, agent: &crate::domain::AgentName) -> Result<(), QueueError> {
        self.inner.on_agent_deployed(agent).await
    }

    async fn on_agent_deleted(&self, agent: &crate::domain::AgentName) -> Result<(), QueueError> {
        self.inner.on_agent_deleted(agent).await
    }

    // -------------------------------------------------------------------------
    // Send methods — record DAG node, then forward
    // -------------------------------------------------------------------------

    async fn send_invoke(
        &self,
        key: DataRoutingKey,
        mut msg: InvokeMessage,
    ) -> Result<(), QueueError> {
        let dag_id = self.record_invoke(&key, &msg).await;
        msg.dag_id = dag_id;
        self.inner.send_invoke(key, msg).await
    }

    async fn receive_invoke(
        &self,
        agent: &crate::domain::AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        self.inner.receive_invoke(agent).await
    }

    async fn send_complete(
        &self,
        key: DataRoutingKey,
        msg: CompleteMessage,
    ) -> Result<(), QueueError> {
        self.record_complete(&key, &msg).await;
        self.inner.send_complete(key, msg).await
    }

    async fn send_request(
        &self,
        key: DataRoutingKey,
        msg: crate::domain::RequestMessage,
    ) -> Result<(), QueueError> {
        self.record_request(&key, &msg).await;
        self.inner.send_request(key, msg).await
    }

    async fn send_response(
        &self,
        key: DataRoutingKey,
        msg: crate::domain::ResponseMessage,
    ) -> Result<(), QueueError> {
        self.record_response(&key, &msg).await;
        self.inner.send_response(key, msg).await
    }

    // -------------------------------------------------------------------------
    // Receive methods — delegate straight through
    // -------------------------------------------------------------------------

    async fn receive_complete(
        &self,
        submission: &SubmissionId,
        harness: crate::domain::HarnessType,
        agent: &crate::domain::AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        self.inner
            .receive_complete(submission, harness, agent)
            .await
    }

    async fn receive_request(
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
        self.inner.receive_request(service, operation).await
    }

    async fn receive_response(
        &self,
        submission: &SubmissionId,
        agent: &crate::domain::AgentName,
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
            .receive_response(submission, agent, service, operation, sequence)
            .await
    }

    // -------------------------------------------------------------------------
    // V2 harness-mediated service dispatch
    // -------------------------------------------------------------------------

    async fn send_svc_request(
        &self,
        key: crate::domain::SvcRoutingKey,
        mut msg: crate::domain::RequestV2,
    ) -> Result<(), QueueError> {
        let _dag_id = self.record_svc_request(&key, &mut msg).await;
        self.inner.send_svc_request(key, msg).await
    }

    async fn receive_svc_request_mcp(
        &self,
    ) -> Result<
        (
            crate::domain::SvcRoutingKey,
            crate::domain::RequestV2,
            Acknowledgement,
        ),
        QueueError,
    > {
        self.inner.receive_svc_request_mcp().await
    }

    async fn send_svc_response(
        &self,
        key: crate::domain::SvcRoutingKey,
        msg: crate::domain::ResponseV2,
    ) -> Result<(), QueueError> {
        self.inner.send_svc_response(key, msg).await
    }

    async fn receive_svc_response(
        &self,
        key: &crate::domain::SvcRoutingKey,
    ) -> Result<
        (
            crate::domain::SvcRoutingKey,
            crate::domain::ResponseV2,
            Acknowledgement,
        ),
        QueueError,
    > {
        let (key, mut response, ack) = self.inner.receive_svc_response(key).await?;
        let _dag_id = self.record_svc_response(&key, &mut response).await;
        Ok((key, response, ack))
    }

    // -------------------------------------------------------------------------
    // Session plane
    // -------------------------------------------------------------------------

    async fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        self.record_fork(&key, &msg).await;

        // Create the branch row
        match self
            .store
            .create_branch(&msg.branch_name, &key.session, Some(&msg.fork_point))
            .await
        {
            Ok(id) => {
                tracing::info!(
                    branch_id = id.as_i64(),
                    branch = %msg.branch_name,
                    "Created branch on fork"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    branch = %msg.branch_name,
                    "Failed to create branch on fork"
                );
            }
        }

        // Forward to inner (fire-and-forget)
        let _ = self.inner.send_fork(key, msg).await;
        Ok(())
    }

    async fn send_promote(
        &self,
        key: SessionRoutingKey,
        msg: PromoteMessage,
    ) -> Result<(), QueueError> {
        self.record_promote(&key, &msg).await;

        // Promote: seal old main, rename promoted branch to "main"
        let branch_to_promote = self.store.get_branch(msg.branch_id).await.ok().flatten();

        let old_main = self
            .store
            .get_branch_by_name("main")
            .await
            .ok()
            .flatten()
            .filter(|b| b.session_id == key.session);

        if let Some(old) = old_main {
            let sealed_name = format!("broken-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
            if let Err(e) = self.store.seal_branch(old.id, chrono::Utc::now()).await {
                tracing::warn!(error = %e, branch = old.id.as_i64(), "Failed to seal old main");
            }
            if let Err(e) = self.store.rename_branch(old.id, &sealed_name).await {
                tracing::warn!(error = %e, branch = old.id.as_i64(), "Failed to rename old main");
            }
        }

        if let Some(promoted) = branch_to_promote {
            if let Err(e) = self.store.rename_branch(promoted.id, "main").await {
                tracing::warn!(error = %e, "Failed to rename promoted branch to main");
            }
            if let Err(e) = self
                .store
                .update_session_default_branch(&key.session, promoted.id)
                .await
            {
                tracing::warn!(error = %e, "Failed to update session default branch");
            }
        }

        let _ = self.inner.send_promote(key, msg).await;
        Ok(())
    }

    async fn send_session_start(
        &self,
        key: SessionRoutingKey,
        msg: SessionStartMessage,
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
        if let Err(e) = self.store.create_session(&session).await {
            tracing::warn!(error = %e, "Failed to persist session");
        }
        let default_branch = self
            .store
            .create_branch("main", &key.session, None)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "Failed to create default branch");
                placeholder_branch
            });
        if let Err(e) = self
            .store
            .update_session_default_branch(&key.session, default_branch)
            .await
        {
            tracing::warn!(error = %e, "Failed to update session default branch");
        }

        let _ = self.inner.send_session_start(key, msg).await;
        Ok(default_branch)
    }

    async fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        self.record_deploy_agent(&key, &msg).await;
        let _ = self.inner.send_deploy_agent(key, msg).await;
        Ok(())
    }

    async fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        self.record_delete_agent(&key, &msg).await;
        let _ = self.inner.send_delete_agent(key, msg).await;
        Ok(())
    }

    async fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        self.inner.receive_deploy_agent().await
    }

    async fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        self.inner.receive_delete_agent().await
    }

    async fn receive_any(&self) -> Result<(String, Vec<u8>, Acknowledgement), QueueError> {
        self.inner.receive_any().await
    }
}

impl RecordingQueue {
    /// Record a fork DAG node.
    async fn record_fork(&self, key: &SessionRoutingKey, msg: &ForkMessage) {
        let SessionMessageKind::Fork { .. } = &key.kind else {
            return;
        };

        // Fork's parent is the fork point itself
        let parent_node = self
            .store
            .get_node(&msg.fork_point)
            .await
            .unwrap_or_else(|e| {
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

        if let Err(e) = self
            .store
            .insert_fork_node(&id, &parent_id, Utc::now(), &parent_state, key, msg)
            .await
        {
            tracing::warn!(
                dag_id = %id,
                session = %key.session,
                submission = %key.submission,
                fork_point = %msg.fork_point,
                error = %e,
                "failed to record fork node — DAG chain may have a gap"
            );
        }
    }

    /// Record a promote DAG node.
    async fn record_promote(&self, key: &SessionRoutingKey, msg: &PromoteMessage) {
        // Promote's parent is the latest node on the branch being promoted
        let parent_node = match self.store.latest_node_on_branch(msg.branch_id, None).await {
            Ok(Some(node)) => Some(node),
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to query latest node for promote");
                None
            }
        };

        let parent_id = parent_node
            .as_ref()
            .map_or_else(DagNodeId::root, |n| n.id.clone());
        let parent_state = parent_node
            .as_ref()
            .map(|n| &n.state)
            .cloned()
            .unwrap_or_else(Snapshot::empty);

        let id = hash_dag_node(&[], &parent_id, &MessageType::Promote, &[], &key.session);

        if let Err(e) = self
            .store
            .insert_promote_node(&id, &parent_id, Utc::now(), &parent_state, key, msg)
            .await
        {
            tracing::warn!(
                dag_id = %id,
                session = %key.session,
                submission = %key.submission,
                branch = msg.branch_id.as_i64(),
                error = %e,
                "failed to record promote node — DAG chain may have a gap"
            );
        }
    }

    /// Record a deploy-agent DAG node.
    async fn record_deploy_agent(&self, key: &InfraRoutingKey, msg: &DeployAgentMessage) {
        // HACK: hash_dag_node requires a SessionId, but infra nodes are cluster-scoped
        // and have no session. Using a synthetic zero UUID as a placeholder. The real fix
        // is to make session_id optional in hash_dag_node.
        let synthetic_session =
            crate::domain::SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string())
                .unwrap();
        let id = hash_dag_node(
            msg.manifest.name.as_bytes(),
            &DagNodeId::root(),
            &MessageType::DeployAgent,
            &[],
            &synthetic_session,
        );

        if let Err(e) = self
            .store
            .insert_deploy_agent_node(
                &id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                key,
                msg,
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                agent = %msg.manifest.name,
                submission = %key.submission,
                error = %e,
                "failed to record deploy-agent node — DAG chain may have a gap"
            );
        }
    }

    /// Record a delete-agent DAG node.
    async fn record_delete_agent(&self, key: &InfraRoutingKey, msg: &DeleteAgentMessage) {
        // HACK: same synthetic session as record_deploy_agent — see comment above.
        let synthetic_session =
            crate::domain::SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string())
                .unwrap();
        let id = hash_dag_node(
            msg.agent.as_str().as_bytes(),
            &DagNodeId::root(),
            &MessageType::DeleteAgent,
            &[],
            &synthetic_session,
        );

        if let Err(e) = self
            .store
            .insert_delete_agent_node(
                &id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                key,
                msg,
            )
            .await
        {
            tracing::warn!(
                dag_id = %id,
                agent = %msg.agent,
                submission = %key.submission,
                error = %e,
                "failed to record delete-agent node — DAG chain may have a gap"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        AgentName, BranchId, DagNode, DataMessageKind, DataRoutingKey, HarnessType,
        InMemoryDagStore, InvokeDiagnostics, InvokeMessage, Message, MessageId, MessageType,
        RuntimeDiagnostics, RuntimeType, SessionId, SubmissionId,
    };
    use crate::queue::InMemoryQueue;

    async fn test_store() -> Arc<dyn DagStore> {
        let store = Arc::new(InMemoryDagStore::new());
        // Seed "main" branch (id=1) — mirrors production setup.
        store
            .create_branch("main", &test_session(), None)
            .await
            .unwrap();
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
            history: vec![],
            current_input: vec![Message::User {
                content: "hello".to_string(),
            }],
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
            content: None,
            tool_calls: None,
            payload: b"done".to_vec(),
        };
        (key, msg)
    }

    #[tokio::test]
    async fn send_invoke_records_dag_node() {
        let store = test_store().await;
        let queue = test_queue(Arc::clone(&store));

        let (key, msg) = test_invoke();
        let sid = key.session.clone();

        queue.send_invoke(key, msg).await.unwrap();

        let nodes = store.get_session_nodes(&sid).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].message_type(), MessageType::Invoke);
        assert_eq!(nodes[0].parent_id, DagNodeId::root());
    }

    #[tokio::test]
    async fn send_complete_records_dag_node() {
        let store = test_store().await;
        let queue = test_queue(Arc::clone(&store));

        let (key, msg) = test_complete();
        let sid = key.session.clone();

        queue.send_complete(key, msg).await.unwrap();

        let nodes = store.get_session_nodes(&sid).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].message_type(), MessageType::Complete);
    }

    #[tokio::test]
    async fn same_timeline_chains_across_sessions() {
        let store = test_store().await;
        let queue = test_queue(Arc::clone(&store));

        // Two sessions on the same timeline chain sequentially via the
        // shared timeline head pointer.
        let ses_aaa =
            SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();
        let ses_bbb =
            SessionId::try_from("7897b0a7-937b-4457-87c3-07c4cab30c55".to_string()).unwrap();

        let (mut key1, mut msg1) = test_invoke();
        key1.session = ses_aaa.clone();
        msg1.current_input = vec![Message::User {
            content: "hello-aaa".to_string(),
        }];

        let (mut key2, mut msg2) = test_invoke();
        key2.session = ses_bbb.clone();
        msg2.current_input = vec![Message::User {
            content: "hello-bbb".to_string(),
        }];

        queue.send_invoke(key1, msg1).await.unwrap();
        queue.send_invoke(key2, msg2).await.unwrap();

        let nodes1 = store.get_session_nodes(&ses_aaa).await.unwrap();
        let nodes2 = store.get_session_nodes(&ses_bbb).await.unwrap();

        assert_eq!(nodes1.len(), 1);
        assert_eq!(nodes2.len(), 1);
        // First invoke is root, second chains off first via timeline head
        assert_eq!(nodes1[0].parent_id, DagNodeId::root());
        assert_eq!(nodes2[0].parent_id, nodes1[0].id);
    }

    #[tokio::test]
    async fn receive_methods_delegate_through() {
        let store = test_store().await;
        let inner = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(
            Arc::clone(&inner) as Arc<dyn MessageQueue + Send + Sync>,
            store,
        );

        // Send a message through the inner queue's trait method
        let (key, msg) = test_invoke();
        inner.send_invoke(key, msg).await.unwrap();

        // Receive through the recording queue — should delegate to inner
        let result = queue.receive_invoke(&test_agent_id()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn dag_store_error_does_not_block_send() {
        // Use a store that always fails on insert
        struct FailStore;
        #[async_trait]
        impl DagStore for FailStore {
            async fn get_node(
                &self,
                _: &crate::domain::DagNodeId,
            ) -> Result<Option<DagNode>, String> {
                Ok(None)
            }
            async fn get_session_nodes(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            async fn get_children(
                &self,
                _: &crate::domain::DagNodeId,
            ) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            async fn create_branch(
                &self,
                _: &str,
                _: &crate::domain::SessionId,
                _: Option<&crate::domain::DagNodeId>,
            ) -> Result<crate::domain::BranchId, String> {
                Ok(crate::domain::BranchId::from(0))
            }
            async fn get_branch_by_name(
                &self,
                _: &str,
            ) -> Result<Option<crate::domain::Branch>, String> {
                Ok(None)
            }
            async fn get_branch(
                &self,
                _: crate::domain::BranchId,
            ) -> Result<Option<crate::domain::Branch>, String> {
                Ok(None)
            }
            async fn list_sessions(&self) -> Result<Vec<crate::domain::SessionSummary>, String> {
                Ok(vec![])
            }
            async fn get_nodes_by_submission(&self, _: &str) -> Result<Vec<DagNode>, String> {
                Ok(vec![])
            }
            async fn get_branches_for_session(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Vec<crate::domain::Branch>, String> {
                Ok(vec![])
            }
            async fn latest_node_on_branch(
                &self,
                _: crate::domain::BranchId,
                _: Option<crate::domain::MessageType>,
            ) -> Result<Option<crate::domain::DagNode>, String> {
                Ok(None)
            }
            async fn create_session(&self, _: &crate::domain::Session) -> Result<(), String> {
                Ok(())
            }
            async fn get_session(
                &self,
                _: &crate::domain::SessionId,
            ) -> Result<Option<crate::domain::Session>, String> {
                Ok(None)
            }
            async fn rename_branch(
                &self,
                _: crate::domain::BranchId,
                _: &str,
            ) -> Result<(), String> {
                Ok(())
            }
            async fn seal_branch(
                &self,
                _: crate::domain::BranchId,
                _: chrono::DateTime<chrono::Utc>,
            ) -> Result<(), String> {
                Ok(())
            }
            async fn update_session_default_branch(
                &self,
                _: &crate::domain::SessionId,
                _: crate::domain::BranchId,
            ) -> Result<(), String> {
                Ok(())
            }
            async fn get_session_by_name(
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
        let result = queue.send_invoke(key, msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn invoke_with_dag_parent_overrides_chain() {
        let store = test_store().await;
        let queue = test_queue(Arc::clone(&store));

        // Send a normal invoke first to populate the chain
        let (key1, msg1) = test_invoke();
        let sid = key1.session.clone();
        queue.send_invoke(key1, msg1).await.unwrap();

        let nodes = store.get_session_nodes(&sid).await.unwrap();
        assert_eq!(nodes.len(), 1);
        let first_id = nodes[0].id.clone();

        // Send a second invoke (same session) — normally chains off first
        let (key2, mut msg2) = test_invoke();
        msg2.current_input = vec![Message::User {
            content: "second".to_string(),
        }];
        queue.send_invoke(key2, msg2).await.unwrap();

        let nodes = store.get_session_nodes(&sid).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].parent_id, first_id, "normal chaining");

        // Send a third invoke with explicit dag_parent pointing to first node,
        // bypassing the chain cache (which would point to second node)
        let (key3, mut msg3) = test_invoke();
        msg3.current_input = vec![Message::User {
            content: "forked".to_string(),
        }];
        msg3.dag_parent = first_id.clone();
        queue.send_invoke(key3, msg3).await.unwrap();

        let nodes = store.get_session_nodes(&sid).await.unwrap();
        assert_eq!(nodes.len(), 3);
        assert_eq!(
            nodes[2].parent_id, first_id,
            "dag_parent should override chain cache"
        );
    }

    #[tokio::test]
    async fn send_svc_request_dag_id_is_non_root() {
        let store = test_store().await;
        let inner: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(Arc::clone(&inner), Arc::clone(&store));

        let session = test_session();
        let submission = test_submission();
        let agent = test_agent_id();
        let branch = BranchId::from(1);
        let service = crate::domain::ServiceBackendV2::Mcp("brave".to_string());
        let operation = crate::domain::ServiceOperation::new("echo");
        let sequence = crate::domain::Sequence::first();

        let svc_key = crate::domain::SvcRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: crate::domain::SvcMessageKind::SvcRequest {
                agent: agent.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence,
            },
        };
        let request_msg = crate::domain::RequestV2 {
            id: crate::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: crate::domain::ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({})).unwrap(),
        };

        queue.send_svc_request(svc_key, request_msg).await.unwrap();

        let node = store.get_session_nodes(&session).await.unwrap();
        assert_eq!(node.len(), 1);
        assert!(!node[0].id.is_empty(), "dag_id should be a non-empty hash");
        assert!(
            node[0].id.as_str().len() > 10,
            "dag_id should be a full SHA-256 hash, got: {}",
            node[0].id
        );
    }

    #[tokio::test]
    async fn send_svc_request_and_receive_svc_response_record_nodes() {
        let store = test_store().await;
        let inner: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(Arc::clone(&inner), Arc::clone(&store));

        let session = test_session();
        let submission = test_submission();
        let agent = test_agent_id();
        let branch = BranchId::from(1);
        let service = crate::domain::ServiceBackendV2::Mcp("brave".to_string());
        let operation = crate::domain::ServiceOperation::new("echo");
        let sequence = crate::domain::Sequence::first();

        let svc_key = crate::domain::SvcRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: crate::domain::SvcMessageKind::SvcRequest {
                agent: agent.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence,
            },
        };
        let request_msg = crate::domain::RequestV2 {
            id: crate::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: crate::domain::ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
        };

        // send_svc_request — should record node and forward
        queue
            .send_svc_request(svc_key.clone(), request_msg)
            .await
            .unwrap();

        let nodes = store.get_session_nodes(&session).await.unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].message_type(), MessageType::SvcRequest);
        assert_eq!(nodes[0].protocol_version(), "v1");

        // Now send a svc_response into the inner queue and receive it via RecordingQueue
        let response_key = crate::domain::SvcRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: crate::domain::SvcMessageKind::SvcResponse {
                agent: agent.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence,
            },
        };
        let response_msg = crate::domain::ResponseV2 {
            id: crate::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: crate::domain::MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"result".to_vec(),
        };

        // Put response into inner queue first
        inner
            .send_svc_response(response_key.clone(), response_msg)
            .await
            .unwrap();

        // receive_svc_response — should receive from inner and record node
        let result = queue.receive_svc_response(&response_key).await;
        assert!(result.is_ok());

        let nodes = store.get_session_nodes(&session).await.unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].message_type(), MessageType::SvcResponse);
        assert_eq!(nodes[1].protocol_version(), "v1");
    }

    #[tokio::test]
    async fn send_svc_response_and_receive_svc_request_mcp_delegate_no_recording() {
        let store = test_store().await;
        let inner: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let queue = RecordingQueue::new(Arc::clone(&inner), Arc::clone(&store));

        let session = test_session();
        let submission = test_submission();
        let agent = test_agent_id();
        let branch = BranchId::from(1);
        let service = crate::domain::ServiceBackendV2::Mcp("brave".to_string());
        let operation = crate::domain::ServiceOperation::new("echo");
        let sequence = crate::domain::Sequence::first();

        // send_svc_response should delegate without recording
        let response_key = crate::domain::SvcRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: crate::domain::SvcMessageKind::SvcResponse {
                agent: agent.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence,
            },
        };
        let response_msg = crate::domain::ResponseV2 {
            id: crate::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: crate::domain::MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"result".to_vec(),
        };

        queue
            .send_svc_response(response_key, response_msg)
            .await
            .unwrap();

        // No nodes should be recorded
        let nodes = store.get_session_nodes(&session).await.unwrap();
        assert!(nodes.is_empty(), "send_svc_response should not record");

        // Put a request into inner and verify receive_svc_request_mcp delegates
        let request_key = crate::domain::SvcRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: crate::domain::SvcMessageKind::SvcRequest {
                agent: agent.clone(),
                service: service.clone(),
                operation: operation.clone(),
                sequence,
            },
        };
        let request_msg = crate::domain::RequestV2 {
            id: crate::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            tool_call_id: crate::domain::ToolCallId::new(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
        };
        inner
            .send_svc_request(request_key, request_msg)
            .await
            .unwrap();

        let result = queue.receive_svc_request_mcp().await;
        assert!(result.is_ok());

        // Still no nodes recorded
        let nodes = store.get_session_nodes(&session).await.unwrap();
        assert!(
            nodes.is_empty(),
            "receive_svc_request_mcp should not record"
        );
    }
}
