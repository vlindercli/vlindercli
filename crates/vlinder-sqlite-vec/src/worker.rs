//! SQLite-vec worker — receives vector storage requests from the queue,
//! opens `SqliteVectorStorage` per agent, and sends responses back.
//!
//! Spawns one tokio task per operation (`Store`/`Search`/`Delete`) and
//! drives each until shutdown. Each task owns its own queue slot so a
//! long poll on one operation cannot starve another.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use vlinder_core::domain::Registry;
use vlinder_core::domain::{
    Acknowledgement, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue,
    Operation, QueueError, RequestMessage, ResponseMessage, ServiceBackend, ServiceDiagnostics,
};

use crate::storage::SqliteVectorStorage;
use crate::types::{SqliteVecDeleteRequest, SqliteVecSearchRequest, SqliteVecStoreRequest};

// ============================================================================
// Helpers
// ============================================================================

fn extract_agent(key: &DataRoutingKey) -> &str {
    match &key.kind {
        DataMessageKind::Request { agent, .. } => agent.as_str(),
        _ => "",
    }
}

fn response_key_from_request(req_key: &DataRoutingKey) -> DataRoutingKey {
    let DataMessageKind::Request {
        agent,
        service,
        operation,
        sequence,
    } = &req_key.kind
    else {
        panic!("response_key_from_request called with non-Request key");
    };
    DataRoutingKey {
        session: req_key.session.clone(),
        branch: req_key.branch,
        submission: req_key.submission.clone(),
        kind: DataMessageKind::Response {
            agent: agent.clone(),
            service: *service,
            operation: *operation,
            sequence: *sequence,
        },
    }
}

// ============================================================================
// Worker
// ============================================================================

pub struct SqliteVecWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    stores: RwLock<HashMap<String, Arc<SqliteVectorStorage>>>,
    service: ServiceBackend,
}

impl SqliteVecWorker {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        registry: Arc<dyn Registry>,
        service: ServiceBackend,
    ) -> Self {
        Self {
            queue,
            registry,
            stores: RwLock::new(HashMap::new()),
            service,
        }
    }

    /// Spawn one tokio task per operation this worker serves and drive
    /// each until `shutdown` fires. Three tasks, all on `self.service`:
    /// `Store`, `Search`, `Delete`.
    ///
    /// Each task loops: `select!` on shutdown vs. `receive_request`; once a
    /// request resolves, processing runs outside the select so the
    /// receive-then-ack path cannot be cancelled mid-flight.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut set: JoinSet<()> = JoinSet::new();

        for op in [Operation::Store, Operation::Search, Operation::Delete] {
            let me = Arc::clone(&self);
            let shutdown = shutdown.clone();
            set.spawn(async move {
                let service = me.service;
                loop {
                    let (key, msg, ack) = tokio::select! {
                        () = shutdown.cancelled() => break,
                        result = me.queue.receive_request(service, op) => {
                            match result {
                                Ok(tuple) => tuple,
                                Err(QueueError::Timeout) => continue,
                                Err(e) => {
                                    tracing::warn!(error = %e, operation = ?op, "sqlite-vec worker receive error");
                                    continue;
                                }
                            }
                        }
                    };
                    match op {
                        Operation::Store => me.process_store_request(key, msg, ack).await,
                        Operation::Search => me.process_search_request(key, msg, ack).await,
                        Operation::Delete => me.process_delete_request(key, msg, ack).await,
                        _ => unreachable!("sqlite-vec worker only spawns Store/Search/Delete tasks"),
                    }
                }
            });
        }

        while set.join_next().await.is_some() {}
    }

    pub async fn process_store_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
        ack: Acknowledgement,
    ) {
        let agent = extract_agent(&key);
        let start = std::time::Instant::now();
        let response_payload = self.handle_store(agent, &msg.payload).await;
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let diag = ServiceDiagnostics::storage(
            self.service.service_type(),
            self.service.backend_str(),
            Operation::Store,
            response_payload.len() as u64,
            duration_ms,
        );
        let response_key = response_key_from_request(&key);
        let response = ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: msg.id,
            state: msg.state,
            diagnostics: diag,
            payload: response_payload,
            status_code: 200,
            checkpoint: msg.checkpoint,
        };
        let _ = self.queue.send_response(response_key, response).await;
        let _ = ack().await;
    }

    pub async fn process_search_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
        ack: Acknowledgement,
    ) {
        let agent = extract_agent(&key);
        let start = std::time::Instant::now();
        let response_payload = self.handle_search(agent, &msg.payload).await;
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let diag = ServiceDiagnostics::storage(
            self.service.service_type(),
            self.service.backend_str(),
            Operation::Search,
            response_payload.len() as u64,
            duration_ms,
        );
        let response_key = response_key_from_request(&key);
        let response = ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: msg.id,
            state: msg.state,
            diagnostics: diag,
            payload: response_payload,
            status_code: 200,
            checkpoint: msg.checkpoint,
        };
        let _ = self.queue.send_response(response_key, response).await;
        let _ = ack().await;
    }

    pub async fn process_delete_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
        ack: Acknowledgement,
    ) {
        let agent = extract_agent(&key);
        let start = std::time::Instant::now();
        let response_payload = self.handle_delete(agent, &msg.payload).await;
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        let diag = ServiceDiagnostics::storage(
            self.service.service_type(),
            self.service.backend_str(),
            Operation::Delete,
            response_payload.len() as u64,
            duration_ms,
        );
        let response_key = response_key_from_request(&key);
        let response = ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            correlation_id: msg.id,
            state: msg.state,
            diagnostics: diag,
            payload: response_payload,
            status_code: 200,
            checkpoint: msg.checkpoint,
        };
        let _ = self.queue.send_response(response_key, response).await;
        let _ = ack().await;
    }

    /// Get storage for an agent, opening lazily if needed.
    async fn get_or_open(&self, agent_id: &str) -> Result<Arc<SqliteVectorStorage>, String> {
        if let Some(storage) = self.stores.read().unwrap().get(agent_id) {
            return Ok(storage.clone());
        }

        let agent = self
            .registry
            .get_agent_by_name(agent_id)
            .await
            .ok_or_else(|| format!("unknown agent: {agent_id}"))?;
        let uri = agent
            .vector_storage
            .ok_or_else(|| format!("agent has no vector_storage declared: {agent_id}"))?;
        let path = uri
            .path()
            .ok_or_else(|| format!("vector_storage URI has no path: {}", uri.as_str()))?;

        let storage = Arc::new(SqliteVectorStorage::open_at(std::path::Path::new(path))?);
        self.stores
            .write()
            .expect("stores lock poisoned")
            .insert(agent_id.to_string(), storage.clone());
        Ok(storage)
    }

    async fn handle_store(&self, agent_id: &str, payload: &[u8]) -> Vec<u8> {
        let req: SqliteVecStoreRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        let store = match self.get_or_open(agent_id).await {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.store_embedding(&req.key, &req.vector, &req.metadata) {
            Ok(()) => b"ok".to_vec(),
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }

    async fn handle_search(&self, agent_id: &str, payload: &[u8]) -> Vec<u8> {
        let req: SqliteVecSearchRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        let store = match self.get_or_open(agent_id).await {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.search_by_vector(&req.vector, req.limit) {
            Ok(results) => {
                let formatted: Vec<serde_json::Value> = results
                    .iter()
                    .map(|(key, metadata, distance)| {
                        serde_json::json!({
                            "key": key,
                            "metadata": metadata,
                            "distance": distance
                        })
                    })
                    .collect();
                serde_json::to_string(&formatted).map_or_else(
                    |e| format!("[error] {e}").into_bytes(),
                    std::string::String::into_bytes,
                )
            }
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }

    async fn handle_delete(&self, agent_id: &str, payload: &[u8]) -> Vec<u8> {
        let req: SqliteVecDeleteRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        let store = match self.get_or_open(agent_id).await {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.delete_embedding(&req.key) {
            Ok(true) => b"ok".to_vec(),
            Ok(false) => b"not_found".to_vec(),
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::InMemoryRegistry;
    use vlinder_core::domain::InMemorySecretStore;
    use vlinder_core::domain::SecretStore;
    use vlinder_core::domain::{Agent, AgentName, Registry};
    use vlinder_core::domain::{
        BranchId, Operation, RequestDiagnostics, RequestMessage, Sequence, ServiceBackend,
        SessionId, SubmissionId, VectorStorageType,
    };
    use vlinder_core::queue::InMemoryQueue;

    fn test_secret_store() -> Arc<dyn SecretStore> {
        Arc::new(InMemorySecretStore::new())
    }

    fn test_request_diag() -> RequestDiagnostics {
        RequestDiagnostics {
            sequence: 0,
            endpoint: String::new(),
            request_bytes: 0,
            received_at_ms: 0,
        }
    }

    fn test_agent_id() -> AgentName {
        AgentName::new("test-agent")
    }

    fn test_submission() -> SubmissionId {
        SubmissionId::from("sub-test-123".to_string())
    }

    fn test_agent_with_vector_storage(db_path: &std::path::Path) -> Agent {
        let uri = format!("sqlite://{}", db_path.display());
        let manifest = format!(
            r#"
            name = "test-agent"
            description = "Test agent for vector storage"
            runtime = "container"
            executable = "localhost/test-agent:latest"
            vector_storage = "{uri}"
            [requirements]
            "#,
        );
        Agent::from_toml(&manifest).unwrap()
    }

    fn make_request_key(
        submission: SubmissionId,
        agent: AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> DataRoutingKey {
        DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission,
            kind: DataMessageKind::Request {
                agent,
                service,
                operation,
                sequence,
            },
        }
    }

    fn make_request_msg(payload: Vec<u8>, state: Option<String>) -> RequestMessage {
        RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state,
            diagnostics: test_request_diag(),
            payload,
            checkpoint: None,
        }
    }

    async fn drain_store(queue: &Arc<dyn MessageQueue + Send + Sync>, worker: &SqliteVecWorker) {
        let (key, msg, ack) = queue
            .receive_request(worker.service, Operation::Store)
            .await
            .expect("store request should be available");
        worker.process_store_request(key, msg, ack).await;
    }

    async fn drain_search(queue: &Arc<dyn MessageQueue + Send + Sync>, worker: &SqliteVecWorker) {
        let (key, msg, ack) = queue
            .receive_request(worker.service, Operation::Search)
            .await
            .expect("search request should be available");
        worker.process_search_request(key, msg, ack).await;
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn vector_search_response_echoes_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vec.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_vector_storage(VectorStorageType::SqliteVec);
        let agent = test_agent_with_vector_storage(&db_path);
        registry.register_agent(agent).await.unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = SqliteVecWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Vec(VectorStorageType::SqliteVec),
        );

        let submission = test_submission();
        let service = ServiceBackend::Vec(VectorStorageType::SqliteVec);

        let embedding: Vec<f32> = (0_i16..768).map(|i| f32::from(i) * 0.001).collect();
        let store_payload = serde_json::json!({
            "key": "doc1",
            "vector": embedding,
            "metadata": "test"
        });
        let store_key = make_request_key(
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Store,
            Sequence::first(),
        );
        let store_msg = make_request_msg(
            serde_json::to_vec(&store_payload).unwrap(),
            Some("state-vec".to_string()),
        );
        queue.send_request(store_key, store_msg).await.unwrap();
        drain_store(&queue, &handler).await;
        let (_key, store_resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Store,
                Sequence::first(),
            )
            .await
            .unwrap();
        ack().await.unwrap();
        assert_eq!(
            store_resp.state,
            Some("state-vec".to_string()),
            "store should echo request.state"
        );

        let search_payload = serde_json::json!({
            "vector": embedding,
            "limit": 1
        });
        let search_key = make_request_key(
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Search,
            Sequence::from(2),
        );
        let search_msg = make_request_msg(
            serde_json::to_vec(&search_payload).unwrap(),
            Some("state-vec2".to_string()),
        );
        queue.send_request(search_key, search_msg).await.unwrap();
        drain_search(&queue, &handler).await;
        let (_key, search_resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Search,
                Sequence::from(2),
            )
            .await
            .unwrap();
        ack().await.unwrap();
        assert_eq!(
            search_resp.state,
            Some("state-vec2".to_string()),
            "search should echo request.state"
        );
    }

    #[tokio::test]
    async fn handles_store_and_search() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("vec.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_vector_storage(VectorStorageType::SqliteVec);
        let agent = test_agent_with_vector_storage(&db_path);
        registry.register_agent(agent).await.unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = SqliteVecWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Vec(VectorStorageType::SqliteVec),
        );

        let submission = test_submission();
        let service = ServiceBackend::Vec(VectorStorageType::SqliteVec);

        let embedding: Vec<f32> = (0_i16..768).map(|i| f32::from(i) * 0.001).collect();
        let store_payload = serde_json::json!({
            "key": "doc1",
            "vector": embedding,
            "metadata": "test document"
        });
        let store_key = make_request_key(
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Store,
            Sequence::first(),
        );
        let store_msg = make_request_msg(serde_json::to_vec(&store_payload).unwrap(), None);

        queue.send_request(store_key, store_msg).await.unwrap();
        drain_store(&queue, &handler).await;
        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Store,
                Sequence::first(),
            )
            .await
            .unwrap();
        assert_eq!(response.payload.as_slice(), b"ok");
        ack().await.unwrap();

        let search_payload = serde_json::json!({
            "vector": embedding,
            "limit": 1
        });
        let search_key = make_request_key(
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Search,
            Sequence::from(2),
        );
        let search_msg = make_request_msg(serde_json::to_vec(&search_payload).unwrap(), None);

        queue.send_request(search_key, search_msg).await.unwrap();
        drain_search(&queue, &handler).await;
        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Search,
                Sequence::from(2),
            )
            .await
            .unwrap();
        let results: Vec<serde_json::Value> =
            serde_json::from_slice(response.payload.as_slice()).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["key"], "doc1");
        ack().await.unwrap();
    }
}
