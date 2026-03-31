//! KV worker — receives object storage requests from the queue,
//! opens `SqliteObjectStorage` + `SqliteStateStore` per agent, and sends
//! responses back.
//!
//! Follows the same pattern as `SqliteVecWorker`: 3-arg constructor,
//! lazy `get_or_open()`, `tick()` polling.
//!
//! Key differences from the old `ObjectServiceWorker`:
//! - State comes from the message envelope (request.state), not JSON payload
//! - No base64 — content is stored as plain bytes
//! - Concrete types only (no ObjectStorage/StateStore traits)

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use vlinder_core::domain::Registry;
use vlinder_core::domain::{
    DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue, Operation,
    ResponseMessage, ServiceBackend, ServiceDiagnostics,
};

use crate::state_store::{hash_snapshot, hash_state_commit, hash_value, SqliteStateStore};
use crate::storage::SqliteObjectStorage;
use crate::types::{KvDeleteRequest, KvGetRequest, KvListRequest, KvPutRequest};

// ============================================================================
// Helpers
// ============================================================================

/// Extract agent name and session ID from a data-plane routing key.
fn extract_agent_session(key: &DataRoutingKey) -> (&str, &str) {
    let agent = match &key.kind {
        DataMessageKind::Request { agent, .. } => agent.as_str(),
        _ => "",
    };
    (agent, key.session.as_str())
}

/// Build a Response routing key from a Request routing key.
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

pub struct KvWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    stores: RwLock<HashMap<String, Arc<SqliteObjectStorage>>>,
    state_stores: RwLock<HashMap<String, Arc<SqliteStateStore>>>,
    service: ServiceBackend,
}

impl KvWorker {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        registry: Arc<dyn Registry>,
        service: ServiceBackend,
    ) -> Self {
        Self {
            queue,
            registry,
            stores: RwLock::new(HashMap::new()),
            state_stores: RwLock::new(HashMap::new()),
            service,
        }
    }

    /// Get object storage for an agent+session, opening lazily if needed.
    ///
    /// Storage is scoped to the session: each session gets its own database
    /// under `<agent_storage_dir>/sessions/<session_id>/objects.db`.
    fn get_or_open(
        &self,
        agent_id: &str,
        session_id: &str,
    ) -> Result<Arc<SqliteObjectStorage>, String> {
        let cache_key = format!("{agent_id}:{session_id}");
        if let Some(storage) = self
            .stores
            .read()
            .expect("stores lock poisoned")
            .get(&cache_key)
        {
            return Ok(storage.clone());
        }

        let agent = self
            .registry
            .get_agent_by_name(agent_id)
            .ok_or_else(|| format!("unknown agent: {agent_id}"))?;
        let uri = agent
            .object_storage
            .ok_or_else(|| format!("agent has no object_storage declared: {agent_id}"))?;
        let base_path = uri
            .path()
            .ok_or_else(|| format!("object_storage URI has no path: {}", uri.as_str()))?;

        let parent = std::path::Path::new(base_path)
            .parent()
            .ok_or_else(|| "object_storage path has no parent".to_string())?;
        let session_path = parent.join("sessions").join(session_id).join("objects.db");

        let storage = Arc::new(SqliteObjectStorage::open_at(&session_path)?);
        self.stores
            .write()
            .expect("stores lock poisoned")
            .insert(cache_key, storage.clone());
        Ok(storage)
    }

    /// Get or open a `SqliteStateStore` for an agent+session.
    ///
    /// State store is co-located with the session-scoped object storage:
    /// `<agent_storage_dir>/sessions/<session_id>/state.db`.
    fn get_or_open_state_store(
        &self,
        agent_id: &str,
        session_id: &str,
    ) -> Result<Arc<SqliteStateStore>, String> {
        let cache_key = format!("{agent_id}:{session_id}");
        if let Some(store) = self
            .state_stores
            .read()
            .expect("state_stores lock poisoned")
            .get(&cache_key)
        {
            return Ok(store.clone());
        }

        let agent = self
            .registry
            .get_agent_by_name(agent_id)
            .ok_or_else(|| format!("unknown agent: {agent_id}"))?;
        let uri = agent
            .object_storage
            .ok_or_else(|| format!("agent has no object_storage declared: {agent_id}"))?;

        let path = match uri.scheme() {
            Some("sqlite") => {
                let db_path = uri
                    .path()
                    .ok_or_else(|| "sqlite URI has no path".to_string())?;
                let parent = std::path::Path::new(db_path)
                    .parent()
                    .ok_or_else(|| "sqlite path has no parent".to_string())?;
                parent.join("sessions").join(session_id).join("state.db")
            }
            Some("memory") => {
                let dir = std::env::temp_dir().join("vlinder-state");
                std::fs::create_dir_all(&dir).ok();
                dir.join(format!(
                    "{}_{}.db",
                    agent_id.replace(['/', ':'], "_"),
                    session_id
                ))
            }
            _ => return Err("unsupported storage scheme for state store".to_string()),
        };

        let store = Arc::new(SqliteStateStore::open(&path)?);
        self.state_stores
            .write()
            .expect("state_stores lock poisoned")
            .insert(cache_key, store.clone());
        Ok(store)
    }

    /// Process one message if available. Returns true if processed.
    pub fn tick(&self) -> bool {
        if self.try_get_v2() {
            return true;
        }
        if self.try_put_v2() {
            return true;
        }
        if self.try_list_v2() {
            return true;
        }
        if self.try_delete_v2() {
            return true;
        }
        false
    }

    fn try_get_v2(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Get) {
            Ok((key, msg, ack)) => {
                let (agent, session) = extract_agent_session(&key);
                let start = std::time::Instant::now();
                let response_payload =
                    self.handle_get(agent, session, &msg.payload, msg.state.as_deref());
                let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::Get,
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
                let _ = self.queue.send_response(response_key, response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn try_put_v2(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Put) {
            Ok((key, msg, ack)) => {
                let (agent, session) = extract_agent_session(&key);
                let start = std::time::Instant::now();
                let (response_payload, new_state) =
                    self.handle_put(agent, session, &msg.payload, msg.state.as_deref());
                let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::Put,
                    response_payload.len() as u64,
                    duration_ms,
                );
                let response_key = response_key_from_request(&key);
                let response = ResponseMessage {
                    id: MessageId::new(),
                    dag_id: DagNodeId::root(),
                    correlation_id: msg.id,
                    state: new_state.or(msg.state),
                    diagnostics: diag,
                    payload: response_payload,
                    status_code: 200,
                    checkpoint: msg.checkpoint,
                };
                let _ = self.queue.send_response(response_key, response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn try_list_v2(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::List) {
            Ok((key, msg, ack)) => {
                let (agent, session) = extract_agent_session(&key);
                let start = std::time::Instant::now();
                let response_payload =
                    self.handle_list(agent, session, &msg.payload, msg.state.as_deref());
                let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::List,
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
                let _ = self.queue.send_response(response_key, response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn try_delete_v2(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Delete) {
            Ok((key, msg, ack)) => {
                let (agent, session) = extract_agent_session(&key);
                let start = std::time::Instant::now();
                let response_payload = self.handle_delete(agent, session, &msg.payload);
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
                let _ = self.queue.send_response(response_key, response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn handle_get(
        &self,
        agent_id: &str,
        session_id: &str,
        payload: &[u8],
        state: Option<&str>,
    ) -> Vec<u8> {
        let req: KvGetRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        // Versioned get (ADR 055): resolve through state commit -> snapshot -> value
        // State comes from the envelope, not the payload.
        if let Some(state_hash) = state {
            if !state_hash.is_empty() {
                return match self.versioned_get(agent_id, session_id, state_hash, &req.path) {
                    Ok(Some(content)) => content,
                    Ok(None) => Vec::new(),
                    Err(e) => format!("[error] {e}").into_bytes(),
                };
            }
        }

        // Unversioned fallback
        let store = match self.get_or_open(agent_id, session_id) {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.get_file(&req.path) {
            Ok(Some(content)) => content,
            Ok(None) => Vec::new(),
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }

    /// Returns `(response_payload, new_state_option)`.
    fn handle_put(
        &self,
        agent_id: &str,
        session_id: &str,
        payload: &[u8],
        state: Option<&str>,
    ) -> (Vec<u8>, Option<String>) {
        let req: KvPutRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return (format!("[error] invalid request: {e}").into_bytes(), None),
        };

        let store = match self.get_or_open(agent_id, session_id) {
            Ok(s) => s,
            Err(e) => return (format!("[error] {e}").into_bytes(), None),
        };

        // No base64 — store content bytes directly
        let content = req.content.as_bytes();

        // Always write to ObjectStorage (current-state access for unversioned reads)
        if let Err(e) = store.put_file(&req.path, content) {
            return (format!("[error] {e}").into_bytes(), None);
        }

        // Versioned put (ADR 055): state comes from the envelope
        if let Some(parent_state) = state {
            return match self.versioned_put(agent_id, session_id, parent_state, &req.path, content)
            {
                Ok(new_state) => {
                    let response = serde_json::json!({"state": new_state});
                    (
                        serde_json::to_vec(&response).expect("json! value always serializes"),
                        Some(new_state),
                    )
                }
                Err(e) => (format!("[error] {e}").into_bytes(), None),
            };
        }

        // Unversioned
        (b"ok".to_vec(), None)
    }

    fn handle_list(
        &self,
        agent_id: &str,
        session_id: &str,
        payload: &[u8],
        state: Option<&str>,
    ) -> Vec<u8> {
        let req: KvListRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        // Versioned list: resolve paths from the state snapshot
        if let Some(state_hash) = state {
            if !state_hash.is_empty() {
                return match self.versioned_list(agent_id, session_id, state_hash, &req.path) {
                    Ok(files) => serde_json::to_string(&files).map_or_else(
                        |e| format!("[error] {e}").into_bytes(),
                        std::string::String::into_bytes,
                    ),
                    Err(e) => format!("[error] {e}").into_bytes(),
                };
            }
        }

        // Unversioned fallback
        let store = match self.get_or_open(agent_id, session_id) {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.list_files(&req.path) {
            Ok(files) => serde_json::to_string(&files).map_or_else(
                |e| format!("[error] {e}").into_bytes(),
                std::string::String::into_bytes,
            ),
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }

    fn handle_delete(&self, agent_id: &str, session_id: &str, payload: &[u8]) -> Vec<u8> {
        let req: KvDeleteRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {e}").into_bytes(),
        };

        let store = match self.get_or_open(agent_id, session_id) {
            Ok(s) => s,
            Err(e) => return format!("[error] {e}").into_bytes(),
        };

        match store.delete_file(&req.path) {
            Ok(true) => b"ok".to_vec(),
            Ok(false) => b"not_found".to_vec(),
            Err(e) => format!("[error] {e}").into_bytes(),
        }
    }

    // --- Versioned operations (ADR 055) ---

    /// Versioned put: store value, update snapshot, create state commit.
    fn versioned_put(
        &self,
        agent_id: &str,
        session_id: &str,
        parent_state: &str,
        path: &str,
        content: &[u8],
    ) -> Result<String, String> {
        let state_store = self.get_or_open_state_store(agent_id, session_id)?;

        // 1. Store value
        let value_hash = hash_value(content);
        state_store.put_value(&value_hash, content)?;

        // 2. Load parent snapshot (or start from empty)
        let parent_entries = if parent_state.is_empty() {
            HashMap::new()
        } else {
            let commit = state_store
                .get_state_commit(parent_state)?
                .ok_or_else(|| format!("unknown parent state: {parent_state}"))?;
            state_store
                .get_snapshot(&commit.snapshot_hash)?
                .unwrap_or_default()
        };

        // 3. Update snapshot with new path -> value_hash
        let mut new_entries = parent_entries;
        new_entries.insert(path.to_string(), value_hash);

        // 4. Store snapshot
        let snapshot_hash = hash_snapshot(&new_entries);
        state_store.put_snapshot(&snapshot_hash, &new_entries)?;

        // 5. Create and store state commit
        let commit_hash = hash_state_commit(&snapshot_hash, parent_state);
        state_store.put_state_commit(&commit_hash, &snapshot_hash, parent_state)?;

        Ok(commit_hash)
    }

    /// Versioned get: resolve through state commit -> snapshot -> value.
    fn versioned_get(
        &self,
        agent_id: &str,
        session_id: &str,
        state_hash: &str,
        path: &str,
    ) -> Result<Option<Vec<u8>>, String> {
        let state_store = self.get_or_open_state_store(agent_id, session_id)?;

        // Load state commit
        let commit = state_store
            .get_state_commit(state_hash)?
            .ok_or_else(|| format!("unknown state: {state_hash}"))?;

        // Load snapshot
        let entries = state_store
            .get_snapshot(&commit.snapshot_hash)?
            .unwrap_or_default();

        // Look up path
        let Some(value_hash) = entries.get(path) else {
            return Ok(None);
        };

        // Load value
        state_store.get_value(value_hash)
    }

    /// Versioned list: return paths from the snapshot that match a prefix.
    fn versioned_list(
        &self,
        agent_id: &str,
        session_id: &str,
        state_hash: &str,
        prefix: &str,
    ) -> Result<Vec<String>, String> {
        let state_store = self.get_or_open_state_store(agent_id, session_id)?;

        let commit = state_store
            .get_state_commit(state_hash)?
            .ok_or_else(|| format!("unknown state: {state_hash}"))?;

        let entries = state_store
            .get_snapshot(&commit.snapshot_hash)?
            .unwrap_or_default();

        let mut files: Vec<String> = entries
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        files.sort();
        Ok(files)
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
        BranchId, ObjectStorageType, Operation, RequestDiagnostics, RequestMessage, Sequence,
        ServiceBackend, SessionId, SubmissionId,
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

    fn test_agent_with_object_storage(db_path: &std::path::Path) -> Agent {
        let uri = format!("sqlite://{}", db_path.display());
        let manifest = format!(
            r#"
            name = "test-agent"
            description = "Test agent for KV storage"
            runtime = "container"
            executable = "localhost/test-agent:latest"
            object_storage = "{uri}"
            [requirements]
            "#,
        );
        Agent::from_toml(&manifest).unwrap()
    }

    fn make_request_key(
        session: SessionId,
        submission: SubmissionId,
        agent: AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> DataRoutingKey {
        DataRoutingKey {
            session,
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

    #[test]
    fn handles_put_and_get() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("objects.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_object_storage(ObjectStorageType::Sqlite);
        let agent = test_agent_with_object_storage(&db_path);
        registry.register_agent(agent).unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = KvWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
        );

        let session = SessionId::new();
        let submission = test_submission();
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);

        // Put request — no base64, plain string
        let put_payload = serde_json::json!({
            "path": "/hello.txt",
            "content": "hello world"
        });
        let put_key = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::first(),
        );
        let put_msg = make_request_msg(serde_json::to_vec(&put_payload).unwrap(), None);

        queue.send_request(put_key, put_msg).unwrap();
        assert!(handler.tick());
        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::first(),
            )
            .unwrap();
        assert_eq!(response.payload.as_slice(), b"ok");
        ack().unwrap();

        // Get request
        let get_payload = serde_json::json!({"path": "/hello.txt"});
        let get_key = make_request_key(
            session,
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Get,
            Sequence::from(2),
        );
        let get_msg = make_request_msg(serde_json::to_vec(&get_payload).unwrap(), None);

        queue.send_request(get_key, get_msg).unwrap();
        assert!(handler.tick());
        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Get,
                Sequence::from(2),
            )
            .unwrap();
        assert_eq!(response.payload.as_slice(), b"hello world");
        ack().unwrap();
    }

    #[test]
    fn versioned_put_returns_state_hash() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("objects.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_object_storage(ObjectStorageType::Sqlite);
        let agent = test_agent_with_object_storage(&db_path);
        registry.register_agent(agent).unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = KvWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
        );

        let submission = test_submission();
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);

        let put_payload = serde_json::json!({
            "path": "/todos.json",
            "content": "[\"buy milk\"]"
        });
        // State comes from the envelope (empty string = root state)
        let put_key = make_request_key(
            SessionId::new(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::first(),
        );
        let put_msg = make_request_msg(
            serde_json::to_vec(&put_payload).unwrap(),
            Some(String::new()), // root state via envelope
        );

        queue.send_request(put_key, put_msg).unwrap();
        assert!(handler.tick());

        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        // Response should be JSON with a state field
        let resp: serde_json::Value = serde_json::from_slice(response.payload.as_slice()).unwrap();
        let state = resp["state"].as_str().unwrap();
        assert!(!state.is_empty());
        assert_eq!(state.len(), 64); // SHA-256 hex
                                     // response.state should also have the new hash
        assert_eq!(response.state, Some(state.to_string()));
    }

    #[test]
    fn versioned_put_chains_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("objects.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_object_storage(ObjectStorageType::Sqlite);
        let agent = test_agent_with_object_storage(&db_path);
        registry.register_agent(agent).unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = KvWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
        );

        let session = SessionId::new();
        let submission = test_submission();
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);

        // First put
        let put1 = serde_json::json!({"path": "/a.txt", "content": "aaa"});
        let key1 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::first(),
        );
        let msg1 = make_request_msg(serde_json::to_vec(&put1).unwrap(), Some(String::new()));
        queue.send_request(key1, msg1).unwrap();
        handler.tick();
        let (_key, resp1, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();
        let state1: serde_json::Value = serde_json::from_slice(resp1.payload.as_slice()).unwrap();
        let hash1 = state1["state"].as_str().unwrap().to_string();

        // Second put chained from first — state via envelope
        let put2 = serde_json::json!({"path": "/b.txt", "content": "bbb"});
        let key2 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::from(2),
        );
        let msg2 = make_request_msg(
            serde_json::to_vec(&put2).unwrap(),
            Some(hash1.clone()), // chain from previous state via envelope
        );
        queue.send_request(key2, msg2).unwrap();
        handler.tick();
        let (_key, resp2, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::from(2),
            )
            .unwrap();
        ack().unwrap();
        let state2: serde_json::Value = serde_json::from_slice(resp2.payload.as_slice()).unwrap();
        let hash2 = state2["state"].as_str().unwrap().to_string();

        // Hashes should differ
        assert_ne!(hash1, hash2);

        // Reading /a.txt from state2 should still work (inherited from snapshot)
        let get = serde_json::json!({"path": "/a.txt"});
        let get_key = make_request_key(
            session,
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Get,
            Sequence::from(3),
        );
        let get_msg = make_request_msg(
            serde_json::to_vec(&get).unwrap(),
            Some(hash2.clone()), // state via envelope
        );
        queue.send_request(get_key, get_msg).unwrap();
        handler.tick();
        let (_key, resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Get,
                Sequence::from(3),
            )
            .unwrap();
        ack().unwrap();
        assert_eq!(resp.payload.as_slice(), b"aaa");
    }

    #[test]
    fn versioned_list_reflects_state_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("objects.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_object_storage(ObjectStorageType::Sqlite);
        let agent = test_agent_with_object_storage(&db_path);
        registry.register_agent(agent).unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = KvWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
        );
        let session = SessionId::new();
        let submission = test_submission();
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);

        // Put /a.txt → state1
        let key1 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::first(),
        );
        let msg1 = make_request_msg(
            serde_json::to_vec(&serde_json::json!({"path": "/a.txt", "content": "aaa"})).unwrap(),
            Some(String::new()),
        );
        queue.send_request(key1, msg1).unwrap();
        handler.tick();
        let (_key, resp1, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();
        let state1: serde_json::Value = serde_json::from_slice(resp1.payload.as_slice()).unwrap();
        let hash1 = state1["state"].as_str().unwrap().to_string();

        // Put /b.txt → state2
        let key2 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::from(2),
        );
        let msg2 = make_request_msg(
            serde_json::to_vec(&serde_json::json!({"path": "/b.txt", "content": "bbb"})).unwrap(),
            Some(hash1.clone()),
        );
        queue.send_request(key2, msg2).unwrap();
        handler.tick();
        let (_key, resp2, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::from(2),
            )
            .unwrap();
        ack().unwrap();
        let state2: serde_json::Value = serde_json::from_slice(resp2.payload.as_slice()).unwrap();
        let hash2 = state2["state"].as_str().unwrap().to_string();

        // List from state2 — should see both files
        let list_key2 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::List,
            Sequence::from(3),
        );
        let list_msg2 = make_request_msg(
            serde_json::to_vec(&serde_json::json!({"path": "/"})).unwrap(),
            Some(hash2),
        );
        queue.send_request(list_key2, list_msg2).unwrap();
        handler.tick();
        let (_key, resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::List,
                Sequence::from(3),
            )
            .unwrap();
        ack().unwrap();
        let files: Vec<String> = serde_json::from_slice(resp.payload.as_slice()).unwrap();
        assert_eq!(files, vec!["/a.txt", "/b.txt"]);

        // List from state1 (time travel) — should only see /a.txt
        let list_key1 = make_request_key(
            session.clone(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::List,
            Sequence::from(4),
        );
        let list_msg1 = make_request_msg(
            serde_json::to_vec(&serde_json::json!({"path": "/"})).unwrap(),
            Some(hash1),
        );
        queue.send_request(list_key1, list_msg1).unwrap();
        handler.tick();
        let (_key, resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::List,
                Sequence::from(4),
            )
            .unwrap();
        ack().unwrap();
        let files: Vec<String> = serde_json::from_slice(resp.payload.as_slice()).unwrap();
        assert_eq!(files, vec!["/a.txt"]);
    }

    #[test]
    fn kv_get_response_echoes_state() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("objects.db");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(vlinder_core::domain::RuntimeType::Container);
        registry.register_object_storage(ObjectStorageType::Sqlite);
        let agent = test_agent_with_object_storage(&db_path);
        registry.register_agent(agent).unwrap();
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let handler = KvWorker::new(
            Arc::clone(&queue),
            Arc::clone(&registry),
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
        );

        let submission = test_submission();
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);

        // Put a file first (unversioned)
        let put_payload = serde_json::json!({"path": "/hello.txt", "content": "hello"});
        let put_key = make_request_key(
            SessionId::new(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Put,
            Sequence::first(),
        );
        let put_msg = make_request_msg(serde_json::to_vec(&put_payload).unwrap(), None);
        queue.send_request(put_key, put_msg).unwrap();
        handler.tick();
        let (_key, _resp, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Put,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        // Get with state in envelope — should echo it back
        let get_payload = serde_json::json!({"path": "/hello.txt"});
        let get_key = make_request_key(
            SessionId::new(),
            submission.clone(),
            test_agent_id(),
            service,
            Operation::Get,
            Sequence::from(2),
        );
        let get_msg = make_request_msg(
            serde_json::to_vec(&get_payload).unwrap(),
            Some("hash123".to_string()),
        );
        queue.send_request(get_key, get_msg).unwrap();
        handler.tick();
        let (_key, response, ack) = queue
            .receive_response(
                &submission,
                &test_agent_id(),
                service,
                Operation::Get,
                Sequence::from(2),
            )
            .unwrap();
        ack().unwrap();

        assert_eq!(
            response.state,
            Some("hash123".to_string()),
            "get should echo request.state"
        );
    }
}
