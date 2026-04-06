//! Dolt worker — receives postgres wire protocol bytes from the queue,
//! forwards to Doltgres, injects control plane operations, and sends
//! response bytes + state hash back.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use vlinder_core::domain::{
    BranchId, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue, Operation,
    ResponseMessage, ServiceBackend, ServiceDiagnostics, SessionId, SqlStorageType,
};

/// A connection to Doltgres that can forward wire protocol bytes
/// and run control plane queries.
pub trait DoltConnection {
    /// Forward raw frontend bytes to Doltgres, return raw backend response bytes.
    fn forward(&mut self, frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError>;

    /// Run a control plane query and return the first column of the first row as a string.
    /// Used for `SELECT HASHOF('HEAD')`.
    fn query_scalar(&mut self, sql: &str) -> Result<Option<String>, DoltError>;

    /// Run a control plane statement (no result needed).
    /// Used for `SET dolt_transaction_commit TO 1`, `SELECT DOLT_CHECKOUT(...)`.
    fn execute(&mut self, sql: &str) -> Result<(), DoltError>;
}

/// Factory for creating connections to Doltgres.
pub trait DoltConnectionFactory: Send + Sync {
    fn connect(&self) -> Result<Box<dyn DoltConnection>, DoltError>;
}

#[derive(Debug)]
pub enum DoltError {
    Connection(String),
    Protocol(String),
}

impl std::fmt::Display for DoltError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DoltError::Connection(msg) => write!(f, "connection error: {msg}"),
            DoltError::Protocol(msg) => write!(f, "protocol error: {msg}"),
        }
    }
}

impl std::error::Error for DoltError {}

/// Build a postgres ErrorResponse + ReadyForQuery wire message.
///
/// This is what Doltgres would send if a query failed. Returning raw
/// English text instead causes the agent's postgres driver to desync
/// (it reads the first byte as a message tag and interprets garbage).
fn error_response_bytes(message: &str) -> Vec<u8> {
    // ErrorResponse: tag 'E', then null-terminated field strings.
    // Required fields: 'S' severity, 'V' severity (non-localized),
    // 'C' SQLSTATE code, 'M' message. Terminated by 0x00.
    let mut fields = Vec::new();
    fields.push(b'S');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'V');
    fields.extend_from_slice(b"ERROR\0");
    fields.push(b'C');
    fields.extend_from_slice(b"XX000\0"); // internal_error
    fields.push(b'M');
    fields.extend_from_slice(message.as_bytes());
    fields.push(0); // null terminator for message
    fields.push(0); // end of fields

    let len = (4 + fields.len()) as i32; // length includes itself
    let mut buf = Vec::with_capacity(1 + len as usize + 6);
    buf.push(b'E');
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&fields);

    // ReadyForQuery(Idle) — the agent expects this after every response.
    buf.push(b'Z');
    buf.extend_from_slice(&5_i32.to_be_bytes());
    buf.push(b'I');

    buf
}

/// Derive the Dolt branch name from session and branch IDs.
///
/// Format: `vlinder/{session_id}/{branch_id}`
/// This ensures each session+branch combination maps to a unique Dolt branch.
pub fn dolt_branch_name(session: &SessionId, branch: BranchId) -> String {
    format!("vlinder/{}/{}", session.as_str(), branch.as_i64())
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

pub struct DoltWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    service: ServiceBackend,
    factory: Arc<dyn DoltConnectionFactory>,
    connections: RefCell<HashMap<(SessionId, BranchId), Box<dyn DoltConnection>>>,
}

impl DoltWorker {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        factory: Arc<dyn DoltConnectionFactory>,
    ) -> Self {
        Self {
            queue,
            service: ServiceBackend::Sql(SqlStorageType::Doltgres),
            factory,
            connections: RefCell::new(HashMap::new()),
        }
    }

    /// Process one message if available. Returns true if processed.
    pub fn tick(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Execute) {
            Ok((key, msg, ack)) => {
                let start = std::time::Instant::now();

                let session_id = &key.session;
                let branch = key.branch;
                tracing::info!(
                    event = "dolt_worker.request_received",
                    session = %session_id,
                    branch = branch.as_i64(),
                    submission = %key.submission,
                    payload_len = msg.payload.len(),
                    state = ?msg.state,
                    "Received SQL Execute request"
                );

                let (response_payload, state_hash) =
                    self.handle_request(session_id, branch, &msg.payload, msg.state.as_deref());

                let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::Execute,
                    response_payload.len() as u64,
                    duration_ms,
                );

                let response_key = response_key_from_request(&key);
                let response = ResponseMessage {
                    id: MessageId::new(),
                    dag_id: DagNodeId::root(),
                    correlation_id: msg.id,
                    state: state_hash.clone().or_else(|| msg.state.clone()),
                    diagnostics: diag,
                    payload: response_payload,
                    status_code: 200,
                    checkpoint: msg.checkpoint,
                };

                if let Err(ref e) = self.queue.send_response(response_key, response) {
                    tracing::error!(event = "dolt_worker.send_failed", error = ?e);
                } else {
                    tracing::info!(
                        event = "dolt_worker.response_sent",
                        session = %session_id,
                        branch = branch.as_i64(),
                        state_hash = ?state_hash,
                        duration_ms = duration_ms,
                        "Sent SQL Execute response"
                    );
                }
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn handle_request(
        &self,
        session: &SessionId,
        branch: BranchId,
        frontend_bytes: &[u8],
        start_hash: Option<&str>,
    ) -> (Vec<u8>, Option<String>) {
        let mut connections = self.connections.borrow_mut();

        let key = (session.clone(), branch);
        let conn = match connections.entry(key) {
            std::collections::hash_map::Entry::Occupied(e) => {
                tracing::debug!(
                    event = "dolt_worker.connection_reused",
                    session = %session,
                    branch = branch.as_i64(),
                    "Reusing existing Dolt connection"
                );
                e.into_mut()
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                tracing::info!(
                    event = "dolt_worker.connection_creating",
                    session = %session,
                    branch = branch.as_i64(),
                    start_hash = ?start_hash,
                    "Creating new Dolt connection"
                );
                match self.create_session_connection(session, branch, start_hash) {
                    Ok(c) => {
                        tracing::info!(
                            event = "dolt_worker.connection_created",
                            "Dolt connection established"
                        );
                        e.insert(c)
                    }
                    Err(err) => {
                        tracing::error!(event = "dolt_worker.connection_failed", error = %err);
                        return (
                            error_response_bytes(&format!("connection error: {err}")),
                            None,
                        );
                    }
                }
            }
        };

        // Forward agent's bytes to Doltgres
        let backend_bytes = match conn.forward(frontend_bytes) {
            Ok(bytes) => bytes,
            Err(err) => {
                tracing::error!(event = "dolt_worker.forward_error", error = %err);
                return (error_response_bytes(&format!("forward error: {err}")), None);
            }
        };

        // Stage and commit — ignore errors (e.g. nothing to commit after a SELECT).
        let _ = conn.execute("SELECT DOLT_ADD('-A')");
        let _ = conn.execute("SELECT DOLT_COMMIT('-m', 'auto')");

        // Capture state hash
        let state_hash = match conn.query_scalar("SELECT HASHOF('HEAD')") {
            Ok(hash) => hash,
            Err(e) => {
                tracing::warn!(event = "dolt_worker.hashof_error", error = %e);
                None
            }
        };

        (backend_bytes, state_hash)
    }

    fn create_session_connection(
        &self,
        session: &SessionId,
        branch: BranchId,
        start_hash: Option<&str>,
    ) -> Result<Box<dyn DoltConnection>, DoltError> {
        let mut conn = self.factory.connect()?;
        conn.execute("SET dolt_transaction_commit TO 1")?;

        let branch_name = dolt_branch_name(session, branch);

        // Create and checkout the branch for this session.
        // If start_hash is provided (fork), create branch from that commit.
        // If the branch already exists (reconnecting), just checkout.
        let create_result = match start_hash {
            Some(hash) => conn.execute(&format!(
                "SELECT DOLT_CHECKOUT('-b', '{branch_name}', '{hash}')"
            )),
            None => conn.execute(&format!("SELECT DOLT_CHECKOUT('-b', '{branch_name}')")),
        };

        if create_result.is_err() {
            // Branch already exists — checkout and reset to fork point if needed.
            conn.execute(&format!("SELECT DOLT_CHECKOUT('{branch_name}')"))?;
            if let Some(hash) = start_hash {
                conn.execute(&format!("SELECT DOLT_RESET('--hard', '{hash}')"))
                    .map_err(|e| {
                        tracing::error!(event = "dolt_worker.reset_failed", branch = %branch_name, error = %e);
                        e
                    })?;
            }
        }

        Ok(conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use vlinder_core::domain::{
        AgentName, DataMessageKind, DataRoutingKey, MessageId, RequestDiagnostics, RequestMessage,
        Sequence, SubmissionId,
    };
    use vlinder_core::queue::InMemoryQueue;

    // --- Fake connection for testing ---

    struct FakeConnection {
        forward_response: Vec<u8>,
        hash: Option<String>,
        executed: Vec<String>,
    }

    impl FakeConnection {
        fn new(response: Vec<u8>, hash: Option<String>) -> Self {
            Self {
                forward_response: response,
                hash,
                executed: Vec::new(),
            }
        }
    }

    impl DoltConnection for FakeConnection {
        fn forward(&mut self, _frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError> {
            Ok(self.forward_response.clone())
        }

        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>, DoltError> {
            Ok(self.hash.clone())
        }

        fn execute(&mut self, sql: &str) -> Result<(), DoltError> {
            self.executed.push(sql.to_string());
            Ok(())
        }
    }

    struct FakeFactory {
        response: Vec<u8>,
        hash: Option<String>,
        /// Collects the executed statements from all connections created.
        all_executed: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl FakeFactory {
        fn new(response: Vec<u8>, hash: Option<String>) -> Self {
            Self {
                response,
                hash,
                all_executed: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl DoltConnectionFactory for FakeFactory {
        fn connect(&self) -> Result<Box<dyn DoltConnection>, DoltError> {
            let conn = FakeConnection::new(self.response.clone(), self.hash.clone());
            // We'll capture executed statements via a wrapper
            Ok(Box::new(SpyConnection {
                inner: conn,
                log: Arc::clone(&self.all_executed),
            }))
        }
    }

    /// Wraps FakeConnection to capture executed statements in the factory's log.
    struct SpyConnection {
        inner: FakeConnection,
        log: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl DoltConnection for SpyConnection {
        fn forward(&mut self, frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError> {
            self.inner.forward(frontend_bytes)
        }

        fn query_scalar(&mut self, sql: &str) -> Result<Option<String>, DoltError> {
            self.inner.query_scalar(sql)
        }

        fn execute(&mut self, sql: &str) -> Result<(), DoltError> {
            self.inner.execute(sql)?;
            // Append to the last connection's log
            let mut logs = self.log.lock().unwrap();
            if logs.is_empty() || sql == "SET dolt_transaction_commit TO 1" {
                // New connection starts with SET
                if sql == "SET dolt_transaction_commit TO 1" {
                    logs.push(vec![sql.to_string()]);
                } else if let Some(last) = logs.last_mut() {
                    last.push(sql.to_string());
                }
            } else if let Some(last) = logs.last_mut() {
                last.push(sql.to_string());
            }
            Ok(())
        }
    }

    fn make_query_payload() -> Vec<u8> {
        let mut buf = Vec::new();
        buf.push(b'Q');
        buf.extend_from_slice(&11_i32.to_be_bytes());
        buf.extend_from_slice(b"SELECT\0");
        buf
    }

    fn send_request_with_branch(
        queue: &Arc<dyn MessageQueue + Send + Sync>,
        payload: Vec<u8>,
        session: SessionId,
        branch: BranchId,
    ) -> (DataRoutingKey, RequestMessage) {
        let key = DataRoutingKey {
            session,
            branch,
            submission: SubmissionId::from("sub-1".to_string()),
            kind: DataMessageKind::Request {
                agent: AgentName::new("test-agent"),
                service: ServiceBackend::Sql(SqlStorageType::Doltgres),
                operation: Operation::Execute,
                sequence: Sequence::first(),
            },
        };
        let msg = RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: RequestDiagnostics {
                sequence: 0,
                endpoint: String::new(),
                request_bytes: 0,
                received_at_ms: 0,
            },
            payload,
            checkpoint: None,
        };
        queue.send_request(key.clone(), msg.clone()).unwrap();
        (key, msg)
    }

    fn send_request(
        queue: &Arc<dyn MessageQueue + Send + Sync>,
        payload: Vec<u8>,
        session: SessionId,
    ) -> (DataRoutingKey, RequestMessage) {
        send_request_with_branch(queue, payload, session, BranchId::from(1))
    }

    fn make_worker_with_factory(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        factory: Arc<FakeFactory>,
    ) -> DoltWorker {
        DoltWorker::new(queue, factory)
    }

    fn make_worker(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        response: Vec<u8>,
        hash: Option<String>,
    ) -> DoltWorker {
        let factory = Arc::new(FakeFactory::new(response, hash));
        DoltWorker::new(queue, factory)
    }

    // --- dolt_branch_name tests ---

    #[test]
    fn branch_name_includes_session_and_branch() {
        let session = SessionId::new();
        let branch = BranchId::from(3);
        let name = dolt_branch_name(&session, branch);
        assert_eq!(name, format!("vlinder/{}/3", session.as_str()));
    }

    // --- Worker tick tests ---

    #[test]
    fn no_message_returns_false() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(Arc::clone(&queue), Vec::new(), None);
        assert!(!worker.tick());
    }

    #[test]
    fn forwards_bytes_and_returns_response() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let backend_response = b"fake-backend-response".to_vec();
        let worker = make_worker(Arc::clone(&queue), backend_response.clone(), None);

        let (key, _msg) = send_request(&queue, make_query_payload(), SessionId::new());
        assert!(worker.tick());

        let (_rkey, response, ack) = queue
            .receive_response(
                &key.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();
        assert_eq!(response.payload, backend_response);
    }

    #[test]
    fn attaches_state_hash_to_response() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let hash = "abc123def456".to_string();
        let worker = make_worker(Arc::clone(&queue), Vec::new(), Some(hash.clone()));

        let (key, _msg) = send_request(&queue, make_query_payload(), SessionId::new());
        assert!(worker.tick());

        let (_rkey, response, ack) = queue
            .receive_response(
                &key.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();
        assert_eq!(response.state, Some(hash));
    }

    #[test]
    fn reuses_connection_for_same_session() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(Arc::clone(&queue), b"resp".to_vec(), None);

        let session = SessionId::new();

        let (key1, _) = send_request(&queue, make_query_payload(), session.clone());
        assert!(worker.tick());
        let (_, _, ack) = queue
            .receive_response(
                &key1.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        let (key2, _) = send_request(&queue, make_query_payload(), session);
        assert!(worker.tick());
        let (_, _, ack) = queue
            .receive_response(
                &key2.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        assert_eq!(worker.connections.borrow().len(), 1);
    }

    #[test]
    fn different_sessions_get_different_connections() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(Arc::clone(&queue), b"resp".to_vec(), None);

        let (key1, _) = send_request(&queue, make_query_payload(), SessionId::new());
        assert!(worker.tick());
        let (_, _, ack) = queue
            .receive_response(
                &key1.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        let (key2, _) = send_request(&queue, make_query_payload(), SessionId::new());
        assert!(worker.tick());
        let (_, _, ack) = queue
            .receive_response(
                &key2.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        assert_eq!(worker.connections.borrow().len(), 2);
    }

    // --- Control plane injection tests ---

    #[test]
    fn connection_setup_creates_branch_then_checkouts() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let factory = Arc::new(FakeFactory::new(b"resp".to_vec(), None));
        let worker = make_worker_with_factory(Arc::clone(&queue), Arc::clone(&factory));

        let session = SessionId::new();
        let branch = BranchId::from(7);
        let (key, _msg) =
            send_request_with_branch(&queue, make_query_payload(), session.clone(), branch);
        assert!(worker.tick());

        let (_, _, ack) = queue
            .receive_response(
                &key.submission,
                &AgentName::new("test-agent"),
                ServiceBackend::Sql(SqlStorageType::Doltgres),
                Operation::Execute,
                Sequence::first(),
            )
            .unwrap();
        ack().unwrap();

        let logs = factory.all_executed.lock().unwrap();
        assert_eq!(logs.len(), 1, "should have created one connection");

        let stmts = &logs[0];
        assert_eq!(
            stmts.len(),
            4,
            "should have executed SET, CHECKOUT -b, DOLT_ADD, and DOLT_COMMIT"
        );
        assert_eq!(stmts[0], "SET dolt_transaction_commit TO 1");

        let expected_branch = dolt_branch_name(&session, branch);
        assert_eq!(
            stmts[1],
            format!("SELECT DOLT_CHECKOUT('-b', '{expected_branch}')")
        );
        assert_eq!(stmts[2], "SELECT DOLT_ADD('-A')");
        assert_eq!(stmts[3], "SELECT DOLT_COMMIT('-m', 'auto')");
    }

    /// Integration test: sends SET datestyle, CREATE TABLE, INSERT, SELECT through a
    /// real DoltWorker with TcpDoltConnection. Verifies response payloads contain the
    /// correct wire protocol messages (not contaminated by control plane queries).
    ///
    /// Run with: cargo test -p vlinder-dolt -- --ignored worker_e2e
    #[test]
    #[ignore]
    fn worker_e2e_responses_match_queries() {
        use crate::connection::TcpConnectionFactory;
        use bytes::BytesMut;
        use postgres_protocol::message::{backend, frontend};

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let factory = Arc::new(TcpConnectionFactory::new(
            "localhost:5433",
            "postgres",
            "postgres",
        ));
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        let session = SessionId::new();
        let branch = BranchId::from(1);
        let submission = SubmissionId::from("sub-1".to_string());
        let agent = AgentName::new("test-agent");
        let service = ServiceBackend::Sql(SqlStorageType::Doltgres);

        fn make_query(sql: &str) -> Vec<u8> {
            let mut buf = BytesMut::new();
            frontend::query(sql, &mut buf).unwrap();
            buf.to_vec()
        }

        fn parse_command_tags(resp: &[u8]) -> Vec<String> {
            let mut buf = BytesMut::from(resp);
            let mut tags = Vec::new();
            while let Ok(Some(msg)) = backend::Message::parse(&mut buf) {
                if let backend::Message::CommandComplete(body) = msg {
                    tags.push(body.tag().unwrap_or("?").to_string());
                }
            }
            tags
        }

        fn has_column(resp: &[u8], col_name: &str) -> bool {
            let name_bytes = col_name.as_bytes();
            resp.windows(name_bytes.len()).any(|w| w == name_bytes)
        }

        let mut seq = Sequence::first();

        // Helper to send a request and receive response
        let mut send_and_recv = |sql: &str, state: Option<String>| -> ResponseMessage {
            let key = DataRoutingKey {
                session: session.clone(),
                branch,
                submission: submission.clone(),
                kind: DataMessageKind::Request {
                    agent: agent.clone(),
                    service,
                    operation: Operation::Execute,
                    sequence: seq,
                },
            };
            let msg = RequestMessage {
                id: MessageId::new(),
                dag_id: DagNodeId::root(),
                state,
                diagnostics: RequestDiagnostics {
                    sequence: seq.as_u32(),
                    endpoint: String::new(),
                    request_bytes: 0,
                    received_at_ms: 0,
                },
                payload: make_query(sql),
                checkpoint: None,
            };
            queue.send_request(key.clone(), msg).unwrap();
            assert!(worker.tick());
            let (_, resp, ack) = queue
                .receive_response(&submission, &agent, service, Operation::Execute, seq)
                .unwrap();
            ack().unwrap();
            seq = seq.next();
            resp
        };

        // --- Request 1: SET datestyle TO 'ISO' ---
        let resp1 = send_and_recv("SET datestyle TO 'ISO'", None);
        let tags1 = parse_command_tags(&resp1.payload);
        assert!(
            tags1.iter().any(|t| t == "SET"),
            "expected SET, got: {tags1:?}"
        );
        assert!(!has_column(&resp1.payload, "hashof"));

        // --- Request 2: CREATE TABLE ---
        let resp2 = send_and_recv(
            "CREATE TABLE IF NOT EXISTS todos (id SERIAL PRIMARY KEY, text TEXT NOT NULL, done BOOLEAN DEFAULT FALSE)",
            resp1.state.clone(),
        );
        let tags2 = parse_command_tags(&resp2.payload);
        assert!(
            tags2.iter().any(|t| t.starts_with("CREATE")),
            "expected CREATE, got: {tags2:?}"
        );
        assert!(!has_column(&resp2.payload, "hashof"));

        // --- Request 3: INSERT ---
        let resp3 = send_and_recv(
            "INSERT INTO todos (text) VALUES ('buy milk')",
            resp2.state.clone(),
        );
        let tags3 = parse_command_tags(&resp3.payload);
        assert!(
            tags3.iter().any(|t| t.starts_with("INSERT")),
            "expected INSERT, got: {tags3:?}"
        );

        // --- Request 4: SELECT ---
        let resp4 = send_and_recv(
            "SELECT id, text, done FROM todos ORDER BY id",
            resp3.state.clone(),
        );
        let tags4 = parse_command_tags(&resp4.payload);
        assert!(
            tags4.iter().any(|t| t.starts_with("SELECT")),
            "expected SELECT, got: {tags4:?}"
        );
        assert!(has_column(&resp4.payload, "id"));
        assert!(!has_column(&resp4.payload, "hashof"));
    }
}
