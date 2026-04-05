//! Sidecar-side postgres proxy — accepts agent connections, fakes the startup
//! handshake, frames query batches, and bridges them to the queue.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, RwLock};

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue,
    Operation, RequestDiagnostics, RequestMessage, Sequence, ServiceBackend, SessionId,
    SqlStorageType, SubmissionId,
};

use crate::wire;

/// Build the fake startup handshake response that makes the agent
/// believe it connected to a real Postgres server.
///
/// Sequence: AuthenticationOk → ReadyForQuery(Idle)
/// Minimal — just enough for libpq/psycopg2/pgx to proceed.
pub fn build_startup_response() -> Vec<u8> {
    let mut buf = Vec::new();

    // AuthenticationOk: tag 'R', length 8, auth type 0
    buf.push(b'R');
    buf.extend_from_slice(&8_i32.to_be_bytes());
    buf.extend_from_slice(&0_i32.to_be_bytes());

    // ParameterStatus: 'client_encoding' 'UTF8' (Tag 'S', length 25)
    buf.push(b'S');
    buf.extend_from_slice(&25_i32.to_be_bytes());
    buf.extend_from_slice(b"client_encoding\0UTF8\0");

    // ReadyForQuery: tag 'Z', length 5, status 'I' (idle)
    buf.push(b'Z');
    buf.extend_from_slice(&5_i32.to_be_bytes());
    buf.push(b'I');

    buf
}

/// SSL request magic number (80877103).
const SSL_REQUEST_CODE: i32 = 80_877_103;

/// Consume the startup phase: handle optional SSLRequest, then read and
/// discard the real StartupMessage.
///
/// Postgres clients (libpq, psycopg2) typically send an SSLRequest first.
/// We reject SSL with 'N', then the client retries with the real StartupMessage.
fn consume_startup(stream: &mut (impl Read + Write)) -> Result<(), SidecarError> {
    loop {
        let mut header = [0u8; 8];
        stream
            .read_exact(&mut header)
            .map_err(|e| SidecarError::Io(format!("read startup header: {e}")))?;

        let len = i32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let code = i32::from_be_bytes([header[4], header[5], header[6], header[7]]);

        if code == SSL_REQUEST_CODE {
            stream
                .write_all(b"N")
                .map_err(|e| SidecarError::Io(format!("write SSL rejection: {e}")))?;
            continue;
        }

        if len > 8 {
            let remaining = len - 8;
            let mut body = vec![0u8; remaining];
            stream
                .read_exact(&mut body)
                .map_err(|e| SidecarError::Io(format!("read startup body: {e}")))?;
        }

        return Ok(());
    }
}

/// Handle one agent postgres session on the given stream.
///
/// 1. Consume the StartupMessage
/// 2. Send the fake handshake (AuthOk + ReadyForQuery)
/// 3. Loop: read query batch → send over queue → receive response → write back
#[allow(clippy::too_many_arguments)]
pub fn handle_session(
    stream: &mut (impl Read + Write),
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    session: SessionId,
    branch: BranchId,
    agent_id: AgentName,
    submission: SubmissionId,
    state: Option<String>,
    shared_state: Option<Arc<RwLock<Option<String>>>>,
) -> Result<(), SidecarError> {
    // Step 1: handle SSLRequest (if any) and consume the startup message
    consume_startup(stream)?;

    // Step 2: send fake handshake
    let handshake = build_startup_response();
    stream
        .write_all(&handshake)
        .map_err(|e| SidecarError::Io(format!("write handshake: {e}")))?;

    // Step 3: query batch loop
    let mut read_buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let mut sequence = Sequence::first();
    let service = ServiceBackend::Sql(SqlStorageType::Doltgres);
    let mut current_state = state;

    loop {
        // Read from agent
        let n = stream
            .read(&mut tmp)
            .map_err(|e| SidecarError::Io(format!("read: {e}")))?;
        if n == 0 {
            return Ok(()); // Agent disconnected
        }
        read_buf.extend_from_slice(&tmp[..n]);

        // Extract complete query batches
        while let Some(batch) = wire::extract_query_batch(&read_buf)
            .map_err(|e| SidecarError::Protocol(format!("frame: {e}")))?
        {
            let payload = read_buf[..batch.bytes_consumed].to_vec();
            read_buf.drain(..batch.bytes_consumed);

            // Send request over queue
            let key = DataRoutingKey {
                session: session.clone(),
                branch,
                submission: submission.clone(),
                kind: DataMessageKind::Request {
                    agent: agent_id.clone(),
                    service,
                    operation: Operation::Execute,
                    sequence,
                },
            };
            let request = RequestMessage {
                id: MessageId::new(),
                dag_id: DagNodeId::root(),
                state: current_state.clone(),
                diagnostics: RequestDiagnostics {
                    sequence: sequence.as_u32(),
                    endpoint: crate::HOSTNAME.to_string(),
                    request_bytes: 0,
                    received_at_ms: 0,
                },
                payload,
                checkpoint: None,
            };

            let response = queue
                .call_service(key, request)
                .map_err(|e| SidecarError::Queue(format!("{e}")))?;

            // Track latest state hash from DoltWorker responses
            if let Some(ref new_state) = response.state {
                current_state = Some(new_state.clone());
                if let Some(ref shared) = shared_state {
                    *shared.write().unwrap() = Some(new_state.clone());
                }
            }

            // Write backend response bytes back to agent
            stream
                .write_all(&response.payload)
                .map_err(|e| SidecarError::Io(format!("write response: {e}")))?;

            sequence = sequence.next();
        }
    }
}

/// Like `handle_session`, but reads session context from a shared `Arc<RwLock>`
/// on each query batch. This allows the proxy to serve a long-lived TCP
/// connection across multiple invocations — when a fork changes the branch
/// and state, the next query picks up the updated values.
pub fn handle_session_shared(
    stream: &mut (impl Read + Write),
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    context: Arc<RwLock<SessionContext>>,
) -> Result<(), SidecarError> {
    consume_startup(stream)?;

    let handshake = build_startup_response();
    stream
        .write_all(&handshake)
        .map_err(|e| SidecarError::Io(format!("write handshake: {e}")))?;

    let mut read_buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let mut sequence = Sequence::first();
    let service = ServiceBackend::Sql(SqlStorageType::Doltgres);

    // Seed current_state from the context at connection start.
    let mut current_state = context.read().unwrap().state.clone();

    loop {
        let n = stream
            .read(&mut tmp)
            .map_err(|e| SidecarError::Io(format!("read: {e}")))?;
        if n == 0 {
            return Ok(());
        }
        read_buf.extend_from_slice(&tmp[..n]);

        while let Some(batch) = wire::extract_query_batch(&read_buf)
            .map_err(|e| SidecarError::Protocol(format!("frame: {e}")))?
        {
            let payload = read_buf[..batch.bytes_consumed].to_vec();
            read_buf.drain(..batch.bytes_consumed);

            // Read CURRENT context — may have been updated by a new invocation.
            let ctx = context.read().unwrap().clone();

            // If context changed (new invocation), adopt the new state and
            // reset sequence. This happens when a fork invocation updates the
            // context while the agent's TCP connection is still open.
            if ctx.state != current_state {
                current_state = ctx.state.clone();
                sequence = Sequence::first();
            }

            let key = DataRoutingKey {
                session: ctx.session.clone(),
                branch: ctx.branch,
                submission: ctx.submission.clone(),
                kind: DataMessageKind::Request {
                    agent: ctx.agent_id.clone(),
                    service,
                    operation: Operation::Execute,
                    sequence,
                },
            };
            let request = RequestMessage {
                id: MessageId::new(),
                dag_id: DagNodeId::root(),
                state: current_state.clone(),
                diagnostics: RequestDiagnostics {
                    sequence: sequence.as_u32(),
                    endpoint: crate::HOSTNAME.to_string(),
                    request_bytes: 0,
                    received_at_ms: 0,
                },
                payload,
                checkpoint: None,
            };

            let response = queue
                .call_service(key, request)
                .map_err(|e| SidecarError::Queue(format!("{e}")))?;

            if let Some(ref new_state) = response.state {
                current_state = Some(new_state.clone());
                // Update the shared state so the CompleteMessage carries the
                // latest hash. Read the CURRENT shared_state from context in
                // case a new invocation swapped it.
                let ctx = context.read().unwrap();
                *ctx.shared_state.write().unwrap() = Some(new_state.clone());
            }

            stream
                .write_all(&response.payload)
                .map_err(|e| SidecarError::Io(format!("write response: {e}")))?;

            sequence = sequence.next();
        }
    }
}

/// Session context for a postgres proxy connection.
#[derive(Clone)]
pub struct SessionContext {
    pub session: SessionId,
    pub branch: BranchId,
    pub agent_id: AgentName,
    pub submission: SubmissionId,
    pub state: Option<String>,
    /// Updated with the latest Dolt commit hash from responses.
    /// Shared with the dispatch layer so the CompleteMessage carries final state.
    pub shared_state: Arc<RwLock<Option<String>>>,
}

/// Start a TCP listener that accepts postgres connections and proxies them
/// through the queue.
///
/// Blocks the calling thread. Each accepted connection runs `handle_session`
/// synchronously — one connection at a time per listener. The caller provides
/// a function that returns the session context for each new connection.
pub fn listen(
    addr: &str,
    queue: Arc<dyn MessageQueue + Send + Sync>,
    context_fn: impl Fn() -> SessionContext,
) -> Result<(), SidecarError> {
    let listener =
        TcpListener::bind(addr).map_err(|e| SidecarError::Io(format!("bind {addr}: {e}")))?;

    for stream in listener.incoming() {
        let mut stream = stream.map_err(|e| SidecarError::Io(format!("accept: {e}")))?;

        let ctx = context_fn();
        if let Err(e) = handle_session(
            &mut stream,
            &queue,
            ctx.session,
            ctx.branch,
            ctx.agent_id,
            ctx.submission,
            ctx.state,
            None,
        ) {
            eprintln!("vlinder-dolt: session error: {e}");
        }
    }

    Ok(())
}

#[derive(Debug)]
pub enum SidecarError {
    Io(String),
    Protocol(String),
    Queue(String),
}

impl std::fmt::Display for SidecarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SidecarError::Io(msg) => write!(f, "io: {msg}"),
            SidecarError::Protocol(msg) => write!(f, "protocol: {msg}"),
            SidecarError::Queue(msg) => write!(f, "queue: {msg}"),
        }
    }
}

impl std::error::Error for SidecarError {}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use postgres_protocol::message::backend;
    use std::net::TcpStream;
    use vlinder_core::queue::InMemoryQueue;

    use crate::worker::{DoltConnection, DoltConnectionFactory, DoltError, DoltWorker};

    /// Build a minimal StartupMessage (no tag byte, just length + protocol version + params).
    fn build_startup_message() -> Vec<u8> {
        let mut buf = BytesMut::new();
        postgres_protocol::message::frontend::startup_message(
            [("user", "agent"), ("database", "test")].iter().copied(),
            &mut buf,
        )
        .unwrap();
        buf.to_vec()
    }

    /// Build a simple Query message: Q + length + "SELECT 1\0"
    fn build_query(sql: &str) -> Vec<u8> {
        let mut buf = BytesMut::new();
        postgres_protocol::message::frontend::query(sql, &mut buf).unwrap();
        buf.to_vec()
    }

    // --- Fake connection that echoes a canned backend response ---

    struct FakeConnection {
        response: Vec<u8>,
    }

    impl DoltConnection for FakeConnection {
        fn forward(&mut self, _frontend_bytes: &[u8]) -> Result<Vec<u8>, DoltError> {
            Ok(self.response.clone())
        }
        fn query_scalar(&mut self, _sql: &str) -> Result<Option<String>, DoltError> {
            Ok(Some("fakehash".to_string()))
        }
        fn execute(&mut self, _sql: &str) -> Result<(), DoltError> {
            Ok(())
        }
    }

    struct FakeFactory {
        response: Vec<u8>,
    }

    impl DoltConnectionFactory for FakeFactory {
        fn connect(&self) -> Result<Box<dyn DoltConnection>, DoltError> {
            Ok(Box::new(FakeConnection {
                response: self.response.clone(),
            }))
        }
    }

    /// Build a fake backend response: CommandComplete("SELECT 1") + ReadyForQuery(Idle)
    fn build_backend_response() -> Vec<u8> {
        let mut buf = Vec::new();

        // CommandComplete: tag 'C', body = "SELECT 1\0"
        let tag = b"SELECT 1\0";
        let len = 4 + tag.len() as i32;
        buf.push(b'C');
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(tag);

        // ReadyForQuery: tag 'Z', length 5, status 'I'
        buf.push(b'Z');
        buf.extend_from_slice(&5_i32.to_be_bytes());
        buf.push(b'I');

        buf
    }

    #[test]
    fn startup_response_parses_as_auth_ok_then_ready() {
        let bytes = build_startup_response();
        let mut buf = BytesMut::from(bytes.as_slice());

        let msg = backend::Message::parse(&mut buf).unwrap().unwrap();
        assert!(
            matches!(msg, backend::Message::AuthenticationOk),
            "expected AuthenticationOk"
        );

        // ParameterStatus(client_encoding=UTF8)
        let msg = backend::Message::parse(&mut buf).unwrap().unwrap();
        assert!(
            matches!(msg, backend::Message::ParameterStatus(_)),
            "expected ParameterStatus"
        );

        let msg = backend::Message::parse(&mut buf).unwrap().unwrap();
        match msg {
            backend::Message::ReadyForQuery(body) => {
                assert_eq!(body.status(), b'I', "expected idle status");
            }
            _ => panic!("expected ReadyForQuery"),
        }

        assert!(
            backend::Message::parse(&mut buf).unwrap().is_none(),
            "should have no trailing bytes"
        );
    }

    #[test]
    fn full_session_roundtrip_through_queue() {
        // Set up: queue + worker with fake Doltgres connection
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let backend_response = build_backend_response();
        let factory = Arc::new(FakeFactory {
            response: backend_response.clone(),
        });
        // Worker stays on main thread (not Send due to RefCell)
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        // Build the agent's byte stream: StartupMessage + Query
        let mut agent_input = Vec::new();
        agent_input.extend_from_slice(&build_startup_message());
        agent_input.extend_from_slice(&build_query("SELECT 1"));

        // Use a Unix socket pair so sidecar can run on a background thread
        let (mut agent_end, mut sidecar_end) = std::os::unix::net::UnixStream::pair().unwrap();

        // Set a read timeout so the worker poll doesn't hang forever
        sidecar_end
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Run sidecar (handle_session) on a background thread
        let queue_for_sidecar = Arc::clone(&queue);
        let sidecar_handle = std::thread::spawn(move || {
            handle_session(
                &mut sidecar_end,
                &queue_for_sidecar,
                SessionId::new(),
                BranchId::from(1),
                AgentName::new("test-agent"),
                SubmissionId::from("sub-1".to_string()),
                None,
                None,
            )
        });

        // Agent side: write startup + query, then read the response
        agent_end.write_all(&agent_input).unwrap();
        // Shut down write half so sidecar sees EOF after processing the query
        agent_end.shutdown(std::net::Shutdown::Write).unwrap();

        // Worker: tick until it processes the request
        loop {
            if worker.tick() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Read everything the sidecar wrote back to the agent
        let mut agent_output = Vec::new();
        agent_end.read_to_end(&mut agent_output).unwrap();

        // Wait for sidecar thread to finish
        let result = sidecar_handle.join().unwrap();
        assert!(
            result.is_ok(),
            "session should complete: {:?}",
            result.err()
        );

        // The output should contain:
        // 1. The fake handshake (AuthOk + ReadyForQuery)
        // 2. The backend response forwarded from the worker
        let handshake = build_startup_response();
        assert!(
            agent_output.starts_with(&handshake),
            "output should start with handshake"
        );

        let response_part = &agent_output[handshake.len()..];
        assert_eq!(
            response_part, &backend_response,
            "response bytes should be forwarded from worker"
        );
    }

    #[test]
    fn handles_ssl_request_before_startup() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let backend_response = build_backend_response();
        let factory = Arc::new(FakeFactory {
            response: backend_response.clone(),
        });
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        // Agent sends: SSLRequest + StartupMessage + Query
        let mut agent_input = Vec::new();
        // SSLRequest: length=8, code=80877103
        agent_input.extend_from_slice(&8_i32.to_be_bytes());
        agent_input.extend_from_slice(&80877103_i32.to_be_bytes());
        agent_input.extend_from_slice(&build_startup_message());
        agent_input.extend_from_slice(&build_query("SELECT 1"));

        let (mut agent_end, mut sidecar_end) = std::os::unix::net::UnixStream::pair().unwrap();
        sidecar_end
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        let queue_for_sidecar = Arc::clone(&queue);
        let sidecar_handle = std::thread::spawn(move || {
            handle_session(
                &mut sidecar_end,
                &queue_for_sidecar,
                SessionId::new(),
                BranchId::from(1),
                AgentName::new("test-agent"),
                SubmissionId::from("sub-1".to_string()),
                None,
                None,
            )
        });

        agent_end.write_all(&agent_input).unwrap();
        agent_end.shutdown(std::net::Shutdown::Write).unwrap();

        loop {
            if worker.tick() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let mut agent_output = Vec::new();
        agent_end.read_to_end(&mut agent_output).unwrap();

        let result = sidecar_handle.join().unwrap();
        assert!(
            result.is_ok(),
            "session should complete: {:?}",
            result.err()
        );

        // First byte should be 'N' (SSL rejected), then handshake, then response
        assert_eq!(agent_output[0], b'N', "should reject SSL with 'N'");

        let after_ssl = &agent_output[1..];
        let handshake = build_startup_response();
        assert!(
            after_ssl.starts_with(&handshake),
            "handshake should follow SSL rejection"
        );

        let response_part = &after_ssl[handshake.len()..];
        assert_eq!(response_part, &backend_response);
    }

    /// Reproduces the "got message type 'c'" bug: DoltWorker returns plain text
    /// error bytes when connection creation fails, psycopg2 loses sync.
    ///
    /// Run with: cargo test -p vlinder-dolt -- --ignored message_type_c
    #[test]
    #[ignore]
    fn connection_error_returns_proper_error_response() {
        // FakeFactory that always fails on connect — simulates branch creation failure
        struct FailingFactory;
        impl DoltConnectionFactory for FailingFactory {
            fn connect(&self) -> Result<Box<dyn DoltConnection>, crate::worker::DoltError> {
                Err(crate::worker::DoltError::Connection(
                    "branch already exists".into(),
                ))
            }
        }

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let factory = Arc::new(FailingFactory);
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = tcp_listener.local_addr().unwrap().port();

        let queue_for_proxy = Arc::clone(&queue);
        let proxy_handle = std::thread::spawn(move || {
            let (mut stream, _) = tcp_listener.accept().unwrap();
            handle_session(
                &mut stream,
                &queue_for_proxy,
                SessionId::new(),
                BranchId::from(1),
                AgentName::new("test-agent"),
                SubmissionId::from("sub-1".to_string()),
                None,
                None,
            )
        });

        let py_handle = std::thread::spawn(move || {
            std::process::Command::new("python3")
                .arg("-c")
                .arg(format!(r#"
import psycopg2, sys

try:
    conn = psycopg2.connect("host=127.0.0.1 port={port} user=postgres password=password dbname=test")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1")
    print("UNEXPECTED: query succeeded")
except Exception as e:
    err = str(e)
    print(f"ERROR: {{type(e).__name__}}: {{err}}")
    if "message type" in err.lower() or "lost synchronization" in err.lower():
        print("BUG: plain text error caused protocol desync")
    elif "connection error" in err.lower() or "branch already exists" in err.lower():
        print("FIXED: got proper ErrorResponse with message")
    else:
        print(f"DIFFERENT ERROR")
"#))
                .output()
                .expect("failed to run python3")
        });

        // Tick worker until python finishes
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while std::time::Instant::now() < deadline {
            worker.tick();
            if py_handle.is_finished() {
                for _ in 0..50 {
                    worker.tick();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let py_result = py_handle.join().unwrap();
        let stdout = String::from_utf8_lossy(&py_result.stdout);
        let stderr = String::from_utf8_lossy(&py_result.stderr);
        eprintln!("=== stdout ===\n{stdout}");
        eprintln!("=== stderr ===\n{stderr}");

        let _ = proxy_handle.join();

        assert!(
            stdout.contains("FIXED"),
            "Expected proper ErrorResponse, not protocol desync.\nstdout: {stdout}\nstderr: {stderr}"
        );
    }

    /// Full end-to-end: psycopg2 → sidecar proxy → InMemoryQueue → DoltWorker → real Doltgres.
    /// This is the definitive test for the proxy path.
    ///
    /// Run with: cargo test -p vlinder-dolt -- --ignored psycopg2_through_proxy
    #[test]
    #[ignore]
    fn psycopg2_through_proxy_to_real_doltgres() {
        use crate::connection::TcpConnectionFactory;

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let factory = Arc::new(TcpConnectionFactory::new(
            "localhost:5433",
            "postgres",
            "postgres",
        ));
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        // Bind proxy to a random port
        let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = tcp_listener.local_addr().unwrap();
        let port = addr.port();

        // Start proxy on a background thread
        let queue_for_proxy = Arc::clone(&queue);
        let proxy_handle = std::thread::spawn(move || {
            let (mut stream, _) = tcp_listener.accept().unwrap();
            handle_session(
                &mut stream,
                &queue_for_proxy,
                SessionId::new(),
                BranchId::from(1),
                AgentName::new("test-agent"),
                SubmissionId::from("sub-1".to_string()),
                None,
                None,
            )
        });

        // Run psycopg2 on a background thread (worker must stay on main thread — not Send)
        let dsn =
            format!("host=127.0.0.1 port={port} user=postgres password=password dbname=vlinder");
        let py_handle = std::thread::spawn(move || {
            std::process::Command::new("python3")
                .arg("-c")
                .arg(format!(r#"
import psycopg2, sys

conn = psycopg2.connect("{dsn}")
conn.autocommit = True

def safe_execute(conn, sql):
    with conn.cursor() as cur:
        try:
            cur.execute(sql)
        except psycopg2.DatabaseError as e:
            print(f"[safe_execute] caught {{type(e).__name__}}: {{e}} for: {{sql[:80]}}", file=sys.stderr)

print("1. CREATE TABLE")
safe_execute(conn, """
    CREATE TABLE IF NOT EXISTS todos (
        id SERIAL PRIMARY KEY,
        text TEXT NOT NULL,
        done BOOLEAN DEFAULT FALSE
    )
""")
print("   OK")

print("2. INSERT")
safe_execute(conn, "INSERT INTO todos (text) VALUES ('buy milk')")
print("   OK")

print("3. SELECT")
with conn.cursor() as cur:
    cur.execute("SELECT id, text, done FROM todos ORDER BY id")
    rows = cur.fetchall()
    print(f"   rows: {{rows}}")
    assert len(rows) >= 1, f"Expected at least 1 row, got {{len(rows)}}"
    for row in rows:
        assert len(row) == 3, f"Expected 3 columns, got {{len(row)}}: {{row}}"
        print(f"   id={{row[0]}} text={{row[1]}} done={{row[2]}}")

conn.close()
print("ALL PASSED")
"#))
                .output()
                .expect("failed to run python3")
        });

        // Main thread: tick the worker until psycopg2 finishes
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            worker.tick();
            if py_handle.is_finished() {
                // Drain remaining ticks
                for _ in 0..100 {
                    worker.tick();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        let py_result = py_handle.join().unwrap();
        let stdout = String::from_utf8_lossy(&py_result.stdout);
        let stderr = String::from_utf8_lossy(&py_result.stderr);

        eprintln!("=== psycopg2 stdout ===\n{stdout}");
        eprintln!("=== psycopg2 stderr ===\n{stderr}");

        // Wait for proxy to finish
        let proxy_result = proxy_handle.join().unwrap();
        assert!(
            proxy_result.is_ok(),
            "proxy session failed: {:?}",
            proxy_result.err()
        );

        assert!(
            py_result.status.success(),
            "psycopg2 test failed (exit {}):\nstdout: {stdout}\nstderr: {stderr}",
            py_result.status
        );
        assert!(
            stdout.contains("ALL PASSED"),
            "psycopg2 test did not complete:\nstdout: {stdout}\nstderr: {stderr}"
        );
    }

    #[test]
    fn tcp_listener_accepts_connection_and_proxies() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let backend_response = build_backend_response();
        let factory = Arc::new(FakeFactory {
            response: backend_response.clone(),
        });
        let worker = DoltWorker::new(Arc::clone(&queue), factory);

        // Bind to port 0 to get a random available port
        let tcp_listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = tcp_listener.local_addr().unwrap();

        // Start listener on a background thread (move the bound listener)
        let queue_for_listener = Arc::clone(&queue);
        let listener_handle = std::thread::spawn(move || {
            let (mut stream, _) = tcp_listener.accept().unwrap();
            handle_session(
                &mut stream,
                &queue_for_listener,
                SessionId::new(),
                BranchId::from(1),
                AgentName::new("test-agent"),
                SubmissionId::from("sub-1".to_string()),
                None,
                None,
            )
        });

        // Connect as the agent
        let mut client = TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();

        // Send startup + query
        client.write_all(&build_startup_message()).unwrap();
        client.write_all(&build_query("SELECT 1")).unwrap();
        client.shutdown(std::net::Shutdown::Write).unwrap();

        // Worker ticks to process the request
        loop {
            if worker.tick() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }

        // Read response
        let mut output = Vec::new();
        client.read_to_end(&mut output).unwrap();

        listener_handle.join().unwrap().unwrap();

        // Verify: handshake + backend response
        let handshake = build_startup_response();
        assert!(output.starts_with(&handshake));
        assert_eq!(&output[handshake.len()..], &backend_response);
    }
}
