//! NATS integration test — exercises the actual production path:
//! Sidecar → NATS → DoltWorker → Doltgres → NATS → Sidecar
//!
//! Requires: NATS on localhost:4222, Doltgres on localhost:5433
//! Run with: cargo test -p vlinder-dolt --test nats_integration -- --ignored --nocapture

use std::sync::Arc;

use bytes::BytesMut;
use postgres_protocol::message::{backend, frontend};

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue,
    Operation, QueueError, RequestDiagnostics, RequestMessage, Sequence, ServiceBackend, SessionId,
    SqlStorageType, SubmissionId,
};
use vlinder_dolt::{DoltWorker, TcpConnectionFactory};
use vlinder_nats::{NatsConfig, NatsQueue};

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

/// The full production path through NATS + Doltgres.
///
/// This is the ONLY test that exercises the actual message delivery path.
/// If this fails but the InMemoryQueue tests pass, the bug is in NATS delivery.
#[test]
#[ignore]
fn nats_doltgres_round_trip_no_pipeline_offset() {
    let config = NatsConfig {
        url: "nats://localhost:4222".to_string(),
        creds_file: None,
        creds_content: None,
    };

    // Two separate NATS connections — one acts as sidecar, one as worker
    let sidecar_queue: Arc<dyn MessageQueue + Send + Sync> =
        Arc::new(NatsQueue::connect(&config).expect("sidecar NATS connect failed"));
    let worker_queue: Arc<dyn MessageQueue + Send + Sync> =
        Arc::new(NatsQueue::connect(&config).expect("worker NATS connect failed"));

    let factory = Arc::new(TcpConnectionFactory::new(
        "localhost:5433",
        "postgres",
        "postgres",
    ));
    let worker = DoltWorker::new(Arc::clone(&worker_queue), factory);

    let session = SessionId::new();
    let branch = BranchId::from(1);
    let submission = SubmissionId::from("nats-test-sub".to_string());
    let agent = AgentName::new("test-agent");
    let service = ServiceBackend::Sql(SqlStorageType::Doltgres);

    // Replicate the exact agent SQL sequence from the todoapp-dolt agent
    let queries = vec![
        ("SET datestyle TO 'ISO'", "SET"),
        ("BEGIN", "BEGIN"),
        (
            "CREATE TABLE IF NOT EXISTS todos (id SERIAL PRIMARY KEY, text TEXT NOT NULL, done BOOLEAN DEFAULT FALSE)",
            "CREATE TABLE",
        ),
        ("COMMIT", "COMMIT"),
        ("BEGIN", "BEGIN"),
        ("INSERT INTO todos (text) VALUES ('buy milk')", "INSERT"),
        ("COMMIT", "COMMIT"),
        ("BEGIN", "BEGIN"),
        ("SELECT id, text, done FROM todos ORDER BY id", "SELECT"),
    ];

    let mut sequence = Sequence::first();
    let mut current_state: Option<String> = None;

    for (i, (sql, expected_tag)) in queries.iter().enumerate() {
        let payload = make_query(sql);

        let key = DataRoutingKey {
            session: session.clone(),
            branch,
            submission: submission.clone(),
            kind: DataMessageKind::Request {
                agent: agent.clone(),
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
                endpoint: String::new(),
                request_bytes: 0,
                received_at_ms: 0,
            },
            payload,
            checkpoint: None,
        };

        // Sidecar sends request
        sidecar_queue
            .send_request(key.clone(), request)
            .expect("send_request failed");

        // Worker processes (might need a few ticks due to NATS latency)
        let mut processed = false;
        for _ in 0..100 {
            if worker.tick() {
                processed = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        assert!(processed, "worker did not process request {i} ({sql})");

        // Sidecar receives response
        let mut response = None;
        for _ in 0..100 {
            match sidecar_queue.receive_response(
                &submission,
                &agent,
                service,
                Operation::Execute,
                sequence,
            ) {
                Ok((_rkey, resp, ack)) => {
                    ack().unwrap();
                    response = Some(resp);
                    break;
                }
                Err(QueueError::Timeout) => {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                }
                Err(e) => panic!("receive_response failed for request {i}: {e}"),
            }
        }
        let resp = response.expect(&format!("no response for request {i} ({sql})"));

        // Check the response payload
        let tags = parse_command_tags(&resp.payload);
        let has_hashof = has_column(&resp.payload, "hashof");

        // Verify no HASHOF contamination
        assert!(
            !has_hashof,
            "Request {i} ({sql}) response contains HASHOF data — pipeline offset bug! tags={tags:?}"
        );

        // Verify the response contains the expected command tag
        if *expected_tag == "SELECT" {
            // SELECT returns RowDescription, not just CommandComplete
            assert!(
                tags.iter().any(|t| t.starts_with("SELECT")),
                "Request {i} ({sql}) should have SELECT tag, got: {tags:?}"
            );
        } else if *expected_tag == "INSERT" {
            assert!(
                tags.iter().any(|t| t.starts_with("INSERT")),
                "Request {i} ({sql}) should have INSERT tag, got: {tags:?}"
            );
        } else if *expected_tag == "CREATE TABLE" {
            assert!(
                tags.iter().any(|t| t.starts_with("CREATE")),
                "Request {i} ({sql}) should have CREATE tag, got: {tags:?}"
            );
        } else {
            assert!(
                tags.iter().any(|t| t == *expected_tag),
                "Request {i} ({sql}) should have {expected_tag} tag, got: {tags:?}"
            );
        }

        current_state = resp.state.clone();
        sequence = sequence.next();
    }
}
