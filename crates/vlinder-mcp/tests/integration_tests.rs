//! Integration tests for harness‑mediated MCP dispatch.
//!
//! Requires:
//! - A running NATS server (`nats://localhost:4222`)
//! - `npx` installed and network access for `@modelcontextprotocol/server-everything`
//! - The vlinder‑mcp worker (spawned by the test)

use std::time::Duration;

use tokio::time::timeout;
use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, MessageId, MessageQueue, RequestV2, Sequence, ServiceBackendV2,
    ServiceOperation, SessionId, SubmissionId, SvcMessageKind, SvcRoutingKey,
};
use vlinder_nats::{NatsConfig, NatsQueue};

/// Spawn the vlinder‑mcp worker in a background task.
fn spawn_mcp_worker(queue: NatsQueue) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(async move { vlinder_mcp::run_mcp_worker(queue).await })
}

/// Build a V2 service request key for the echo tool.
fn echo_key() -> SvcRoutingKey {
    SvcRoutingKey {
        session: SessionId::new(),
        branch: BranchId::from(1),
        submission: SubmissionId::new(),
        kind: SvcMessageKind::SvcRequest {
            agent: AgentName::new("test_agent"),
            service: ServiceBackendV2::Mcp("server-everything".to_string()),
            operation: ServiceOperation::new("echo"),
            sequence: Sequence::first(),
        },
    }
}

/// Build a `RequestV2` for the echo tool with the given message.
fn echo_request(message: &str) -> RequestV2 {
    RequestV2 {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        tool_call_id: vlinder_core::domain::ToolCallId::new(),
        arguments: serde_json::json!({ "message": message }),
    }
}

#[tokio::test]
#[ignore = "requires npx, NATS server, and network"]
async fn mcp_echo_dispatch_e2e() {
    // 1. Set up NATS connection
    let config = NatsConfig {
        url: "nats://localhost:4222".to_string(),
        creds_file: None,
        creds_content: None,
    };
    let queue = NatsQueue::connect_async(&config)
        .await
        .expect("NATS connection failed");

    // 2. Start vlinder‑mcp worker
    let worker_queue = queue.clone();
    let worker_handle = spawn_mcp_worker(worker_queue);

    // Give the worker a moment to subscribe
    tokio::time::sleep(Duration::from_millis(100)).await;

    // 3. Simulate what the harness does
    let key = echo_key();
    let req = echo_request("hello");
    let req_id = req.id.clone(); // save before moving

    // 4. Send via send_svc_request
    queue
        .send_svc_request(key.clone(), req)
        .await
        .expect("send_svc_request failed");

    // 5. Worker picks it up, calls MCP server, sends ResponseV2
    // 6. Receive via receive_svc_response
    let (resp_key, resp, ack) = timeout(Duration::from_secs(10), queue.receive_svc_response(&key))
        .await
        .expect("receive_svc_response timeout")
        .expect("receive_svc_response error");

    ack().await.expect("ack failed");

    // 7. Assert response matches request
    assert_eq!(resp_key.session, key.session);
    assert_eq!(resp_key.branch, key.branch);
    assert_eq!(resp_key.submission, key.submission);
    assert_eq!(resp.correlation_id, req_id);

    // 8. Assert content contains "hello"
    assert!(
        resp.content.contains("hello"),
        "Expected response to contain 'hello', got: {}",
        resp.content
    );

    // 9. Assert no error
    assert!(!resp.is_error, "Response marked as error: {}", resp.content);

    // Stop the worker (it will exit when the NATS connection drops)
    worker_handle.abort();
    let _ = worker_handle.await;
}
