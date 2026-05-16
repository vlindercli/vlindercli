//! gRPC roundtrip tests for node-getter RPCs (4.3).
//!
//! Each test starts a real gRPC state service backed by `SqliteDagStore`,
//! inserts a known-good row through the store, then reads it back through
//! the gRPC client. This catches proto-schema regressions that SQL-store
//! unit tests and e2e tests would miss.

use std::sync::Arc;

use tokio::sync::oneshot;
use tonic::transport::Server;

use crate::dag_store::SqliteDagStore;
use crate::state_service::proto::state_service_client::StateServiceClient;
use crate::state_service::proto::{
    GetCompleteMessageRequest, GetCompleteNodeRequest, GetInvokeMessageRequest,
    GetRequestNodeRequest, GetRequestV2Request, GetResponseNodeRequest, GetResponseV2Request,
    GetSvcRequestNodeRequest, GetSvcResponseNodeRequest, LatestNodesOnBranchRequest,
};
use crate::state_service::StateServiceServer;

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, DagStore, HarnessType, MessageId, Operation, Sequence,
    ServiceBackend, ServiceBackendV2, ServiceOperation, SessionId, Snapshot, SubmissionId,
    ToolCallId,
};

/// Start a gRPC state service on a random port, returning a connected
/// client and a shutdown sender.
async fn grpc_test_setup() -> (
    StateServiceClient<tonic::transport::Channel>,
    Arc<SqliteDagStore>,
    oneshot::Sender<()>,
) {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("test.db");
    let store = Arc::new(SqliteDagStore::open(&path).unwrap());

    // Create session + default branch to satisfy FK constraints.
    let session_id =
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
    let external_id = vlinder_core::domain::ExternalSessionId::new("test-ext-id").unwrap();
    let session = vlinder_core::domain::session::Session {
        id: session_id.clone(),
        external_id,
        name: "test-session".to_string(),
        agent: "agent-a".to_string(),
        default_branch: BranchId::from(1),
        created_at: chrono::Utc::now(),
    };
    store.create_session(&session).await.unwrap();
    store
        .create_branch("main", &session_id, None)
        .await
        .unwrap();

    // Start gRPC server on a random port.
    // We bind the listener first so the port is known, then pass it to
    // tonic via serve_with_incoming (which requires the `tokio-stream`
    // crate). Since tokio-stream may not be available, use a bind-then-
    // serve pattern with a retry on the client side.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Drop the listener — tonic will rebind via serve_with_shutdown,
    // which is fine since the port was already reserved (the OS may
    // reassign it, but we retry the client connect to handle the race).
    drop(listener);

    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let svc = StateServiceServer::new(store.clone()).into_service();

    tokio::spawn(async move {
        Server::builder()
            .add_service(svc)
            .serve_with_shutdown(addr, async {
                shutdown_rx.await.ok();
            })
            .await
            .ok();
    });

    // Retry connecting until the server is ready
    let addr_str = format!("http://127.0.0.1:{port}");
    let client = loop {
        match StateServiceClient::connect(addr_str.clone()).await {
            Ok(c) => break c,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(30)).await;
            }
        }
    };
    (client, store, shutdown_tx)
}

fn sess_id() -> SessionId {
    SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
}

fn sub_id() -> SubmissionId {
    SubmissionId::from("sub-1".to_string())
}

// ============================================================================
// GetCompleteNode
// ============================================================================

#[tokio::test]
async fn grpc_get_complete_node_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let harness = HarnessType::Grpc;
    let dag_id = DagNodeId::from("grpc-complete-1".to_string());

    let msg = vlinder_core::domain::CompleteMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        state: Some("done".to_string()),
        diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(100),
        content: Some("result".to_string()),
        tool_calls: None,
        payload: b"output".to_vec(),
    };

    store
        .insert_complete_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            harness,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_complete_node(GetCompleteNodeRequest {
            dag_hash: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let node = response.node.expect("Some, not None");
    assert_eq!(node.agent, "test-agent");
    assert_eq!(node.harness, "grpc");
    assert_eq!(node.session_id, session.to_string());
    assert_eq!(node.branch, branch.as_i64());
    assert_eq!(node.state, Some("done".to_string()));
}

// ============================================================================
// GetRequestNode
// ============================================================================

#[tokio::test]
async fn grpc_get_request_node_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let service = ServiceBackend::Infer(vlinder_core::domain::InferenceBackendType::OpenRouter);
    let operation = Operation::Get;
    let sequence = Sequence::first();
    let dag_id = DagNodeId::from("grpc-request-1".to_string());

    let msg = vlinder_core::domain::RequestMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        state: None,
        diagnostics: vlinder_core::domain::RequestDiagnostics {
            sequence: 1,
            endpoint: "infer".to_string(),
            request_bytes: 100,
            received_at_ms: 0,
        },
        payload: b"hello".to_vec(),
        checkpoint: None,
    };

    store
        .insert_request_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            service,
            operation,
            sequence,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_request_node(GetRequestNodeRequest {
            dag_hash: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let node = response.node.expect("Some, not None");
    assert_eq!(node.agent, "test-agent");
    assert_eq!(node.session_id, session.to_string());
    assert_eq!(node.branch, branch.as_i64());
    assert_eq!(node.message_id, msg.id.to_string());
}

// ============================================================================
// GetResponseNode
// ============================================================================

#[tokio::test]
async fn grpc_get_response_node_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let service = ServiceBackend::Infer(vlinder_core::domain::InferenceBackendType::OpenRouter);
    let operation = Operation::Get;
    let sequence = Sequence::first();
    let dag_id = DagNodeId::from("grpc-response-1".to_string());

    let msg = vlinder_core::domain::ResponseMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        correlation_id: MessageId::new(),
        state: Some("done".to_string()),
        diagnostics: vlinder_core::domain::ServiceDiagnostics::storage(
            vlinder_core::domain::ServiceType::Kv,
            "test",
            Operation::Get,
            100,
            0,
        ),
        payload: b"result".to_vec(),
        status_code: 200,
        checkpoint: None,
    };

    store
        .insert_response_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            service,
            operation,
            sequence,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_response_node(GetResponseNodeRequest {
            dag_hash: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let node = response.node.expect("Some, not None");
    assert_eq!(node.agent, "test-agent");
    assert_eq!(node.session_id, session.to_string());
    assert_eq!(node.branch, branch.as_i64());
    assert_eq!(node.message_id, msg.id.to_string());
}

// ============================================================================
// GetSvcRequestNode
// ============================================================================

#[tokio::test]
async fn grpc_get_svc_request_node_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let service = ServiceBackendV2::Mcp("brave".to_string());
    let operation = ServiceOperation::new("echo");
    let sequence = Sequence::first();
    let dag_id = DagNodeId::from("grpc-svc-req-1".to_string());

    let msg = vlinder_core::domain::RequestV2 {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        tool_call_id: ToolCallId::new(),
        state: None,
        diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
        payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
    };

    store
        .insert_svc_request_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            service.clone(),
            operation.clone(),
            sequence,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_svc_request_node(GetSvcRequestNodeRequest {
            dag_hash: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let node = response.node.expect("Some, not None");
    // Check routing-key fields
    assert_eq!(node.agent, "test-agent");
    assert_eq!(node.session_id, session.to_string());
    assert_eq!(node.branch, branch.as_i64());
    assert_eq!(node.service_type, "mcp");
    assert_eq!(node.service_backend, "brave");
    assert_eq!(node.operation, "echo");
    assert_eq!(node.sequence, 1u32);
    // Check message fields
    assert_eq!(node.message_id, msg.id.to_string());
    assert_eq!(node.tool_call_id, msg.tool_call_id.to_string());
}

// ============================================================================
// LatestNodesOnBranch
// ============================================================================

#[tokio::test]
async fn grpc_latest_nodes_on_branch_returns_n_most_recent() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let branch_id: i64 = 1;
    let session = sess_id();
    let submission = sub_id();
    let agent = AgentName::new("test-agent");
    let harness = HarnessType::Grpc;

    // Insert 5 nodes in a chain with staggered timestamps
    let mut parent = DagNodeId::root();
    for i in 0..5 {
        let dag_id = DagNodeId::from(format!("grpc-latest-n-{i}"));
        let msg = vlinder_core::domain::CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some(format!("state-{i}")),
            diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(100),
            content: Some(format!("result-{i}")),
            tool_calls: None,
            payload: format!("payload-{i}").into_bytes(),
        };

        store
            .insert_complete_node(
                &dag_id,
                &parent,
                chrono::Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                BranchId::from(branch_id),
                &agent,
                harness,
                &msg,
            )
            .await
            .unwrap();
        parent = dag_id;
        // Stagger timestamps
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let response = client
        .latest_nodes_on_branch(LatestNodesOnBranchRequest { branch_id, n: 3 })
        .await
        .expect("RPC must succeed")
        .into_inner();

    assert_eq!(response.nodes.len(), 3);
    // Order is oldest-first
    assert!(response.nodes[0].created_at <= response.nodes[1].created_at);
    assert!(response.nodes[1].created_at <= response.nodes[2].created_at);
    // All belong to the right branch
    for node in &response.nodes {
        assert_eq!(node.branch_id, branch_id);
    }
}

#[tokio::test]
async fn grpc_latest_nodes_on_branch_n_larger_than_chain_returns_all() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let branch_id: i64 = 1;
    let session = sess_id();
    let submission = sub_id();
    let agent = AgentName::new("test-agent");
    let harness = HarnessType::Grpc;

    let mut parent = DagNodeId::root();
    for i in 0..3 {
        let dag_id = DagNodeId::from(format!("grpc-latest-n-all-{i}"));
        let msg = vlinder_core::domain::CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some(format!("state-{i}")),
            diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(100),
            content: Some(format!("result-{i}")),
            tool_calls: None,
            payload: format!("payload-{i}").into_bytes(),
        };

        store
            .insert_complete_node(
                &dag_id,
                &parent,
                chrono::Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                BranchId::from(branch_id),
                &agent,
                harness,
                &msg,
            )
            .await
            .unwrap();
        parent = dag_id;
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }

    let response = client
        .latest_nodes_on_branch(LatestNodesOnBranchRequest { branch_id, n: 100 })
        .await
        .expect("RPC must succeed")
        .into_inner();

    assert_eq!(response.nodes.len(), 3);
}

#[tokio::test]
async fn grpc_latest_nodes_on_branch_unknown_branch_returns_empty() {
    let (mut client, _store, _shutdown) = grpc_test_setup().await;

    let response = client
        .latest_nodes_on_branch(LatestNodesOnBranchRequest {
            branch_id: 999,
            n: 3,
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    assert!(response.nodes.is_empty());
}

// ============================================================================
// GetSvcResponseNode
// ============================================================================

#[tokio::test]
async fn grpc_get_svc_response_node_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let service = ServiceBackendV2::Mcp("brave".to_string());
    let operation = ServiceOperation::new("echo");
    let sequence = Sequence::first();
    let dag_id = DagNodeId::from("grpc-svc-res-1".to_string());

    let msg = vlinder_core::domain::ResponseV2 {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        correlation_id: MessageId::new(),
        state: None,
        diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
        payload: b"result".to_vec(),
    };

    store
        .insert_svc_response_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            service.clone(),
            operation.clone(),
            sequence,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_svc_response_node(GetSvcResponseNodeRequest {
            dag_hash: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let node = response.node.expect("Some, not None");
    // Check routing-key fields
    assert_eq!(node.agent, "test-agent");
    assert_eq!(node.session_id, session.to_string());
    assert_eq!(node.branch, branch.as_i64());
    assert_eq!(node.service_type, "mcp");
    assert_eq!(node.service_backend, "brave");
    assert_eq!(node.operation, "echo");
    assert_eq!(node.sequence, 1u32);
    // Check message fields
    assert_eq!(node.message_id, msg.id.to_string());
    assert_eq!(node.correlation_id, msg.correlation_id.to_string());
    assert_eq!(node.state, msg.state);
}

// ============================================================================
// GetInvokeMessage
// ============================================================================

#[tokio::test]
async fn grpc_get_invoke_message_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let dag_id = DagNodeId::from("grpc-invoke-msg-1".to_string());

    let msg = vlinder_core::domain::InvokeMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        state: Some("abc".to_string()),
        diagnostics: vlinder_core::domain::InvokeDiagnostics {
            harness_version: "0.1.0".to_string(),
        },
        current_input: vec![vlinder_core::domain::Message::User {
            content: "hello".to_string(),
        }],
    };

    let key = vlinder_core::domain::DataRoutingKey {
        session: session.clone(),
        branch,
        submission: submission.clone(),
        kind: vlinder_core::domain::DataMessageKind::Invoke {
            harness: HarnessType::Grpc,
            runtime: vlinder_core::domain::RuntimeType::Container,
            agent: AgentName::new("test-agent"),
        },
    };

    store
        .insert_invoke_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &key,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_invoke_message(GetInvokeMessageRequest {
            dag_node_id: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let out = response.message.expect("Some, not None");
    assert_eq!(out.id, msg.id.to_string());
    assert_eq!(out.state, Some("abc".to_string()));
    assert_eq!(out.dag_parent, DagNodeId::root().to_string());
}

// ============================================================================
// GetCompleteMessage
// ============================================================================

#[tokio::test]
async fn grpc_get_complete_message_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let dag_id = DagNodeId::from("grpc-complete-msg-1".to_string());

    let msg = vlinder_core::domain::CompleteMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        state: Some("done".to_string()),
        diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(100),
        content: Some("final answer".to_string()),
        tool_calls: Some(vec![vlinder_core::domain::ToolCall {
            id: ToolCallId::new(),
            name: "get_weather".to_string(),
            arguments: serde_json::json!({"city": "NYC"}),
        }]),
        payload: b"raw".to_vec(),
    };

    store
        .insert_complete_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            HarnessType::Grpc,
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_complete_message(GetCompleteMessageRequest {
            dag_node_id: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let out = response.message.expect("Some, not None");
    assert_eq!(out.id, msg.id.to_string());
    assert_eq!(out.state, Some("done".to_string()));
    assert_eq!(out.dag_parent, DagNodeId::root().to_string());
}

// ============================================================================
// GetRequestV2
// ============================================================================

#[tokio::test]
async fn grpc_get_request_v2_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let dag_id = DagNodeId::from("grpc-req-v2-1".to_string());

    let msg = vlinder_core::domain::RequestV2 {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        tool_call_id: ToolCallId::new(),
        state: Some("st".to_string()),
        diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
        payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
    };

    store
        .insert_svc_request_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            ServiceBackendV2::Mcp("brave".to_string()),
            ServiceOperation::new("echo"),
            Sequence::first(),
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_request_v2(GetRequestV2Request {
            dag_node_id: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let out = response.message.expect("Some, not None");
    assert_eq!(out.id, msg.id.to_string());
    assert_eq!(out.tool_call_id, msg.tool_call_id.to_string());
    assert_eq!(out.state, Some("st".to_string()));
}

// ============================================================================
// GetResponseV2
// ============================================================================

#[tokio::test]
async fn grpc_get_response_v2_roundtrip() {
    let (mut client, store, _shutdown) = grpc_test_setup().await;
    let session = sess_id();
    let submission = sub_id();
    let branch = BranchId::from(1);
    let agent = AgentName::new("test-agent");
    let dag_id = DagNodeId::from("grpc-resp-v2-1".to_string());

    let msg = vlinder_core::domain::ResponseV2 {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        correlation_id: MessageId::new(),
        state: Some("final".to_string()),
        diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
        payload: b"output".to_vec(),
    };

    store
        .insert_svc_response_node(
            &dag_id,
            &DagNodeId::root(),
            chrono::Utc::now(),
            &Snapshot::empty(),
            &session,
            &submission,
            branch,
            &agent,
            ServiceBackendV2::Mcp("brave".to_string()),
            ServiceOperation::new("echo"),
            Sequence::first(),
            &msg,
        )
        .await
        .unwrap();

    let response = client
        .get_response_v2(GetResponseV2Request {
            dag_node_id: dag_id.to_string(),
        })
        .await
        .expect("RPC must succeed")
        .into_inner();

    let out = response.message.expect("Some, not None");
    assert_eq!(out.id, msg.id.to_string());
    assert_eq!(out.correlation_id, msg.correlation_id.to_string());
    assert_eq!(out.state, Some("final".to_string()));
}
