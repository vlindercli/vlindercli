//! Project DAG chain into conversation history.
//!
//! Walks the chain of nodes on a branch (via `DagStore::latest_nodes_on_branch`)
//! and projects each typed node into a `Message` value that can be used as
//! conversation history for the next invoke.
//!
//! This replaces the old approach of embedding the full `Vec<Message>` history
//! in `InvokeMessage.history`, which caused quadratic storage growth.

use vlinder_core::domain::{BranchId, DagStore, Message, MessageType, SessionId};

use crate::dispatch::decode_tool_content;

/// Walk every node on a branch and project into a flat `Vec<Message>` in
/// chain order (oldest first).
///
/// Projection table:
///
/// | `msg_type`   | What gets projected                        | `Message` variant                 |
/// |--------------|--------------------------------------------|-----------------------------------|
/// | `Invoke`     | `invoke.current_input` (already `Vec<Message>`) | extend result                 |
/// | `Complete`   | `complete.content` + `complete.tool_calls`  | `Agent { content, tool_calls }`   |
/// | `SvcResponse`| `request.tool_call_id` + `response.payload` | `Tool { tool_call_id, content }` |
/// | `(all other)`| skip                                       | —                                 |
///
/// Returns `Ok(empty vec)` (not error) when the branch has no nodes.
pub async fn walk_chain_to_messages(
    store: &dyn DagStore,
    _session: &SessionId,
    branch: BranchId,
) -> Result<Vec<Message>, String> {
    let nodes = store.latest_nodes_on_branch(branch, u32::MAX).await?;
    let mut messages: Vec<Message> = Vec::with_capacity(nodes.len());

    for node in &nodes {
        match node.msg_type {
            MessageType::Invoke => {
                if let Ok(Some(invoke)) = store.get_invoke_message(&node.id).await {
                    messages.extend(invoke.current_input);
                }
            }
            MessageType::Complete => {
                if let Ok(Some(complete)) = store.get_complete_message(&node.id).await {
                    messages.push(Message::Agent {
                        content: complete.content,
                        tool_calls: complete.tool_calls,
                    });
                }
            }
            MessageType::SvcResponse => {
                // Fetch the matching SvcRequest to get tool_call_id.
                let response = store.get_response_v2(&node.id).await;
                let Ok(Some(resp)) = response else {
                    continue;
                };
                // The parent of the SvcResponse envelope is the SvcRequest.
                let request = store.get_request_v2(&node.parent_id).await;
                let Ok(Some(req)) = request else {
                    continue;
                };
                let content = decode_tool_content(&resp.payload);
                messages.push(Message::Tool {
                    tool_call_id: req.tool_call_id,
                    content: content.into_bytes(),
                });
            }
            // All other types (Request, Response, SvcRequest, Fork, Promote,
            // DeployAgent, DeleteAgent) — nothing to project into conversation.
            _ => {}
        }
    }

    Ok(messages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use vlinder_core::domain::{
        hash_dag_node, session::Session, AgentName, BranchId, CompleteMessage, DagNodeId,
        DataMessageKind, DataRoutingKey, ExternalSessionId, HarnessType, InvokeDiagnostics,
        InvokeMessage, Message, MessageId, RequestV2, ResponseV2, RuntimeDiagnostics, RuntimeType,
        Sequence, ServiceBackendV2, ServiceOperation, Snapshot, SubmissionId,
        SvcRequestDiagnostics, SvcResponseDiagnostics, ToolCall, ToolCallId,
    };
    use vlinder_sql_state::SqliteDagStore;

    fn msg_id(s: &str) -> MessageId {
        MessageId::from(s.to_string())
    }

    fn tool_call_id(s: &str) -> ToolCallId {
        ToolCallId::from(s.to_string())
    }

    fn sess_id() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }

    async fn setup_store(dir: &Path) -> SqliteDagStore {
        let path = dir.join("test.db");
        let store = SqliteDagStore::open(&path).unwrap();
        let external_id = ExternalSessionId::new("test-ext-id").unwrap();
        let session = Session {
            id: sess_id(),
            external_id,
            name: "test-session".to_string(),
            agent: "test-agent".to_string(),
            default_branch: BranchId::from(1),
            created_at: chrono::Utc::now(),
        };
        store.create_session(&session).await.unwrap();
        store.create_branch("main", &sess_id(), None).await.unwrap();
        store
    }

    /// Build a test chain and assert `walk_chain_to_messages` produces the
    /// expected Vec<Message>.
    ///
    /// Chain (in creation order):
    ///   1. Invoke  — `current_input`: [User("hello")]
    ///   2. Complete — content: Some("hi"), `tool_calls`: None
    ///   3. Invoke  — `current_input`: [User("again")]
    ///   4. Complete — content: None, `tool_calls`: Some([tc("tool1")])
    ///   5. `SvcRequest` — `tool_call_id`: "tc1"
    ///   6. `SvcResponse` — payload: "tool result" (parent = node 5)
    ///
    /// Expected messages:
    ///   - User("hello")
    ///   - Agent { content: Some("hi"), `tool_calls`: None }
    ///   - User("again")
    ///   - Agent { content: None, `tool_calls`: Some([tc("tool1")]) }
    ///   - Tool { `tool_call_id`: "tc1", content: "tool result" }
    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn walk_chain_happy_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = setup_store(dir.path()).await;
        let session = sess_id();
        let branch = BranchId::from(1);
        let submission = SubmissionId::new();
        let agent = AgentName::new("test-agent");
        let harness = HarnessType::Cli;
        let t0 = chrono::Utc::now();

        // --- Node 1: Invoke ---
        let created_at_1 = t0;
        let created_at_2 = t0 + chrono::Duration::milliseconds(1);
        let created_at_3 = t0 + chrono::Duration::milliseconds(2);
        let created_at_4 = t0 + chrono::Duration::milliseconds(3);
        let created_at_5 = t0 + chrono::Duration::milliseconds(4);
        let created_at_6 = t0 + chrono::Duration::milliseconds(5);
        let invoke1 = InvokeMessage {
            id: msg_id("invoke-1"),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            current_input: vec![Message::User {
                content: "hello".to_string(),
            }],
        };
        let invoke1_hash = hash_dag_node(
            &serde_json::to_vec(&invoke1).unwrap(),
            &DagNodeId::root(),
            &MessageType::Invoke,
            b"",
            &session,
        );
        store
            .insert_invoke_node(
                &invoke1_hash,
                &DagNodeId::root(),
                created_at_1,
                &Snapshot::empty(),
                &DataRoutingKey {
                    session: session.clone(),
                    branch,
                    submission: submission.clone(),
                    kind: DataMessageKind::Invoke {
                        harness,
                        runtime: RuntimeType::Container,
                        agent: agent.clone(),
                    },
                },
                &invoke1,
            )
            .await
            .unwrap();

        // --- Node 2: Complete ---
        let complete1 = CompleteMessage {
            id: msg_id("complete-1"),
            dag_id: DagNodeId::root(),
            dag_parent: invoke1_hash.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            content: Some("hi".to_string()),
            tool_calls: None,
            payload: b"raw output".to_vec(),
        };
        let complete1_hash = hash_dag_node(
            &serde_json::to_vec(&complete1).unwrap(),
            &invoke1_hash,
            &MessageType::Complete,
            b"",
            &session,
        );
        store
            .insert_complete_node(
                &complete1_hash,
                &invoke1_hash,
                created_at_2,
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                harness,
                &complete1,
            )
            .await
            .unwrap();

        // --- Node 3: Invoke ---
        let invoke2 = InvokeMessage {
            id: msg_id("invoke-2"),
            dag_id: DagNodeId::root(),
            dag_parent: complete1_hash.clone(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            current_input: vec![Message::User {
                content: "again".to_string(),
            }],
        };
        let invoke2_hash = hash_dag_node(
            &serde_json::to_vec(&invoke2).unwrap(),
            &complete1_hash,
            &MessageType::Invoke,
            b"",
            &session,
        );
        store
            .insert_invoke_node(
                &invoke2_hash,
                &complete1_hash,
                created_at_3,
                &Snapshot::empty(),
                &DataRoutingKey {
                    session: session.clone(),
                    branch,
                    submission: submission.clone(),
                    kind: DataMessageKind::Invoke {
                        harness,
                        runtime: RuntimeType::Container,
                        agent: agent.clone(),
                    },
                },
                &invoke2,
            )
            .await
            .unwrap();

        // --- Node 4: Complete with tool_calls ---
        let tool1 = ToolCall {
            id: tool_call_id("tc1"),
            name: "tool1".to_string(),
            arguments: serde_json::json!({"key": "val"}),
        };
        let complete2 = CompleteMessage {
            id: msg_id("complete-2"),
            dag_id: DagNodeId::root(),
            dag_parent: invoke2_hash.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            content: None,
            tool_calls: Some(vec![tool1.clone()]),
            payload: b"raw output 2".to_vec(),
        };
        let complete2_hash = hash_dag_node(
            &serde_json::to_vec(&complete2).unwrap(),
            &invoke2_hash,
            &MessageType::Complete,
            b"",
            &session,
        );
        store
            .insert_complete_node(
                &complete2_hash,
                &invoke2_hash,
                created_at_4,
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                harness,
                &complete2,
            )
            .await
            .unwrap();

        // --- Node 5: SvcRequest ---
        let req = RequestV2 {
            id: msg_id("svc-req-1"),
            dag_id: DagNodeId::root(),
            dag_parent: complete2_hash.clone(),
            tool_call_id: tool_call_id("tc1"),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({"input": "query"})).unwrap(),
        };
        let req_hash = hash_dag_node(
            &serde_json::to_vec(&req).unwrap(),
            &complete2_hash,
            &MessageType::SvcRequest,
            b"",
            &session,
        );
        store
            .insert_svc_request_node(
                &req_hash,
                &complete2_hash,
                created_at_5,
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                ServiceBackendV2::Mcp("brave".to_string()),
                ServiceOperation::new("search"),
                Sequence::first(),
                &req,
            )
            .await
            .unwrap();

        // --- Node 6: SvcResponse ---
        let resp = ResponseV2 {
            id: msg_id("svc-resp-1"),
            dag_id: DagNodeId::root(),
            dag_parent: req_hash.clone(),
            correlation_id: msg_id("svc-req-1"),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"tool result".to_vec(),
        };
        let resp_hash = hash_dag_node(
            &serde_json::to_vec(&resp).unwrap(),
            &req_hash,
            &MessageType::SvcResponse,
            b"",
            &session,
        );
        store
            .insert_svc_response_node(
                &resp_hash,
                &req_hash,
                created_at_6,
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                ServiceBackendV2::Mcp("brave".to_string()),
                ServiceOperation::new("search"),
                Sequence::first(),
                &resp,
            )
            .await
            .unwrap();

        // --- Walk ---
        let result = walk_chain_to_messages(&store, &session, branch)
            .await
            .unwrap();

        assert_eq!(result.len(), 5, "expected 5 messages, got {result:?}");

        assert_eq!(
            result[0],
            Message::User {
                content: "hello".to_string()
            }
        );
        assert_eq!(
            result[1],
            Message::Agent {
                content: Some("hi".to_string()),
                tool_calls: None,
            }
        );
        assert_eq!(
            result[2],
            Message::User {
                content: "again".to_string()
            }
        );
        assert_eq!(
            result[3],
            Message::Agent {
                content: None,
                tool_calls: Some(vec![tool1.clone()]),
            }
        );
        assert_eq!(
            result[4],
            Message::Tool {
                tool_call_id: tool_call_id("tc1"),
                content: b"tool result".to_vec(),
            }
        );
    }

    #[tokio::test]
    async fn empty_branch_returns_empty_vec() {
        let dir = tempfile::TempDir::new().unwrap();
        let store = setup_store(dir.path()).await;
        let session = sess_id();
        let branch = BranchId::from(1);

        let result = walk_chain_to_messages(&store, &session, branch)
            .await
            .unwrap();
        assert!(result.is_empty(), "expected empty vec, got {result:?}");
    }
}
