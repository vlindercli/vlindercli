//! Walks the chain on a branch and projects each entry into the live-path
//! `Entry` shape, producing the transcript a user *would have seen* if they
//! had been watching live.

use std::collections::HashMap;

use vlinder_core::domain::{
    BranchId, DagStore, Message, MessageType, SessionId, ToolCall, ToolCallId,
};

use super::app::{Entry, Role, ToolTraceDisplay};

/// Walk every node on `branch` (oldest-first) and project each into the
/// live-path `Entry` shape.
///
/// Returns an empty `Vec` (not an error) if the branch has no nodes.
///
/// # How tool entries are paired
///
/// The walk processes nodes in chain order. When a `Complete` envelope
/// carries `tool_calls`, the tool names and ids are stashed in a side-map.
/// When the subsequent `SvcResponse` arrives (whose `dag_parent` points at
/// the corresponding `SvcRequest` node), the matching tool-call id is
/// looked up via `RequestV2::tool_call_id` and the tool entry is finalized
/// with args from the request payload and result from the response payload.
#[allow(dead_code)] // used by commands/agent.rs and commands/fleet.rs
pub async fn walk_chain_to_entries(
    store: &dyn DagStore,
    _session: &SessionId,
    branch: BranchId,
) -> Result<Vec<Entry>, String> {
    let envelopes = store.latest_nodes_on_branch(branch, u32::MAX).await?;
    let mut entries: Vec<Entry> = Vec::new();

    // Side-map: tool_call_id → ToolCall, populated from Complete entries
    // and consumed by SvcResponse entries. Cleared on each new Complete
    // or Invoke (so tool calls from different turns don't leak across).
    let mut pending_tool_calls: HashMap<ToolCallId, ToolCall> = HashMap::new();

    for env in &envelopes {
        match env.msg_type {
            MessageType::Invoke => {
                if let Some(invoke) = store.get_invoke_message(&env.id).await? {
                    for msg in invoke.current_input {
                        if let Message::User { content } = msg {
                            entries.push(Entry {
                                role: Role::User,
                                text: content,
                                tool: None,
                            });
                        }
                        // Other Message variants in current_input are not
                        // expected post-migration; ignore defensively.
                    }
                }
                // New turn → clear stale tool-call references.
                pending_tool_calls.clear();
            }

            MessageType::Complete => {
                if let Some(complete) = store.get_complete_message(&env.id).await? {
                    // Assistant text entry (if any).
                    if let Some(content) = complete.content {
                        if !content.is_empty() {
                            entries.push(Entry {
                                role: Role::Assistant,
                                text: content,
                                tool: None,
                            });
                        }
                    }
                    // Stash tool calls for pairing with SvcResponses.
                    if let Some(tool_calls) = complete.tool_calls {
                        pending_tool_calls.clear();
                        for tc in tool_calls {
                            pending_tool_calls.insert(tc.id.clone(), tc);
                        }
                    }
                }
            }

            MessageType::SvcResponse => {
                let response = store
                    .get_response_v2(&env.id)
                    .await?
                    .ok_or_else(|| "svc_response payload missing".to_string())?;
                let request = store.get_request_v2(&env.parent_id).await?.ok_or_else(|| {
                    "svc_request payload missing for parent of svc_response".to_string()
                })?;

                let args_pretty = String::from_utf8_lossy(&request.payload).to_string();
                let result = String::from_utf8_lossy(&response.payload).to_string();
                let result_preview: String = result.chars().take(80).collect();
                let name = pending_tool_calls
                    .get(&request.tool_call_id)
                    .map_or_else(|| "<unknown>".to_string(), |tc| tc.name.clone());

                let one_liner = format!("⏵ {}(…) [0ms] {}", name, result_preview.trim());

                entries.push(Entry {
                    role: Role::ToolCall,
                    text: one_liner,
                    tool: Some(ToolTraceDisplay {
                        name,
                        args: args_pretty,
                        result,
                        duration_ms: 0,
                        is_error: false,
                    }),
                });
            }

            // Nodes that don't produce transcript entries.
            MessageType::SvcRequest
            | MessageType::Request
            | MessageType::Response
            | MessageType::Fork
            | MessageType::Promote
            | MessageType::DeployAgent
            | MessageType::DeleteAgent => {}
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{
        hash_dag_node, AgentName, CompleteMessage, DagNode, DagNodeId, DataMessageKind,
        DataRoutingKey, ExternalSessionId, HarnessType, InvokeDiagnostics, InvokeMessage,
        MessageId, RequestV2, ResponseV2, RuntimeDiagnostics, RuntimeType, Sequence, Session,
        Snapshot, SubmissionId, SvcRequestDiagnostics, SvcResponseDiagnostics,
    };
    use vlinder_sql_state::SqliteDagStore;

    fn sess() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }

    fn sub() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }

    fn branch_id() -> BranchId {
        BranchId::from(1)
    }

    fn invoke_routing_key() -> DataRoutingKey {
        DataRoutingKey {
            session: sess(),
            branch: branch_id(),
            submission: sub(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: AgentName::new("test-agent"),
            },
        }
    }

    async fn new_store() -> (SqliteDagStore, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteDagStore::open(&path).unwrap();

        let session = Session {
            id: sess(),
            external_id: ExternalSessionId::new("test-ext").unwrap(),
            name: "test-session".to_string(),
            agent: "agent-a".to_string(),
            default_branch: branch_id(),
            created_at: chrono::Utc::now(),
        };
        store.create_session(&session).await.unwrap();
        store.create_branch("main", &sess(), None).await.unwrap();
        (store, dir)
    }

    fn test_node(payload: &[u8], parent: &DagNodeId, msg_type: MessageType) -> DagNode {
        let id = hash_dag_node(payload, parent, &msg_type, &[], &sess());
        DagNode {
            id,
            parent_id: parent.clone(),
            created_at: chrono::Utc::now(),
            state: Snapshot::empty(),
            msg_type,
            session: sess(),
            submission: sub(),
            branch: branch_id(),
            protocol_version: "v1".to_string(),
        }
    }

    #[tokio::test]
    async fn empty_branch_returns_empty_vec() {
        let (store, _dir) = new_store().await;
        let entries = walk_chain_to_entries(&store, &sess(), branch_id())
            .await
            .unwrap();
        assert!(entries.is_empty(), "expected empty vec for empty branch");
    }

    #[tokio::test]
    async fn simple_invoke_complete_no_tool_calls() {
        let (store, _dir) = new_store().await;
        let root = DagNodeId::root();

        // Insert an Invoke node with user input.
        let invoke_msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: root.clone(),
            current_input: vec![Message::User {
                content: "hello".to_string(),
            }],
        };
        let invoke_node = test_node(b"invoke", &root, MessageType::Invoke);
        store
            .insert_invoke_node(
                &invoke_node.id,
                &invoke_node.parent_id,
                invoke_node.created_at,
                &invoke_node.state,
                &invoke_routing_key(),
                &invoke_msg,
            )
            .await
            .unwrap();

        // Insert a Complete node with text content, no tool calls.
        let complete_msg = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: invoke_node.id.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::default(),
            content: Some("Hello, how can I help?".to_string()),
            tool_calls: None,
            payload: b"ok".to_vec(),
        };
        let complete_node = test_node(b"complete", &invoke_node.id, MessageType::Complete);
        store
            .insert_complete_node(
                &complete_node.id,
                &complete_node.parent_id,
                complete_node.created_at,
                &complete_node.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                HarnessType::Cli,
                &complete_msg,
            )
            .await
            .unwrap();

        let entries = walk_chain_to_entries(&store, &sess(), branch_id())
            .await
            .unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].role, Role::User);
        assert_eq!(entries[0].text, "hello");
        assert!(entries[0].tool.is_none());
        assert_eq!(entries[1].role, Role::Assistant);
        assert_eq!(entries[1].text, "Hello, how can I help?");
        assert!(entries[1].tool.is_none());
    }

    #[tokio::test]
    async fn invoke_with_empty_input_produces_no_entry() {
        let (store, _dir) = new_store().await;
        let root = DagNodeId::root();

        let invoke_msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: root.clone(),
            current_input: vec![],
        };
        let invoke_node = test_node(b"invoke-empty", &root, MessageType::Invoke);
        store
            .insert_invoke_node(
                &invoke_node.id,
                &invoke_node.parent_id,
                invoke_node.created_at,
                &invoke_node.state,
                &invoke_routing_key(),
                &invoke_msg,
            )
            .await
            .unwrap();

        let entries = walk_chain_to_entries(&store, &sess(), branch_id())
            .await
            .unwrap();
        assert!(entries.is_empty(), "empty input should produce no entries");
    }

    #[tokio::test]
    async fn complete_with_only_empty_content_omits_entry() {
        let (store, _dir) = new_store().await;
        let root = DagNodeId::root();

        // Invoke
        let invoke_msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: root.clone(),
            current_input: vec![Message::User {
                content: "hi".to_string(),
            }],
        };
        let invoke_node = test_node(b"invoke-empty-content", &root, MessageType::Invoke);
        store
            .insert_invoke_node(
                &invoke_node.id,
                &invoke_node.parent_id,
                invoke_node.created_at,
                &invoke_node.state,
                &invoke_routing_key(),
                &invoke_msg,
            )
            .await
            .unwrap();

        // Complete with None content
        let complete_msg = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: invoke_node.id.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::default(),
            content: None,
            tool_calls: None,
            payload: b"ok".to_vec(),
        };
        let complete_node = test_node(b"complete-empty", &invoke_node.id, MessageType::Complete);
        store
            .insert_complete_node(
                &complete_node.id,
                &complete_node.parent_id,
                complete_node.created_at,
                &complete_node.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                HarnessType::Cli,
                &complete_msg,
            )
            .await
            .unwrap();

        let entries = walk_chain_to_entries(&store, &sess(), branch_id())
            .await
            .unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].role, Role::User);
    }

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn tool_turn_round_trip() {
        let (store, _dir) = new_store().await;
        let root = DagNodeId::root();

        // 1) Invoke with user input.
        let invoke_msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: root.clone(),
            current_input: vec![Message::User {
                content: "what's the weather?".to_string(),
            }],
        };
        let invoke_node = test_node(b"invoke-tool", &root, MessageType::Invoke);
        store
            .insert_invoke_node(
                &invoke_node.id,
                &invoke_node.parent_id,
                invoke_node.created_at,
                &invoke_node.state,
                &invoke_routing_key(),
                &invoke_msg,
            )
            .await
            .unwrap();

        // 2) Complete with text + tool call.
        let tool_call_id = ToolCallId::from("tc-1".to_string());
        let tool_call = vlinder_core::domain::ToolCall {
            id: tool_call_id.clone(),
            name: "get_weather".to_string(),
            arguments: serde_json::json!({"location": "SF"}),
        };
        let complete_msg = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: invoke_node.id.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::default(),
            content: Some("I'll look that up.".to_string()),
            tool_calls: Some(vec![tool_call.clone()]),
            payload: b"ok".to_vec(),
        };
        let complete_node = test_node(b"complete-tool-1", &invoke_node.id, MessageType::Complete);
        store
            .insert_complete_node(
                &complete_node.id,
                &complete_node.parent_id,
                complete_node.created_at,
                &complete_node.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                HarnessType::Cli,
                &complete_msg,
            )
            .await
            .unwrap();

        // 3) SvcRequest node.
        let svc_req_id = test_node(b"svc-req-1", &complete_node.id, MessageType::SvcRequest);
        let request_payload = serde_json::to_vec(&serde_json::json!({"location": "SF"})).unwrap();
        let request_msg = RequestV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: complete_node.id.clone(),
            tool_call_id: tool_call_id.clone(),
            state: None,
            diagnostics: SvcRequestDiagnostics::default(),
            payload: request_payload,
        };
        store
            .insert_svc_request_node(
                &svc_req_id.id,
                &svc_req_id.parent_id,
                svc_req_id.created_at,
                &svc_req_id.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                vlinder_core::domain::ServiceBackendV2::Mcp("test-service".to_string()),
                vlinder_core::domain::ServiceOperation::new("test-op"),
                Sequence::first(),
                &request_msg,
            )
            .await
            .unwrap();

        // 4) SvcResponse node (dag_parent = SvcRequest's id).
        let svc_resp_id = test_node(b"svc-resp-1", &svc_req_id.id, MessageType::SvcResponse);
        let response_msg = ResponseV2 {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: svc_req_id.id.clone(),
            correlation_id: MessageId::new(),
            state: None,
            diagnostics: SvcResponseDiagnostics::default(),
            payload: b"sunny, 72F".to_vec(),
        };
        store
            .insert_svc_response_node(
                &svc_resp_id.id,
                &svc_resp_id.parent_id,
                svc_resp_id.created_at,
                &svc_resp_id.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                vlinder_core::domain::ServiceBackendV2::Mcp("test-service".to_string()),
                vlinder_core::domain::ServiceOperation::new("test-op"),
                Sequence::first(),
                &response_msg,
            )
            .await
            .unwrap();

        // 5) Re-invoke (empty input) — clears pending tool calls.
        let invoke_msg2 = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: svc_resp_id.id.clone(),
            current_input: vec![],
        };
        let invoke_node2 = test_node(b"invoke-tool-2", &svc_resp_id.id, MessageType::Invoke);
        store
            .insert_invoke_node(
                &invoke_node2.id,
                &invoke_node2.parent_id,
                invoke_node2.created_at,
                &invoke_node2.state,
                &invoke_routing_key(),
                &invoke_msg2,
            )
            .await
            .unwrap();

        // 6) Final Complete with final answer.
        let complete_msg2 = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: invoke_node2.id.clone(),
            state: None,
            diagnostics: RuntimeDiagnostics::default(),
            content: Some("It's 72F and sunny.".to_string()),
            tool_calls: None,
            payload: b"ok".to_vec(),
        };
        let complete_node2 = test_node(b"complete-tool-2", &invoke_node2.id, MessageType::Complete);
        store
            .insert_complete_node(
                &complete_node2.id,
                &complete_node2.parent_id,
                complete_node2.created_at,
                &complete_node2.state,
                &sess(),
                &sub(),
                branch_id(),
                &AgentName::new("agent-a"),
                HarnessType::Cli,
                &complete_msg2,
            )
            .await
            .unwrap();

        // Assert: User entry + Assistant entry (from first complete) + ToolCall
        // entry (finalized, with args from request and result from response) +
        // Assistant entry (final answer) = 4 entries.
        let entries = walk_chain_to_entries(&store, &sess(), branch_id())
            .await
            .unwrap();
        assert_eq!(entries.len(), 4);

        // Entry 0: User
        assert_eq!(entries[0].role, Role::User);
        assert_eq!(entries[0].text, "what's the weather?");

        // Entry 1: Assistant
        assert_eq!(entries[1].role, Role::Assistant);
        assert_eq!(entries[1].text, "I'll look that up.");

        // Entry 2: ToolCall (finalized)
        assert_eq!(entries[2].role, Role::ToolCall);
        let tool = entries[2]
            .tool
            .as_ref()
            .expect("tool entry should have tool data");
        assert_eq!(tool.name, "get_weather");
        assert!(!tool.args.is_empty());
        assert!(tool.result.contains("sunny"));
        assert_eq!(tool.duration_ms, 0);

        // Entry 3: Assistant (final answer)
        assert_eq!(entries[3].role, Role::Assistant);
        assert_eq!(entries[3].text, "It's 72F and sunny.");
    }
}
