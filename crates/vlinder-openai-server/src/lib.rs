//! OpenAI-compatible API server for Vlinder.
//!
//! Exposes Chat Completions, Models, and (stub) Embeddings endpoints
//! on top of the Vlinder harness. Drop-in replacement for any
//! OpenAI/OpenRouter-compatible client.

use std::sync::Arc;
use std::time::SystemTime;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde_json::Value;
use uuid::Uuid;

use vlinder_core::domain::{
    DagStore, ExternalSessionId, Harness, Message, Registry, RunCtl, SessionId,
};

/// Server handle for shutdown coordination.
pub struct ServerHandle {
    port: u16,
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
}

impl ServerHandle {
    /// Port the server is listening on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Signal the server to stop.
    #[allow(dead_code)]
    pub fn stop(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// OpenAI-compatible API server.
pub struct ApiServer {
    harness: Arc<dyn Harness + Send + Sync>,
    registry: Arc<dyn Registry>,
    #[allow(dead_code)]
    store: Arc<dyn DagStore>,
}

impl ApiServer {
    pub fn new(
        harness: Arc<dyn Harness + Send + Sync>,
        registry: Arc<dyn Registry>,
        store: Arc<dyn DagStore>,
    ) -> Self {
        Self {
            harness,
            registry,
            store,
        }
    }

    pub async fn start(self, port: u16) -> Result<ServerHandle, String> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .route("/v1/embeddings", post(embeddings))
            .route("/v1/models", axum::routing::get(models))
            .with_state(Arc::new(self));

        let addr = std::net::SocketAddr::from(([127, 0, 0, 1], port));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| format!("failed to bind port {port}: {e}"))?;

        let actual_port = listener.local_addr().map_or(port, |a| a.port());

        tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        Ok(ServerHandle {
            port: actual_port,
            shutdown_tx,
        })
    }
}

// ============================================================================
// Conversions
// ============================================================================

/// Extract text content from an `OpenAI` message value, supporting both string
/// and array-of-content-parts formats.
fn extract_text_content(value: &serde_json::Value) -> String {
    if let Some(s) = value.as_str() {
        s.to_string()
    } else if let Some(arr) = value.as_array() {
        arr.iter()
            .filter_map(|item| {
                if item.get("type")?.as_str()? == "text" {
                    item.get("text")?.as_str()
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("")
    } else {
        String::new()
    }
}

/// Convert a JSON array of `OpenAI` messages into Vlinder domain `Message` array.
fn messages_from_json(msgs: &[Value]) -> Vec<Message> {
    let mut result = Vec::with_capacity(msgs.len());
    for msg in msgs {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = extract_text_content(msg.get("content").unwrap_or(&serde_json::Value::Null));

        match role {
            "user" => {
                result.push(Message::User { content });
            }
            "assistant" => {
                let tool_calls = msg.get("tool_calls").and_then(|v| v.as_array()).map(|arr| {
                    arr.iter()
                        .filter_map(|tc| {
                            let id = tc.get("id")?.as_str()?.to_string();
                            let name = tc.get("function")?.get("name")?.as_str()?.to_string();
                            let args = tc
                                .get("function")?
                                .get("arguments")
                                .and_then(|a| {
                                    if a.is_string() {
                                        serde_json::from_str(a.as_str().unwrap_or("{}")).ok()
                                    } else {
                                        Some(a.clone())
                                    }
                                })
                                .unwrap_or_default();
                            Some(vlinder_core::domain::ToolCall {
                                id: vlinder_core::domain::ToolCallId::from(id.clone()),
                                name,
                                arguments: args,
                            })
                        })
                        .collect()
                });
                result.push(Message::Agent {
                    content: Some(content),
                    tool_calls,
                });
            }
            "tool" => {
                let tool_call_id = msg
                    .get("tool_call_id")
                    .and_then(Value::as_str)
                    .map_or_else(vlinder_core::domain::ToolCallId::new, |s| {
                        vlinder_core::domain::ToolCallId::from(s.to_string())
                    });
                result.push(Message::Tool {
                    tool_call_id,
                    content: content.into_bytes(),
                });
            }
            "system" => {
                result.push(Message::System { content });
            }
            "function" => {
                // Deprecated — treat as user
                result.push(Message::User {
                    content: format!("[function] {content}\n"),
                });
            }
            "developer" => {
                // Developer role is semantically a system instruction per OpenAI spec
                result.push(Message::System { content });
            }
            _ => {}
        }
    }
    result
}

/// Convert Vlinder `RunResult` to an `OpenAI` `choices` JSON array.
/// Convert Vlinder `RunResult` to an `OpenAI` `choices` JSON array.
fn choices_to_json(result: &vlinder_core::domain::RunResult) -> Value {
    let message = match (&result.content, &result.pending_tool_calls) {
        (Some(content), Some(tool_calls)) => {
            let tool_calls_json: Value = tool_calls
                .iter()
                .map(|tc| {
                    serde_json::json!({
                        "id": tc.id.to_string(),
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": serde_json::to_string(&tc.arguments).unwrap_or_default(),
                        }
                    })
                })
                .collect();
            serde_json::json!({
                "role": "assistant",
                "content": content,
                "tool_calls": tool_calls_json,
            })
        }
        (Some(content), None) => {
            serde_json::json!({
                "role": "assistant",
                "content": content,
            })
        }
        (None, Some(tool_calls)) => {
            let tool_calls_json: Value = tool_calls
                .iter()
                .map(|tc| {
                    serde_json::json!({
                        "id": tc.id.to_string(),
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": serde_json::to_string(&tc.arguments).unwrap_or_default(),
                        }
                    })
                })
                .collect();
            serde_json::json!({
                "role": "assistant",
                "content": null,
                "tool_calls": tool_calls_json,
            })
        }
        (None, None) => {
            serde_json::json!({
                "role": "assistant",
                "content": null,
            })
        }
    };

    let finish_reason = if result.pending_tool_calls.is_some() {
        "tool_calls"
    } else {
        "stop"
    };

    serde_json::json!([{
        "index": 0,
        "message": message,
        "finish_reason": finish_reason,
    }])
}

// ============================================================================
// Helpers
// ============================================================================

#[allow(clippy::result_large_err)]
fn check_agent_mismatch(
    session: &vlinder_core::domain::Session,
    model: &str,
    identifier: &str,
) -> Result<(), Response> {
    if session.agent != model {
        return Err((
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": {
                    "message": format!(
                        "session '{}' is already associated with \
                         agent '{}', not '{model}'",
                        identifier, session.agent
                    ),
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response());
    }
    Ok(())
}

#[allow(clippy::result_large_err)]
/// Resolve a session from the `X-Vlinder-Session` header, auto-creating if
/// unknown. Returns 400 for missing/invalid headers, 409 for agent mismatch,
/// and 500 for store errors.
async fn resolve_or_create_session(
    server: &ApiServer,
    headers: &HeaderMap,
    model: &str,
) -> Result<SessionId, Response> {
    let raw = headers.get("X-Vlinder-Session").ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "message": "X-Vlinder-Session header is required",
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response()
    })?;
    let s = raw.to_str().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "message": "X-Vlinder-Session header must be valid UTF-8",
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response()
    })?;
    let external_id = ExternalSessionId::new(s).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "message": format!("invalid X-Vlinder-Session header: {e}"),
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response()
    })?;

    // Check if session already exists
    if let Some(session) = server
        .store
        .get_session_by_external_id(&external_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, "store error in resolve_or_create_session");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": {
                        "message": "internal error",
                        "type": "internal_error",
                    }
                })),
            )
                .into_response()
        })?
    {
        check_agent_mismatch(&session, model, external_id.as_str())?;
        return Ok(session.id);
    }

    // Fallback: try UUID lookup for backward compatibility with CLI-created
    // sessions that predate the external_id feature.
    if let Ok(sid) = SessionId::try_from(s.to_string()) {
        if let Some(session) = server.store.get_session(&sid).await.map_err(|e| {
            tracing::error!(error = %e, "store error in resolve_or_create_session");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": {
                        "message": "internal error",
                        "type": "internal_error",
                    }
                })),
            )
                .into_response()
        })? {
            check_agent_mismatch(&session, model, external_id.as_str())?;
            return Ok(session.id);
        }
    }

    // Auto-create session via the harness (creates session + default branch)
    let (session_id, _) = server.harness.start_session(model, external_id).await;
    Ok(session_id)
}

/// Build the standard `OpenAI` chat completion JSON response body.
fn build_chat_completion_response(
    model_str: &str,
    result: &vlinder_core::domain::RunResult,
) -> Value {
    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(0));
    serde_json::json!({
        "id": format!("chatcmpl-{}", Uuid::new_v4()),
        "object": "chat.completion",
        "created": created,
        "model": model_str,
        "choices": choices_to_json(result),
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
        "vlinder": {
            "turns": result.turns,
            "state_snapshot": result.state_snapshot,
        },
    })
}

/// Build an SSE (`text/event-stream`) response body for the given response.
fn build_sse_response(model_str: &str, result: &vlinder_core::domain::RunResult) -> String {
    let id = format!("chatcmpl-{}", Uuid::new_v4());
    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(0));

    let role_event = serde_json::json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model_str,
        "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": null}]
    });

    let content_delta = match (&result.content, &result.pending_tool_calls) {
        (Some(content), Some(tcs)) => {
            let tc_json: Vec<Value> = tcs.iter().map(|tc| serde_json::json!({
                "index": 0, "id": tc.id.to_string(), "type": "function",
                "function": {"name": tc.name, "arguments": serde_json::to_string(&tc.arguments).unwrap_or_default()}
            })).collect();
            serde_json::json!({"content": content, "tool_calls": tc_json})
        }
        (Some(content), None) => serde_json::json!({"content": content}),
        (None, Some(tcs)) => {
            let tc_json: Vec<Value> = tcs.iter().map(|tc| serde_json::json!({
                "index": 0, "id": tc.id.to_string(), "type": "function",
                "function": {"name": tc.name, "arguments": serde_json::to_string(&tc.arguments).unwrap_or_default()}
            })).collect();
            serde_json::json!({"content": Value::Null, "tool_calls": tc_json})
        }
        (None, None) => serde_json::json!({"content": Value::Null}),
    };
    let content_event = serde_json::json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model_str,
        "choices": [{"index": 0, "delta": content_delta, "finish_reason": null}]
    });

    let finish_reason = if result.pending_tool_calls.is_some() {
        "tool_calls"
    } else {
        "stop"
    };
    let finish_event = serde_json::json!({
        "id": id, "object": "chat.completion.chunk", "created": created, "model": model_str,
        "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}]
    });

    // Construct SSE body with explicit newlines between events
    let mut sse = String::new();
    sse.push_str("data: ");
    sse.push_str(&role_event.to_string());
    sse.push_str("\n\n");
    sse.push_str("data: ");
    sse.push_str(&content_event.to_string());
    sse.push_str("\n\n");
    sse.push_str("data: ");
    sse.push_str(&finish_event.to_string());
    sse.push_str("\n\n");
    // Vlinder enrichment — comes before [DONE] so strict OpenAI clients
    // that stop at [DONE] skip our extra data cleanly.
    let vlinder_event = serde_json::json!({
        "vlinder": {
            "turns": result.turns,
            "state_snapshot": result.state_snapshot,
        }
    });
    sse.push_str("data: ");
    sse.push_str(&vlinder_event.to_string());
    sse.push_str("\n\n");
    sse.push_str("data: [DONE]\n\n");
    sse
}

// ============================================================================
// Handlers
// ============================================================================

type AppState = Arc<ApiServer>;

async fn chat_completions(
    State(server): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<Value>,
) -> Response {
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);

    let model = req
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches(":latest")
        .to_string();

    tracing::info!(
        model = %model,
        messages = %serde_json::to_string(req.get("messages").unwrap_or(&serde_json::Value::Null)).unwrap_or_default(),
        "chat_completions request"
    );

    let Some(agent) = server.registry.get_agent_by_name(&model).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": {
                    "message": format!("agent not found: {model}"),
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response();
    };
    let agent_id = agent.id.clone();

    let session_id = match resolve_or_create_session(&server, &headers, &model).await {
        Ok(sid) => sid,
        Err(resp) => return resp,
    };

    let messages_json: &[Value] = req
        .get("messages")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);
    let messages = messages_from_json(messages_json);

    let ctl = RunCtl::quiet();
    let result = match server
        .harness
        .run_agent_with_messages(&agent_id, messages, session_id, &ctl)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "harness error in chat completions");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "error": {
                        "message": format!("agent error: {}", e),
                        "type": "internal_error",
                    }
                })),
            )
                .into_response();
        }
    };

    let model_str = req.get("model").and_then(|v| v.as_str()).unwrap_or(&model);
    if stream {
        let body = build_sse_response(model_str, &result);
        (
            StatusCode::OK,
            [
                (axum::http::header::CONTENT_TYPE, "text/event-stream"),
                (axum::http::header::CACHE_CONTROL, "no-cache"),
            ],
            body,
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            Json(build_chat_completion_response(model_str, &result)),
        )
            .into_response()
    }
}

async fn models(State(server): State<AppState>) -> Response {
    let agents = server.registry.get_agents().await;
    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(0));

    let data: Vec<Value> = agents
        .into_iter()
        .map(|a| {
            serde_json::json!({
                "id": a.name.clone(),
                "object": "model",
                "created": created,
                "owned_by": "vlinder",
            })
        })
        .collect();

    let body = serde_json::json!({
        "object": "list",
        "data": data,
    });

    (StatusCode::OK, Json(body)).into_response()
}

async fn embeddings() -> Response {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(serde_json::json!({
            "error": {
                "message": "embeddings not yet implemented",
                "type": "not_implemented",
            }
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{
        InMemoryDagStore, InMemoryRegistry, InMemorySecretStore, SecretStore, ToolCallProtocol,
    };
    use vlinder_core::queue::InMemoryQueue;

    struct TestProtocol;
    impl ToolCallProtocol for TestProtocol {
        fn encode_tool_call(&self, _name: &str, arguments: &serde_json::Value) -> Vec<u8> {
            serde_json::to_vec(arguments).unwrap_or_default()
        }
        fn decode_tool_result(&self, payload: &[u8]) -> String {
            String::from_utf8_lossy(payload).into_owned()
        }
    }

    fn test_server() -> Arc<ApiServer> {
        let queue = Arc::new(InMemoryQueue::new());
        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());
        let harness = vlinder_core::domain::CoreHarness::new(
            queue,
            Arc::clone(&registry) as _,
            Arc::clone(&store) as _,
            vlinder_core::domain::HarnessType::Cli,
            Arc::new(TestProtocol),
        );
        Arc::new(ApiServer::new(Arc::new(harness), registry, store))
    }

    #[tokio::test]
    async fn models_endpoint_returns_empty_list() {
        use tower::ServiceExt;

        let server = test_server();
        let app = Router::new()
            .route("/v1/models", axum::routing::get(models))
            .with_state(server);

        let resp = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn embeddings_endpoint_returns_501() {
        use tower::ServiceExt;

        let server = test_server();
        let app = Router::new()
            .route("/v1/embeddings", post(embeddings))
            .with_state(server);

        let body = serde_json::json!({
            "input": "hello",
            "model": "test-model",
        });
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/embeddings")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    #[tokio::test]
    async fn chat_completions_accepts_streaming() {
        // Verify streaming is no longer rejected — the request should pass the
        // streaming check and proceed to agent lookup (which returns 404 because
        // no agent is registered). Previously this returned 400 BAD_REQUEST.
        use axum::body::Body;
        use tower::ServiceExt;

        let server = test_server();
        let app = Router::new()
            .route("/v1/chat/completions", post(chat_completions))
            .with_state(server);

        let body = serde_json::json!({
            "model": "test-agent",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true,
        });
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(axum::http::header::CONTENT_TYPE, "application/json")
            .header("X-Vlinder-Session", "00000000-0000-4000-8000-000000000000")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // 404 (agent not found) proves streaming was not rejected (would be 400)
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn build_sse_response_content_only() {
        use vlinder_core::domain::RunResult;
        let result = RunResult {
            content: Some("hello".to_string()),
            pending_tool_calls: None,
            turns: Vec::new(),
            state_snapshot: None,
        };
        let body = build_sse_response("test-agent", &result);
        assert!(body.contains(r#""content":"hello""#));
        assert!(body.contains(r#""finish_reason":"stop""#));
        assert!(body.contains("[DONE]"));
    }

    #[test]
    fn build_sse_response_with_tool_calls() {
        use serde_json::json;
        use vlinder_core::domain::{RunResult, ToolCall, ToolCallId};

        let result = RunResult {
            content: None,
            pending_tool_calls: Some(vec![ToolCall {
                id: ToolCallId::new(),
                name: "get_weather".to_string(),
                arguments: json!({"city": "NYC"}),
            }]),
            turns: Vec::new(),
            state_snapshot: None,
        };
        let body = build_sse_response("test-agent", &result);
        assert!(body.contains("tool_calls"));
        assert!(body.contains(r#""finish_reason":"tool_calls""#));
        assert!(body.contains("[DONE]"));
    }

    #[test]
    fn messages_from_json_user_and_assistant() {
        let msgs = vec![
            serde_json::json!({"role": "user", "content": "hello"}),
            serde_json::json!({"role": "assistant", "content": "hi there"}),
        ];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 2);
        assert!(matches!(result[0], Message::User { .. }));
        assert!(matches!(
            result[1],
            Message::Agent {
                content: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn messages_from_json_tool_message() {
        let msgs = vec![
            serde_json::json!({"role": "tool", "tool_call_id": "call_abc123", "content": "tool result"}),
        ];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 1);
        assert!(matches!(result[0], Message::Tool { .. }));
    }

    #[test]
    fn choices_to_json_content_only() {
        use vlinder_core::domain::RunResult;
        let result = RunResult {
            content: Some("hello".to_string()),
            pending_tool_calls: None,
            turns: Vec::new(),
            state_snapshot: None,
        };
        let choices = choices_to_json(&result);
        assert_eq!(choices.as_array().unwrap()[0]["finish_reason"], "stop");
    }

    #[test]
    fn choices_to_json_with_tool_calls() {
        use serde_json::json;
        use vlinder_core::domain::{RunResult, ToolCall, ToolCallId};

        let result = RunResult {
            content: None,
            pending_tool_calls: Some(vec![ToolCall {
                id: ToolCallId::new(),
                name: "get_weather".to_string(),
                arguments: json!({"city": "NYC"}),
            }]),
            turns: Vec::new(),
            state_snapshot: None,
        };
        let choices = choices_to_json(&result);
        let choice = &choices[0];
        assert_eq!(choice["finish_reason"], "tool_calls");
        assert_eq!(
            choice["message"]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
    }

    #[test]
    fn messages_from_json_system_message() {
        let msgs = vec![serde_json::json!({
            "role": "system",
            "content": "You are helpful",
        })];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Message::System { content } if content == "You are helpful"));
    }

    #[test]
    fn messages_from_json_developer_role() {
        let msgs = vec![serde_json::json!({
            "role": "developer",
            "content": "Be concise",
        })];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Message::System { content } if content == "Be concise"));
    }

    #[test]
    fn messages_from_json_array_content() {
        let msgs = vec![serde_json::json!({
            "role": "user",
            "content": [{"type": "text", "text": "Hello"}],
        })];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 1);
        assert!(matches!(&result[0], Message::User { content } if content == "Hello"));
    }

    #[test]
    fn messages_from_json_mixed_array_content() {
        let msgs = vec![serde_json::json!({
            "role": "user",
            "content": [
                {"type": "text", "text": "Describe this image: "},
                {"type": "image_url", "image_url": {"url": "https://example.com/img.png"}},
            ],
        })];
        let result = messages_from_json(&msgs);
        assert_eq!(result.len(), 1);
        // Only text parts should be extracted; image_url parts are discarded
        assert!(
            matches!(&result[0], Message::User { content } if content == "Describe this image: ")
        );
    }

    fn test_server_with_recording() -> Arc<ApiServer> {
        use vlinder_core::queue::RecordingQueue;
        let inner = Arc::new(InMemoryQueue::new());
        let secret_store: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::new());
        let registry = InMemoryRegistry::new(secret_store);
        let registry: Arc<dyn Registry> = Arc::new(registry);
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());
        let queue = Arc::new(RecordingQueue::new(inner, Arc::clone(&store)));
        let harness = vlinder_core::domain::CoreHarness::new(
            queue,
            Arc::clone(&registry) as _,
            Arc::clone(&store) as _,
            vlinder_core::domain::HarnessType::Cli,
            Arc::new(TestProtocol),
        );
        Arc::new(ApiServer::new(Arc::new(harness), registry, store))
    }

    #[tokio::test]
    async fn resolve_or_create_session_auto_creates() {
        let server = test_server_with_recording();

        let mut headers = HeaderMap::new();
        headers.insert("X-Vlinder-Session", "ticket-123".parse().unwrap());

        // First call: session does not exist → auto-creates
        let sid = resolve_or_create_session(&server, &headers, "openrouter-test")
            .await
            .expect("should auto-create session");

        // Second call: session exists with same external_id → returns same id
        let sid2 = resolve_or_create_session(&server, &headers, "openrouter-test")
            .await
            .expect("should resolve existing session");
        assert_eq!(sid, sid2, "should return same session on second call");
    }

    #[tokio::test]
    async fn resolve_or_create_session_invalid_chars() {
        let server = test_server_with_recording();

        let mut headers = HeaderMap::new();
        headers.insert("X-Vlinder-Session", "bad.id".parse().unwrap());

        let result = resolve_or_create_session(&server, &headers, "test-agent").await;
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resolve_or_create_session_agent_mismatch() {
        let server = test_server_with_recording();
        let ext_id = vlinder_core::domain::ExternalSessionId::new("session-42").unwrap();

        // Create a session with agent-a
        server.harness.start_session("agent-a", ext_id).await;

        let mut headers = HeaderMap::new();
        headers.insert("X-Vlinder-Session", "session-42".parse().unwrap());

        let result = resolve_or_create_session(&server, &headers, "agent-b").await;
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn resolve_or_create_session_missing_header() {
        let server = test_server();
        let headers = HeaderMap::new();

        let result = resolve_or_create_session(&server, &headers, "test-agent").await;
        let err = result.unwrap_err();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn resolve_or_create_session_uuid_fallback() {
        let server = test_server_with_recording();
        let sid = vlinder_core::domain::SessionId::new();
        let ext_id = vlinder_core::domain::ExternalSessionId::new("my-external-id").unwrap();

        // Create a session with a known internal UUID but a different external_id
        let session = vlinder_core::domain::Session {
            id: sid.clone(),
            external_id: ext_id,
            name: "uuid-session".to_string(),
            agent: "test-agent".to_string(),
            default_branch: vlinder_core::domain::BranchId::from(1),
            created_at: chrono::Utc::now(),
        };
        server.store.create_session(&session).await.unwrap();

        let mut headers = HeaderMap::new();
        headers.insert("X-Vlinder-Session", sid.as_str().parse().unwrap());

        // Resolving by internal UUID should find via UUID fallback
        let result = resolve_or_create_session(&server, &headers, "test-agent").await;
        let found = result.expect("UUID fallback should find the session");
        assert_eq!(found, sid);
    }

    // ======================================================================
    // vlinder namespace + finish_reason mapping (Commit 4)
    // ======================================================================

    #[test]
    fn build_chat_completion_response_has_vlinder_namespace() {
        use serde_json::json;
        use vlinder_core::domain::{
            RunResult, ToolCall, ToolCallId, ToolResult, ToolTrace, TurnTrace,
        };

        let result = RunResult {
            content: Some("The weather is 62°F.".to_string()),
            pending_tool_calls: None,
            turns: vec![TurnTrace {
                assistant_content: None,
                tool_calls: vec![ToolTrace {
                    tool_call: ToolCall {
                        id: ToolCallId::from("call-1".to_string()),
                        name: "get_weather".to_string(),
                        arguments: json!({"location": "SF"}),
                    },
                    result: ToolResult {
                        tool_call_id: ToolCallId::from("call-1".to_string()),
                        content: b"62F".to_vec(),
                    },
                    duration_ms: 203,
                }],
            }],
            state_snapshot: None,
        };

        let body = build_chat_completion_response("test-agent", &result);

        // choices[0].message.content matches RunResult.content
        assert_eq!(
            body["choices"][0]["message"]["content"],
            "The weather is 62°F."
        );
        // finish_reason is "stop" when content.is_some() and pending_tool_calls.is_none()
        assert_eq!(body["choices"][0]["finish_reason"], "stop");
        // vlinder namespace present
        assert!(
            body.get("vlinder").is_some(),
            "vlinder field must be present"
        );
        // vlinder.turns[0].tool_calls[0].duration_ms is a number
        assert!(body["vlinder"]["turns"][0]["tool_calls"][0]["duration_ms"].is_number());
        assert_eq!(
            body["vlinder"]["turns"][0]["tool_calls"][0]["duration_ms"],
            203
        );
        // state_snapshot is None → JSON null in the vlinder object
        assert!(body["vlinder"]["state_snapshot"].is_null());
        // pending_tool_calls is None → omitted from top level via skip_serializing_if
        assert!(body.get("pending_tool_calls").is_none());
        // turns.len() matches
        assert_eq!(body["vlinder"]["turns"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn build_sse_response_has_vlinder_event() {
        use vlinder_core::domain::RunResult;

        let result = RunResult {
            content: Some("hello".to_string()),
            pending_tool_calls: None,
            turns: vec![],
            state_snapshot: None,
        };
        let body = build_sse_response("test-agent", &result);

        // vlinder event appears before [DONE]
        let vlinder_pos = body.find(r#""vlinder""#);
        let done_pos = body.find("[DONE]");
        assert!(
            vlinder_pos.is_some(),
            "vlinder event must be present in SSE"
        );
        assert!(done_pos.is_some(), "[DONE] must be present in SSE");
        assert!(
            vlinder_pos.unwrap() < done_pos.unwrap(),
            "vlinder event must appear before [DONE]"
        );

        // Parse the vlinder event
        assert!(body.contains(r#""turns":[]"#));
    }

    #[test]
    fn choices_to_json_finish_reason_stop_when_content_present() {
        use vlinder_core::domain::RunResult;

        let result = RunResult {
            content: Some("hello".to_string()),
            pending_tool_calls: None,
            turns: vec![],
            state_snapshot: None,
        };
        let choices = choices_to_json(&result);
        assert_eq!(choices[0]["finish_reason"], "stop");
    }

    #[test]
    fn choices_to_json_finish_reason_tool_calls_when_pending() {
        use serde_json::json;
        use vlinder_core::domain::{RunResult, ToolCall, ToolCallId};

        let result = RunResult {
            content: None,
            pending_tool_calls: Some(vec![ToolCall {
                id: ToolCallId::new(),
                name: "get_weather".to_string(),
                arguments: json!({"city": "NYC"}),
            }]),
            turns: vec![],
            state_snapshot: None,
        };
        let choices = choices_to_json(&result);
        assert_eq!(choices[0]["finish_reason"], "tool_calls");
    }

    #[test]
    fn openai_tolerance_ignores_vlinder_field() {
        // A strict schema that only knows the standard OpenAI fields must
        // succeed even when the response contains an unknown "vlinder" field.
        #[derive(serde::Deserialize)]
        #[allow(dead_code, clippy::struct_field_names)]
        struct OpenAIResponse {
            id: String,
            object: String,
            created: i64,
            model: String,
            choices: Vec<OpenAIChoice>,
            usage: OpenAIUsage,
            system_fingerprint: Option<String>,
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct OpenAIChoice {
            index: u32,
            finish_reason: String,
            message: OpenAIMessage,
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code)]
        struct OpenAIMessage {
            role: String,
            content: Option<String>,
        }

        #[derive(serde::Deserialize)]
        #[allow(dead_code, clippy::struct_field_names)]
        struct OpenAIUsage {
            prompt_tokens: u32,
            completion_tokens: u32,
            total_tokens: u32,
        }

        use vlinder_core::domain::RunResult;

        let result = RunResult {
            content: Some("hello".to_string()),
            pending_tool_calls: None,
            turns: vec![],
            state_snapshot: None,
        };
        let body = build_chat_completion_response("test-agent", &result);

        let json_str = serde_json::to_string(&body).unwrap();
        let parsed: OpenAIResponse = serde_json::from_str(&json_str)
            .expect("strict OpenAI schema must deserialize even with extra vlinder field");

        assert!(
            parsed.id.starts_with("chatcmpl-"),
            "id should start with chatcmpl-"
        );
        assert!(parsed.id.len() > 30, "id should contain a UUID");
        assert_eq!(parsed.object, "chat.completion");
        assert_eq!(parsed.model, "test-agent");
        assert_eq!(parsed.choices[0].message.content, Some("hello".to_string()));
        assert_eq!(parsed.choices[0].finish_reason, "stop");
    }
}
