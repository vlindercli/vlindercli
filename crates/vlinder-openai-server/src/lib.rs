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

use vlinder_core::domain::{DagStore, Harness, Message, Registry, SessionId};

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

/// Convert a JSON array of `OpenAI` messages into Vlinder domain `Message` array.
fn messages_from_json(msgs: &[Value]) -> Vec<Message> {
    let mut result = Vec::with_capacity(msgs.len());
    for msg in msgs {
        let role = msg.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let content = msg.get("content").and_then(|v| v.as_str()).unwrap_or("");

        match role {
            "user" => {
                result.push(Message::User {
                    content: content.to_string(),
                });
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
                    content: Some(content.to_string()),
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
                    content: content.as_bytes().to_vec(),
                });
            }
            "system" => {
                result.push(Message::User {
                    content: format!("[system] {content}\n"),
                });
            }
            "function" | "developer" => {
                // Deprecated / dev override — treat as user
                result.push(Message::User {
                    content: format!("[{role}] {content}\n"),
                });
            }
            _ => {}
        }
    }
    result
}

/// Convert Vlinder `ParsedResponse` to an `OpenAI` `choices` JSON array.
fn choices_to_json(parsed: &vlinder_core::domain::ParsedResponse) -> Value {
    let message = match (&parsed.content, &parsed.tool_calls) {
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

    let finish_reason = if parsed.tool_calls.is_some() {
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
// Handlers
// ============================================================================

type AppState = Arc<ApiServer>;

#[allow(clippy::too_many_lines)]
async fn chat_completions(
    State(server): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<Value>,
) -> Response {
    // Reject streaming.
    let stream = req.get("stream").and_then(Value::as_bool).unwrap_or(false);
    if stream {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "message": "streaming is not supported",
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response();
    }

    let model = req
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_end_matches(":latest")
        .to_string();

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

    let session_id = if let Some(raw) = headers.get("X-Vlinder-Session") {
        match raw.to_str() {
            Ok(s) => match SessionId::try_from(s.to_string()) {
                Ok(sid) => sid,
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "error": {
                                "message": "invalid X-Vlinder-Session header value",
                                "type": "invalid_request_error",
                            }
                        })),
                    )
                        .into_response();
                }
            },
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": {
                            "message": "X-Vlinder-Session header must be valid UTF-8",
                            "type": "invalid_request_error",
                        }
                    })),
                )
                    .into_response();
            }
        }
    } else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "error": {
                    "message": "X-Vlinder-Session header is required — create a session first via the session plane",
                    "type": "invalid_request_error",
                }
            })),
        )
            .into_response();
    };

    let messages_json: &[Value] = req
        .get("messages")
        .and_then(Value::as_array)
        .map_or(&[], Vec::as_slice);
    let messages = messages_from_json(messages_json);

    let parsed = match server
        .harness
        .run_agent_with_messages(&agent_id, messages, session_id)
        .await
    {
        Ok(p) => p,
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

    let created = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(0));

    let response = serde_json::json!({
        "id": format!("chatcmpl-{}", Uuid::new_v4()),
        "object": "chat.completion",
        "created": created,
        "model": req.get("model").and_then(|v| v.as_str()).unwrap_or(&model),
        "choices": choices_to_json(&parsed),
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": 0,
            "total_tokens": 0,
        },
    });

    (StatusCode::OK, Json(response)).into_response()
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
    async fn chat_completions_rejects_streaming() {
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
            .body(axum::body::Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
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
        use vlinder_core::domain::ParsedResponse;
        let parsed = ParsedResponse {
            content: Some("hello".to_string()),
            tool_calls: None,
        };
        let choices = choices_to_json(&parsed);
        assert_eq!(choices.as_array().unwrap()[0]["finish_reason"], "stop");
    }

    #[test]
    fn choices_to_json_with_tool_calls() {
        use serde_json::json;
        use vlinder_core::domain::{ParsedResponse, ToolCall, ToolCallId};

        let parsed = ParsedResponse {
            content: None,
            tool_calls: Some(vec![ToolCall {
                id: ToolCallId::new(),
                name: "get_weather".to_string(),
                arguments: json!({"city": "NYC"}),
            }]),
        };
        let choices = choices_to_json(&parsed);
        let choice = &choices[0];
        assert_eq!(choice["finish_reason"], "tool_calls");
        assert_eq!(
            choice["message"]["tool_calls"][0]["function"]["name"],
            "get_weather"
        );
    }
}
