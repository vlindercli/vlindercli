//! Provider server — virtual-host HTTP server that emulates upstream provider APIs.
//!
//! Pure HTTP plumbing: bind, recv, route, respond. Data-plane logic lives
//! in the `handler` module — this module never touches the queue or registry.

use std::sync::{Arc, Mutex, RwLock};

use http::Method;
use vlinder_core::domain::{DagNodeId, HttpMethod, ProviderHost, ProviderRoute};

use crate::handler::{InvokeHandler, RoutingContext};

// =========================================================================
// HTTP plumbing — server lifecycle, routing, host extraction
// =========================================================================

/// A running provider server, session-scoped.
///
/// Shuts down automatically when dropped: the `Drop` impl aborts the task.
pub struct ProviderServer {
    handle: Option<tokio::task::JoinHandle<()>>,
    state: Arc<RwLock<Option<String>>>,
    chain_head: Arc<Mutex<DagNodeId>>,
    routing: Arc<Mutex<RoutingContext>>,
}

/// State shared between the axum router and the request handler.
struct ServerState {
    handler: InvokeHandler,
    hosts: Vec<ProviderHost>,
    /// Pre-computed tool definitions (`OpenAI` format) for the agent's MCP servers.
    tools: Vec<serde_json::Value>,
}

impl ProviderServer {
    /// Start the provider server on the given port.
    ///
    /// The caller constructs the `InvokeHandler` and the shared state `Arc`.
    /// The provider server only does HTTP plumbing — it never touches the
    /// queue, registry, or invoke message directly.
    pub async fn start(
        handler: InvokeHandler,
        hosts: Vec<ProviderHost>,
        state: Arc<RwLock<Option<String>>>,
        port: u16,
        tools: Vec<serde_json::Value>,
    ) -> Self {
        let chain_head_arc = handler.chain_head_arc();
        let routing_arc = handler.routing_arc();
        let hosts: Vec<ProviderHost> = hosts
            .into_iter()
            .map(|mut h| {
                h.hostname = format!("{}:{}", h.hostname, port);
                h
            })
            .collect();

        let server_state = Arc::new(ServerState {
            handler,
            hosts,
            tools,
        });

        let router = axum::Router::new()
            .fallback(handle_request)
            .with_state(server_state);

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
            .await
            .unwrap_or_else(|_| panic!("failed to bind provider server on port {port}"));

        tracing::info!(
            event = "provider_server.listening",
            port = port,
            "Provider server started"
        );

        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        Self {
            handle: Some(handle),
            state,
            chain_head: chain_head_arc,
            routing: routing_arc,
        }
    }

    /// Read the final state hash after an invocation completes.
    pub fn final_state(&self) -> Option<String> {
        self.state.read().unwrap().clone()
    }

    /// Read the final `chain_head` (last DAG node id) after an invocation completes.
    pub fn chain_head(&self) -> DagNodeId {
        self.chain_head.lock().unwrap().clone()
    }

    /// Reset `chain_head` to a new value. Called on each new invoke so that
    /// outgoing requests on the upcoming turn parent on the invoke's `dag_id`.
    /// The DAG is the source of truth; this resets the derived in-memory state.
    pub fn set_chain_head(&self, head: DagNodeId) {
        *self.chain_head.lock().unwrap() = head;
    }

    /// Seed the agent state for the upcoming turn. Called per-invoke before
    /// the agent POST. `forward_provider` may further mutate state during the turn.
    pub fn set_state(&self, state: Option<String>) {
        *self.state.write().unwrap() = state;
    }

    /// Reset routing identifiers (branch/submission/session) on each new invoke,
    /// so outgoing request routing keys carry the current user-turn's submission.
    /// Without this, the session-scoped handler stamps every turn's requests
    /// with the submission from when the sidecar started.
    pub fn set_routing(&self, ctx: RoutingContext) {
        *self.routing.lock().unwrap() = ctx;
    }
}

impl Drop for ProviderServer {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

async fn handle_request(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
    request: axum::extract::Request,
) -> impl axum::response::IntoResponse {
    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();

    let checkpoint = request
        .headers()
        .get("x-vlinder-checkpoint")
        .and_then(|v| v.to_str().ok())
        .map(std::string::ToString::to_string);

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    tracing::info!(
        event = "provider_server.request_received",
        host = %host, path = %path, method = %method,
        "Provider server received request"
    );

    // Metadata endpoints — serve from local state (no body read needed)
    if host.starts_with("metadata.vlinder.local") {
        if method == Method::GET && path == "/v1/tools" {
            let tools_json =
                serde_json::to_string(&state.tools).unwrap_or_else(|_| "[]".to_string());
            return axum::response::Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(tools_json))
                .unwrap();
        }
        return axum::response::Response::builder()
            .status(404)
            .body(axum::body::Body::from("not found"))
            .unwrap();
    }

    let Ok(body) = axum::body::to_bytes(request.into_body(), usize::MAX).await else {
        return axum::response::Response::builder()
            .status(400)
            .body(axum::body::Body::from("failed to read body"))
            .unwrap();
    };

    let (status, response_body) = match match_route(&state.hosts, &method, &host, &path, &body) {
        Ok(route) => {
            state
                .handler
                .forward_provider(route, body.to_vec(), checkpoint)
                .await
        }
        Err(err) => err,
    };

    tracing::info!(
        event = "provider_server.request_done",
        status = status,
        response_bytes = response_body.len(),
        "Provider server sending response"
    );

    axum::response::Response::builder()
        .status(status)
        .body(axum::body::Body::from(response_body))
        .unwrap()
}

/// Match a request against the virtual host table (pure routing).
fn match_route<'a>(
    hosts: &'a [ProviderHost],
    method: &Method,
    host: &str,
    path: &str,
    body: &[u8],
) -> Result<&'a ProviderRoute, (u16, Vec<u8>)> {
    for vhost in hosts {
        if vhost.hostname == host {
            for r in &vhost.routes {
                if to_http_method(r.method) == *method && r.path == path {
                    if let Err(e) = (r.validate_request)(body) {
                        return Err((400, e.to_string().into_bytes()));
                    }
                    return Ok(r);
                }
            }
        }
    }
    Err((404, b"not found".to_vec()))
}

/// Convert a core `HttpMethod` into `http::Method`.
fn to_http_method(m: HttpMethod) -> Method {
    match m {
        HttpMethod::Get => Method::GET,
        HttpMethod::Post => Method::POST,
        HttpMethod::Put => Method::PUT,
        HttpMethod::Delete => Method::DELETE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    fn test_hosts() -> Vec<ProviderHost> {
        use vlinder_core::domain::{InferenceBackendType, Operation, ServiceBackend};
        vec![ProviderHost::new(
            "test.vlinder.local:3544",
            vec![ProviderRoute::new::<String, String>(
                HttpMethod::Post,
                "/test",
                ServiceBackend::Infer(InferenceBackendType::Ollama),
                Operation::Run,
            )],
        )]
    }

    #[test]
    fn matching_route_returns_ok() {
        let hosts = test_hosts();
        assert!(match_route(
            &hosts,
            &Method::POST,
            "test.vlinder.local:3544",
            "/test",
            b"\"hello\""
        )
        .is_ok());
    }

    #[test]
    fn invalid_payload_returns_400() {
        let hosts = test_hosts();
        let err = match_route(
            &hosts,
            &Method::POST,
            "test.vlinder.local:3544",
            "/test",
            b"not json",
        )
        .err()
        .unwrap();
        assert_eq!(err.0, 400);
    }

    #[test]
    fn missing_host_returns_404() {
        let hosts = test_hosts();
        let err = match_route(&hosts, &Method::POST, "", "/test", b"\"hello\"")
            .err()
            .unwrap();
        assert_eq!(err.0, 404);
    }

    #[test]
    fn wrong_host_returns_404() {
        let hosts = test_hosts();
        let err = match_route(
            &hosts,
            &Method::POST,
            "other.vlinder.local:3544",
            "/test",
            b"\"hello\"",
        )
        .err()
        .unwrap();
        assert_eq!(err.0, 404);
    }

    #[test]
    fn wrong_method_returns_404() {
        let hosts = test_hosts();
        let err = match_route(
            &hosts,
            &Method::GET,
            "test.vlinder.local:3544",
            "/test",
            b"",
        )
        .err()
        .unwrap();
        assert_eq!(err.0, 404);
    }

    #[test]
    fn wrong_path_returns_404() {
        let hosts = test_hosts();
        let err = match_route(
            &hosts,
            &Method::POST,
            "test.vlinder.local:3544",
            "/other",
            b"\"hello\"",
        )
        .err()
        .unwrap();
        assert_eq!(err.0, 404);
    }

    // ── Metadata endpoint (`metadata.vlinder.local/v1/tools`) ──

    fn metadata_tools_state() -> Arc<ServerState> {
        use crate::handler::InvokeHandler;
        use std::sync::RwLock;
        use vlinder_core::domain::{AgentName, BranchId, DagNodeId, SessionId, SubmissionId};
        use vlinder_core::queue::InMemoryQueue;

        let queue = Arc::new(InMemoryQueue::new());
        let state = Arc::new(RwLock::new(None::<String>));
        let handler = InvokeHandler::new(
            queue,
            crate::handler::RoutingContext {
                branch: BranchId::from(1),
                submission: SubmissionId::new(),
                session: SessionId::new(),
            },
            AgentName::new("test-agent"),
            state,
            DagNodeId::root(),
        );
        Arc::new(ServerState {
            handler,
            hosts: vec![],
            tools: vec![serde_json::json!({
                "type": "function",
                "function": {
                    "name": "test_tool",
                    "description": "A test tool",
                    "parameters": {
                        "type": "object",
                        "properties": {},
                        "required": [],
                    }
                }
            })],
        })
    }

    #[tokio::test]
    async fn metadata_tools_returns_tools() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = metadata_tools_state();
        let router = axum::Router::new()
            .fallback(handle_request)
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/tools")
            .header("host", "metadata.vlinder.local:3544")
            .body(Body::empty())
            .unwrap();

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&body).expect("valid JSON");
        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 1);
        assert_eq!(parsed[0]["function"]["name"].as_str().unwrap(), "test_tool");
    }

    #[tokio::test]
    async fn metadata_wrong_host_returns_404() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = metadata_tools_state();
        let router = axum::Router::new()
            .fallback(handle_request)
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/tools")
            .header("host", "other.vlinder.local:3544")
            .body(Body::empty())
            .unwrap();

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn metadata_wrong_path_returns_404() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = metadata_tools_state();
        let router = axum::Router::new()
            .fallback(handle_request)
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/other")
            .header("host", "metadata.vlinder.local:3544")
            .body(Body::empty())
            .unwrap();

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn old_tools_path_returns_404() {
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;

        let state = metadata_tools_state();
        let router = axum::Router::new()
            .fallback(handle_request)
            .with_state(state);

        let request = Request::builder()
            .method("GET")
            .uri("/tools")
            .header("host", "metadata.vlinder.local:3544")
            .body(Body::empty())
            .unwrap();

        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
