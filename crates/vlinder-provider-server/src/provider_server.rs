//! Provider server — virtual-host HTTP server that emulates upstream provider APIs.
//!
//! Pure HTTP plumbing: bind, recv, route, respond. Data-plane logic lives
//! in the `handler` module — this module never touches the queue or registry.

use std::sync::{Arc, RwLock};

use http::Method;
use vlinder_core::domain::{HttpMethod, ProviderHost, ProviderRoute};

use crate::handler::InvokeHandler;

// =========================================================================
// HTTP plumbing — server lifecycle, routing, host extraction
// =========================================================================

/// A running provider server, scoped to one invoke.
///
/// Shuts down automatically when dropped: the `Drop` impl aborts the task.
pub struct ProviderServer {
    handle: Option<tokio::task::JoinHandle<()>>,
    state: Arc<RwLock<Option<String>>>,
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
            .route("/tools", axum::routing::get(handle_tools))
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
        }
    }

    /// Read the final state hash after an invocation completes.
    pub fn final_state(&self) -> Option<String> {
        self.state.read().unwrap().clone()
    }
}

impl Drop for ProviderServer {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

/// Handle GET /tools — return the pre-computed tool definitions as a JSON array.
async fn handle_tools(
    axum::extract::State(state): axum::extract::State<Arc<ServerState>>,
) -> impl axum::response::IntoResponse {
    let tools_json = serde_json::to_string(&state.tools).unwrap_or_else(|_| "[]".to_string());
    axum::response::Response::builder()
        .status(200)
        .header("content-type", "application/json")
        .body(axum::body::Body::from(tools_json))
        .unwrap()
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
}
