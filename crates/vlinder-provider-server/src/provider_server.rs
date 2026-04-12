//! Provider server — virtual-host HTTP server that emulates upstream provider APIs.
//!
//! Pure HTTP plumbing: bind, recv, route, respond. Data-plane logic lives
//! in the `handler` module — this module never touches the queue or registry.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tiny_http::{Method, StatusCode};
use vlinder_core::domain::{HttpMethod, ProviderHost, ProviderRoute};

use crate::handler::InvokeHandler;

// =========================================================================
// HTTP plumbing — server lifecycle, routing, host extraction
// =========================================================================

/// A running provider server, scoped to one invoke.
///
/// Shuts down automatically when dropped: the `Drop` impl sets the signal
/// and joins the thread.
pub struct ProviderServer {
    shutdown_signal: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
    state: Arc<RwLock<Option<String>>>,
}

impl ProviderServer {
    /// Start the provider server on the given port.
    ///
    /// The caller constructs the `InvokeHandler` and the shared state `Arc`.
    /// The provider server only does HTTP plumbing — it never touches the
    /// queue, registry, or invoke message directly.
    pub fn start(
        handler: InvokeHandler,
        hosts: Vec<ProviderHost>,
        state: Arc<RwLock<Option<String>>>,
        port: u16,
    ) -> Self {
        let bind = format!("0.0.0.0:{port}");
        let server = tiny_http::Server::http(&bind)
            .unwrap_or_else(|_| panic!("failed to bind provider server on port {port}"));
        tracing::info!(
            event = "provider_server.listening",
            port = port,
            "Provider server started"
        );

        let hosts: Vec<ProviderHost> = hosts
            .into_iter()
            .map(|mut h| {
                h.hostname = format!("{}:{}", h.hostname, port);
                h
            })
            .collect();

        let shutdown_signal = Arc::new(AtomicBool::new(false));
        let should_stop = Arc::clone(&shutdown_signal);

        let thread = std::thread::spawn(move || {
            request_loop(should_stop, server, hosts, handler);
        });

        Self {
            shutdown_signal,
            thread: Some(thread),
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
        self.shutdown_signal.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// The main request loop — recv, route, call handler, respond.
#[allow(clippy::needless_pass_by_value)]
fn request_loop(
    should_stop: Arc<AtomicBool>,
    server: tiny_http::Server,
    hosts: Vec<ProviderHost>,
    handler: InvokeHandler,
) {
    let rt = tokio::runtime::Runtime::new()
        .expect("Failed to create tokio runtime for provider-server thread");
    while !should_stop.load(Ordering::Relaxed) {
        match server.recv_timeout(Duration::from_millis(100)) {
            Ok(Some(mut request)) => {
                let method = request.method().clone();
                let host = extract_host(&request).to_string();
                let path = request.url().to_string();
                tracing::info!(
                    event = "provider_server.request_received",
                    host = %host, path = %path, method = %method,
                    "Provider server received request"
                );

                let mut body = Vec::new();
                let _ = request.as_reader().read_to_end(&mut body);

                let checkpoint = extract_checkpoint(&request);

                let (status, response_body) =
                    match match_route(&hosts, &method, &host, &path, &body) {
                        Ok(route) => rt.block_on(handler.forward_provider(route, body, checkpoint)),
                        Err(err) => err,
                    };

                tracing::info!(
                    event = "provider_server.request_done",
                    status = status,
                    response_bytes = response_body.len(),
                    "Provider server sending response"
                );
                let response = tiny_http::Response::from_data(response_body)
                    .with_status_code(StatusCode(status));
                let _ = request.respond(response);
            }
            Ok(None) => {}
            Err(_) => break,
        }
    }
    tracing::info!(event = "provider_server.stopped", "Provider server stopped");
}

/// Extract the Host header value from a request, or "" if absent.
fn extract_host(request: &tiny_http::Request) -> &str {
    request
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case("host"))
        .map_or("", |h| h.value.as_str())
}

/// Extract the X-Vlinder-Checkpoint header value, if present.
fn extract_checkpoint(request: &tiny_http::Request) -> Option<String> {
    request
        .headers()
        .iter()
        .find(|h| {
            h.field
                .as_str()
                .as_str()
                .eq_ignore_ascii_case("x-vlinder-checkpoint")
        })
        .map(|h| h.value.as_str().to_string())
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
                if to_tiny_method(r.method) == *method && r.path == path {
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

/// Convert a core `HttpMethod` into the `tiny_http::Method` the server needs.
fn to_tiny_method(m: HttpMethod) -> Method {
    match m {
        HttpMethod::Get => Method::Get,
        HttpMethod::Post => Method::Post,
        HttpMethod::Put => Method::Put,
        HttpMethod::Delete => Method::Delete,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            &Method::Post,
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
            &Method::Post,
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
        let err = match_route(&hosts, &Method::Post, "", "/test", b"\"hello\"")
            .err()
            .unwrap();
        assert_eq!(err.0, 404);
    }

    #[test]
    fn wrong_host_returns_404() {
        let hosts = test_hosts();
        let err = match_route(
            &hosts,
            &Method::Post,
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
            &Method::Get,
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
            &Method::Post,
            "test.vlinder.local:3544",
            "/other",
            b"\"hello\"",
        )
        .err()
        .unwrap();
        assert_eq!(err.0, 404);
    }
}
