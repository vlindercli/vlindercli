//! `/v1/dispatch` HTTP endpoint exposed by the sidecar (Phase 5.1).
//!
//! Receives an `(InvokeRoutingKey, InvokeMessage)` from the runtime, runs the
//! existing `dispatch_one_invoke` flow, and returns the agent's output along
//! with everything the runtime needs to call `workspace_store.snapshot` and
//! emit a `Complete` message.
//!
//! In Phase 5.1 the endpoint coexists with the sidecar's queue polling loop;
//! it has no caller yet. Phase 5.4 hooks the runtime up and removes the
//! polling loop.
//!
//! ## `SessionProvider` creation
//!
//! Per ADR 133's revised design, the in-pod `SessionProvider` is created
//! **lazily on the first incoming dispatch**. The `(session, branch,
//! submission)` routing context is taken from the request payload; subsequent
//! dispatches reuse the provider and call `set_routing` / `set_chain_head`
//! per-invoke via [`vlinder_provider_server::dispatch::dispatch_one_invoke`]
//! (which already does this internally).

use std::sync::Arc;

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use vlinder_core::domain::{
    AgentName, DagNodeId, DagStore, DataMessageKind, DataRoutingKey, InvokeMessage, MessageQueue,
    Registry, ToolCall,
};
use vlinder_provider_server::dispatch::{
    self as shared, dispatch_one_invoke, start_session_provider, SessionProvider,
};

/// State shared with every `/v1/dispatch` handler invocation. The
/// `Mutex<Option<…>>` realises the lazy-on-first-dispatch `SessionProvider`.
pub struct DispatchEndpointState {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub store: Arc<dyn DagStore>,
    pub container_port: u16,
    pub agent_name: AgentName,
    pub session_provider: Mutex<Option<SessionProvider>>,
}

/// Wire-format for `POST /v1/dispatch` request body.
#[derive(Debug, Deserialize)]
pub struct DispatchRequest {
    pub key: DataRoutingKey,
    pub msg: InvokeMessage,
}

/// Wire-format for `POST /v1/dispatch` success response. Mirrors
/// `vlinder_provider_server::dispatch::DispatchResult` field-for-field but
/// derives Serialize so the runtime can deserialize on the wire.
#[derive(Debug, Serialize, Deserialize)]
pub struct DispatchResponse {
    pub output: Vec<u8>,
    pub content: Option<String>,
    pub tool_calls: Option<Vec<ToolCall>>,
    pub state: Option<String>,
    pub duration_ms: u64,
    pub chain_head: DagNodeId,
}

/// Error envelope returned with non-2xx responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Build the `Router` for the dispatch endpoint. Exposed for unit tests that
/// drive the router via `tower::ServiceExt::oneshot`.
pub fn router(state: Arc<DispatchEndpointState>) -> Router {
    Router::new()
        .route("/v1/dispatch", post(handle_dispatch))
        .with_state(state)
}

/// Bind and serve the dispatch endpoint on the given port. Returns when the
/// listener errors or the future is dropped.
pub async fn serve(state: Arc<DispatchEndpointState>, port: u16) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(
        event = "sidecar.dispatch_endpoint.listening",
        port = port,
        "Sidecar /v1/dispatch endpoint up"
    );
    axum::serve(listener, router(state)).await
}

async fn handle_dispatch(
    State(state): State<Arc<DispatchEndpointState>>,
    Json(req): Json<DispatchRequest>,
) -> Result<Json<DispatchResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Guard: the routing key must carry an Invoke kind.
    let DataMessageKind::Invoke { ref agent, .. } = req.key.kind else {
        return Err(bad_request("dispatch key kind must be Invoke".into()));
    };
    if agent.as_str() != state.agent_name.as_str() {
        return Err(bad_request(format!(
            "dispatch key agent '{}' does not match sidecar agent '{}'",
            agent.as_str(),
            state.agent_name.as_str()
        )));
    }

    // Lazy SessionProvider: build on first dispatch using the request's
    // routing context, reuse from then on.
    let mut provider_slot = state.session_provider.lock().await;
    if provider_slot.is_none() {
        let provider = start_session_provider(
            &state.queue,
            &state.registry,
            &state.agent_name,
            req.key.branch,
            req.key.submission.clone(),
            req.key.session.clone(),
        )
        .await
        .map_err(internal_error)?;
        *provider_slot = Some(provider);
    }
    let provider = provider_slot
        .as_ref()
        .expect("session provider just inserted");

    let result = dispatch_one_invoke(
        provider,
        &*state.store,
        state.container_port,
        &req.key,
        &req.msg,
    )
    .await
    .map_err(internal_error)?;

    drop(provider_slot);

    // Silence the unused-import warning on `shared` until Phase 5.4 adds an
    // explicit caller of shared::send_complete_with_parsed (which lives in
    // the runtime, not here).
    let _ = std::any::type_name::<shared::SessionProvider>();

    Ok(Json(DispatchResponse {
        output: result.output,
        content: result.content,
        tool_calls: result.tool_calls,
        state: result.state,
        duration_ms: result.duration_ms,
        chain_head: result.chain_head,
    }))
}

fn bad_request(msg: String) -> (StatusCode, Json<ErrorResponse>) {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: msg }))
}

fn internal_error(msg: String) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: msg }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use vlinder_core::domain::{
        AgentName, InMemoryDagStore, InMemoryRegistry, InMemorySecretStore,
    };
    use vlinder_core::queue::InMemoryQueue;

    fn test_state(agent: &str) -> Arc<DispatchEndpointState> {
        let secret_store: Arc<dyn vlinder_core::domain::SecretStore> =
            Arc::new(InMemorySecretStore::new());
        let registry: Arc<dyn Registry> = Arc::new(InMemoryRegistry::new(secret_store));
        let store: Arc<dyn DagStore> = Arc::new(InMemoryDagStore::new());
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        Arc::new(DispatchEndpointState {
            queue,
            registry,
            store,
            container_port: 8080,
            agent_name: AgentName::new(agent),
            session_provider: Mutex::new(None),
        })
    }

    #[tokio::test]
    async fn rejects_bad_json() {
        let state = test_state("my-agent");
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/dispatch")
                    .header("content-type", "application/json")
                    .body(Body::from("not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        // axum returns 400 for invalid JSON
        assert!(resp.status().is_client_error());
    }

    #[tokio::test]
    async fn unknown_route_404s() {
        let state = test_state("my-agent");
        let app = router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/v1/unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn session_provider_starts_empty() {
        let state = test_state("a");
        assert!(state.session_provider.lock().await.is_none());
    }
}
