//! Dispatch — handles a single agent invocation.
//!
//! Delegates to `vlinder_provider_server::dispatch` for the shared
//! invoke flow, then adds sidecar-specific concerns (health diagnostics,
//! container metadata).
//!
//! For SQL-backed agents the sidecar runs a `PostgresProxy` that needs
//! the same state Arc as the provider server. Because `dispatch_invoke`
//! creates that Arc internally, SQL invocations use a sidecar-local
//! dispatch path so the proxy and provider server share state.

use std::io::Read;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Instant;

use vlinder_core::domain::{
    ContainerId, DataMessageKind, DataRoutingKey, HealthWindow, ImageDigest, ImageRef,
    InvokeMessage, MessageQueue, Registry, RuntimeDiagnostics,
};

use vlinder_provider_server::dispatch as shared;
use vlinder_provider_server::handler::InvokeHandler;
use vlinder_provider_server::hosts::build_hosts;
use vlinder_provider_server::provider_server::ProviderServer;

use crate::health;

/// Everything the dispatch loop needs from the sidecar.
pub struct DispatchContext {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub container_port: u16,
    pub container_id: ContainerId,
    pub image_ref: Option<ImageRef>,
    pub image_digest: Option<ImageDigest>,
}

/// Shared session context for the postgres proxy.
type SharedProxyContext = Arc<std::sync::RwLock<vlinder_dolt::sidecar::SessionContext>>;

/// Long-lived postgres proxy — started once, serves all invocations.
///
/// The proxy thread reads the current `SessionContext` from a shared
/// `Arc<RwLock>` on each new TCP connection. Each invocation updates
/// the context so the agent's reused TCP connection gets the right
/// branch/state/submission.
pub struct PostgresProxy {
    _handle: std::thread::JoinHandle<()>,
    context: SharedProxyContext,
}

impl PostgresProxy {
    /// Spawn the proxy thread. Call once at sidecar startup.
    pub fn start(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        key: &DataRoutingKey,
        state: Option<String>,
        shared_state: Arc<std::sync::RwLock<Option<String>>>,
    ) -> Result<Self, String> {
        let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
            return Err("expected Invoke key".into());
        };

        let listener =
            TcpListener::bind("0.0.0.0:5432").map_err(|e| format!("bind postgres proxy: {e}"))?;

        let context: SharedProxyContext = Arc::new(std::sync::RwLock::new(
            vlinder_dolt::sidecar::SessionContext {
                session: key.session.clone(),
                branch: key.branch,
                agent_id: agent.clone(),
                submission: key.submission.clone(),
                state,
                shared_state,
            },
        ));

        let ctx = Arc::clone(&context);
        let handle = std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        if let Err(e) = vlinder_dolt::sidecar::handle_session_shared(
                            &mut stream,
                            &queue,
                            Arc::clone(&ctx),
                        ) {
                            tracing::warn!(error = %e, "Postgres proxy session error");
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Postgres proxy accept failed");
                        break;
                    }
                }
            }
        });

        Ok(Self {
            _handle: handle,
            context,
        })
    }

    /// Update the session context for the next invocation.
    pub fn update(
        &self,
        key: &DataRoutingKey,
        state: Option<String>,
        shared_state: Arc<std::sync::RwLock<Option<String>>>,
    ) {
        let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
            return;
        };
        let mut ctx = self.context.write().unwrap();
        ctx.session = key.session.clone();
        ctx.branch = key.branch;
        ctx.agent_id = agent.clone();
        ctx.submission = key.submission.clone();
        ctx.state = state;
        ctx.shared_state = shared_state;
    }
}

/// Handle a single invocation: dispatch to agent and send complete.
pub fn handle_invoke(
    ctx: &DispatchContext,
    health: &mut HealthWindow,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
    postgres_proxy: &mut Option<PostgresProxy>,
) {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        tracing::error!("handle_invoke called with non-Invoke key");
        return;
    };

    let started_at = Instant::now();

    match dispatch_to_agent(ctx, key, msg, postgres_proxy) {
        Ok((output, final_state)) => {
            let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
            let diagnostics = health::build_diagnostics(
                health,
                ctx.container_port,
                duration_ms,
                &ctx.container_id,
                ctx.image_ref.as_ref(),
                ctx.image_digest.as_ref(),
            );
            shared::send_complete(
                ctx.queue.as_ref(),
                key,
                agent,
                output,
                final_state,
                diagnostics,
            );
        }
        Err(e) => {
            tracing::warn!(event = "dispatch.error", error = %e, "Dispatch failed");
            shared::send_complete(
                ctx.queue.as_ref(),
                key,
                agent,
                format!("[error] {e}").into_bytes(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
        }
    }
}

/// Set up the provider server, POST to the agent, and return the output.
///
/// Uses a sidecar-local dispatch path (rather than `shared::dispatch_invoke`)
/// because the `PostgresProxy` needs the same state `Arc` as the provider
/// server. The shared function creates that Arc internally.
fn dispatch_to_agent(
    ctx: &DispatchContext,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
    postgres_proxy: &mut Option<PostgresProxy>,
) -> Result<(Vec<u8>, Option<String>), String> {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        return Err("expected Invoke key".into());
    };

    let agent_info = ctx
        .registry
        .get_agent_by_name(agent.as_str())
        .ok_or_else(|| format!("agent '{}' not found in registry", agent.as_str()))?;

    let hosts = build_hosts(&agent_info);
    let resolved_state = msg.state.clone();

    let state = Arc::new(std::sync::RwLock::new(resolved_state));
    let handler = InvokeHandler::new(
        ctx.queue.clone(),
        key.branch,
        key.submission.clone(),
        key.session.clone(),
        agent.clone(),
        Arc::clone(&state),
    );
    let provider_server = ProviderServer::start(handler, hosts, state.clone(), 3544);

    // Start or update the postgres proxy for SQL-backed agents.
    match postgres_proxy {
        Some(proxy) => proxy.update(key, msg.state.clone(), state),
        None => match PostgresProxy::start(ctx.queue.clone(), key, msg.state.clone(), state) {
            Ok(proxy) => *postgres_proxy = Some(proxy),
            Err(e) => tracing::warn!(error = %e, "Failed to start postgres proxy"),
        },
    }

    let http = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{}/invoke", ctx.container_port);

    let response = http
        .post(&agent_url)
        .send_bytes(&msg.payload)
        .map_err(|e| format!("POST to agent failed: {e}"))?;

    let mut output = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut output)
        .map_err(|e| format!("failed to read agent response: {e}"))?;

    let final_state = provider_server.final_state();
    Ok((output, final_state))
}
