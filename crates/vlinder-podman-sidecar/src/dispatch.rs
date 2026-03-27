//! Dispatch — handles a single agent invocation.
//!
//! Sets up the provider server, POSTs to the agent container, and builds
//! the `CompleteMessage` from the response.
//!
//! Durable agents (ADR 111) return JSON actions with X-Vlinder-Mode: durable.
//! The sidecar sends service requests to the queue and delivers responses
//! back to the agent via /invoke callbacks.

use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use vlinder_core::domain::{
    AgentName, BranchId, CompleteMessage, ContainerId, DagNodeId, DataMessageKind, DataRoutingKey,
    HarnessType, HealthWindow, HttpMethod, ImageDigest, ImageRef, MessageId, MessageQueue,
    Operation, ProviderHost, ProviderRoute, Registry, RequestDiagnostics, RequestMessage,
    ResponseMessage, RuntimeDiagnostics, Sequence, SequenceCounter, ServiceBackend, SessionId,
    SubmissionId,
};

use vlinder_provider_server::handler::InvokeHandler;
use vlinder_provider_server::hosts::build_hosts;
use vlinder_provider_server::provider_server::ProviderServer;

use crate::health;
use crate::trace::TraceLog;

/// Everything the dispatch loop needs from the sidecar — avoids passing
/// 9 individual parameters.
pub struct DispatchContext {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub container_port: u16,
    pub container_id: ContainerId,
    pub image_ref: Option<ImageRef>,
    pub image_digest: Option<ImageDigest>,
}

/// State for an in-progress durable invocation waiting for a service response.
pub struct DurableSession {
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub session: SessionId,
    pub agent_id: AgentName,
    pub harness: HarnessType,
    pub hosts: Vec<ProviderHost>,
    pub sequence: SequenceCounter,
    pub pending_service: ServiceBackend,
    pub pending_operation: Operation,
    pub pending_sequence: Sequence,
    pub started_at: Instant,
}

/// Result of handling an invoke or a service response.
pub enum InvokeOutcome {
    /// Invocation fully handled — `CompleteMessage` sent.
    Done,
    /// Durable mode — waiting for a service response.
    Pending(Box<DurableSession>),
}

/// Handle a single invocation: POST to agent, detect mode, handle response.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::needless_pass_by_value
)]
pub fn handle_invoke(
    ctx: &DispatchContext,
    health: &mut HealthWindow,
    branch: BranchId,
    submission: SubmissionId,
    session: SessionId,
    agent_id: AgentName,
    harness: HarnessType,
    payload: Vec<u8>,
    initial_state: Option<String>,
) -> Result<InvokeOutcome, String> {
    let started_at = Instant::now();
    let mut trace = TraceLog::new();

    // Look up agent to build provider hosts and resolve initial state.
    let agent = ctx
        .registry
        .get_agent_by_name(agent_id.as_str())
        .expect("agent not found");
    let hosts = build_hosts(&agent);
    let resolved_state = if agent.object_storage.is_some() {
        Some(initial_state.unwrap_or_default())
    } else {
        None
    };

    // Spawn provider server for unmanaged mode — drops when this function returns.
    let state = std::sync::Arc::new(std::sync::RwLock::new(resolved_state));
    let handler = InvokeHandler::new(
        ctx.queue.clone(),
        branch,
        submission.clone(),
        session.clone(),
        agent_id.clone(),
        std::sync::Arc::clone(&state),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544);

    let client = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{}/invoke", ctx.container_port);

    trace.log(format!("POST {} ({} bytes)", agent_url, payload.len()));

    match client.post(&agent_url).send_bytes(&payload) {
        Ok(response) => {
            let is_durable = response
                .header("X-Vlinder-Mode")
                .is_some_and(|v| v == "durable");

            let mut output = Vec::new();
            response
                .into_reader()
                .read_to_end(&mut output)
                .map_err(|e| format!("Failed to read agent response body: {e}"))?;
            trace.log(format!(
                "Agent responded ({} bytes, {}ms)",
                output.len(),
                started_at.elapsed().as_millis()
            ));

            if is_durable {
                trace.log("Agent running in durable mode");
                // Provider server not needed for durable mode — drop it.
                drop(provider_server);

                // Build checkpoint hosts with :3544 suffix for URL matching.
                let checkpoint_hosts: Vec<ProviderHost> = build_hosts(&agent)
                    .into_iter()
                    .map(|mut h| {
                        h.hostname = format!("{}:{}", h.hostname, 3544);
                        h
                    })
                    .collect();

                let sequence = SequenceCounter::new();
                handle_action(
                    ctx,
                    &output,
                    branch,
                    submission,
                    session,
                    agent_id,
                    harness,
                    checkpoint_hosts,
                    sequence,
                    started_at,
                )
            } else {
                // Unmanaged mode — response is the final output.
                let final_state = provider_server.final_state();
                let duration_ms =
                    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
                let diagnostics = health::build_diagnostics(
                    health,
                    ctx.container_port,
                    duration_ms,
                    &ctx.container_id,
                    ctx.image_ref.as_ref(),
                    ctx.image_digest.as_ref(),
                );
                trace.log("Sending complete");
                send_complete(
                    &ctx.queue,
                    branch,
                    submission,
                    session,
                    agent_id,
                    harness,
                    output,
                    final_state,
                    diagnostics,
                );
                Ok(InvokeOutcome::Done)
            }
        }
        Err(ureq::Error::Status(code, response)) => {
            let err_body = response
                .into_string()
                .unwrap_or_else(|_| "unknown error".to_string());
            tracing::warn!(
                event = "container.error",
                container = %ctx.container_id,
                status = code,
                reason = %err_body,
                "Agent container returned an error"
            );
            send_complete(
                &ctx.queue,
                branch,
                submission,
                session,
                agent_id,
                harness,
                format!("[error] agent container error: {err_body}").into_bytes(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
            Err(format!("Agent returned error: {err_body}"))
        }
        Err(e) => {
            let msg = format!("Request to agent failed: {e}");
            tracing::warn!(event = "container.unreachable", error = %msg);
            send_complete(
                &ctx.queue,
                branch,
                submission,
                session,
                agent_id,
                harness,
                format!("[error] {msg}").into_bytes(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
            Err(msg)
        }
    }
}

/// Handle a service response arriving for a durable session.
///
/// Builds the callback JSON and POSTs it to the agent's /invoke endpoint.
pub fn handle_service_response(
    ctx: &DispatchContext,
    session: DurableSession,
    response: &ResponseMessage,
) -> Result<InvokeOutcome, String> {
    let mut trace = TraceLog::new();

    let checkpoint = response
        .checkpoint
        .as_deref()
        .ok_or("service response missing checkpoint")?;

    trace.log(format!(
        "Service response for checkpoint '{}' ({} bytes)",
        checkpoint,
        response.payload.len()
    ));

    let result_json: serde_json::Value =
        serde_json::from_slice(&response.payload).unwrap_or(serde_json::Value::Null);

    let callback = serde_json::json!({
        "handler": checkpoint,
        "result": result_json,
    });

    let callback_bytes =
        serde_json::to_vec(&callback).map_err(|e| format!("Failed to serialize callback: {e}"))?;

    let client = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{}/invoke", ctx.container_port);

    trace.log(format!(
        "POST {} callback ({} bytes)",
        agent_url,
        callback_bytes.len()
    ));

    match client
        .post(&agent_url)
        .set("Content-Type", "application/json")
        .send_bytes(&callback_bytes)
    {
        Ok(resp) => {
            let mut output = Vec::new();
            resp.into_reader()
                .read_to_end(&mut output)
                .map_err(|e| format!("Failed to read callback response: {e}"))?;
            trace.log(format!(
                "Callback responded ({} bytes, {}ms elapsed)",
                output.len(),
                session.started_at.elapsed().as_millis()
            ));

            handle_action(
                ctx,
                &output,
                session.branch,
                session.submission,
                session.session,
                session.agent_id,
                session.harness,
                session.hosts,
                session.sequence,
                session.started_at,
            )
        }
        Err(e) => {
            let msg = format!("Callback to agent failed: {e}");
            trace.log(&msg);
            send_complete(
                &ctx.queue,
                session.branch,
                session.submission,
                session.session,
                session.agent_id,
                session.harness,
                format!("[error] {msg}").into_bytes(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
            Err(msg)
        }
    }
}

/// Parse and execute a JSON action from the agent.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn handle_action(
    ctx: &DispatchContext,
    action_bytes: &[u8],
    branch: BranchId,
    submission: SubmissionId,
    session: SessionId,
    agent_id: AgentName,
    harness: HarnessType,
    hosts: Vec<ProviderHost>,
    sequence: SequenceCounter,
    started_at: Instant,
) -> Result<InvokeOutcome, String> {
    let mut trace = TraceLog::new();

    let action: serde_json::Value =
        serde_json::from_slice(action_bytes).map_err(|e| format!("Failed to parse action: {e}"))?;

    let action_type = action["action"]
        .as_str()
        .ok_or("Missing 'action' field in agent response")?;

    trace.log(format!(
        "Action '{}' ({} bytes)",
        action_type,
        action_bytes.len()
    ));

    match action_type {
        "complete" => {
            let payload = action["payload"].as_str().unwrap_or("");
            trace.log(format!(
                "Durable complete ({} bytes, {}ms elapsed)",
                payload.len(),
                started_at.elapsed().as_millis()
            ));
            send_complete(
                &ctx.queue,
                branch,
                submission,
                session,
                agent_id,
                harness,
                payload.as_bytes().to_vec(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
            Ok(InvokeOutcome::Done)
        }
        "call" => {
            let url = action["url"]
                .as_str()
                .ok_or("Missing 'url' in call action")?;
            let checkpoint = action["then"]
                .as_str()
                .ok_or("Missing 'then' in call action")?;

            let call_body = serde_json::to_vec(&action["json"])
                .map_err(|e| format!("Failed to serialize call body: {e}"))?;

            let (host, path) = parse_url_host_path(url)?;
            let route = match_route(&hosts, &host, &path)
                .ok_or_else(|| format!("No route for {host}:{path}"))?;

            let seq = sequence.next();
            let route_service = route.service_backend;
            let route_operation = route.operation;
            let diagnostics = RequestDiagnostics {
                sequence: seq.as_u32(),
                endpoint: format!("/{}", route.service_backend.service_type().as_str()),
                request_bytes: u64::try_from(call_body.len()).unwrap_or(u64::MAX),
                received_at_ms: u64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis(),
                )
                .unwrap_or(u64::MAX),
            };

            let request_key = DataRoutingKey {
                session: session.clone(),
                branch,
                submission: submission.clone(),
                kind: DataMessageKind::Request {
                    agent: agent_id.clone(),
                    service: route_service,
                    operation: route_operation,
                    sequence: seq,
                },
            };
            let request = RequestMessage {
                id: MessageId::new(),
                dag_id: DagNodeId::root(),
                state: None,
                diagnostics,
                payload: call_body,
                checkpoint: Some(checkpoint.to_string()),
            };

            trace.log(format!(
                "Sending request to {} (checkpoint '{}', seq {})",
                route_service.service_type().as_str(),
                checkpoint,
                seq.as_u32()
            ));

            ctx.queue
                .send_request(request_key, request.clone())
                .map_err(|e| format!("Failed to send request: {e}"))?;

            Ok(InvokeOutcome::Pending(Box::new(DurableSession {
                branch,
                submission,
                session,
                agent_id,
                harness,
                hosts,
                sequence,
                pending_service: route_service,
                pending_operation: route_operation,
                pending_sequence: seq,
                started_at,
            })))
        }
        other => Err(format!("Unknown action: {other}")),
    }
}

/// Send a `CompleteMessage` on the data plane.
#[allow(clippy::too_many_arguments)]
fn send_complete(
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    branch: BranchId,
    submission: SubmissionId,
    session: SessionId,
    agent_id: AgentName,
    harness: HarnessType,
    payload: Vec<u8>,
    state: Option<String>,
    diagnostics: RuntimeDiagnostics,
) {
    let key = DataRoutingKey {
        session,
        branch,
        submission,
        kind: DataMessageKind::Complete {
            agent: agent_id,
            harness,
        },
    };
    let msg = CompleteMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        state,
        diagnostics,
        payload,
    };
    if let Err(e) = queue.send_complete(key, msg) {
        tracing::error!(error = %e, "Failed to send complete");
    }
}

/// Parse `http://host:port/path` into `("host:port", "/path")`.
fn parse_url_host_path(url: &str) -> Result<(String, String), String> {
    let without_scheme = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .ok_or_else(|| format!("URL missing scheme: {url}"))?;
    match without_scheme.find('/') {
        Some(i) => Ok((
            without_scheme[..i].to_string(),
            without_scheme[i..].to_string(),
        )),
        None => Ok((without_scheme.to_string(), "/".to_string())),
    }
}

/// Find the route matching a host and path in the provider host table.
fn match_route<'a>(hosts: &'a [ProviderHost], host: &str, path: &str) -> Option<&'a ProviderRoute> {
    for vhost in hosts {
        if vhost.hostname == host {
            for route in &vhost.routes {
                if route.method == HttpMethod::Post && route.path == path {
                    return Some(route);
                }
            }
        }
    }
    None
}
