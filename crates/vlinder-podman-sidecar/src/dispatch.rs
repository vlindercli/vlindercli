//! Dispatch — handles a single agent invocation.
//!
//! Sets up the provider server, POSTs to the agent container, and builds
//! the `CompleteMessage` from the response.

use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use vlinder_core::domain::{
    AgentName, BranchId, CompleteMessage, ContainerId, DagNodeId, DataMessageKind, DataRoutingKey,
    HarnessType, HealthWindow, ImageDigest, ImageRef, MessageId, MessageQueue, Registry,
    RuntimeDiagnostics, SessionId, SubmissionId,
};

use vlinder_provider_server::handler::InvokeHandler;
use vlinder_provider_server::hosts::build_hosts;
use vlinder_provider_server::provider_server::ProviderServer;

use crate::health;
use crate::trace::TraceLog;

/// Everything the dispatch loop needs from the sidecar.
pub struct DispatchContext {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub container_port: u16,
    pub container_id: ContainerId,
    pub image_ref: Option<ImageRef>,
    pub image_digest: Option<ImageDigest>,
}

/// Handle a single invocation: POST to agent, read response, send complete.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub fn handle_invoke(
    ctx: &DispatchContext,
    health: &mut HealthWindow,
    branch: BranchId,
    submission: SubmissionId,
    session: SessionId,
    agent_id: AgentName,
    harness: HarnessType,
    payload: &[u8],
    initial_state: Option<String>,
) {
    let started_at = Instant::now();
    let mut trace = TraceLog::new();

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

    let state = Arc::new(std::sync::RwLock::new(resolved_state));
    let handler = InvokeHandler::new(
        ctx.queue.clone(),
        branch,
        submission.clone(),
        session.clone(),
        agent_id.clone(),
        Arc::clone(&state),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544);

    let client = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{}/invoke", ctx.container_port);

    trace.log(format!("POST {} ({} bytes)", agent_url, payload.len()));

    match client.post(&agent_url).send_bytes(payload) {
        Ok(response) => {
            let mut output = Vec::new();
            if let Err(e) = response.into_reader().read_to_end(&mut output) {
                tracing::warn!(error = %e, "Failed to read agent response body");
            }
            trace.log(format!(
                "Agent responded ({} bytes, {}ms)",
                output.len(),
                started_at.elapsed().as_millis()
            ));

            let final_state = provider_server.final_state();
            let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
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
        }
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
