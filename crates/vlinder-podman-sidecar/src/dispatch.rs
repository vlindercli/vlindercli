//! Dispatch — handles a single agent invocation.
//!
//! Sets up the provider server, POSTs to the agent container, and builds
//! the `CompleteMessage` from the response.

use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use vlinder_core::domain::{
    AgentName, CompleteMessage, ContainerId, DagNodeId, DataMessageKind, DataRoutingKey,
    HealthWindow, ImageDigest, ImageRef, InvokeMessage, MessageId, MessageQueue, Registry,
    RuntimeDiagnostics,
};

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

/// Handle a single invocation: dispatch to agent and send complete.
pub fn handle_invoke(
    ctx: &DispatchContext,
    health: &mut HealthWindow,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
) {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        tracing::error!("handle_invoke called with non-Invoke key");
        return;
    };

    let started_at = Instant::now();

    match dispatch_to_agent(ctx, key, msg) {
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
            send_complete(&ctx.queue, key, agent, output, final_state, diagnostics);
        }
        Err(error_payload) => {
            send_complete(
                &ctx.queue,
                key,
                agent,
                error_payload,
                None,
                RuntimeDiagnostics::placeholder(0),
            );
        }
    }
}

/// Set up the provider server, POST to the agent, and return the output.
///
/// On success: returns `(output_bytes, final_state)`.
/// On failure: returns an error payload (bytes) suitable for the complete message.
fn dispatch_to_agent(
    ctx: &DispatchContext,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
) -> Result<(Vec<u8>, Option<String>), Vec<u8>> {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        return Err(b"[error] expected Invoke key".to_vec());
    };

    let agent_info = ctx
        .registry
        .get_agent_by_name(agent.as_str())
        .expect("agent not found");
    let hosts = build_hosts(&agent_info);
    let resolved_state = if agent_info.object_storage.is_some() {
        Some(msg.state.clone().unwrap_or_default())
    } else {
        None
    };

    let state = Arc::new(std::sync::RwLock::new(resolved_state));
    let handler = InvokeHandler::new(
        ctx.queue.clone(),
        key.branch,
        key.submission.clone(),
        key.session.clone(),
        agent.clone(),
        Arc::clone(&state),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544);

    let client = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{}/invoke", ctx.container_port);

    match client.post(&agent_url).send_bytes(&msg.payload) {
        Ok(response) => {
            let mut output = Vec::new();
            if let Err(e) = response.into_reader().read_to_end(&mut output) {
                tracing::warn!(error = %e, "Failed to read agent response body");
            }
            let final_state = provider_server.final_state();
            Ok((output, final_state))
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
            Err(format!("[error] agent container error: {err_body}").into_bytes())
        }
        Err(e) => {
            let msg = format!("Request to agent failed: {e}");
            tracing::warn!(event = "container.unreachable", error = %msg);
            Err(format!("[error] {msg}").into_bytes())
        }
    }
}

/// Send a `CompleteMessage` on the data plane.
fn send_complete(
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    invoke_key: &DataRoutingKey,
    agent: &AgentName,
    payload: Vec<u8>,
    state: Option<String>,
    diagnostics: RuntimeDiagnostics,
) {
    let DataMessageKind::Invoke { harness, .. } = invoke_key.kind else {
        return;
    };
    let key = DataRoutingKey {
        session: invoke_key.session.clone(),
        branch: invoke_key.branch,
        submission: invoke_key.submission.clone(),
        kind: DataMessageKind::Complete {
            agent: agent.clone(),
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
