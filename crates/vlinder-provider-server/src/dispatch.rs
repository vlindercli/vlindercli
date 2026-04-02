//! Shared dispatch — handles a single agent invocation.
//!
//! Used by both the Podman sidecar and Lambda adapter. The caller
//! provides the invoke (however it arrived) and receives the result.
//! Building diagnostics and acknowledging the invoke are the caller's
//! responsibility — they differ per runtime.

use std::io::Read;
use std::sync::Arc;
use std::time::Instant;

use vlinder_core::domain::{
    AgentName, CompleteMessage, DagNodeId, DataMessageKind, DataRoutingKey, InvokeMessage,
    MessageId, MessageQueue, Registry, RuntimeDiagnostics,
};

use crate::handler::InvokeHandler;
use crate::hosts::build_hosts;
use crate::provider_server::ProviderServer;

/// Result of a successful dispatch.
pub struct DispatchResult {
    /// Raw output from the agent.
    pub output: Vec<u8>,
    /// Final KV state after the invocation.
    pub state: Option<String>,
    /// Wall-clock duration of the invocation in milliseconds.
    pub duration_ms: u64,
}

/// Dispatch a single invoke to an agent and return the result.
///
/// 1. Look up agent in registry
/// 2. Start `ProviderServer` for service calls
/// 3. POST payload to agent on localhost
/// 4. Return the output, final state, and duration
///
/// The caller is responsible for building diagnostics, sending
/// the `CompleteMessage`, and acknowledging the invoke.
pub fn dispatch_invoke(
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    registry: &Arc<dyn Registry>,
    agent_port: u16,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
) -> Result<DispatchResult, String> {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        return Err("dispatch_invoke: expected Invoke key".into());
    };

    let started_at = Instant::now();

    let agent_info = registry
        .get_agent_by_name(agent.as_str())
        .ok_or_else(|| format!("agent '{}' not found in registry", agent.as_str()))?;

    let hosts = build_hosts(&agent_info);
    let initial_state = if agent_info.object_storage.is_some() {
        Some(msg.state.clone().unwrap_or_default())
    } else {
        None
    };

    let state = Arc::new(std::sync::RwLock::new(initial_state));
    let handler = InvokeHandler::new(
        queue.clone(),
        key.branch,
        key.submission.clone(),
        key.session.clone(),
        agent.clone(),
        Arc::clone(&state),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544);

    let http = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{agent_port}/invoke");

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
    let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

    Ok(DispatchResult {
        output,
        state: final_state,
        duration_ms,
    })
}

/// Build and send a `CompleteMessage` on the data plane.
pub fn send_complete(
    queue: &dyn MessageQueue,
    key: &DataRoutingKey,
    agent: &AgentName,
    output: Vec<u8>,
    state: Option<String>,
    diagnostics: RuntimeDiagnostics,
) {
    let complete_key = DataRoutingKey {
        session: key.session.clone(),
        branch: key.branch,
        submission: key.submission.clone(),
        kind: DataMessageKind::Complete {
            agent: agent.clone(),
            harness: match &key.kind {
                DataMessageKind::Invoke { harness, .. } => *harness,
                _ => vlinder_core::domain::HarnessType::Cli,
            },
        },
    };
    let msg = CompleteMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        state,
        diagnostics,
        payload: output,
    };
    if let Err(e) = queue.send_complete(complete_key, msg) {
        tracing::error!(error = %e, "failed to send complete");
    }
}
