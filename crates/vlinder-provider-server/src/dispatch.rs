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
    AgentName, CompleteMessage, DagNodeId, DataMessageKind, DataRoutingKey, InvokeMessage, Message,
    MessageId, MessageQueue, Registry, RuntimeDiagnostics,
};

use crate::handler::InvokeHandler;
use crate::hosts::build_hosts;
use crate::provider_server::ProviderServer;

/// Serialize a conversation (`history` + `current_input`) to `OpenAI` Chat Completions format.
///
/// Maps domain `Message::Agent` → wire-format `"assistant"`,
/// `Message::User` → `"user"`. Wraps in `{"messages": [...]}` and serializes as JSON.
pub fn serialize_openai_conversation(messages: &[Message]) -> Vec<u8> {
    let wire_messages: Vec<serde_json::Value> = messages
        .iter()
        .map(|m| match m {
            Message::User { content } => serde_json::json!({
                "role": "user",
                "content": content,
            }),
            Message::Agent { content } => serde_json::json!({
                "role": "assistant",
                "content": content,
            }),
        })
        .collect();

    let body = serde_json::json!({"messages": wire_messages});
    serde_json::to_vec(&body).unwrap_or_default()
}

/// Extract text content from an `OpenAI` Chat Completions response.
///
/// Parses the agent's HTTP response as `OpenAI` JSON and returns
/// just the assistant's text content. Falls back to the raw bytes
/// if parsing fails (e.g., the agent returned plain text).
pub fn extract_openai_content(raw: &[u8]) -> Vec<u8> {
    serde_json::from_slice::<serde_json::Value>(raw)
        .ok()
        .and_then(|v| {
            v.get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str().map(|s| s.to_string().into_bytes()))
        })
        .unwrap_or_else(|| raw.to_vec())
}

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
pub async fn dispatch_invoke(
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
        .await
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
    let provider_server = ProviderServer::start(handler, hosts, state, 3544).await;

    // Combine history + current_input into a single conversation and
    // serialize to OpenAI Chat Completions format JSON.
    let mut conversation: Vec<Message> = msg.history.clone();
    conversation.extend(msg.current_input.clone());
    let body = serialize_openai_conversation(&conversation);

    let http = ureq::Agent::new();
    let agent_url = format!("http://127.0.0.1:{agent_port}/invoke");

    let response = http
        .post(&agent_url)
        .send_bytes(&body)
        .map_err(|e| format!("POST to agent failed: {e}"))?;

    let mut output = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut output)
        .map_err(|e| format!("failed to read agent response: {e}"))?;

    let final_state = provider_server.final_state();
    let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

    // Extract the assistant's text content from the OpenAI response.
    // Falls back to raw bytes if the response isn't OpenAI JSON.
    let output = extract_openai_content(&output);

    Ok(DispatchResult {
        output,
        state: final_state,
        duration_ms,
    })
}

/// Build and send a `CompleteMessage` on the data plane.
pub async fn send_complete(
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
    if let Err(e) = queue.send_complete(complete_key, msg).await {
        tracing::error!(error = %e, "failed to send complete");
    }
}
