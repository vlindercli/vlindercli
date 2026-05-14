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
    MessageId, MessageQueue, ParsedResponse, Registry, RuntimeDiagnostics, ToolCall,
    ToolCallParser,
};

use crate::handler::InvokeHandler;
use crate::hosts::build_hosts;
use crate::provider_server::ProviderServer;

#[cfg(feature = "openrouter")]
use vlinder_infer_openrouter::OpenAiToolCallParser;

#[cfg(feature = "mcp")]
use vlinder_core::domain::ToolCallProtocol;
#[cfg(feature = "mcp")]
use vlinder_mcp::McpProtocol;

/// Serialize a conversation (`history` + `current_input`) to `OpenAI` Chat Completions format.
///
/// Maps domain `Message::Agent` → wire-format `"assistant"`,
/// `Message::User` → `"user"`. Wraps in `{"messages": [...]}` and serializes as JSON.
pub fn serialize_openai_conversation(messages: &[Message]) -> Vec<u8> {
    #[cfg(feature = "openrouter")]
    {
        OpenAiToolCallParser.serialize_conversation(messages)
    }
    #[cfg(not(feature = "openrouter"))]
    {
        let wire_messages: Vec<serde_json::Value> = messages
            .iter()
            .map(|m| match m {
                Message::User { content } => serde_json::json!({
                    "role": "user",
                    "content": content,
                }),
                Message::Agent { content, .. } => serde_json::json!({
                    "role": "assistant",
                    "content": content.unwrap_or_default(),
                }),
                Message::System { content } => serde_json::json!({
                    "role": "system",
                    "content": content,
                }),
                Message::Tool { content, .. } => serde_json::json!({
                    "role": "tool",
                    "content": String::from_utf8_lossy(content).into_owned(),
                }),
            })
            .collect();

        let body = serde_json::json!({"messages": wire_messages});
        serde_json::to_vec(&body).unwrap_or_default()
    }
}

/// Decode tool result bytes into a text string for LLM serialization.
///
/// Uses the MCP protocol decoder when the `mcp` feature is enabled,
/// falling back to lossy UTF-8 conversion otherwise.
pub fn decode_tool_content(content: &[u8]) -> String {
    #[cfg(feature = "mcp")]
    {
        McpProtocol.decode_tool_result(content)
    }
    #[cfg(not(feature = "mcp"))]
    {
        String::from_utf8_lossy(content).into_owned()
    }
}

/// Extract text content from an `OpenAI` Chat Completions response.
///
/// Returns `Some(bytes)` only when `raw` is valid JSON exposing a string
/// at `choices[0].message.content`. Returns `None` otherwise — including
/// when the body is not JSON, lacks the expected shape, or has a non-string
/// content. Callers decide how to handle `None`; this function intentionally
/// does not fall back to the raw bytes, because doing so silently lets a
/// malformed agent response masquerade as a normal assistant turn.
pub fn extract_openai_content(raw: &[u8]) -> Option<Vec<u8>> {
    serde_json::from_slice::<serde_json::Value>(raw)
        .ok()
        .and_then(|v| {
            v.get("choices")
                .and_then(|c| c.get(0))
                .and_then(|c| c.get("message"))
                .and_then(|m| m.get("content"))
                .and_then(|c| c.as_str().map(|s| s.to_string().into_bytes()))
        })
}

/// Format a sentinel string used as `ParsedResponse.content` when the
/// agent's response can't be parsed. The sentinel is emitted into the
/// assistant turn so the failure is visible to the caller (e.g. the API
/// client) rather than being laundered into a plausible-looking message.
fn parse_error_sentinel(reason: &str, raw: &[u8]) -> String {
    let preview = String::from_utf8_lossy(&raw[..raw.len().min(200)]);
    format!("[parse error: {reason}; first 200 bytes: {preview}]")
}

/// Session-scoped provider — built once per sidecar lifetime, reused
/// across all invokes for that session. Holds the long-lived `ProviderServer`
/// plus any per-session metadata `dispatch_one_invoke` needs.
///
/// Rationale: per-invoke handler creation races with axum's per-connection
/// task lifecycle when the agent uses HTTP keep-alive — old handlers stay
/// alive and answer requests they shouldn't. Session-scoped resolves this
/// by having exactly one handler at any time. `chain_head` is reset to the
/// invoke's `dag_id` on each new invoke via [`ProviderServer::set_chain_head`].
pub struct SessionProvider {
    provider_server: ProviderServer,
    /// Whether this agent has object storage configured. Controls whether
    /// `msg.state` is honored on each invoke. Snapshotted at session start.
    has_object_storage: bool,
}

impl SessionProvider {
    /// Borrow the underlying `ProviderServer` (for tests / advanced uses).
    pub fn provider_server(&self) -> &ProviderServer {
        &self.provider_server
    }
}

/// Set up the session-scoped `ProviderServer`. Called **once** per sidecar
/// lifetime, after the agent container is ready and the registry is reachable.
/// The returned `SessionProvider` is reused across every invoke for this run.
///
/// The `InvokeHandler` is initialized with `chain_head = root()`. On each
/// incoming invoke, [`dispatch_one_invoke`] resets `chain_head` to the
/// invoke's `dag_id` — so the chain reconstructs correctly for fork-resume
/// because the harness encodes "what `chain_head` should be" via the invoke's
/// `dag_parent`, and the recording queue stamps `invoke.dag_id` from that.
pub async fn start_session_provider(
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    registry: &Arc<dyn Registry>,
    agent_name: &AgentName,
    branch: vlinder_core::domain::BranchId,
    submission: vlinder_core::domain::SubmissionId,
    session: vlinder_core::domain::SessionId,
) -> Result<SessionProvider, String> {
    let agent_info = registry
        .get_agent_by_name(agent_name.as_str())
        .await
        .ok_or_else(|| format!("agent '{}' not found in registry", agent_name.as_str()))?;

    let hosts = build_hosts(&agent_info);
    let has_object_storage = agent_info.object_storage.is_some();

    // Collect tool definitions from MCP servers referenced by the agent.
    let mut tools: Vec<serde_json::Value> = Vec::new();
    for mcp_name in &agent_info.requirements.mcp {
        if let Some(server) = registry.get_mcp_server(mcp_name).await {
            tools.extend(server.tools);
        }
    }

    let state = Arc::new(std::sync::RwLock::new(None));
    let handler = InvokeHandler::new(
        queue.clone(),
        crate::handler::RoutingContext {
            branch,
            submission,
            session,
        },
        agent_name.clone(),
        Arc::clone(&state),
        DagNodeId::root(),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544, tools).await;

    Ok(SessionProvider {
        provider_server,
        has_object_storage,
    })
}

/// Dispatch a single invoke against the session-scoped provider.
///
/// 1. Reset `chain_head` to `msg.dag_id` so outgoing requests parent on the invoke.
/// 2. Seed agent state from `msg.state` (if the agent has object storage).
/// 3. POST conversation to agent on localhost.
/// 4. Read response, parse, return.
///
/// The caller is responsible for building diagnostics, sending the
/// `CompleteMessage` (with `complete_dag_parent = result.chain_head`), and
/// acknowledging the invoke.
pub fn dispatch_one_invoke(
    session: &SessionProvider,
    agent_port: u16,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
) -> Result<DispatchResult, String> {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        return Err("dispatch_one_invoke: expected Invoke key".into());
    };

    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_one_invoke.entry",
        session = %key.session,
        submission = %key.submission,
        agent = %agent.as_str(),
        msg_dag_id = %msg.dag_id,
        msg_dag_parent = %msg.dag_parent,
        "dispatch_one_invoke entered"
    );

    let started_at = Instant::now();

    reset_session_state_for_invoke(session, key, msg, agent.as_str());

    // Combine history + current_input into a single conversation.
    let mut conversation: Vec<Message> = msg.history.clone();
    conversation.extend(msg.current_input.clone());

    // Convert tool result bytes to text strings via protocol decoder.
    let conversation_for_serialization: Vec<Message> = conversation
        .iter()
        .map(|m| match m {
            Message::Tool {
                tool_call_id,
                content,
            } => {
                let text = decode_tool_content(content);
                Message::Tool {
                    tool_call_id: tool_call_id.clone(),
                    content: text.into_bytes(),
                }
            }
            other => other.clone(),
        })
        .collect();

    let body = serialize_openai_conversation(&conversation_for_serialization);

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

    let final_state = session.provider_server.final_state();
    let duration_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX);

    let parsed = parse_agent_response(&output);

    let final_chain_head = session.provider_server.chain_head();
    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_one_invoke.exit",
        session = %key.session,
        agent = %agent.as_str(),
        msg_dag_id = %msg.dag_id,
        chain_head_final = %final_chain_head,
        "dispatch_one_invoke returning"
    );
    Ok(DispatchResult {
        output,
        content: parsed.content,
        tool_calls: parsed.tool_calls,
        state: final_state,
        duration_ms,
        chain_head: final_chain_head,
    })
}

/// Reset all derived in-memory state on the session-scoped provider to the
/// values this incoming invoke encodes. Called once per invoke, before the
/// agent POST. Keeps `chain_head`, routing ids, and state seed in sync with
/// the user's current turn.
fn reset_session_state_for_invoke(
    session: &SessionProvider,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
    agent: &str,
) {
    session.provider_server.set_chain_head(msg.dag_id.clone());

    session
        .provider_server
        .set_routing(crate::handler::RoutingContext {
            branch: key.branch,
            submission: key.submission.clone(),
            session: key.session.clone(),
        });

    let initial_state = if session.has_object_storage {
        Some(msg.state.clone().unwrap_or_default())
    } else {
        None
    };
    session.provider_server.set_state(initial_state);

    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_one_invoke.chain_head_reset",
        session = %key.session,
        submission = %key.submission,
        agent = agent,
        chain_head_init = %msg.dag_id,
        "chain_head + routing reset for this invoke"
    );
}

/// Parse the raw bytes the agent returned into structured content + tool calls.
/// Honors the `openrouter` feature flag to choose strict vs lenient parsing.
fn parse_agent_response(output: &[u8]) -> ParsedResponse {
    if cfg!(feature = "openrouter") {
        match OpenAiToolCallParser.parse_response(output) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    event = "dispatch.parse_failed",
                    error = ?e,
                    bytes = output.len(),
                    first_200 = %String::from_utf8_lossy(&output[..output.len().min(200)]),
                    "agent response failed strict parse, falling back"
                );
                let content = extract_openai_content(output)
                    .and_then(|b| String::from_utf8(b).ok())
                    .unwrap_or_else(|| parse_error_sentinel(&format!("{e:?}"), output));
                ParsedResponse {
                    content: Some(content),
                    tool_calls: None,
                }
            }
        }
    } else {
        let content = extract_openai_content(output)
            .and_then(|b| String::from_utf8(b).ok())
            .or_else(|| {
                tracing::warn!(
                    event = "dispatch.extract_failed",
                    bytes = output.len(),
                    first_200 = %String::from_utf8_lossy(&output[..output.len().min(200)]),
                    "agent response did not expose choices[0].message.content; passing raw bytes through"
                );
                String::from_utf8(output.to_vec()).ok()
            });
        ParsedResponse {
            content,
            tool_calls: None,
        }
    }
}

/// Result of a successful dispatch.
pub struct DispatchResult {
    /// Raw output from the agent.
    pub output: Vec<u8>,
    /// Parsed text content from the agent's response.
    pub content: Option<String>,
    /// Parsed tool calls from the agent's response.
    pub tool_calls: Option<Vec<ToolCall>>,
    /// Final KV state after the invocation.
    pub state: Option<String>,
    /// Wall-clock duration of the invocation in milliseconds.
    pub duration_ms: u64,
    /// DAG id of the last node (chain head) for parent chaining.
    pub chain_head: DagNodeId,
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
#[allow(clippy::too_many_lines)]
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

    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_invoke.entry",
        session = %key.session,
        submission = %key.submission,
        agent = %agent.as_str(),
        msg_dag_id = %msg.dag_id,
        msg_dag_parent = %msg.dag_parent,
        "dispatch_invoke entered"
    );

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

    // Collect tool definitions from MCP servers referenced by the agent.
    let mut tools: Vec<serde_json::Value> = Vec::new();
    for mcp_name in &agent_info.requirements.mcp {
        if let Some(server) = registry.get_mcp_server(mcp_name).await {
            tools.extend(server.tools);
        }
    }

    let state = Arc::new(std::sync::RwLock::new(initial_state));
    let chain_head_init = msg.dag_id.clone();
    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_invoke.handler_init",
        session = %key.session,
        agent = %agent.as_str(),
        chain_head_init = %chain_head_init,
        "creating InvokeHandler with chain_head_init"
    );
    let handler = InvokeHandler::new(
        queue.clone(),
        crate::handler::RoutingContext {
            branch: key.branch,
            submission: key.submission.clone(),
            session: key.session.clone(),
        },
        agent.clone(),
        Arc::clone(&state),
        chain_head_init.clone(),
    );
    let provider_server = ProviderServer::start(handler, hosts, state, 3544, tools).await;

    // Combine history + current_input into a single conversation.
    let mut conversation: Vec<Message> = msg.history.clone();
    conversation.extend(msg.current_input.clone());

    // Convert tool result bytes to text strings via protocol decoder.
    let conversation_for_serialization: Vec<Message> = conversation
        .iter()
        .map(|m| match m {
            Message::Tool {
                tool_call_id,
                content,
            } => {
                let text = decode_tool_content(content);
                Message::Tool {
                    tool_call_id: tool_call_id.clone(),
                    content: text.into_bytes(),
                }
            }
            other => other.clone(),
        })
        .collect();

    let body = serialize_openai_conversation(&conversation_for_serialization);

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

    // Parse the agent's response using the OpenAI‑compatible parser.
    let parsed = if cfg!(feature = "openrouter") {
        match OpenAiToolCallParser.parse_response(&output) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(
                    event = "dispatch.parse_failed",
                    error = ?e,
                    bytes = output.len(),
                    first_200 = %String::from_utf8_lossy(&output[..output.len().min(200)]),
                    "agent response failed strict parse, falling back"
                );
                let content = extract_openai_content(&output)
                    .and_then(|b| String::from_utf8(b).ok())
                    .unwrap_or_else(|| parse_error_sentinel(&format!("{e:?}"), &output));
                ParsedResponse {
                    content: Some(content),
                    tool_calls: None,
                }
            }
        }
    } else {
        let content = extract_openai_content(&output)
            .and_then(|b| String::from_utf8(b).ok())
            .or_else(|| {
                tracing::warn!(
                    event = "dispatch.extract_failed",
                    bytes = output.len(),
                    first_200 = %String::from_utf8_lossy(&output[..output.len().min(200)]),
                    "agent response did not expose choices[0].message.content; passing raw bytes through"
                );
                String::from_utf8(output.clone()).ok()
            });
        ParsedResponse {
            content,
            tool_calls: None,
        }
    };

    let final_chain_head = provider_server.chain_head();
    tracing::debug!(
        event = "chain_head_trace",
        site = "dispatch_invoke.exit",
        session = %key.session,
        agent = %agent.as_str(),
        msg_dag_id = %msg.dag_id,
        chain_head_init = %chain_head_init,
        chain_head_final = %final_chain_head,
        "dispatch_invoke returning"
    );
    Ok(DispatchResult {
        output,
        content: parsed.content,
        tool_calls: parsed.tool_calls,
        state: final_state,
        duration_ms,
        chain_head: final_chain_head,
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
    send_complete_with_parsed(
        queue,
        key,
        agent,
        output,
        None,
        None,
        state,
        diagnostics,
        DagNodeId::root(),
    )
    .await;
}

/// Build and send a `CompleteMessage` with parsed content and tool calls.
#[allow(clippy::too_many_arguments)]
pub async fn send_complete_with_parsed(
    queue: &dyn MessageQueue,
    key: &DataRoutingKey,
    agent: &AgentName,
    output: Vec<u8>,
    content: Option<String>,
    tool_calls: Option<Vec<ToolCall>>,
    state: Option<String>,
    diagnostics: RuntimeDiagnostics,
    complete_dag_parent: DagNodeId,
) {
    tracing::debug!(
        event = "chain_head_trace",
        site = "send_complete_with_parsed.entry",
        session = %key.session,
        agent = %agent.as_str(),
        complete_dag_parent = %complete_dag_parent,
        "sending complete"
    );
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
        dag_parent: complete_dag_parent,
        content,
        tool_calls,
        state,
        diagnostics,
        payload: output,
    };
    if let Err(e) = queue.send_complete(complete_key, msg).await {
        tracing::error!(error = %e, "failed to send complete");
    }
}
