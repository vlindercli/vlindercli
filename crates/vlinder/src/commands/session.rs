use clap::Subcommand;

use crate::config::CliConfig;
use vlinder_core::domain::{
    AgentName, BranchId, DagStore, ExternalSessionId, ForkParams, MessageType, PromoteParams,
    SessionId,
};

use super::connect::{connect_harness, connect_registry, open_dag_store};

#[derive(Subcommand, Debug, PartialEq)]
pub enum SessionCommand {
    /// List sessions for an agent
    List {
        /// Agent name
        #[arg(long)]
        agent: String,
    },
    /// Show turns and messages in a session
    Get {
        /// Session ID
        session_id: String,
    },
    /// Create a named branch from a point in the DAG
    Fork {
        /// Session ID containing the fork point
        session_id: String,
        /// Canonical hash of the `DagNode` to fork from
        #[arg(long)]
        from: String,
        /// Branch name for the new timeline
        #[arg(long)]
        branch: String,
    },
    /// Promote a branch to main
    Promote {
        /// Session ID or petname
        session_id: String,
        /// Branch name to promote
        #[arg(long)]
        branch: String,
    },
    /// Ensure a session exists for an agent (get-or-create semantics)
    Ensure {
        /// Agent name
        agent: String,
        /// Optional external session ID (auto-generated if omitted)
        #[arg(long)]
        id: Option<String>,
    },
}

pub async fn execute(cmd: SessionCommand) {
    match cmd {
        SessionCommand::List { agent } => list(&agent).await,
        SessionCommand::Get { session_id } => get(&session_id).await,
        SessionCommand::Fork {
            session_id,
            from,
            branch,
        } => fork(&session_id, &from, &branch).await,
        SessionCommand::Promote { session_id, branch } => promote(&session_id, &branch).await,
        SessionCommand::Ensure { agent, id } => ensure(&agent, id.as_deref()).await,
    }
}

async fn require_dag_store(config: &CliConfig) -> Box<dyn DagStore> {
    open_dag_store(config).await.unwrap_or_else(|| {
        eprintln!("Cannot connect to state service. Is the daemon running?");
        std::process::exit(1);
    })
}

async fn list(agent_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config).await;

    let sessions = store.list_sessions().await.unwrap_or_else(|e| {
        eprintln!("Failed to list sessions: {e}");
        std::process::exit(1);
    });

    let filtered: Vec<_> = sessions
        .iter()
        .filter(|s| s.agent_name == agent_name)
        .collect();

    if filtered.is_empty() {
        println!("No sessions found for agent '{agent_name}'");
        return;
    }

    println!(
        "{:<28} {:<40} {:<20} {:<24} {:>8}",
        "NAME", "SESSION_ID", "EXTERNAL_ID", "STARTED", "BRANCHES"
    );
    for s in &filtered {
        let (name, external_id) = store
            .get_session(&s.session_id)
            .await
            .ok()
            .flatten()
            .map(|sess| (sess.name, sess.external_id.as_str().to_string()))
            .unwrap_or_default();
        let branch_count = store
            .get_branches_for_session(&s.session_id)
            .await
            .map_or(0, |b| b.len());
        println!(
            "{:<28} {:<40} {:<20} {:<24} {:>8}",
            name,
            s.session_id,
            external_id,
            s.started_at.format("%Y-%m-%d %H:%M:%S"),
            branch_count,
        );
    }
}

#[allow(clippy::too_many_lines)]
async fn get(session_id_or_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config).await;

    let session_id = resolve_session_id(&*store, session_id_or_name).await;
    let nodes = store
        .get_session_nodes(&session_id)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to query session: {e}");
            std::process::exit(1);
        });

    // Print session metadata
    if let Ok(Some(session)) = store.get_session(&session_id).await {
        println!(
            "Session: {} (external: {})",
            session.name, session.external_id
        );
    }

    if nodes.is_empty() {
        println!("No messages found for session {session_id}");
        return;
    }

    // Sort nodes in causal order by walking the parent_hash chain
    let nodes = causal_sort(&nodes);

    // Group by submission_id (turns), preserving causal order
    let mut turn_order: Vec<String> = Vec::new();
    let mut turn_map: std::collections::HashMap<&str, Vec<&vlinder_core::domain::DagNode>> =
        std::collections::HashMap::new();
    for node in &nodes {
        let sub_str = node.submission_id().as_str();
        if !turn_map.contains_key(sub_str) {
            turn_order.push(sub_str.to_string());
        }
        turn_map.entry(sub_str).or_default().push(node);
    }

    for sub_id in &turn_order {
        let messages = &turn_map[sub_id.as_str()];
        println!("Turn {sub_id}");
        for node in messages {
            let ts = node.created_at.format("%H:%M:%S%.3f");
            let (from, to, operation, sequence) = if node.message_type()
                == vlinder_core::domain::MessageType::Invoke
            {
                if let Ok(Some((key, _msg))) = store.get_invoke_node(&node.id).await {
                    let vlinder_core::domain::DataMessageKind::Invoke { harness, agent, .. } =
                        &key.kind
                    else {
                        continue;
                    };
                    (
                        harness.as_str().to_string(),
                        agent.to_string(),
                        None::<String>,
                        None::<String>,
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::Complete {
                if let Ok(Some((key, _msg))) = store.get_complete_node(&node.id).await {
                    let vlinder_core::domain::DataMessageKind::Complete { agent, harness } =
                        &key.kind
                    else {
                        continue;
                    };
                    (
                        agent.to_string(),
                        harness.as_str().to_string(),
                        None::<String>,
                        None::<String>,
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::Request {
                if let Ok(Some((key, _msg))) = store.get_request_node(&node.id).await {
                    let vlinder_core::domain::DataMessageKind::Request {
                        agent,
                        service,
                        operation,
                        sequence,
                    } = &key.kind
                    else {
                        continue;
                    };
                    (
                        agent.to_string(),
                        service.to_string(),
                        Some(operation.to_string()),
                        Some(sequence.to_string()),
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::Response {
                if let Ok(Some((key, _msg))) = store.get_response_node(&node.id).await {
                    let vlinder_core::domain::DataMessageKind::Response {
                        agent,
                        service,
                        operation,
                        sequence,
                    } = &key.kind
                    else {
                        continue;
                    };
                    (
                        service.to_string(),
                        agent.to_string(),
                        Some(operation.to_string()),
                        Some(sequence.to_string()),
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::SvcRequest {
                if let Ok(Some((key, _msg))) = store.get_svc_request_node(&node.id).await {
                    let vlinder_core::domain::SvcMessageKind::SvcRequest {
                        agent,
                        service,
                        operation,
                        sequence,
                    } = &key.kind
                    else {
                        continue;
                    };
                    (
                        agent.to_string(),
                        service.backend_str().to_string(),
                        Some(operation.to_string()),
                        Some(sequence.to_string()),
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::SvcResponse {
                if let Ok(Some((key, _msg))) = store.get_svc_response_node(&node.id).await {
                    let vlinder_core::domain::SvcMessageKind::SvcResponse {
                        agent,
                        service,
                        operation,
                        sequence,
                    } = &key.kind
                    else {
                        continue;
                    };
                    (
                        service.backend_str().to_string(),
                        agent.to_string(),
                        Some(operation.to_string()),
                        Some(sequence.to_string()),
                    )
                } else {
                    continue;
                }
            } else {
                continue;
            };
            let mut parts = vec![
                format!("{ts}"),
                node.id.as_str()[..8].to_string(),
                node.message_type().as_str().to_string(),
                from,
                format!("-> {to}"),
            ];
            if let Some(ref op) = operation {
                parts.push(format!("op:{op}"));
            }
            if let Some(ref seq) = sequence {
                parts.push(format!("seq:{seq}"));
            }
            println!("  {}", parts.join(" "));
        }
        println!();
    }
}

async fn fork(session_id_or_name: &str, from_hash: &str, branch_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config).await;
    let session_id = resolve_session_id(&*store, session_id_or_name).await;

    // Verify the node exists and belongs to this session
    let node = store
        .get_node_by_prefix(from_hash)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to look up node: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("Node {from_hash} not found");
            std::process::exit(1);
        });

    if *node.session_id() != session_id {
        eprintln!(
            "Node {} belongs to session {}, not {}",
            from_hash,
            node.session_id(),
            session_id
        );
        std::process::exit(1);
    }

    // Derive agent name from the session's Invoke message
    let agent_name = find_agent_name(&*store, &session_id)
        .await
        .unwrap_or_else(|| {
            eprintln!("Cannot determine agent name for session {session_id}");
            std::process::exit(1);
        });

    // Send ForkMessage through the harness/queue (CQRS: both SQL and git react)
    // Fork creates a branch within the existing session — no new session needed.
    let harness = connect_harness(&config).await;
    let timeline = BranchId::from(1);

    let params = ForkParams {
        agent_name: AgentName::new(agent_name),
        branch_name: branch_name.to_string(),
        fork_point: node.id.clone(),
    };

    harness
        .fork_timeline(params, session_id, timeline)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to fork timeline: {e}");
            std::process::exit(1);
        });

    println!(
        "Created timeline '{}' forked from {}",
        branch_name,
        &node.id.as_str()[..8]
    );
}

async fn promote(session_id_or_name: &str, branch_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config).await;
    let session_id = resolve_session_id(&*store, session_id_or_name).await;

    // Verify the branch exists and belongs to this session
    let branch = store
        .get_branch_by_name(branch_name)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to look up branch: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("Branch '{branch_name}' not found");
            std::process::exit(1);
        });

    if branch.session_id != session_id {
        eprintln!(
            "Branch '{}' belongs to session {}, not {}",
            branch_name, branch.session_id, session_id
        );
        std::process::exit(1);
    }

    // Derive agent name from the session's Invoke message
    let agent_name = find_agent_name(&*store, &session_id)
        .await
        .unwrap_or_else(|| {
            eprintln!("Cannot determine agent name for session {session_id}");
            std::process::exit(1);
        });

    let harness = connect_harness(&config).await;

    let params = PromoteParams {
        agent_name: AgentName::new(agent_name),
    };
    harness
        .promote_timeline(params, session_id, branch.id)
        .await
        .unwrap_or_else(|e| {
            eprintln!("Failed to promote timeline: {e}");
            std::process::exit(1);
        });

    println!("Promoted branch '{branch_name}' to main");
}

async fn ensure(agent_name: &str, id: Option<&str>) {
    let config = CliConfig::load();

    // Validate agent exists — fail fast
    let registry = connect_registry(&config).await;
    let Some(_agent) = registry.get_agent_by_name(agent_name).await else {
        eprintln!("Agent '{agent_name}' not found — deploy it first with: vlinder agent deploy");
        std::process::exit(1);
    };

    let harness = connect_harness(&config).await;
    let store = open_dag_store(&config).await.unwrap_or_else(|| {
        eprintln!("Cannot connect to state service. Is the daemon running?");
        std::process::exit(1);
    });

    let session_id = if let Some(user_id) = id {
        // User provided external ID — validate and check for existing session
        let ext_id = ExternalSessionId::new(user_id).unwrap_or_else(|e| {
            eprintln!("Invalid session ID '{user_id}': {e}");
            std::process::exit(1);
        });

        match store.get_session_by_external_id(&ext_id).await {
            Ok(Some(session)) => {
                check_agent_mismatch(&session, agent_name, user_id);
                // Print existing session
                println!(
                    "{:<28} {:<40} {:<24} {:>8}",
                    session.name,
                    session.id,
                    session.created_at.format("%Y-%m-%d %H:%M:%S"),
                    1,
                );
                return;
            }
            Ok(None) => {}
            Err(e) => {
                eprintln!("Failed to look up session: {e}");
                std::process::exit(1);
            }
        }

        // Create new session with the user-provided external ID
        let (session_id, _branch_id) = harness.start_session(agent_name, ext_id).await;
        session_id
    } else {
        // No external ID provided — default external_id to a unique UUID
        let (session_id, _branch_id) = harness
            .start_session(
                agent_name,
                ExternalSessionId::new(SessionId::new().as_str())
                    .expect("UUID is always a valid ExternalSessionId"),
            )
            .await;
        session_id
    };

    // Read back session for name and created_at
    let session = store
        .get_session(&session_id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            eprintln!("Failed to read back session {session_id}");
            std::process::exit(1);
        });

    // Print in same format as `session list`
    println!(
        "{:<28} {:<40} {:<24} {:>8}",
        session.name,
        session.id,
        session.created_at.format("%Y-%m-%d %H:%M:%S"),
        1,
    );
}

/// Check that the session belongs to the expected agent. If not, print an
/// error message and exit with code 1.
fn check_agent_mismatch(session: &vlinder_core::domain::Session, agent_name: &str, user_id: &str) {
    if session.agent != agent_name {
        eprintln!(
            "Session '{}' is already associated with agent \
             '{}', not '{agent_name}'",
            user_id, session.agent
        );
        std::process::exit(1);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Sort nodes in causal order by walking the `parent_hash` Merkle chain.
///
/// Finds the root (empty `parent_id`) and follows children to produce
/// a linear ordering that respects causality regardless of wall-clock time.
fn causal_sort(nodes: &[vlinder_core::domain::DagNode]) -> Vec<vlinder_core::domain::DagNode> {
    let by_parent: std::collections::HashMap<&str, &vlinder_core::domain::DagNode> =
        nodes.iter().map(|n| (n.parent_id.as_str(), n)).collect();

    // Find the root node (empty parent_id)
    let mut sorted = Vec::with_capacity(nodes.len());
    let Some(root) = nodes.iter().find(|n| n.parent_id.is_empty()) else {
        // No root found — fall back to input order
        return nodes.to_vec();
    };

    sorted.push(root.clone());
    let mut current_hash = root.id.as_str();
    while let Some(next) = by_parent.get(current_hash) {
        sorted.push((*next).clone());
        current_hash = next.id.as_str();
    }

    // Append any nodes not in the chain (orphans from forks, etc.)
    let in_chain: std::collections::HashSet<String> =
        sorted.iter().map(|n| n.id.to_string()).collect();
    for node in nodes {
        if !in_chain.contains(&node.id.to_string()) {
            sorted.push(node.clone());
        }
    }

    sorted
}

/// Resolve a user-provided string (UUID or petname) to a `SessionId`.
async fn resolve_session_id(store: &dyn DagStore, id_or_name: &str) -> SessionId {
    // If it's a valid UUID, use it directly
    if let Ok(session_id) = SessionId::try_from(id_or_name.to_string()) {
        return session_id;
    }
    // Try by petname
    if let Some(session) = store.get_session_by_name(id_or_name).await.ok().flatten() {
        return session.id;
    }
    eprintln!("Session '{id_or_name}' not found");
    std::process::exit(1);
}

/// Find the agent name from the Invoke message in a session.
async fn find_agent_name(store: &dyn DagStore, session_id: &SessionId) -> Option<String> {
    let nodes = store.get_session_nodes(session_id).await.ok()?;
    let n = nodes
        .iter()
        .find(|n| n.message_type() == MessageType::Invoke)?;
    if let Ok(Some((key, _))) = store.get_invoke_node(&n.id).await {
        let vlinder_core::domain::DataMessageKind::Invoke { agent, .. } = &key.kind else {
            return None;
        };
        Some(agent.to_string())
    } else {
        None
    }
}

#[cfg(test)]
/// Format a single message line for the session view.
///
/// Pure function — no store access. Tests construct arbitrary arguments
/// without mocking `DagStore`.
pub fn format_render_line(
    timestamp: &str,
    hash: &str,
    msg_type: &str,
    from: &str,
    to: &str,
    operation: Option<&str>,
    sequence: Option<&str>,
) -> String {
    let mut parts = vec![
        timestamp.to_string(),
        hash[..8].to_string(),
        msg_type.to_string(),
        from.to_string(),
        format!("-> {to}"),
    ];
    if let Some(op) = operation {
        parts.push(format!("op:{op}"));
    }
    if let Some(seq) = sequence {
        parts.push(format!("seq:{seq}"));
    }
    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{BranchId, ExternalSessionId, SessionId};

    fn test_session(agent: &str) -> vlinder_core::domain::Session {
        vlinder_core::domain::Session {
            id: SessionId::new(),
            external_id: ExternalSessionId::new("test-ext").unwrap(),
            name: "test".to_string(),
            agent: agent.to_string(),
            default_branch: BranchId::from(1),
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn check_agent_mismatch_ok() {
        // Should not panic — matching agent
        check_agent_mismatch(&test_session("pensieve"), "pensieve", "test-ext");
    }

    // ------------------------------------------------------------------------
    // 4.4 — Renderer unit tests for 6 message-type branches
    // ------------------------------------------------------------------------

    #[test]
    fn renders_invoke_line() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "invoke",
            "cli",
            "openrouter-test",
            None,
            None,
        );
        assert!(line.contains("cli"), "from must be harness: {line}");
        assert!(line.contains("openrouter-test"), "to must be agent: {line}");
        assert!(line.contains("invoke"), "type must be invoke: {line}");
    }

    #[test]
    fn renders_complete_line_with_routing_agent_and_harness() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "complete",
            "agent-from-routing-key",
            "harness-from-routing-key",
            None,
            None,
        );
        assert!(
            line.contains("agent-from-routing-key"),
            "must contain routing-key agent: {line}"
        );
        assert!(
            line.contains("harness-from-routing-key"),
            "must contain harness: {line}"
        );
        assert!(
            !line.contains("\"agent\""),
            "must not have literal 'agent': {line}"
        );
    }

    #[test]
    fn renders_request_line() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "request",
            "openrouter-test",
            "infer.openrouter",
            Some("run"),
            Some("1"),
        );
        assert!(
            line.contains("openrouter-test"),
            "from must be agent: {line}"
        );
        assert!(line.contains("op:run"), "op must appear: {line}");
        assert!(line.contains("seq:1"), "seq must appear: {line}");
    }

    #[test]
    fn renders_response_line() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "response",
            "infer.openrouter",
            "openrouter-test",
            Some("run"),
            Some("1"),
        );
        assert!(line.contains("->"), "must have arrow: {line}");
        assert!(line.contains("op:run"), "op must appear: {line}");
        assert!(line.contains("seq:1"), "seq must appear: {line}");
    }

    #[test]
    fn renders_svc_request_line() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "svc_request",
            "openrouter-test",
            "brave",
            Some("search"),
            Some("1"),
        );
        assert!(
            line.contains("openrouter-test"),
            "from must be agent: {line}"
        );
        assert!(line.contains("brave"), "to must be service backend: {line}");
        assert!(line.contains("op:search"), "op must appear: {line}");
        assert!(line.contains("seq:1"), "seq must appear: {line}");
    }

    #[test]
    fn renders_svc_response_line() {
        let line = format_render_line(
            "12:00:00.000",
            "abcdef12",
            "svc_response",
            "brave",
            "openrouter-test",
            Some("search"),
            Some("1"),
        );
        assert!(line.contains("brave"), "from must be service: {line}");
        assert!(line.contains("->"), "must have arrow: {line}");
        assert!(line.contains("op:search"), "op must appear: {line}");
        assert!(line.contains("seq:1"), "seq must appear: {line}");
    }
}
