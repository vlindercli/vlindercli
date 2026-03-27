use clap::Subcommand;

use crate::config::CliConfig;
use vlinder_core::domain::{DagStore, SessionId};

use super::connect::open_dag_store;

#[derive(Subcommand, Debug, PartialEq)]
pub enum BranchCommand {
    /// List branches in a session
    List {
        /// Session ID or petname
        #[arg(long)]
        session: String,
    },
    /// Show messages on a branch
    Get {
        /// Branch name
        branch_name: String,
        /// Session ID or petname
        #[arg(long)]
        session: String,
    },
}

pub fn execute(cmd: BranchCommand) {
    match cmd {
        BranchCommand::List { session } => list(&session),
        BranchCommand::Get {
            branch_name,
            session,
        } => get(&session, &branch_name),
    }
}

fn list(session_id_or_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config);
    let session_id = resolve_session_id(&*store, session_id_or_name);

    let branches = store
        .get_branches_for_session(&session_id)
        .unwrap_or_else(|e| {
            eprintln!("Failed to query branches: {e}");
            std::process::exit(1);
        });

    if branches.is_empty() {
        println!("No branches for session '{session_id}'");
        return;
    }

    // Count turns (distinct submissions) per branch
    let nodes = store.get_session_nodes(&session_id).unwrap_or_default();
    let turn_counts: std::collections::HashMap<vlinder_core::domain::BranchId, usize> = {
        let mut per_branch: std::collections::HashMap<
            vlinder_core::domain::BranchId,
            std::collections::HashSet<String>,
        > = std::collections::HashMap::new();
        for n in &nodes {
            per_branch
                .entry(*n.branch_id())
                .or_default()
                .insert(n.submission_id().as_str().to_string());
        }
        per_branch.into_iter().map(|(k, v)| (k, v.len())).collect()
    };

    println!("{:<4} {:<24} {:>5} FORK_POINT", "ID", "BRANCH", "TURNS");
    for b in &branches {
        let turns = turn_counts.get(&b.id).copied().unwrap_or(0);
        let fork = match &b.fork_point {
            Some(h) => {
                let s = h.as_str();
                &s[..8.min(s.len())]
            }
            None => "-",
        };
        println!("{:<4} {:<24} {:>5} {}", b.id, b.name, turns, fork);
    }
}

#[allow(clippy::too_many_lines)]
fn get(session_id_or_name: &str, branch_name: &str) {
    let config = CliConfig::load();
    let store = require_dag_store(&config);
    let session_id = resolve_session_id(&*store, session_id_or_name);

    let branch = store
        .get_branch_by_name(branch_name)
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

    // Get all session nodes and filter to those on this branch's timeline
    let nodes = store.get_session_nodes(&session_id).unwrap_or_else(|e| {
        eprintln!("Failed to query session nodes: {e}");
        std::process::exit(1);
    });

    let branch_nodes: Vec<_> = nodes
        .into_iter()
        .filter(|n| *n.branch_id() == branch.id)
        .collect();

    if branch_nodes.is_empty() {
        println!("No messages on branch '{branch_name}'");
        return;
    }

    // Group by submission_id (turns), preserving causal order
    let sorted = causal_sort(&branch_nodes);
    let mut turn_order: Vec<String> = Vec::new();
    let mut turn_map: std::collections::HashMap<&str, Vec<&vlinder_core::domain::DagNode>> =
        std::collections::HashMap::new();
    for node in &sorted {
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
            let (from, to, operation, checkpoint) = if node.message_type()
                == vlinder_core::domain::MessageType::Invoke
            {
                if let Ok(Some((key, _msg))) = store.get_invoke_node(&node.id) {
                    let vlinder_core::domain::DataMessageKind::Invoke { harness, agent, .. } =
                        &key.kind
                    else {
                        continue;
                    };
                    (harness.as_str().to_string(), agent.to_string(), None, None)
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::Complete {
                ("agent".to_string(), "harness".to_string(), None, None)
            } else if let Some(ref msg) = node.message {
                let (f, t) = msg.sender_receiver();
                (
                    f,
                    t,
                    msg.operation().map(str::to_string),
                    msg.checkpoint().map(str::to_string),
                )
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
            if let Some(ref ckpt) = checkpoint {
                parts.push(format!("ckpt:{ckpt}"));
            }
            println!("  {}", parts.join(" "));
        }
        println!();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn require_dag_store(config: &CliConfig) -> Box<dyn DagStore> {
    open_dag_store(config).unwrap_or_else(|| {
        eprintln!("Cannot connect to state service. Is the daemon running?");
        std::process::exit(1);
    })
}

fn resolve_session_id(store: &dyn DagStore, id_or_name: &str) -> SessionId {
    if let Ok(session_id) = SessionId::try_from(id_or_name.to_string()) {
        return session_id;
    }
    if let Some(session) = store.get_session_by_name(id_or_name).ok().flatten() {
        return session.id;
    }
    eprintln!("Session '{id_or_name}' not found");
    std::process::exit(1);
}

/// Sort nodes in causal order by walking the `parent_hash` Merkle chain.
fn causal_sort(nodes: &[vlinder_core::domain::DagNode]) -> Vec<vlinder_core::domain::DagNode> {
    let by_parent: std::collections::HashMap<&str, &vlinder_core::domain::DagNode> =
        nodes.iter().map(|n| (n.parent_id.as_str(), n)).collect();

    let mut sorted = Vec::with_capacity(nodes.len());
    let Some(root) = nodes.iter().find(|n| n.parent_id.is_empty()) else {
        return nodes.to_vec();
    };

    sorted.push(root.clone());
    let mut current_hash = root.id.as_str();
    while let Some(next) = by_parent.get(current_hash) {
        sorted.push((*next).clone());
        current_hash = next.id.as_str();
    }

    let in_chain: std::collections::HashSet<String> =
        sorted.iter().map(|n| n.id.to_string()).collect();
    for node in nodes {
        if !in_chain.contains(&node.id.to_string()) {
            sorted.push(node.clone());
        }
    }

    sorted
}
