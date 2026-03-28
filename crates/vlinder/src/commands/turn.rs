use clap::Subcommand;

use crate::config::CliConfig;

use super::connect::open_dag_store;

#[derive(Subcommand, Debug, PartialEq)]
pub enum TurnCommand {
    /// Show all messages in a turn (submission)
    Get {
        /// Submission ID
        submission_id: String,
    },
}

pub fn execute(cmd: TurnCommand) {
    match cmd {
        TurnCommand::Get { submission_id } => get(&submission_id),
    }
}

#[allow(clippy::too_many_lines)]
fn get(submission_id: &str) {
    let config = CliConfig::load();
    let store = open_dag_store(&config).unwrap_or_else(|| {
        eprintln!("Cannot connect to state service. Is the daemon running?");
        std::process::exit(1);
    });

    let nodes = store
        .get_nodes_by_submission(submission_id)
        .unwrap_or_else(|e| {
            eprintln!("Failed to query turn: {e}");
            std::process::exit(1);
        });

    if nodes.is_empty() {
        println!("No messages found for submission {submission_id}");
        return;
    }

    for node in &nodes {
        let (from, to, operation, checkpoint, state, payload) =
            if node.message_type() == vlinder_core::domain::MessageType::Invoke {
                if let Ok(Some((key, msg))) = store.get_invoke_node(&node.id) {
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
                        msg.state.clone(),
                        msg.payload,
                    )
                } else {
                    continue;
                }
            } else if node.message_type() == vlinder_core::domain::MessageType::Complete {
                if let Ok(Some(msg)) = store.get_complete_node(&node.id) {
                    (
                        "agent".to_string(),
                        "harness".to_string(),
                        None::<String>,
                        None::<String>,
                        msg.state,
                        msg.payload,
                    )
                } else {
                    continue;
                }
            } else {
                continue;
            };
        println!("Hash:       {}", node.id);
        println!("Parent:     {}", node.parent_id);
        println!("Type:       {}", node.message_type().as_str());
        println!("From:       {from}");
        println!("To:         {to}");
        println!("Session:    {}", node.session_id());
        if let Some(op) = operation {
            println!("Operation:  {op}");
        }
        if let Some(ckpt) = checkpoint {
            println!("Checkpoint: {ckpt}");
        }
        if let Some(state) = state {
            println!("State:      {state}");
        }
        println!("Created:    {}", node.created_at);
        match std::str::from_utf8(&payload) {
            Ok(text) if !text.is_empty() => println!("Payload:    {text}"),
            _ if !payload.is_empty() => {
                println!("Payload:    <{} bytes binary>", payload.len());
            }
            _ => {}
        }
        println!();
    }
}
