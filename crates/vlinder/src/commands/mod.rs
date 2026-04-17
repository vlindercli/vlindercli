mod agent;
pub(crate) mod branch;
mod connect;
mod context;
mod fleet;
mod help;
mod model;
mod repl;
mod secret;
mod session;

mod turn;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "vlinder")]
#[command(about = "Run AI agents locally")]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum Command {
    /// Manage and run agents
    Agent {
        #[command(subcommand)]
        cmd: agent::AgentCommand,
    },
    /// Run a fleet of agents
    Fleet {
        #[command(subcommand)]
        cmd: fleet::FleetCommand,
    },
    /// Interactive support — runs the bundled support fleet
    Support,
    /// Manage models from catalogs
    Model {
        #[command(subcommand)]
        cmd: model::ModelCommand,
    },
    /// Manage secrets (private keys, `NKeys`, API keys)
    Secret {
        #[command(subcommand)]
        cmd: secret::SecretCommand,
    },
    /// Inspect and repair agent sessions
    Session {
        #[command(subcommand)]
        cmd: session::SessionCommand,
    },
    /// Inspect branches within a session
    Branch {
        #[command(subcommand)]
        cmd: branch::BranchCommand,
    },
    /// Inspect individual turns within a session
    Turn {
        #[command(subcommand)]
        cmd: turn::TurnCommand,
    },
    /// Switch between deployment targets (local, aws, etc.)
    Context {
        #[command(subcommand)]
        cmd: context::ContextCommand,
    },
}

pub async fn run() {
    let cli = Cli::parse();

    match cli.command {
        Command::Agent { cmd } => agent::execute(cmd).await,
        Command::Fleet { cmd } => fleet::execute(cmd).await,
        Command::Support => help::execute().await,
        Command::Model { cmd } => model::execute(cmd).await,
        Command::Secret { cmd } => secret::execute(cmd).await,
        Command::Session { cmd } => session::execute(cmd).await,
        Command::Branch { cmd } => branch::execute(cmd).await,
        Command::Turn { cmd } => turn::execute(cmd).await,
        Command::Context { cmd } => context::execute(cmd).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::path::PathBuf;

    #[test]
    fn cli_agent_run_requires_name() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "run", "todoapp"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Run {
                    name: "todoapp".to_string(),
                    session: None,
                    branch: None,
                    prompt: None,
                }
            }
        );
    }

    #[test]
    fn cli_agent_run_with_branch() {
        let cli =
            Cli::try_parse_from(["vlinder", "agent", "run", "todoapp", "--branch", "fix-typo"])
                .unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Run {
                    name: "todoapp".to_string(),
                    session: None,
                    branch: Some("fix-typo".to_string()),
                    prompt: None,
                }
            }
        );
    }

    #[test]
    fn cli_agent_run_with_session() {
        let cli = Cli::try_parse_from([
            "vlinder",
            "agent",
            "run",
            "todoapp",
            "--session",
            "my-session",
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Run {
                    name: "todoapp".to_string(),
                    session: Some("my-session".to_string()),
                    branch: None,
                    prompt: None,
                }
            }
        );
    }

    #[test]
    fn cli_agent_run_with_session_and_branch() {
        let cli = Cli::try_parse_from([
            "vlinder",
            "agent",
            "run",
            "todoapp",
            "--session",
            "my-session",
            "--branch",
            "fix-typo",
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Run {
                    name: "todoapp".to_string(),
                    session: Some("my-session".to_string()),
                    branch: Some("fix-typo".to_string()),
                    prompt: None,
                }
            }
        );
    }

    #[test]
    fn cli_agent_run_without_name_fails() {
        let result = Cli::try_parse_from(["vlinder", "agent", "run"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_agent_deploy_default_path() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "deploy"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Deploy { path: None }
            }
        );
    }

    #[test]
    fn cli_agent_deploy_with_path() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "deploy", "-p", "/tmp/agent"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Deploy {
                    path: Some(PathBuf::from("/tmp/agent")),
                }
            }
        );
    }

    #[test]
    fn cli_missing_subcommand_fails() {
        let result = Cli::try_parse_from(["vlinder"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_agent_missing_action_fails() {
        let result = Cli::try_parse_from(["vlinder", "agent"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_agent_list() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "list"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::List
            }
        );
    }

    #[test]
    fn cli_agent_get() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "get", "pensieve"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::Get {
                    name: "pensieve".to_string()
                }
            }
        );
    }

    #[test]
    fn cli_fleet_deploy_default_path() {
        let cli = Cli::try_parse_from(["vlinder", "fleet", "deploy"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Fleet {
                cmd: fleet::FleetCommand::Deploy { path: None }
            }
        );
    }

    #[test]
    fn cli_fleet_deploy_with_path() {
        let cli = Cli::try_parse_from(["vlinder", "fleet", "deploy", "-p", "/tmp/fleet"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Fleet {
                cmd: fleet::FleetCommand::Deploy {
                    path: Some(PathBuf::from("/tmp/fleet")),
                }
            }
        );
    }

    #[test]
    fn cli_fleet_run_requires_name() {
        let cli = Cli::try_parse_from(["vlinder", "fleet", "run", "my-fleet"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Fleet {
                cmd: fleet::FleetCommand::Run {
                    name: "my-fleet".to_string(),
                    prompt: None,
                }
            }
        );
    }

    #[test]
    fn cli_fleet_run_without_name_fails() {
        let result = Cli::try_parse_from(["vlinder", "fleet", "run"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_fleet_new() {
        let cli = Cli::try_parse_from(["vlinder", "fleet", "new", "my-fleet"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Fleet {
                cmd: fleet::FleetCommand::New {
                    name: "my-fleet".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_fleet_new_missing_name_fails() {
        let result = Cli::try_parse_from(["vlinder", "fleet", "new"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_fleet_missing_action_fails() {
        let result = Cli::try_parse_from(["vlinder", "fleet"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_support() {
        let cli = Cli::try_parse_from(["vlinder", "support"]).unwrap();
        assert_eq!(cli.command, Command::Support);
    }

    #[test]
    fn cli_agent_new_python() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "python", "my-agent"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Python,
                    name: "my-agent".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_golang() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "golang", "bar"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Golang,
                    name: "bar".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_js() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "js", "foo"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Js,
                    name: "foo".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_ts() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "ts", "bun-agent"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Ts,
                    name: "bun-agent".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_java() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "java", "jvm-agent"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Java,
                    name: "jvm-agent".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_dotnet() {
        let cli = Cli::try_parse_from(["vlinder", "agent", "new", "dotnet", "net-agent"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Agent {
                cmd: agent::AgentCommand::New {
                    language: agent::Language::Dotnet,
                    name: "net-agent".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_agent_new_invalid_language_fails() {
        let result = Cli::try_parse_from(["vlinder", "agent", "new", "ruby", "my-agent"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_agent_new_missing_name_fails() {
        let result = Cli::try_parse_from(["vlinder", "agent", "new", "python"]);
        assert!(result.is_err());
    }

    #[test]
    fn cli_secret_put() {
        let cli =
            Cli::try_parse_from(["vlinder", "secret", "put", "agents/echo/private-key"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Secret {
                cmd: secret::SecretCommand::Put {
                    name: "agents/echo/private-key".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_secret_get() {
        let cli =
            Cli::try_parse_from(["vlinder", "secret", "get", "agents/echo/private-key"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Secret {
                cmd: secret::SecretCommand::Get {
                    name: "agents/echo/private-key".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_secret_delete() {
        let cli = Cli::try_parse_from(["vlinder", "secret", "delete", "agents/echo/private-key"])
            .unwrap();
        assert_eq!(
            cli.command,
            Command::Secret {
                cmd: secret::SecretCommand::Delete {
                    name: "agents/echo/private-key".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_secret_exists() {
        let cli = Cli::try_parse_from(["vlinder", "secret", "exists", "agents/echo/private-key"])
            .unwrap();
        assert_eq!(
            cli.command,
            Command::Secret {
                cmd: secret::SecretCommand::Exists {
                    name: "agents/echo/private-key".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_secret_missing_action_fails() {
        let result = Cli::try_parse_from(["vlinder", "secret"]);
        assert!(result.is_err());
    }

    // --- Session subcommand tests ---

    #[test]
    fn cli_session_list() {
        let cli =
            Cli::try_parse_from(["vlinder", "session", "list", "--agent", "todoapp"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Session {
                cmd: session::SessionCommand::List {
                    agent: "todoapp".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_session_get() {
        let cli = Cli::try_parse_from(["vlinder", "session", "get", "sess-1"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Session {
                cmd: session::SessionCommand::Get {
                    session_id: "sess-1".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_session_fork() {
        let cli = Cli::try_parse_from([
            "vlinder", "session", "fork", "sess-1", "--from", "abc123", "--branch", "repair-1",
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Session {
                cmd: session::SessionCommand::Fork {
                    session_id: "sess-1".to_string(),
                    from: "abc123".to_string(),
                    branch: "repair-1".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_branch_list() {
        let cli = Cli::try_parse_from(["vlinder", "branch", "list", "--session", "wired-pig-543e"])
            .unwrap();
        assert_eq!(
            cli.command,
            Command::Branch {
                cmd: branch::BranchCommand::List {
                    session: "wired-pig-543e".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_branch_get() {
        let cli = Cli::try_parse_from([
            "vlinder",
            "branch",
            "get",
            "repair-1",
            "--session",
            "wired-pig-543e",
        ])
        .unwrap();
        assert_eq!(
            cli.command,
            Command::Branch {
                cmd: branch::BranchCommand::Get {
                    branch_name: "repair-1".to_string(),
                    session: "wired-pig-543e".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_session_missing_action_fails() {
        let result = Cli::try_parse_from(["vlinder", "session"]);
        assert!(result.is_err());
    }

    // --- Turn subcommand tests ---

    #[test]
    fn cli_turn_get() {
        let cli = Cli::try_parse_from(["vlinder", "turn", "get", "sub-1"]).unwrap();
        assert_eq!(
            cli.command,
            Command::Turn {
                cmd: turn::TurnCommand::Get {
                    submission_id: "sub-1".to_string(),
                }
            }
        );
    }

    #[test]
    fn cli_turn_missing_action_fails() {
        let result = Cli::try_parse_from(["vlinder", "turn"]);
        assert!(result.is_err());
    }
}
