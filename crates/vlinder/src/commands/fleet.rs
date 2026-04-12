use std::path::{Path, PathBuf};

use clap::Subcommand;

use crate::config::CliConfig;
use tokio::runtime::Runtime;
use vlinder_core::domain::{agent_routing_key, DagNodeId, Fleet, FleetManifest, Registry};

use super::connect::{connect_harness, connect_registry};
use super::repl;

#[derive(Subcommand, Debug, PartialEq)]
pub enum FleetCommand {
    /// Deploy a fleet manifest to the registry
    Deploy {
        /// Path to fleet directory (default: current directory)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },
    /// Run a deployed fleet interactively
    Run {
        /// Fleet name
        name: String,
        /// Single message to send (non-interactive, prints response and exits)
        #[arg(short, long)]
        prompt: Option<String>,
    },
    /// Create a new fleet from a template
    New {
        /// Fleet name (becomes the directory name)
        name: String,
    },
}

pub fn execute(cmd: FleetCommand) {
    match cmd {
        FleetCommand::Deploy { path } => deploy(path),
        FleetCommand::Run { name, prompt } => run(&name, prompt.as_deref()),
        FleetCommand::New { name } => scaffold(&name),
    }
}

fn scaffold(name: &str) {
    let target = std::path::Path::new(name);

    if target.exists() {
        eprintln!("Error: directory '{name}' already exists.");
        std::process::exit(1);
    }

    std::fs::create_dir(target).unwrap_or_else(|e| {
        eprintln!("Error: failed to create directory '{name}': {e}");
        std::process::exit(1);
    });

    let fleet_toml = format!(
        r#"name = "{name}"

# Entry agent — the agent that receives user input.
# entry = "coordinator"

# Add agents using: vlinder agent new <language> agents/<name>
# Then register them here:
#
# [agents.coordinator]
# path = "agents/coordinator"
#
# [agents.researcher]
# path = "agents/researcher"

# Docs: https://docs.vlinder.ai/fleets
"#
    );

    std::fs::write(target.join("fleet.toml"), fleet_toml).unwrap_or_else(|e| {
        eprintln!("Error: failed to write fleet.toml: {e}");
        std::process::exit(1);
    });

    println!("Created fleet '{name}'.");
    println!();
    println!("Next steps:");
    println!("  cd {name}");
    println!("  mkdir -p agents");
    println!("  vlinder agent new <language> agents/<agent-name>");
    println!("  # repeat for each agent, then update fleet.toml");
    println!("  vlinder fleet deploy");
    println!("  vlinder fleet run {name}");
}

pub fn deploy(path: Option<PathBuf>) {
    let config = CliConfig::load();
    let fleet_path =
        path.unwrap_or_else(|| std::env::current_dir().expect("Failed to get current directory"));

    let absolute_path = fleet_path
        .canonicalize()
        .expect("Failed to resolve fleet path");

    // Load fleet manifest from disk
    let manifest_path = absolute_path.join("fleet.toml");
    let manifest = FleetManifest::load(&manifest_path).unwrap_or_else(|e| {
        eprintln!("Failed to load {}: {}", manifest_path.display(), e);
        eprintln!("Run this command from a fleet directory, or pass --path <dir>.");
        std::process::exit(1);
    });

    // Deploy all agents in the fleet via registry gRPC
    let registry = connect_registry(&config);

    // Deploy fleet-level models from <fleet_dir>/models/*.toml
    let fleet_models = deploy_fleet_models(&absolute_path, &*registry);
    for name in &fleet_models {
        println!("  Model: {name} (fleet-level)");
    }

    for (name, agent_entry) in &manifest.agents {
        let agent_path = absolute_path.join(&agent_entry.path);
        let agent = super::agent::deploy_agent_from_path(&agent_path, &*registry);
        println!("  Agent: {} ({})", name, agent.id);
    }

    // Build Fleet from manifest + registry, then register
    let fleet = Fleet::from_manifest(manifest, &*registry).unwrap_or_else(|e| {
        eprintln!("Failed to build fleet: {e}");
        std::process::exit(1);
    });

    let fleet_name = fleet.name.clone();
    let entry_id = fleet.entry.clone();

    registry.register_fleet(fleet).unwrap_or_else(|e| {
        eprintln!("Failed to register fleet: {e}");
        std::process::exit(1);
    });

    println!("Deployed fleet '{fleet_name}' (entry: {entry_id})");
}

/// Deploy all models found in `<fleet_dir>/models/*.toml`.
///
/// Fleet-level models are registered before agents, so agents can reference
/// shared models without bundling their own copies. Agent-level models deploy
/// after, so an agent can override a fleet-level model if needed.
fn deploy_fleet_models(fleet_dir: &Path, registry: &dyn Registry) -> Vec<String> {
    let models_dir = fleet_dir.join("models");
    if !models_dir.is_dir() {
        return Vec::new();
    }

    let mut entries: Vec<_> = std::fs::read_dir(&models_dir)
        .unwrap_or_else(|e| {
            eprintln!("Failed to read fleet models directory: {e}");
            std::process::exit(1);
        })
        .filter_map(std::result::Result::ok)
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "toml"))
        .collect();

    entries.sort_by_key(std::fs::DirEntry::file_name);

    let mut deployed = Vec::new();
    for entry in entries {
        match super::model::load_and_register_model(&entry.path(), registry) {
            Ok(model) => deployed.push(model.name),
            Err(e) => {
                eprintln!("{e}");
                std::process::exit(1);
            }
        }
    }

    deployed
}

pub fn run(name: &str, prompt: Option<&str>) {
    let config = CliConfig::load();
    let registry = connect_registry(&config);

    let Some(fleet) = registry.get_fleet(name) else {
        eprintln!("Fleet '{name}' not found — deploy it first with: vlinder fleet deploy");
        std::process::exit(1);
    };

    let entry_agent_id = fleet.entry.clone();
    let entry_agent_name = agent_routing_key(&entry_agent_id);

    // Build fleet context for the entry agent
    let fleet_context = build_fleet_context(&*registry, &fleet);

    // Connect harness via gRPC — the daemon owns queue and registry
    let harness = connect_harness(&config);

    // New session — start fresh, no prior state
    let (session_id, branch_id) = harness.start_session(entry_agent_name.as_str());

    tracing::debug!(fleet = %fleet.name, "Fleet session started");

    println!(
        "Fleet '{}' ready. Entry agent: {}",
        fleet.name, entry_agent_name
    );

    let invoke = |input: &str| -> String {
        let rt = Runtime::new().expect("Failed to create tokio runtime");
        let enriched_input = format!("{fleet_context}\n\n{input}");
        match rt.block_on(harness.run_agent(
            &entry_agent_id,
            &enriched_input,
            session_id.clone(),
            branch_id,
            false,
            None,
            DagNodeId::root(),
        )) {
            Ok(result) => result,
            Err(e) => format!("[error] {e}"),
        }
    };

    if let Some(message) = prompt {
        println!("{}", invoke(message));
    } else {
        repl::run(invoke);
    }
}

/// Build a fleet context string from registered agents.
///
/// Lists all non-entry agents with their descriptions so the entry agent
/// knows what it can delegate to.
fn build_fleet_context(registry: &dyn vlinder_core::domain::Registry, fleet: &Fleet) -> String {
    let mut lines = vec![
        format!("Fleet: {}", fleet.name),
        "Available agents for delegation (use /delegate endpoint):".to_string(),
    ];

    for agent_id in &fleet.agents {
        if *agent_id == fleet.entry {
            continue;
        }
        if let Some(agent) = registry.get_agent(agent_id) {
            lines.push(format!("- {}: {}", agent.name, agent.description));
        }
    }

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use vlinder_core::domain::{InMemoryRegistry, InMemorySecretStore, Provider};

    fn test_registry() -> Arc<InMemoryRegistry> {
        let store = Arc::new(InMemorySecretStore::new());
        Arc::new(InMemoryRegistry::new(store))
    }

    fn write_model_toml(dir: &Path, filename: &str, name: &str, provider: &str, model_path: &str) {
        let models_dir = dir.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let content = format!(
            "name = \"{name}\"\ntype = \"inference\"\nprovider = \"{provider}\"\nmodel_path = \"{model_path}\"\n"
        );
        std::fs::write(models_dir.join(filename), content).unwrap();
    }

    // ========================================================================
    // deploy_fleet_models
    // ========================================================================

    #[test]
    fn deploy_fleet_models_registers_all_toml_files() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );
        write_model_toml(
            dir.path(),
            "llama3.toml",
            "llama3",
            "ollama",
            "ollama://localhost:11434/llama3:latest",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);
        registry.register_inference_engine(Provider::Ollama);

        let deployed = deploy_fleet_models(dir.path(), &*registry);

        assert_eq!(deployed.len(), 2);
        assert!(registry.get_model("claude-sonnet").is_some());
        assert!(registry.get_model("llama3").is_some());
    }

    #[test]
    fn deploy_fleet_models_returns_empty_when_no_models_dir() {
        let dir = tempfile::tempdir().unwrap();
        let registry = test_registry();

        let deployed = deploy_fleet_models(dir.path(), &*registry);

        assert!(deployed.is_empty());
    }

    #[test]
    fn deploy_fleet_models_ignores_non_toml_files() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        // Write a non-TOML file that should be ignored
        let models_dir = dir.path().join("models");
        std::fs::write(models_dir.join("README.md"), "# Models").unwrap();

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);

        let deployed = deploy_fleet_models(dir.path(), &*registry);

        assert_eq!(deployed, vec!["claude-sonnet"]);
    }

    #[test]
    fn deploy_fleet_models_returns_sorted_by_filename() {
        let dir = tempfile::tempdir().unwrap();
        // Write in reverse-alpha order to verify sorting
        write_model_toml(
            dir.path(),
            "llama3.toml",
            "llama3",
            "ollama",
            "ollama://localhost:11434/llama3:latest",
        );
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);
        registry.register_inference_engine(Provider::Ollama);

        let deployed = deploy_fleet_models(dir.path(), &*registry);

        // Sorted by filename: claude-sonnet.toml comes before llama3.toml
        assert_eq!(deployed, vec!["claude-sonnet", "llama3"]);
    }
}
