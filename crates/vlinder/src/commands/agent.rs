use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Subcommand, ValueEnum};

use crate::config::CliConfig;
use vlinder_core::domain::{Agent, AgentManifest, BranchId, DagNodeId, DagStore, Registry};

use super::connect::{connect_harness, connect_registry, open_dag_store};
use super::repl;

#[derive(Clone, Debug, PartialEq, ValueEnum)]
pub enum Language {
    Python,
    Golang,
    Js,
    Ts,
    Java,
    Dotnet,
}

impl Language {
    fn repo_suffix(&self) -> &'static str {
        match self {
            Language::Python => "python",
            Language::Golang => "golang",
            Language::Js => "js",
            Language::Ts => "ts",
            Language::Java => "java",
            Language::Dotnet => "dotnet",
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.repo_suffix())
    }
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum AgentCommand {
    /// Deploy an agent manifest to the registry
    Deploy {
        /// Path to agent directory (default: current directory)
        #[arg(short, long)]
        path: Option<PathBuf>,
    },
    /// Run a deployed agent interactively
    Run {
        /// Agent name
        name: String,
        /// Continue an existing session (by name or ID)
        #[arg(long)]
        session: Option<String>,
        /// Resume on a named branch (created by `session fork`)
        #[arg(long)]
        branch: Option<String>,
        /// Single message to send (non-interactive, prints response and exits)
        #[arg(short, long)]
        prompt: Option<String>,
    },
    /// List deployed agents
    List,
    /// Show details for a deployed agent
    Get {
        /// Agent name
        name: String,
    },
    /// Delete a deployed agent
    Delete {
        /// Agent name
        name: String,
    },
    /// Create a new agent from a template
    New {
        /// Template language
        language: Language,
        /// Agent name (becomes the directory name)
        name: String,
    },
}

pub fn execute(cmd: AgentCommand) {
    match cmd {
        AgentCommand::Deploy { path } => deploy(path),
        AgentCommand::Run {
            name,
            session,
            branch,
            prompt,
        } => run(
            &name,
            session.as_deref(),
            branch.as_deref(),
            prompt.as_deref(),
        ),
        AgentCommand::List => list(),
        AgentCommand::Get { name } => get(&name),
        AgentCommand::Delete { name } => delete(&name),
        AgentCommand::New { language, name } => scaffold(&language, &name),
    }
}

fn deploy(path: Option<PathBuf>) {
    let config = CliConfig::load();
    let agent_path =
        path.unwrap_or_else(|| std::env::current_dir().expect("Failed to get current directory"));

    let absolute_path = agent_path
        .canonicalize()
        .expect("Failed to resolve agent path");

    let registry = connect_registry(&config);
    let agent = deploy_agent_from_path(&absolute_path, &*registry);

    println!("Deployed: {} ({})", agent.name, agent.id);
}

/// Load an agent manifest from a directory, auto-deploy its models, and register it.
///
/// Shared by `agent deploy` and `fleet deploy`.
pub(super) fn deploy_agent_from_path(agent_dir: &Path, registry: &dyn Registry) -> Agent {
    let manifest_path = agent_dir.join("agent.toml");
    let manifest = AgentManifest::load(&manifest_path).unwrap_or_else(|e| {
        eprintln!("Failed to load agent manifest: {e:?}");
        std::process::exit(1);
    });

    // Auto-deploy models from <agent_dir>/models/<name>.toml
    match auto_deploy_models(agent_dir, &manifest, registry) {
        Ok(deployed) => {
            for name in &deployed {
                println!("  Model: {name} (auto-deployed)");
            }
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    }

    registry.register_manifest(manifest).unwrap_or_else(|e| {
        eprintln!("Failed to deploy agent: {e}");
        std::process::exit(1);
    })
}

fn run(name: &str, session: Option<&str>, branch: Option<&str>, prompt: Option<&str>) {
    let config = CliConfig::load();
    let registry = connect_registry(&config);

    // Look up already-deployed agent by name (ADR 103)
    let Some(agent) = registry.get_agent_by_name(name) else {
        eprintln!("Agent '{name}' not found — deploy it first with: vlinder agent deploy");
        std::process::exit(1);
    };

    let agent_id = agent.id.clone();

    // Connect to harness via gRPC — the daemon's harness worker owns the
    // queue and registry connection. The CLI is now a pure gRPC client.
    let harness = connect_harness(&config);

    // Resolve (session, branch) — the CLI flags are sugar for this tuple.
    // All paths end at resolve_branch_tip for state resolution.
    let (session_id, branch_id, sealed, initial_state, dag_parent) = match (session, branch) {
        (None, None) => {
            // New session → create session + default branch, resolve tip
            let (session_id, branch_id) = harness.start_session(name);
            (session_id, branch_id, false, None, DagNodeId::root())
        }
        (Some(session_name), None) => {
            // Existing session → resolve its default branch
            resolve_session_default(&config, session_name)
        }
        (Some(_) | None, Some(branch_name)) => {
            // Specific branch → resolve it directly
            resolve_branch(&config, branch_name)
        }
    };

    let invoke = |input: &str| -> String {
        match harness.run_agent(
            &agent_id,
            input,
            session_id.clone(),
            branch_id,
            sealed,
            initial_state.clone(),
            dag_parent.clone(),
        ) {
            Ok(result) => result,
            Err(e) => format!("[error] {e}"),
        }
    };

    if let Some(message) = prompt {
        // Non-interactive: send single message, print response, exit.
        println!("{}", invoke(message));
    } else {
        // Interactive REPL.
        repl::run(invoke);
    }
}

/// Resolve session default branch: look up session by name, continue on its default branch.
fn resolve_session_default(
    config: &CliConfig,
    session_name: &str,
) -> (
    vlinder_core::domain::SessionId,
    BranchId,
    bool,
    Option<String>,
    DagNodeId,
) {
    let store = require_dag_store(config);

    let session = resolve_session(&*store, session_name);
    let branch_id = session.default_branch;

    let branch = store
        .get_branch(branch_id)
        .unwrap_or_else(|e| {
            eprintln!("Failed to look up default branch: {e}");
            std::process::exit(1);
        })
        .unwrap_or_else(|| {
            eprintln!("Default branch not found for session '{session_name}'");
            std::process::exit(1);
        });

    let (_, sealed, initial_state, dag_parent) = resolve_branch_tip(&*store, &branch, &branch.name);

    println!(
        "Continuing session '{}' on branch '{}'",
        session.name, branch.name
    );
    (session.id, branch_id, sealed, initial_state, dag_parent)
}

/// Resolve branch session context: look up branch by name, continue on it.
fn resolve_branch(
    config: &CliConfig,
    branch_name: &str,
) -> (
    vlinder_core::domain::SessionId,
    BranchId,
    bool,
    Option<String>,
    DagNodeId,
) {
    let store = require_dag_store(config);

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

    let session_id = branch.session_id.clone();
    let (_, sealed, initial_state, dag_parent) = resolve_branch_tip(&*store, &branch, branch_name);

    println!("On branch '{branch_name}'");
    (session_id, branch.id, sealed, initial_state, dag_parent)
}

/// Read tip state and `dag_parent` from a branch.
fn resolve_branch_tip(
    store: &dyn DagStore,
    branch: &vlinder_core::domain::Branch,
    branch_name: &str,
) -> (BranchId, bool, Option<String>, DagNodeId) {
    if branch.broken_at.is_some() {
        eprintln!("Branch '{branch_name}' is sealed — cannot run on a sealed branch");
        std::process::exit(1);
    }

    // Find the tip — latest node on branch, falling back to fork_point
    let tip_hash = store
        .latest_node_on_branch(branch.id, None)
        .unwrap_or_else(|e| {
            eprintln!("Failed to query latest node on branch: {e}");
            std::process::exit(1);
        })
        .map(|n| n.id)
        .or_else(|| branch.fork_point.clone())
        .unwrap_or_else(DagNodeId::root);

    // Read state from the tip node
    let initial_state = if let Ok(Some(node)) = store.get_node(&tip_hash) {
        let state = if node.message_type() == vlinder_core::domain::MessageType::Invoke {
            store
                .get_invoke_node(&node.id)
                .ok()
                .flatten()
                .and_then(|(_, msg)| msg.state)
                .unwrap_or_default()
        } else if node.message_type() == vlinder_core::domain::MessageType::Complete {
            store
                .get_complete_node(&node.id)
                .ok()
                .flatten()
                .and_then(|m| m.state)
                .unwrap_or_default()
        } else {
            String::new()
        };
        if state.is_empty() {
            None
        } else {
            println!(
                "Resuming '{}' from state {}…",
                branch_name,
                &state[..8.min(state.len())]
            );
            Some(state.clone())
        }
    } else {
        None
    };

    (branch.id, false, initial_state, tip_hash)
}

/// Resolve a session by name or UUID.
fn resolve_session(store: &dyn DagStore, name_or_id: &str) -> vlinder_core::domain::Session {
    // Try UUID first
    if let Ok(sid) = vlinder_core::domain::SessionId::try_from(name_or_id.to_string()) {
        if let Some(session) = store.get_session(&sid).ok().flatten() {
            return session;
        }
    }
    // Try by petname
    if let Some(session) = store.get_session_by_name(name_or_id).ok().flatten() {
        return session;
    }
    eprintln!("Session '{name_or_id}' not found");
    std::process::exit(1);
}

fn require_dag_store(config: &CliConfig) -> Box<dyn DagStore> {
    open_dag_store(config).unwrap_or_else(|| {
        eprintln!("Cannot connect to state service. Is the daemon running?");
        std::process::exit(1);
    })
}

fn list() {
    let config = CliConfig::load();
    let registry = open_registry(&config);
    let Some(registry) = registry else { return };

    let agents = registry.get_agents();
    if agents.is_empty() {
        println!("No agents deployed.");
        return;
    }
    println!("Deployed agents:");
    for agent in agents {
        println!(
            "  {} ({}, {})",
            agent.name,
            agent.runtime.as_str(),
            agent.executable
        );
    }
}

fn get(name: &str) {
    let config = CliConfig::load();
    let registry = open_registry(&config);
    let Some(registry) = registry else { return };

    let Some(agent) = registry.get_agent_by_name(name) else {
        eprintln!("Agent '{name}' not found");
        return;
    };

    println!("Name:        {}", agent.name);
    println!("Runtime:     {}", agent.runtime.as_str());
    println!("Executable:  {}", agent.executable);
    println!("Description: {}", agent.description);
    if let Some(ref storage) = agent.object_storage {
        println!("Object storage: {storage}");
    }
    if let Some(ref storage) = agent.vector_storage {
        println!("Vector storage: {storage}");
    }
    if !agent.requirements.models.is_empty() {
        println!("Models:");
        for (alias, name) in &agent.requirements.models {
            println!("  {alias} -> {name}");
        }
    }
}

fn delete(name: &str) {
    let config = CliConfig::load();
    let registry = open_registry(&config);
    let Some(registry) = registry else { return };

    match registry.delete_agent(name) {
        Ok(true) => println!("Deleted agent '{name}'"),
        Ok(false) => eprintln!("Agent '{name}' not found"),
        Err(e) => eprintln!("Error: {e}"),
    }
}

fn open_registry(config: &CliConfig) -> Option<Arc<dyn Registry>> {
    super::connect::open_registry(config)
}

/// Scaffold a new agent project from a GitHub template.
fn scaffold(language: &Language, name: &str) {
    let target = PathBuf::from(name);
    if target.exists() {
        eprintln!("Error: directory '{name}' already exists.");
        std::process::exit(1);
    }

    let suffix = language.repo_suffix();
    let url = format!(
        "https://github.com/vlindercli/vlinder-agent-{suffix}/archive/refs/heads/main.tar.gz"
    );

    println!("Downloading {language} template…");

    // Download tarball to a temp file
    let tarball = match download_tarball(&url) {
        Ok(path) => path,
        Err(e) => {
            eprintln!("Error: failed to download template: {e}");
            eprintln!("Check your internet connection and try again.");
            std::process::exit(1);
        }
    };

    // Create target directory
    if let Err(e) = std::fs::create_dir_all(&target) {
        eprintln!("Error: failed to create directory '{name}': {e}");
        std::process::exit(1);
    }

    // Extract tarball into target directory
    if let Err(e) = extract_tarball(&tarball, &target, suffix) {
        eprintln!("Error: failed to extract template: {e}");
        // Clean up the empty directory we created
        let _ = std::fs::remove_dir_all(&target);
        std::process::exit(1);
    }

    // Clean up temp file
    let _ = std::fs::remove_file(&tarball);

    // Patch agent name in agent.toml and build.sh
    patch_file(&target.join("agent.toml"), "hello-agent", name);
    patch_file(&target.join("build.sh"), "hello-agent", name);

    println!("Created agent '{name}' from {language} template.");
    println!();
    println!("Next steps:");
    println!("  cd {name}");
    println!("  ./build.sh");
    println!("  vlinder agent deploy");
    println!("  vlinder agent run {name}");
}

/// Download a URL to a temporary file and return the path.
fn download_tarball(url: &str) -> Result<PathBuf, String> {
    let mut resp = ureq::get(url).call().map_err(|e| format!("{e}"))?;

    let tmp_dir = std::env::temp_dir();
    let tmp_path = tmp_dir.join("vlinder-template.tar.gz");

    let mut file =
        std::fs::File::create(&tmp_path).map_err(|e| format!("failed to create temp file: {e}"))?;

    std::io::copy(&mut resp.body_mut().as_reader(), &mut file)
        .map_err(|e| format!("failed to write temp file: {e}"))?;

    Ok(tmp_path)
}

/// Extract a tarball, moving files from the nested directory up into target.
fn extract_tarball(
    tarball: &std::path::Path,
    target: &std::path::Path,
    suffix: &str,
) -> Result<(), String> {
    // tar xzf into target, then move contents from nested dir up
    let status = std::process::Command::new("tar")
        .args([
            "xzf",
            &tarball.display().to_string(),
            "-C",
            &target.display().to_string(),
        ])
        .status()
        .map_err(|e| format!("failed to run tar: {e}"))?;

    if !status.success() {
        return Err("tar extraction failed".to_string());
    }

    // GitHub archives extract to vlinder-agent-{suffix}-main/
    let nested = target.join(format!("vlinder-agent-{suffix}-main"));
    if nested.exists() {
        // Move all files from nested dir up into target
        let entries = std::fs::read_dir(&nested)
            .map_err(|e| format!("failed to read extracted directory: {e}"))?;

        for entry in entries {
            let entry = entry.map_err(|e| format!("failed to read entry: {e}"))?;
            let dest = target.join(entry.file_name());
            std::fs::rename(entry.path(), dest).map_err(|e| format!("failed to move file: {e}"))?;
        }

        std::fs::remove_dir_all(&nested)
            .map_err(|e| format!("failed to clean up nested directory: {e}"))?;
    }

    Ok(())
}

/// Replace all occurrences of `from` with `to` in a file.
fn patch_file(path: &PathBuf, from: &str, to: &str) {
    if let Ok(content) = std::fs::read_to_string(path) {
        let patched = content.replace(from, to);
        let _ = std::fs::write(path, patched);
    }
}

/// Auto-deploy models found in `<agent_dir>/models/` for each model in the manifest.
///
/// For each unique model name in `manifest.requirements.models`, checks if a
/// corresponding `<agent_dir>/models/<name>.toml` exists. If so, loads and registers it.
/// Models without a `.toml` file are skipped (they must already be registered).
///
/// Returns the names of models that were auto-deployed.
pub(super) fn auto_deploy_models(
    agent_dir: &Path,
    manifest: &AgentManifest,
    registry: &dyn Registry,
) -> Result<Vec<String>, String> {
    let models_dir = agent_dir.join("models");
    let model_names: std::collections::HashSet<&str> = manifest
        .requirements
        .models
        .values()
        .map(std::string::String::as_str)
        .collect();

    let mut deployed = Vec::new();
    for model_name in &model_names {
        let model_toml = models_dir.join(format!("{model_name}.toml"));
        if model_toml.exists() {
            let model = super::model::load_and_register_model(&model_toml, registry)?;
            deployed.push(model.name);
        }
    }
    Ok(deployed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use vlinder_core::domain::RequirementsConfig;
    use vlinder_core::domain::{InMemoryRegistry, InMemorySecretStore, Provider};

    fn test_registry() -> Arc<InMemoryRegistry> {
        let store = Arc::new(InMemorySecretStore::new());
        Arc::new(InMemoryRegistry::new(store))
    }

    fn write_model_toml(dir: &Path, name: &str, provider: &str, model_path: &str) {
        let models_dir = dir.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let content = format!(
            "name = \"{name}\"\ntype = \"inference\"\nprovider = \"{provider}\"\nmodel_path = \"{model_path}\"\n"
        );
        std::fs::write(models_dir.join(format!("{name}.toml")), content).unwrap();
    }

    fn manifest_with_models(models: Vec<(&str, &str)>) -> AgentManifest {
        let mut model_map = std::collections::HashMap::new();
        for (alias, name) in models {
            model_map.insert(alias.to_string(), name.to_string());
        }
        AgentManifest {
            name: "test-agent".to_string(),
            description: "test".to_string(),
            source: None,
            runtime: "container".to_string(),
            executable: "localhost/test:latest".to_string(),
            requirements: RequirementsConfig {
                models: model_map,
                services: std::collections::HashMap::new(),
                mounts: std::collections::HashMap::new(),
            },
            prompts: None,
            object_storage: None,
            vector_storage: None,
        }
    }

    #[test]
    fn auto_deploy_registers_discovered_models() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);

        let manifest = manifest_with_models(vec![("inference_model", "claude-sonnet")]);
        let deployed = auto_deploy_models(dir.path(), &manifest, &*registry).unwrap();

        assert_eq!(deployed, vec!["claude-sonnet"]);
        assert!(registry.get_model("claude-sonnet").is_some());
    }

    #[test]
    fn auto_deploy_skips_models_without_toml() {
        let dir = tempfile::tempdir().unwrap();
        // No models/ directory at all

        let registry = test_registry();
        let manifest = manifest_with_models(vec![("inference_model", "already-registered")]);
        let deployed = auto_deploy_models(dir.path(), &manifest, &*registry).unwrap();

        assert!(deployed.is_empty());
    }

    #[test]
    fn auto_deploy_deduplicates_model_names() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);

        // Two aliases pointing to the same model
        let manifest = manifest_with_models(vec![
            ("inference_model", "claude-sonnet"),
            ("summary_model", "claude-sonnet"),
        ]);
        let deployed = auto_deploy_models(dir.path(), &manifest, &*registry).unwrap();

        // Registered only once
        assert_eq!(deployed.len(), 1);
        assert_eq!(registry.get_models().len(), 1);
    }

    #[test]
    fn auto_deploy_handles_multiple_models() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        // Write an embedding model TOML
        let models_dir = dir.path().join("models");
        std::fs::write(
            models_dir.join("nomic-embed.toml"),
            "name = \"nomic-embed\"\ntype = \"embedding\"\nprovider = \"ollama\"\nmodel_path = \"ollama://localhost:11434/nomic-embed-text:latest\"\n",
        ).unwrap();

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);
        registry.register_embedding_engine(Provider::Ollama);

        let manifest = manifest_with_models(vec![
            ("inference_model", "claude-sonnet"),
            ("embedding_model", "nomic-embed"),
        ]);
        let mut deployed = auto_deploy_models(dir.path(), &manifest, &*registry).unwrap();
        deployed.sort();

        assert_eq!(deployed, vec!["claude-sonnet", "nomic-embed"]);
        assert_eq!(registry.get_models().len(), 2);
    }

    #[test]
    fn auto_deploy_with_no_required_models() {
        let dir = tempfile::tempdir().unwrap();
        let registry = test_registry();

        let manifest = manifest_with_models(vec![]);
        let deployed = auto_deploy_models(dir.path(), &manifest, &*registry).unwrap();

        assert!(deployed.is_empty());
    }

    #[test]
    fn auto_deploy_propagates_registration_error() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        // Don't register OpenRouter engine — registration should fail

        let manifest = manifest_with_models(vec![("inference_model", "claude-sonnet")]);
        let result = auto_deploy_models(dir.path(), &manifest, &*registry);

        assert!(result.is_err());
    }
}
