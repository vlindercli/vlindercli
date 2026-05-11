//! Agent and `AgentManifest` tests.

use std::path::{Path, PathBuf};
use vlinder_core::domain::{Agent, AgentManifest};

const AGENT_FIXTURES: &str = "tests/fixtures/agents";

fn agent_fixture(name: &str) -> PathBuf {
    Path::new(AGENT_FIXTURES).join(name)
}

// ============================================================================
// AgentManifest Tests (inline TOML - no fixtures needed)
// ============================================================================

fn parse_manifest(toml: &str) -> Result<AgentManifest, toml::de::Error> {
    toml::from_str(toml)
}

#[test]
fn manifest_parses_required_fields() {
    let manifest: AgentManifest = parse_manifest(
        r#"
        name = "test-agent"
        description = "A test agent"
        runtime = "container"
        executable = "localhost/test-agent:latest"

        [requirements]

    "#,
    )
    .unwrap();

    assert_eq!(manifest.name, "test-agent");
    assert_eq!(manifest.description, "A test agent");
    assert_eq!(manifest.runtime, "container");
    assert_eq!(manifest.executable, "localhost/test-agent:latest");
}

#[test]
fn manifest_parses_optional_source() {
    let manifest: AgentManifest = parse_manifest(
        r#"
        name = "test-agent"
        description = "A test agent"
        runtime = "container"
        executable = "localhost/test-agent:latest"
        source = "https://github.com/example/agent"

        [requirements]

    "#,
    )
    .unwrap();

    assert_eq!(
        manifest.source,
        Some("https://github.com/example/agent".to_string())
    );
}

#[test]
fn manifest_parses_requirements() {
    use vlinder_core::domain::{Protocol, Provider, ServiceType};

    let manifest: AgentManifest = parse_manifest(
        r#"
        name = "test-agent"
        description = "A test agent"
        runtime = "container"
        executable = "localhost/test-agent:latest"

        [requirements.models]
        phi3 = "phi3"
        nomic-embed = "nomic-embed"

        [requirements.services.infer]
        provider = "openrouter"
        protocol = "openai"
        models = ["phi3"]

        [requirements.services.embed]
        provider = "ollama"
        protocol = "openai"
        models = ["nomic-embed"]
    "#,
    )
    .unwrap();

    assert!(manifest.requirements.models.contains_key("phi3"));
    assert!(manifest.requirements.models.contains_key("nomic-embed"));
    assert!(manifest
        .requirements
        .services
        .contains_key(&ServiceType::Infer));
    assert_eq!(
        manifest.requirements.services[&ServiceType::Infer].provider,
        Provider::OpenRouter
    );
    assert_eq!(
        manifest.requirements.services[&ServiceType::Embed].protocol,
        Protocol::OpenAi
    );
}

#[test]
fn manifest_defaults_empty_optional_fields() {
    let manifest: AgentManifest = parse_manifest(
        r#"
        name = "minimal"
        description = "Minimal valid manifest"
        runtime = "container"
        executable = "localhost/minimal:latest"

        [requirements]

    "#,
    )
    .unwrap();

    assert!(manifest.source.is_none());
    assert!(manifest.requirements.models.is_empty());
}

#[test]
fn manifest_fails_for_invalid_toml() {
    let result = parse_manifest("this is not valid toml {{{{");
    assert!(result.is_err());
}

#[test]
fn manifest_fails_for_missing_required_field() {
    let result = parse_manifest(
        r#"
        name = "incomplete"
        description = "Missing runtime and executable"

        [requirements]

    "#,
    );
    assert!(result.is_err());
}

// ============================================================================
// Agent Tests (need fixtures for directory structure)
// ============================================================================

#[test]
fn agent_load_parses_manifest() {
    let agent = Agent::load(&agent_fixture("echo-agent")).unwrap();
    assert_eq!(agent.name, "echo-agent");
    assert_eq!(agent.description, "Test agent that echoes input");
}

#[test]
fn agent_load_fails_for_unknown() {
    let result = Agent::load(Path::new("nonexistent-agent"));
    assert!(result.is_err());
}

#[test]
fn agent_has_model_from_manifest() {
    let agent = Agent::load(&agent_fixture("pensieve")).unwrap();

    assert!(agent.has_model("phi3"));
    assert!(agent.has_model("nomic-embed-text"));
    assert!(!agent.has_model("llama3"));
}

#[test]
fn agent_id_is_placeholder_before_registration() {
    let agent = Agent::load(&agent_fixture("echo-agent")).unwrap();

    // Before registration, id is a placeholder (ADR 048)
    assert!(agent.id.as_str().contains("pending-registration://"));
    assert!(agent.id.as_str().contains("echo-agent"));

    // Executable carries the native artifact ref
    assert_eq!(agent.executable, "localhost/echo-agent:latest");
}

#[test]
fn manifest_executable_passes_through_for_containers() {
    // Container executables should pass through without file resolution
    let manifest: AgentManifest = parse_manifest(
        r#"
        name = "remote-agent"
        description = "Agent with remote registry image"
        runtime = "container"
        executable = "ghcr.io/user/my-agent:v1.2.3"

        [requirements]

    "#,
    )
    .unwrap();

    assert_eq!(manifest.executable, "ghcr.io/user/my-agent:v1.2.3");
}

#[test]
fn agent_load_fails_for_missing_executable() {
    // Create temp directory with manifest pointing to non-existent file executable
    let temp_dir = std::env::temp_dir().join("vlinder-test-missing-executable");
    let _ = std::fs::remove_dir_all(&temp_dir);
    std::fs::create_dir_all(&temp_dir).unwrap();

    let manifest = r#"
        name = "missing-executable-agent"
        description = "Agent with missing code"
        runtime = "file"
        executable = "nonexistent.wasm"

        [requirements]

    "#;
    std::fs::write(temp_dir.join("agent.toml"), manifest).unwrap();

    let result = Agent::load(&temp_dir);
    assert!(
        result.is_err(),
        "Should fail when executable file doesn't exist"
    );

    // Cleanup
    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn agent_with_empty_models_list() {
    // echo-agent has no model requirements
    let agent = Agent::load(&agent_fixture("echo-agent")).unwrap();
    assert!(agent.requirements.models.is_empty());
}

#[test]
fn agent_services_from_fixture() {
    use vlinder_core::domain::{Protocol, Provider, ServiceType};

    let agent = Agent::load(&agent_fixture("pensieve")).unwrap();

    // Pensieve declares both infer and embed services
    let infer = &agent.requirements.services[&ServiceType::Infer];
    assert_eq!(infer.provider, Provider::OpenRouter);
    assert_eq!(infer.protocol, Protocol::Anthropic);
    assert_eq!(infer.models, vec!["anthropic/claude-3.5-sonnet"]);

    let embed = &agent.requirements.services[&ServiceType::Embed];
    assert_eq!(embed.provider, Provider::Ollama);
    assert_eq!(embed.protocol, Protocol::OpenAi);
    assert_eq!(embed.models, vec!["nomic-embed-text"]);
}

#[test]
fn agent_no_services_when_none_declared() {
    let agent = Agent::load(&agent_fixture("echo-agent")).unwrap();
    assert!(agent.requirements.services.is_empty());
}
