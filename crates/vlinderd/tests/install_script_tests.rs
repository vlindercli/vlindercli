use vlinder_core::domain::{AgentManifest, FleetManifest, Provider, ServiceType};
use vlinderd::config::{Config, QueueBackend, StateBackend};

const INSTALL_SCRIPT: &str = include_str!("../../../scripts/install.sh");
const RELEASE_WORKFLOW: &str = include_str!("../../../.github/workflows/release.yml");

fn heredoc(marker: &str) -> &str {
    let start_marker = format!("<< '{marker}'\n");
    let start = INSTALL_SCRIPT
        .find(&start_marker)
        .unwrap_or_else(|| panic!("missing {marker} heredoc"))
        + start_marker.len();
    let end_marker = format!("\n{marker}\n");
    let end = INSTALL_SCRIPT[start..]
        .find(&end_marker)
        .unwrap_or_else(|| panic!("unterminated {marker} heredoc"))
        + start;
    &INSTALL_SCRIPT[start..end]
}

#[test]
fn generated_config_matches_daemon_schema() {
    let config_toml = heredoc("CONFIG");
    let config: Config = toml::from_str(config_toml).expect("installer config must parse");

    assert_eq!(config.queue.backend, QueueBackend::Nats);
    assert_eq!(config.state.backend, StateBackend::Grpc);
    assert_eq!(config.distributed.workers.registry, 1);
    assert_eq!(config.distributed.workers.harness, 1);
    assert_eq!(config.distributed.workers.agent.container, 1);
    assert_eq!(config.distributed.workers.inference.ollama, 1);
    assert_eq!(config.distributed.workers.storage.object.sqlite, 1);
    assert_eq!(config.distributed.workers.storage.vector.sqlite, 1);
    assert!(!config_toml.contains("enabled ="));
    assert!(!config_toml.contains("[distributed.workers.embedding]"));
}

#[test]
fn generated_support_fleet_matches_manifest_schema() {
    let fleet: FleetManifest =
        toml::from_str(heredoc("FLEET")).expect("installer fleet manifest must parse");
    assert_eq!(fleet.entry, "support");
    assert_eq!(fleet.agents.len(), 3);

    let support: AgentManifest =
        toml::from_str(heredoc("SUPPORT_AGENT")).expect("support manifest must parse");
    assert_eq!(
        support
            .requirements
            .models
            .get("default")
            .map(String::as_str),
        Some("phi3")
    );
    let infer = support
        .requirements
        .services
        .get(&ServiceType::Infer)
        .expect("support agent must declare inference");
    assert_eq!(infer.provider, Provider::Ollama);
    assert_eq!(infer.models, ["phi3"]);

    let code_analyst: AgentManifest =
        toml::from_str(heredoc("CODE_ANALYST_AGENT")).expect("code analyst manifest must parse");
    assert!(code_analyst.requirements.services.is_empty());

    let log_analyst: AgentManifest =
        toml::from_str(heredoc("LOG_ANALYST_AGENT")).expect("log analyst manifest must parse");
    assert!(log_analyst.requirements.services.is_empty());
    assert!(log_analyst.requirements.mounts.is_empty());
    assert!(!heredoc("LOG_ANALYST_AGENT").contains("[[mounts]]"));
}

#[test]
fn installer_starts_the_daemon_binary() {
    assert!(INSTALL_SCRIPT.contains("${INSTALL_DIR}/vlinderd"));
    assert!(!INSTALL_SCRIPT.contains("${INSTALL_DIR}/vlinder daemon"));
}

#[test]
fn release_archive_contains_cli_and_daemon() {
    assert!(RELEASE_WORKFLOW.contains("release/vlinder staging/vlinder"));
    assert!(RELEASE_WORKFLOW.contains("release/vlinderd staging/vlinderd"));
    assert!(RELEASE_WORKFLOW
        .contains("tar czf ../vlinder-${{ matrix.target }}.tar.gz vlinder vlinderd"));
}
