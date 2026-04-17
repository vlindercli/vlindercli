use std::collections::HashMap;
use std::sync::Arc;

use vlinder_core::domain::{
    Fleet, FleetManifest, InMemoryRegistry, InMemorySecretStore, Registry, RuntimeType, SecretStore,
};

// ============================================================================
// FleetManifest Tests (inline TOML - no fixtures needed)
// ============================================================================

fn parse_fleet_manifest(toml: &str) -> Result<FleetManifest, toml::de::Error> {
    toml::from_str(toml)
}

#[test]
fn manifest_parses_required_fields() {
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "test-fleet"
        entry = "main"

        [agents.main]
        path = "agents/main"
    "#,
    )
    .unwrap();

    assert_eq!(manifest.name, "test-fleet");
    assert_eq!(manifest.entry, "main");
    assert!(manifest.agents.contains_key("main"));
}

#[test]
fn manifest_parses_agent_paths() {
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "test-fleet"
        entry = "orchestrator"

        [agents.orchestrator]
        path = "agents/orchestrator"

        [agents.worker]
        path = "agents/worker"
    "#,
    )
    .unwrap();

    assert_eq!(
        manifest.agents.get("orchestrator").unwrap().path,
        "agents/orchestrator"
    );
    assert_eq!(manifest.agents.get("worker").unwrap().path, "agents/worker");
}

#[test]
fn manifest_fails_for_invalid_toml() {
    let result = parse_fleet_manifest("this is not valid toml {{{{");
    assert!(result.is_err());
}

#[test]
fn manifest_fails_for_missing_required_field() {
    // Missing entry field
    let result = parse_fleet_manifest(
        r#"
        name = "incomplete"

        [agents.main]
        path = "agents/main"
    "#,
    );
    assert!(result.is_err());
}

// ============================================================================
// Fleet Tests (from_manifest + registry)
// ============================================================================

fn test_secret_store() -> Arc<dyn SecretStore> {
    Arc::new(InMemorySecretStore::new())
}

fn test_registry_with_agents(agent_names: &[&str]) -> Arc<InMemoryRegistry> {
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);

    for name in agent_names {
        let agent = minimal_agent(name);
        registry.restore_agent(agent).unwrap();
    }
    registry
}

fn minimal_agent(name: &str) -> vlinder_core::domain::Agent {
    use vlinder_core::domain::Requirements;

    vlinder_core::domain::Agent {
        id: vlinder_core::domain::Agent::placeholder_id(name),
        name: name.to_string(),
        description: format!("{name} agent"),
        source: None,
        runtime: RuntimeType::Container,
        executable: format!("localhost/{name}:latest"),
        image_digest: None,
        public_key: None,
        object_storage: None,
        vector_storage: None,
        requirements: Requirements {
            models: HashMap::new(),
            services: HashMap::new(),
            mounts: HashMap::new(),
        },
        prompts: None,
    }
}

fn test_manifest() -> FleetManifest {
    parse_fleet_manifest(
        r#"
        name = "test-fleet"
        entry = "echo"

        [agents.echo]
        path = "agents/echo"

        [agents.upper]
        path = "agents/upper"
    "#,
    )
    .unwrap()
}

#[tokio::test]
async fn fleet_from_manifest_resolves_agent_ids() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let manifest = test_manifest();

    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();

    assert_eq!(fleet.name, "test-fleet");
    assert_eq!(fleet.agents.len(), 2);
    // Entry should be echo's registry ID
    assert!(fleet.entry.as_str().contains("echo"));
}

#[tokio::test]
async fn fleet_from_manifest_fails_when_agent_not_registered() {
    let registry = test_registry_with_agents(&["echo"]); // "upper" missing
    let manifest = test_manifest();

    let result = Fleet::from_manifest(manifest, &*registry).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("upper"),
        "error should mention missing agent: {err}"
    );
}

#[tokio::test]
async fn fleet_from_manifest_fails_when_entry_not_registered() {
    let registry = test_registry_with_agents(&["upper"]); // "echo" (entry) missing
    let manifest = test_manifest();

    let result = Fleet::from_manifest(manifest, &*registry).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("echo"),
        "error should mention missing entry agent: {err}"
    );
}

#[tokio::test]
async fn fleet_has_agent() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let manifest = test_manifest();
    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();

    // Fleet stores ResourceIds, has_agent checks by ResourceId string content
    let echo_id = registry.agent_id("echo").await.unwrap();
    let upper_id = registry.agent_id("upper").await.unwrap();
    assert!(fleet.agents.contains(&echo_id));
    assert!(fleet.agents.contains(&upper_id));
}

#[tokio::test]
async fn fleet_has_placeholder_id_before_registration() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let manifest = test_manifest();
    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();

    assert!(fleet
        .id
        .as_str()
        .contains("pending-registration://fleets/test-fleet"));
}

// ============================================================================
// Fleet Registration Tests
// ============================================================================

#[tokio::test]
async fn register_fleet_assigns_registry_id() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let manifest = test_manifest();
    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();

    registry.register_fleet(fleet).await.unwrap();

    let stored = registry.get_fleet("test-fleet").await.unwrap();
    assert!(stored.id.as_str().contains("/fleets/test-fleet"));
    assert!(!stored.id.as_str().contains("pending-registration"));
}

#[tokio::test]
async fn register_fleet_idempotent() {
    let registry = test_registry_with_agents(&["echo", "upper"]);

    let fleet1 = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();
    let fleet2 = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();

    registry.register_fleet(fleet1).await.unwrap();
    // Second registration of same fleet should succeed
    registry.register_fleet(fleet2).await.unwrap();
}

#[tokio::test]
async fn get_fleets_returns_all() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let fleet = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();
    registry.register_fleet(fleet).await.unwrap();

    let fleets = registry.get_fleets().await;
    assert_eq!(fleets.len(), 1);
    assert_eq!(fleets[0].name, "test-fleet");
}

#[tokio::test]
async fn get_fleet_returns_none_for_missing() {
    let registry = test_registry_with_agents(&["echo"]);
    assert!(registry.get_fleet("nonexistent").await.is_none());
}

// ============================================================================
// Fleet from_manifest — Edge Cases
// ============================================================================

#[tokio::test]
async fn fleet_from_manifest_entry_is_in_agents_set() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let manifest = test_manifest();
    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();

    // The entry agent's ResourceId must be present in the agents set
    assert!(fleet.agents.contains(&fleet.entry));
}

#[tokio::test]
async fn fleet_from_manifest_single_agent_fleet() {
    let registry = test_registry_with_agents(&["solo"]);
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "solo-fleet"
        entry = "solo"

        [agents.solo]
        path = "agents/solo"
    "#,
    )
    .unwrap();

    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();
    assert_eq!(fleet.agents.len(), 1);
    assert_eq!(fleet.entry, registry.agent_id("solo").await.unwrap());
}

#[tokio::test]
async fn fleet_from_manifest_many_agents() {
    let names = &[
        "coordinator",
        "researcher",
        "writer",
        "reviewer",
        "publisher",
    ];
    let registry = test_registry_with_agents(names);
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "big-fleet"
        entry = "coordinator"

        [agents.coordinator]
        path = "agents/coordinator"

        [agents.researcher]
        path = "agents/researcher"

        [agents.writer]
        path = "agents/writer"

        [agents.reviewer]
        path = "agents/reviewer"

        [agents.publisher]
        path = "agents/publisher"
    "#,
    )
    .unwrap();

    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();
    assert_eq!(fleet.agents.len(), 5);
    for name in names {
        let id = registry.agent_id(name).await.unwrap();
        assert!(
            fleet.agents.contains(&id),
            "fleet should contain agent '{name}'"
        );
    }
}

#[tokio::test]
async fn fleet_from_manifest_all_agents_missing() {
    // Registry has no agents at all
    let registry = test_registry_with_agents(&[]);
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "ghost-fleet"
        entry = "missing"

        [agents.missing]
        path = "agents/missing"
    "#,
    )
    .unwrap();

    let result = Fleet::from_manifest(manifest, &*registry).await;
    assert!(result.is_err());
    let err = format!("{}", result.unwrap_err());
    assert!(
        err.contains("missing"),
        "error should mention missing agent: {err}"
    );
}

#[tokio::test]
async fn fleet_agents_count_matches_manifest() {
    let registry = test_registry_with_agents(&["a", "b", "c"]);
    let manifest: FleetManifest = parse_fleet_manifest(
        r#"
        name = "counted"
        entry = "a"

        [agents.a]
        path = "agents/a"

        [agents.b]
        path = "agents/b"

        [agents.c]
        path = "agents/c"
    "#,
    )
    .unwrap();

    let expected_count = manifest.agents.len();
    let fleet = Fleet::from_manifest(manifest, &*registry).await.unwrap();
    assert_eq!(fleet.agents.len(), expected_count);
}

// ============================================================================
// Fleet Registration — Edge Cases
// ============================================================================

#[tokio::test]
async fn register_fleet_rejects_config_mismatch_different_entry() {
    let registry = test_registry_with_agents(&["echo", "upper"]);

    let fleet1 = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();
    registry.register_fleet(fleet1).await.unwrap();

    // Build a fleet with same agents but different entry
    let manifest2: FleetManifest = parse_fleet_manifest(
        r#"
        name = "test-fleet"
        entry = "upper"

        [agents.echo]
        path = "agents/echo"

        [agents.upper]
        path = "agents/upper"
    "#,
    )
    .unwrap();
    let fleet2 = Fleet::from_manifest(manifest2, &*registry).await.unwrap();

    let result = registry.register_fleet(fleet2).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        vlinder_core::domain::RegistrationError::FleetConfigMismatch(name) => {
            assert_eq!(name, "test-fleet");
        }
        other => panic!("expected FleetConfigMismatch, got: {other}"),
    }
}

#[tokio::test]
async fn register_fleet_rejects_config_mismatch_different_agents() {
    let registry = test_registry_with_agents(&["echo", "upper", "third"]);

    let fleet1 = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();
    registry.register_fleet(fleet1).await.unwrap();

    // Build a fleet with same name and entry, but a different agent set
    let manifest2: FleetManifest = parse_fleet_manifest(
        r#"
        name = "test-fleet"
        entry = "echo"

        [agents.echo]
        path = "agents/echo"

        [agents.third]
        path = "agents/third"
    "#,
    )
    .unwrap();
    let fleet2 = Fleet::from_manifest(manifest2, &*registry).await.unwrap();

    let result = registry.register_fleet(fleet2).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        vlinder_core::domain::RegistrationError::FleetConfigMismatch(name) => {
            assert_eq!(name, "test-fleet");
        }
        other => panic!("expected FleetConfigMismatch, got: {other}"),
    }
}

#[tokio::test]
async fn fleet_name_preserved_through_registration() {
    let registry = test_registry_with_agents(&["echo", "upper"]);
    let fleet = Fleet::from_manifest(test_manifest(), &*registry)
        .await
        .unwrap();

    assert_eq!(fleet.name, "test-fleet");
    registry.register_fleet(fleet).await.unwrap();

    let stored = registry.get_fleet("test-fleet").await.unwrap();
    assert_eq!(stored.name, "test-fleet");
}

#[tokio::test]
async fn multiple_fleets_can_share_agents() {
    let registry = test_registry_with_agents(&["shared", "only-a", "only-b"]);

    let manifest_a: FleetManifest = parse_fleet_manifest(
        r#"
        name = "fleet-a"
        entry = "shared"

        [agents.shared]
        path = "agents/shared"

        [agents.only-a]
        path = "agents/only-a"
    "#,
    )
    .unwrap();

    let manifest_b: FleetManifest = parse_fleet_manifest(
        r#"
        name = "fleet-b"
        entry = "shared"

        [agents.shared]
        path = "agents/shared"

        [agents.only-b]
        path = "agents/only-b"
    "#,
    )
    .unwrap();

    let fleet_a = Fleet::from_manifest(manifest_a, &*registry).await.unwrap();
    let fleet_b = Fleet::from_manifest(manifest_b, &*registry).await.unwrap();

    registry.register_fleet(fleet_a).await.unwrap();
    registry.register_fleet(fleet_b).await.unwrap();

    assert_eq!(registry.get_fleets().await.len(), 2);

    // Both fleets contain the shared agent
    let shared_id = registry.agent_id("shared").await.unwrap();
    assert!(registry
        .get_fleet("fleet-a")
        .await
        .unwrap()
        .agents
        .contains(&shared_id));
    assert!(registry
        .get_fleet("fleet-b")
        .await
        .unwrap()
        .agents
        .contains(&shared_id));
}

// ============================================================================
// Fleet::placeholder_id format
// ============================================================================

#[test]
fn placeholder_id_has_expected_format() {
    let id = Fleet::placeholder_id("my-fleet");
    assert_eq!(id.as_str(), "pending-registration://fleets/my-fleet");
}

// ============================================================================
// LoadError Display
// ============================================================================

#[test]
fn load_error_display_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err = vlinder_core::domain::FleetLoadError::Io(io_err);
    let msg = format!("{err}");
    assert!(msg.contains("IO error"));
    assert!(msg.contains("file not found"));
}

#[test]
fn load_error_display_parse() {
    let err = vlinder_core::domain::FleetLoadError::Parse("bad toml".to_string());
    let msg = format!("{err}");
    assert!(msg.contains("parse error"));
    assert!(msg.contains("bad toml"));
}

#[test]
fn load_error_display_validation() {
    let err = vlinder_core::domain::FleetLoadError::Validation("entry not found".to_string());
    let msg = format!("{err}");
    assert!(msg.contains("validation error"));
    assert!(msg.contains("entry not found"));
}
