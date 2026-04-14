//! Integration tests for Registry that require agent fixtures.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use vlinder_core::domain::{
    Agent, InMemoryRegistry, InMemorySecretStore, Model, ModelType, Provider, RegistrationError,
    Registry, ResourceId, RuntimeType, SecretStore,
};

fn test_secret_store() -> Arc<dyn SecretStore> {
    Arc::new(InMemorySecretStore::new())
}

const FIXTURES: &str = "tests/fixtures/agents";

fn fixture(name: &str) -> PathBuf {
    Path::new(FIXTURES).join(name)
}

fn load_agent(name: &str) -> Agent {
    Agent::load(&fixture(name)).unwrap()
}

// ============================================================================
// Agent Registration Tests
// ============================================================================

#[tokio::test]
async fn agent_registration() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);

    // Agent not found initially
    assert!(registry.agent_id("echo-agent").is_none());
    assert!(registry.get_agent_by_name("echo-agent").await.is_none());

    // Register agent — registry assigns identity
    let agent = load_agent("echo-agent");
    registry.register_agent(agent).await.unwrap();

    // Now found by name and registry-assigned id
    let agent_id = registry.agent_id("echo-agent").unwrap();
    let agent = registry.get_agent(&agent_id).await.unwrap();
    assert_eq!(agent.name, "echo-agent");

    // Identity provisioned: public key is 32 bytes (Ed25519)
    let public_key = agent
        .public_key
        .as_ref()
        .expect("public_key must be set after registration");
    assert_eq!(public_key.len(), 32);
}

// ============================================================================
// Runtime Selection Tests
// ============================================================================

#[test]
fn select_runtime_identifies_container_agent() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);

    let agent = load_agent("echo-agent");

    // Agent declares runtime = "container" → Container runtime selected
    assert_eq!(
        registry.select_runtime(&agent),
        Some(RuntimeType::Container)
    );
}

#[test]
fn select_runtime_returns_none_without_registered_runtime() {
    let registry = InMemoryRegistry::new(test_secret_store()); // No runtimes registered

    let agent = load_agent("echo-agent");

    // No Container runtime available
    assert_eq!(registry.select_runtime(&agent), None);
}

// ============================================================================
// Model Deletion Tests
// ============================================================================

async fn register_model_test_agent(registry: &InMemoryRegistry) {
    // model-test-agent requires phi3 and nomic-embed with these exact URIs
    let phi3 = Model {
        id: Model::placeholder_id("phi3"),
        name: "phi3".to_string(),
        model_type: ModelType::Inference,
        provider: Provider::Ollama,
        model_path: ResourceId::new("http://127.0.0.1:9000/models/phi3"),
        digest: String::new(),
    };
    let nomic = Model {
        id: Model::placeholder_id("nomic-embed"),
        name: "nomic-embed".to_string(),
        model_type: ModelType::Embedding,
        provider: Provider::Ollama,
        model_path: ResourceId::new("http://127.0.0.1:9000/models/nomic-embed"),
        digest: String::new(),
    };

    registry.register_runtime(RuntimeType::Container);
    registry.register_inference_engine(Provider::Ollama);
    registry.register_embedding_engine(Provider::Ollama);
    registry.register_model(phi3).await.unwrap();
    registry.register_model(nomic).await.unwrap();
    registry
        .register_agent(load_agent("model-test-agent"))
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_model_blocked_by_deployed_agent() {
    let registry = InMemoryRegistry::new(test_secret_store());
    register_model_test_agent(&registry).await;

    let result = registry.delete_model("phi3").await;
    assert!(result.is_err());

    let err = result.unwrap_err();
    match err {
        RegistrationError::ModelInUse(name, agents) => {
            assert_eq!(name, "phi3");
            assert!(agents.contains(&"model-test-agent".to_string()));
        }
        other => panic!("expected ModelInUse, got: {other}"),
    }

    // Model still exists
    assert!(registry.get_model("phi3").await.is_some());
}

#[tokio::test]
async fn delete_model_succeeds_without_dependent_agents() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_inference_engine(Provider::Ollama);

    let model = Model {
        id: Model::placeholder_id("unused"),
        name: "unused".to_string(),
        model_type: ModelType::Inference,
        provider: Provider::Ollama,
        model_path: ResourceId::new("ollama://localhost:11434/unused"),
        digest: String::new(),
    };
    registry.register_model(model).await.unwrap();

    let deleted = registry.delete_model("unused").await.unwrap();
    assert!(deleted);
    assert!(registry.get_model("unused").await.is_none());
}

// ============================================================================
// Engine Availability Tests
// ============================================================================

#[tokio::test]
async fn register_model_rejected_without_inference_engine() {
    let registry = InMemoryRegistry::new(test_secret_store());
    // No inference engine registered

    let model = Model {
        id: Model::placeholder_id("phi3"),
        name: "phi3".to_string(),
        model_type: ModelType::Inference,
        provider: Provider::Ollama,
        model_path: ResourceId::new("http://127.0.0.1:9000/models/phi3"),
        digest: String::new(),
    };

    let result = registry.register_model(model).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        RegistrationError::InferenceEngineUnavailable(prov, model) => {
            assert_eq!(prov, Provider::Ollama);
            assert_eq!(model, "phi3");
        }
        other => panic!("expected InferenceEngineUnavailable, got: {other}"),
    }
}

#[tokio::test]
async fn register_model_rejected_without_embedding_engine() {
    let registry = InMemoryRegistry::new(test_secret_store());
    // No embedding engine registered

    let model = Model {
        id: Model::placeholder_id("nomic-embed"),
        name: "nomic-embed".to_string(),
        model_type: ModelType::Embedding,
        provider: Provider::Ollama,
        model_path: ResourceId::new("http://127.0.0.1:9000/models/nomic-embed"),
        digest: String::new(),
    };

    let result = registry.register_model(model).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        RegistrationError::EmbeddingEngineUnavailable(prov, model) => {
            assert_eq!(prov, Provider::Ollama);
            assert_eq!(model, "nomic-embed");
        }
        other => panic!("expected EmbeddingEngineUnavailable, got: {other}"),
    }
}

// ============================================================================
// Service Declaration Validation
// ============================================================================

#[tokio::test]
async fn register_agent_rejected_when_inference_model_has_no_service() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);
    registry.register_inference_engine(Provider::Ollama);

    let model = Model {
        id: Model::placeholder_id("phi3"),
        name: "phi3".to_string(),
        model_type: ModelType::Inference,
        provider: Provider::Ollama,
        model_path: ResourceId::new("ollama://localhost:11434/phi3:latest"),
        digest: String::new(),
    };
    registry.register_model(model).await.unwrap();

    // Agent declares an inference model but no services.infer
    let agent = Agent::from_toml(
        r#"
        name = "no-infer-service"
        description = "Has inference model but no infer service"
        runtime = "container"
        executable = "localhost/no-infer:latest"

        [requirements.models]
        phi3 = "phi3"
    "#,
    )
    .unwrap();

    let result = registry.register_agent(agent).await;
    match result.unwrap_err() {
        RegistrationError::InferenceServiceNotDeclared(model) => {
            assert_eq!(model, "phi3");
        }
        other => panic!("expected InferenceServiceNotDeclared, got: {other}"),
    }
}

#[tokio::test]
async fn register_agent_rejected_when_embedding_model_has_no_service() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);
    registry.register_embedding_engine(Provider::Ollama);

    let model = Model {
        id: Model::placeholder_id("nomic-embed"),
        name: "nomic-embed".to_string(),
        model_type: ModelType::Embedding,
        provider: Provider::Ollama,
        model_path: ResourceId::new("ollama://localhost:11434/nomic-embed-text:latest"),
        digest: String::new(),
    };
    registry.register_model(model).await.unwrap();

    // Agent declares an embedding model but no services.embed
    let agent = Agent::from_toml(
        r#"
        name = "no-embed-service"
        description = "Has embedding model but no embed service"
        runtime = "container"
        executable = "localhost/no-embed:latest"

        [requirements.models]
        nomic-embed = "nomic-embed"
    "#,
    )
    .unwrap();

    let result = registry.register_agent(agent).await;
    match result.unwrap_err() {
        RegistrationError::EmbeddingServiceNotDeclared(model) => {
            assert_eq!(model, "nomic-embed");
        }
        other => panic!("expected EmbeddingServiceNotDeclared, got: {other}"),
    }
}

#[tokio::test]
async fn register_agent_rejected_when_infer_service_has_no_model() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);

    // Agent declares services.infer but no models
    let agent = Agent::from_toml(
        r#"
        name = "service-no-model"
        description = "Has infer service but no inference model"
        runtime = "container"
        executable = "localhost/service-no-model:latest"

        [requirements.services.infer]
        provider = "openrouter"
        protocol = "anthropic"
        models = ["anthropic/claude-3.5-sonnet"]
    "#,
    )
    .unwrap();

    let result = registry.register_agent(agent).await;
    match result.unwrap_err() {
        RegistrationError::InferenceServiceWithoutModel => {}
        other => panic!("expected InferenceServiceWithoutModel, got: {other}"),
    }
}

#[tokio::test]
async fn register_agent_rejected_when_embed_service_has_no_model() {
    let registry = InMemoryRegistry::new(test_secret_store());
    registry.register_runtime(RuntimeType::Container);

    // Agent declares services.embed but no models
    let agent = Agent::from_toml(
        r#"
        name = "embed-no-model"
        description = "Has embed service but no embedding model"
        runtime = "container"
        executable = "localhost/embed-no-model:latest"

        [requirements.services.embed]
        provider = "ollama"
        protocol = "openai"
        models = ["nomic-embed-text"]
    "#,
    )
    .unwrap();

    let result = registry.register_agent(agent).await;
    match result.unwrap_err() {
        RegistrationError::EmbeddingServiceWithoutModel => {}
        other => panic!("expected EmbeddingServiceWithoutModel, got: {other}"),
    }
}
