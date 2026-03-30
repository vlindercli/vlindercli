//! Repository trait for Registry persistence.

use std::collections::HashMap;
use std::str::FromStr;

use super::runtime::RuntimeType;
use super::{
    Agent, ImageDigest, Model, ModelType, MountConfig, Prompts, Provider, Requirements, ResourceId,
    ServiceConfig, ServiceType,
};

/// Repository for persisting Registry state.
///
/// Implementations handle storage (`SQLite`, `Postgres`, etc.).
/// Registry uses this for save/load operations.
pub trait RegistryRepository: Send + Sync {
    /// Save a model to the repository.
    fn save_model(&self, model: &Model) -> Result<(), RepositoryError>;

    /// Load all models from the repository.
    fn load_models(&self) -> Result<Vec<Model>, RepositoryError>;

    /// Delete a model by name.
    fn delete_model(&self, name: &str) -> Result<bool, RepositoryError>;

    /// Check if a model exists.
    fn model_exists(&self, name: &str) -> Result<bool, RepositoryError>;

    /// Save an agent to the repository.
    fn save_agent(&self, agent: &Agent) -> Result<(), RepositoryError>;

    /// Load all agents from the repository.
    fn load_agents(&self) -> Result<Vec<Agent>, RepositoryError>;

    /// Delete an agent by name.
    fn delete_agent(&self, name: &str) -> Result<bool, RepositoryError>;

    /// Check if an agent exists.
    fn agent_exists(&self, name: &str) -> Result<bool, RepositoryError>;

    /// Append a state transition to the agent state log.
    fn append_agent_state(&self, _state: &super::AgentState) -> Result<(), RepositoryError> {
        Err(RepositoryError::Database(
            "append_agent_state not implemented".to_string(),
        ))
    }

    /// Get the latest agent deployment state by name.
    fn get_agent_state(&self, _name: &str) -> Result<Option<super::AgentState>, RepositoryError> {
        Ok(None)
    }
}

#[derive(Debug)]
pub enum RepositoryError {
    /// Database connection or query error.
    Database(String),
    /// Serialization/deserialization error.
    Serialization(String),
}

impl std::fmt::Display for RepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepositoryError::Database(msg) => write!(f, "database error: {msg}"),
            RepositoryError::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for RepositoryError {}

/// Stored model representation for persistence.
#[derive(Debug)]
pub struct StoredModel {
    pub name: String,
    pub model_type: ModelType,
    pub provider: Provider,
    pub model_path: String,
    pub digest: String,
}

impl StoredModel {
    pub fn from_model(model: &Model) -> Self {
        Self {
            name: model.name.clone(),
            model_type: model.model_type.clone(),
            provider: model.provider,
            model_path: model.model_path.as_str().to_string(),
            digest: model.digest.clone(),
        }
    }

    pub fn to_model(&self) -> Model {
        Model {
            id: Model::placeholder_id(&self.name),
            name: self.name.clone(),
            model_type: self.model_type.clone(),
            provider: self.provider,
            model_path: ResourceId::new(&self.model_path),
            digest: self.digest.clone(),
        }
    }
}

/// Stored agent representation for persistence.
///
/// Scalar fields stored directly; complex nested fields (requirements,
/// prompts) serialized as JSON strings.
#[derive(Debug)]
pub struct StoredAgent {
    pub name: String,
    pub description: String,
    pub source: Option<String>,
    pub runtime: String,
    pub executable: String,
    pub image_digest: Option<String>,
    pub object_storage: Option<String>,
    pub vector_storage: Option<String>,
    pub requirements_json: String,
    pub prompts_json: Option<String>,
    /// Base64-encoded Ed25519 public key (ADR 084).
    pub public_key: Option<String>,
}

impl StoredAgent {
    pub fn from_agent(agent: &Agent) -> Result<Self, RepositoryError> {
        let requirements = RequirementsJson {
            models: agent.requirements.models.clone(),
            services: agent.requirements.services.clone(),
            mounts: agent.requirements.mounts.clone(),
        };

        let prompts = agent.prompts.as_ref().map(|p| PromptsJson {
            intent_recognition: p.intent_recognition.clone(),
            query_expansion: p.query_expansion.clone(),
            answer_generation: p.answer_generation.clone(),
            map_summarize: p.map_summarize.clone(),
            reduce_summaries: p.reduce_summaries.clone(),
            direct_summarize: p.direct_summarize.clone(),
        });

        let requirements_json = serde_json::to_string(&requirements)
            .map_err(|e| RepositoryError::Serialization(e.to_string()))?;
        let prompts_json = prompts
            .map(|p| serde_json::to_string(&p))
            .transpose()
            .map_err(|e| RepositoryError::Serialization(e.to_string()))?;

        Ok(Self {
            name: agent.name.clone(),
            description: agent.description.clone(),
            source: agent.source.clone(),
            runtime: agent.runtime.as_str().to_string(),
            executable: agent.executable.clone(),
            image_digest: agent.image_digest.as_ref().map(|d| d.as_str().to_string()),
            object_storage: agent
                .object_storage
                .as_ref()
                .map(|r| r.as_str().to_string()),
            vector_storage: agent
                .vector_storage
                .as_ref()
                .map(|r| r.as_str().to_string()),
            requirements_json,
            prompts_json,
            public_key: agent
                .public_key
                .as_ref()
                .map(|k| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, k)),
        })
    }

    pub fn to_agent(&self) -> Result<Agent, RepositoryError> {
        let runtime = RuntimeType::from_str(&self.runtime).map_err(|_| {
            RepositoryError::Serialization(format!("unknown runtime: {}", self.runtime))
        })?;

        let image_digest = self
            .image_digest
            .as_ref()
            .map(ImageDigest::parse)
            .transpose()
            .map_err(RepositoryError::Serialization)?;

        let requirements: RequirementsJson = serde_json::from_str(&self.requirements_json)
            .map_err(|e| RepositoryError::Serialization(e.to_string()))?;

        let prompts: Option<PromptsJson> = self
            .prompts_json
            .as_ref()
            .map(|s| serde_json::from_str(s))
            .transpose()
            .map_err(|e| RepositoryError::Serialization(e.to_string()))?;

        let models = requirements.models;

        let public_key = self
            .public_key
            .as_ref()
            .map(|b64| base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64))
            .transpose()
            .map_err(|e| {
                RepositoryError::Serialization(format!("invalid public_key base64: {e}"))
            })?;

        Ok(Agent {
            id: Agent::placeholder_id(&self.name),
            name: self.name.clone(),
            description: self.description.clone(),
            source: self.source.clone(),
            runtime,
            executable: self.executable.clone(),
            image_digest,
            public_key,
            object_storage: self.object_storage.as_ref().map(ResourceId::new),
            vector_storage: self.vector_storage.as_ref().map(ResourceId::new),
            requirements: Requirements {
                models,
                services: requirements.services,
                mounts: requirements.mounts,
            },
            prompts: prompts.map(|p| Prompts {
                intent_recognition: p.intent_recognition,
                query_expansion: p.query_expansion,
                answer_generation: p.answer_generation,
                map_summarize: p.map_summarize,
                reduce_summaries: p.reduce_summaries,
                direct_summarize: p.direct_summarize,
            }),
        })
    }
}

// Private serde helpers for JSON round-tripping
#[derive(serde::Serialize, serde::Deserialize)]
struct RequirementsJson {
    models: HashMap<String, String>,
    services: HashMap<ServiceType, ServiceConfig>,
    #[serde(default)]
    mounts: HashMap<String, MountConfig>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PromptsJson {
    intent_recognition: Option<String>,
    query_expansion: Option<String>,
    answer_generation: Option<String>,
    map_summarize: Option<String>,
    reduce_summaries: Option<String>,
    direct_summarize: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Protocol, Provider};

    fn minimal_agent() -> Agent {
        Agent {
            id: Agent::placeholder_id("echo"),
            name: "echo".to_string(),
            description: "Echoes input".to_string(),
            source: None,
            runtime: RuntimeType::Container,
            executable: "localhost/echo:latest".to_string(),
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

    fn full_agent() -> Agent {
        let mut models = HashMap::new();
        models.insert("phi3".to_string(), "phi3:latest".to_string());
        models.insert("nomic".to_string(), "nomic-embed".to_string());

        let mut services = HashMap::new();
        services.insert(
            ServiceType::Infer,
            ServiceConfig {
                provider: Provider::Ollama,
                protocol: Protocol::OpenAi,
                models: vec!["phi3:latest".to_string()],
            },
        );

        Agent {
            id: Agent::placeholder_id("thinker"),
            name: "thinker".to_string(),
            description: "A thinking agent".to_string(),
            source: Some("https://github.com/example/thinker".to_string()),
            runtime: RuntimeType::Container,
            executable: "localhost/thinker:latest".to_string(),
            image_digest: Some(ImageDigest::parse("sha256:abc123def456").unwrap()),
            public_key: None,
            object_storage: Some(ResourceId::new("sqlite:///data/objects.db")),
            vector_storage: Some(ResourceId::new("sqlite:///data/vectors.db")),
            requirements: Requirements {
                models,
                services,
                mounts: HashMap::new(),
            },
            prompts: Some(Prompts {
                intent_recognition: Some("Classify intent".to_string()),
                query_expansion: None,
                answer_generation: Some("Generate answer".to_string()),
                map_summarize: None,
                reduce_summaries: None,
                direct_summarize: None,
            }),
        }
    }

    #[test]
    fn stored_agent_round_trip_minimal() {
        let agent = minimal_agent();
        let stored = StoredAgent::from_agent(&agent).unwrap();
        let restored = stored.to_agent().unwrap();

        assert_eq!(restored.name, "echo");
        assert_eq!(restored.description, "Echoes input");
        assert_eq!(restored.runtime, RuntimeType::Container);
        assert_eq!(restored.executable, "localhost/echo:latest");
        assert!(restored.source.is_none());
        assert!(restored.image_digest.is_none());
        assert!(restored.object_storage.is_none());
        assert!(restored.vector_storage.is_none());
        assert!(restored.requirements.models.is_empty());
        assert!(restored.requirements.services.is_empty());
        assert!(restored.prompts.is_none());
    }

    #[test]
    fn stored_agent_round_trip_full() {
        let agent = full_agent();
        let stored = StoredAgent::from_agent(&agent).unwrap();
        let restored = stored.to_agent().unwrap();

        assert_eq!(restored.name, "thinker");
        assert_eq!(restored.description, "A thinking agent");
        assert_eq!(
            restored.source.as_deref(),
            Some("https://github.com/example/thinker")
        );
        assert_eq!(restored.runtime, RuntimeType::Container);
        assert_eq!(restored.executable, "localhost/thinker:latest");
        assert_eq!(
            restored
                .image_digest
                .as_ref()
                .map(super::super::image_digest::ImageDigest::as_str),
            Some("sha256:abc123def456")
        );
        assert_eq!(
            restored
                .object_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/objects.db")
        );
        assert_eq!(
            restored
                .vector_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/vectors.db")
        );

        // Models (ADR 094: values are registry names, not URIs)
        assert_eq!(restored.requirements.models.len(), 2);
        assert_eq!(
            restored
                .requirements
                .models
                .get("phi3")
                .map(std::string::String::as_str),
            Some("phi3:latest")
        );

        // Services
        assert_eq!(restored.requirements.services.len(), 1);
        let infer = &restored.requirements.services[&ServiceType::Infer];
        assert_eq!(infer.provider, Provider::Ollama);
        assert_eq!(infer.protocol, Protocol::OpenAi);
        assert_eq!(infer.models, vec!["phi3:latest"]);

        // Prompts
        let prompts = restored.prompts.unwrap();
        assert_eq!(
            prompts.intent_recognition.as_deref(),
            Some("Classify intent")
        );
        assert!(prompts.query_expansion.is_none());
        assert_eq!(
            prompts.answer_generation.as_deref(),
            Some("Generate answer")
        );
    }

    #[test]
    fn round_trip_with_public_key() {
        let mut agent = minimal_agent();
        agent.public_key = Some(vec![
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
            25, 26, 27, 28, 29, 30, 31, 32,
        ]);

        let stored = StoredAgent::from_agent(&agent).unwrap();
        assert!(stored.public_key.is_some());

        let restored = stored.to_agent().unwrap();
        assert_eq!(restored.public_key, agent.public_key);
    }

    #[test]
    fn round_trip_without_public_key() {
        let agent = minimal_agent();
        assert!(agent.public_key.is_none());

        let stored = StoredAgent::from_agent(&agent).unwrap();
        assert!(stored.public_key.is_none());

        let restored = stored.to_agent().unwrap();
        assert!(restored.public_key.is_none());
    }
}
