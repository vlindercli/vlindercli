//! Registry - source of truth for all system state.
//!
//! Stores:
//! - Runtimes (available at `/runtimes`)
//! - Models (registered model definitions)
//! - Agents (registered agent definitions)
//! - Jobs (submitted, running, completed)
//!
//! The `Registry` trait abstracts state storage. Implementations live
//! outside the domain module:
//! - `InMemoryRegistry` — `crate::registry`
//! - `PersistentRegistry` — `crate::registry`
//! - `GrpcRegistryClient` — `crate::registry_service`

use crate::domain::{
    Agent, AgentManifest, Fleet, Model, ObjectStorageType, Provider, ResourceId, RuntimeType,
    VectorStorageType,
};

use async_trait::async_trait;

/// Unique identifier for a submitted job.
///
/// Format: `<registry_id>/jobs/<uuid>`
/// Example: `http://127.0.0.1:9000/jobs/550e8400-e29b-41d4-a716-446655440000`
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct JobId(String);

impl JobId {
    /// Create a new `JobId` under the given registry.
    pub fn new(registry_id: &ResourceId) -> Self {
        let uuid = uuid::Uuid::new_v4();
        Self(format!("{}/jobs/{uuid}", registry_id.as_str()))
    }

    /// Create a `JobId` from an existing string (e.g., from gRPC).
    pub fn from_string(id: String) -> Self {
        Self(id)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A submitted job and its current state.
#[derive(Clone, Debug)]
pub struct Job {
    pub id: JobId,
    pub submission_id: super::SubmissionId, // ADR 044: tracks message flow
    pub agent_id: ResourceId,
    pub input: String,
    pub status: JobStatus,
}

#[derive(Clone, Debug, PartialEq)]
pub enum JobStatus {
    Pending,
    Running,
    Completed(String),
    Failed(String),
}

/// Error returned when agent registration fails validation.
#[derive(Debug)]
pub enum RegistrationError {
    /// Agent with this name is already registered.
    DuplicateName(String),
    /// Agent with this name exists but the manifest differs.
    ConfigMismatch(String),
    /// No runtime available for this agent's executable scheme/extension.
    NoRuntime(ResourceId),
    /// Agent declares object storage with unknown scheme.
    UnknownObjectStorageScheme(String),
    /// Agent declares object storage type not available.
    ObjectStorageUnavailable(ObjectStorageType),
    /// Agent declares vector storage with unknown scheme.
    UnknownVectorStorageScheme(String),
    /// Agent declares vector storage type not available.
    VectorStorageUnavailable(VectorStorageType),
    /// Agent requires a model that is not registered.
    /// Contains the agent's alias and the registry name.
    ModelNotRegistered(String, String),
    /// Agent requires an inference engine that is not available.
    InferenceEngineUnavailable(Provider, String),
    /// Agent requires an embedding engine that is not available.
    EmbeddingEngineUnavailable(Provider, String),
    /// Agent uses an inference model but does not declare services.infer.
    InferenceServiceNotDeclared(String),
    /// Agent uses an embedding model but does not declare services.embed.
    EmbeddingServiceNotDeclared(String),
    /// Agent declares services.infer but has no inference model.
    InferenceServiceWithoutModel,
    /// Agent declares services.embed but has no embedding model.
    EmbeddingServiceWithoutModel,
    /// Model cannot be removed because deployed agents depend on it.
    ModelInUse(String, Vec<String>),
    /// Agent cannot be removed because it belongs to one or more fleets.
    AgentInUse(String, Vec<String>),
    /// Identity provisioning failed (ADR 084).
    IdentityFailed(String),
    /// Persistence operation failed (disk I/O, database error, etc.).
    Persistence(String),
    /// Fleet with this name is already registered.
    FleetDuplicateName(String),
    /// Fleet with this name exists but the definition differs.
    FleetConfigMismatch(String),
    /// Fleet references an agent that is not registered.
    FleetAgentNotRegistered(String, String),
    /// Error forwarded from a remote registry (gRPC).
    /// Contains the server's error message as-is — already user-friendly.
    Remote(String),
}

impl std::fmt::Display for RegistrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RegistrationError::DuplicateName(name) => {
                write!(f, "agent already registered: {name}")
            }
            RegistrationError::ConfigMismatch(name) => write!(
                f,
                "agent '{name}' already registered with different configuration",
            ),
            RegistrationError::NoRuntime(id) => {
                write!(f, "no runtime available for agent executable: {id}\n\nIs the daemon running? Start it with: vlinder daemon")
            }
            RegistrationError::UnknownObjectStorageScheme(s) => {
                write!(f, "unknown object storage scheme: {s}")
            }
            RegistrationError::ObjectStorageUnavailable(t) => {
                write!(f, "object storage not available: {t:?}")
            }
            RegistrationError::UnknownVectorStorageScheme(s) => {
                write!(f, "unknown vector storage scheme: {s}")
            }
            RegistrationError::VectorStorageUnavailable(t) => {
                write!(f, "vector storage not available: {t:?}")
            }
            RegistrationError::ModelNotRegistered(alias, name) => {
                write!(f, "model '{alias}' (registry name: '{name}') is not registered\n\nAdd it first: vlinder model add {name}")
            }
            RegistrationError::InferenceEngineUnavailable(provider, model) => {
                write!(f, "no {provider:?} inference engine available for model '{model}'\n\nIs the daemon running? Start it with: vlinder daemon")
            }
            RegistrationError::EmbeddingEngineUnavailable(provider, model) => {
                write!(f, "no {provider:?} embedding engine available for model '{model}'\n\nIs the daemon running? Start it with: vlinder daemon")
            }
            RegistrationError::InferenceServiceNotDeclared(model) => {
                write!(f, "model '{model}' is an inference model but agent does not declare [requirements.services.infer]")
            }
            RegistrationError::EmbeddingServiceNotDeclared(model) => {
                write!(f, "model '{model}' is an embedding model but agent does not declare [requirements.services.embed]")
            }
            RegistrationError::InferenceServiceWithoutModel => {
                write!(
                    f,
                    "agent declares [requirements.services.infer] but has no inference model"
                )
            }
            RegistrationError::EmbeddingServiceWithoutModel => {
                write!(
                    f,
                    "agent declares [requirements.services.embed] but has no embedding model"
                )
            }
            RegistrationError::ModelInUse(name, agents) => write!(
                f,
                "model '{name}' is in use by agents: {}",
                agents.join(", ")
            ),
            RegistrationError::AgentInUse(name, fleets) => write!(
                f,
                "agent '{name}' is in use by fleets: {}",
                fleets.join(", ")
            ),
            RegistrationError::IdentityFailed(msg) => {
                write!(f, "identity provisioning failed: {msg}")
            }
            RegistrationError::Persistence(msg) => write!(f, "persistence error: {msg}"),
            RegistrationError::FleetDuplicateName(name) => {
                write!(f, "fleet already registered: {name}")
            }
            RegistrationError::FleetConfigMismatch(name) => write!(
                f,
                "fleet '{name}' already registered with different configuration",
            ),
            RegistrationError::FleetAgentNotRegistered(fleet, agent) => {
                write!(f, "fleet '{fleet}' references agent '{agent}' which is not registered\n\nDeploy the agent first: vlinder agent deploy")
            }
            RegistrationError::Remote(msg) => write!(f, "{msg}"),
        }
    }
}

// ============================================================================
// Registry Trait
// ============================================================================

/// Source of truth for all system state.
///
/// All methods take `&self` — implementations handle internal synchronization.
/// Returns owned values (not references) for network compatibility.
#[async_trait]
pub trait Registry: Send + Sync {
    /// URI where this registry exposes its API.
    fn id(&self) -> ResourceId;

    // --- Agent operations ---

    /// Register an agent after validating requirements.
    /// Assigns registry identity `<registry_id>/agents/<name>`.
    async fn register_agent(&self, agent: Agent) -> Result<(), RegistrationError>;

    /// Register an agent from its manifest (ADR 102).
    ///
    /// Accepts a fully-resolved `AgentManifest`, validates, resolves identity,
    /// stores the manifest, and returns the registered `Agent`.
    ///
    /// Idempotent: same manifest → returns existing agent.
    /// Different manifest for same name → `ConfigMismatch` error.
    async fn register_manifest(&self, manifest: AgentManifest) -> Result<Agent, RegistrationError> {
        let name = manifest.name.clone();
        let agent = Agent::from_manifest(manifest)
            .map_err(|e| RegistrationError::Persistence(format!("{e:?}")))?;
        self.register_agent(agent).await?;
        self.get_agent_by_name(&name).await.ok_or_else(|| {
            RegistrationError::Persistence(format!("agent '{name}' not found after registration"))
        })
    }

    /// Get the registry-issued ID for an agent name.
    fn agent_id(&self, name: &str) -> Option<ResourceId>;

    /// Get an agent by ID.
    async fn get_agent(&self, id: &ResourceId) -> Option<Agent>;

    /// Get all registered agents.
    async fn get_agents(&self) -> Vec<Agent>;

    /// Get an agent by name.
    async fn get_agent_by_name(&self, name: &str) -> Option<Agent> {
        self.get_agents().await.into_iter().find(|a| a.name == name)
    }

    /// Select the appropriate runtime for an agent.
    fn select_runtime(&self, agent: &Agent) -> Option<RuntimeType>;

    /// Get all agents assigned to a specific runtime type.
    async fn get_agents_by_runtime(&self, runtime: RuntimeType) -> Vec<Agent> {
        self.get_agents()
            .await
            .into_iter()
            .filter(|a| self.select_runtime(a) == Some(runtime))
            .collect()
    }

    /// Resolve an agent's model alias to its provider backend string.
    ///
    /// The agent code calls infer/embed with a model name (the alias from
    /// `[requirements.models]`). This method looks up that alias, finds the
    /// registered model by registry name (ADR 094), and returns the provider
    /// as a routing string.
    async fn resolve_model_backend(&self, agent_name: &str, model: &str) -> Result<String, String> {
        let agent = self
            .get_agent_by_name(agent_name)
            .await
            .ok_or_else(|| format!("agent '{agent_name}' not found in registry"))?;
        let model_name = agent.requirements.models.get(model).ok_or_else(|| {
            format!(
                "agent called service with undeclared model '{model}'\n\nDeclared models: {:?}",
                agent.requirements.models.keys().collect::<Vec<_>>()
            )
        })?;
        let registered = self.get_model(model_name).await.ok_or_else(|| {
            format!("model '{model}' (registry name: '{model_name}') not found in registry",)
        })?;
        Ok(match registered.provider {
            Provider::Ollama => "ollama".to_string(),
            Provider::OpenRouter => "openrouter".to_string(),
        })
    }

    /// Get all agents whose model requirements reference the given model name.
    async fn get_agents_requiring_model(&self, model_name: &str) -> Vec<Agent> {
        self.get_agents()
            .await
            .into_iter()
            .filter(|a| {
                a.requirements
                    .models
                    .values()
                    .any(|name| name == model_name)
            })
            .collect()
    }

    // --- Model operations ---

    /// Register a model (assigns registry-issued identity).
    async fn register_model(&self, model: Model) -> Result<(), RegistrationError>;

    /// Get a model by name.
    async fn get_model(&self, name: &str) -> Option<Model>;

    /// Get all registered models.
    async fn get_models(&self) -> Vec<Model>;

    /// Get a model by its `model_path` (the URI that identifies the actual model resource).
    fn get_model_by_path(&self, path: &ResourceId) -> Option<Model>;

    /// Get the registry-issued ID for a model name.
    fn model_id(&self, name: &str) -> ResourceId;

    /// Delete a model by name. Returns true if the model existed.
    fn delete_model(&self, name: &str) -> Result<bool, RegistrationError>;

    /// Delete an agent by name. Returns true if the agent existed.
    /// Fails if the agent belongs to any fleet.
    fn delete_agent(&self, name: &str) -> Result<bool, RegistrationError>;

    // --- Job operations ---

    /// Create a new job with submission tracking (ADR 044).
    async fn create_job(
        &self,
        submission_id: super::SubmissionId,
        agent_id: ResourceId,
        input: String,
    ) -> JobId;

    /// Get a job by ID.
    async fn get_job(&self, id: &JobId) -> Option<Job>;

    /// Update job status.
    async fn update_job_status(&self, id: &JobId, status: JobStatus);

    /// Get all pending jobs.
    fn pending_jobs(&self) -> Vec<Job>;

    // --- Fleet operations ---

    /// Register a fleet after validating all agents are registered.
    fn register_fleet(&self, fleet: Fleet) -> Result<(), RegistrationError>;

    /// Get a fleet by name.
    fn get_fleet(&self, name: &str) -> Option<Fleet>;

    /// Get all registered fleets.
    fn get_fleets(&self) -> Vec<Fleet>;

    // --- Capability registration ---

    fn register_runtime(&self, runtime_type: RuntimeType);
    fn register_object_storage(&self, storage_type: ObjectStorageType);
    fn register_vector_storage(&self, storage_type: VectorStorageType);
    fn register_inference_engine(&self, engine_type: Provider);
    fn register_embedding_engine(&self, engine_type: Provider);

    // --- Capability queries ---

    fn has_object_storage(&self, storage_type: ObjectStorageType) -> bool;
    fn has_vector_storage(&self, storage_type: VectorStorageType) -> bool;
    fn has_inference_engine(&self, engine_type: Provider) -> bool;
    fn has_embedding_engine(&self, engine_type: Provider) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // JobId
    // ========================================================================

    #[test]
    fn job_id_from_string_round_trip() {
        let id = JobId::from_string("http://localhost:9000/jobs/abc-123".to_string());
        assert_eq!(id.as_str(), "http://localhost:9000/jobs/abc-123");
    }

    #[test]
    fn job_id_new_contains_registry_prefix() {
        let registry_id = ResourceId::new("http://127.0.0.1:9000");
        let id = JobId::new(&registry_id);
        assert!(id.as_str().starts_with("http://127.0.0.1:9000/jobs/"));
    }

    #[test]
    fn job_id_new_generates_unique_ids() {
        let registry_id = ResourceId::new("http://127.0.0.1:9000");
        let id1 = JobId::new(&registry_id);
        let id2 = JobId::new(&registry_id);
        assert_ne!(id1.as_str(), id2.as_str());
    }

    // ========================================================================
    // JobStatus
    // ========================================================================

    #[test]
    fn job_status_equality() {
        assert_eq!(JobStatus::Pending, JobStatus::Pending);
        assert_eq!(JobStatus::Running, JobStatus::Running);
        assert_ne!(JobStatus::Pending, JobStatus::Running);
    }

    // ========================================================================
    // RegistrationError Display
    // ========================================================================

    #[test]
    fn display_duplicate_name() {
        let err = RegistrationError::DuplicateName("my-agent".into());
        assert_eq!(format!("{err}"), "agent already registered: my-agent");
    }

    #[test]
    fn display_config_mismatch() {
        let err = RegistrationError::ConfigMismatch("my-agent".into());
        let msg = format!("{err}");
        assert!(msg.contains("my-agent"));
        assert!(msg.contains("different configuration"));
    }

    #[test]
    fn display_model_not_registered_includes_hint() {
        let err =
            RegistrationError::ModelNotRegistered("inference_model".into(), "claude-sonnet".into());
        let msg = format!("{err}");
        assert!(msg.contains("claude-sonnet"));
        assert!(msg.contains("vlinder model add"));
    }

    #[test]
    fn display_model_in_use_lists_agents() {
        let err =
            RegistrationError::ModelInUse("phi3".into(), vec!["agent-a".into(), "agent-b".into()]);
        let msg = format!("{err}");
        assert!(msg.contains("agent-a"));
        assert!(msg.contains("agent-b"));
    }

    #[test]
    fn display_agent_in_use_lists_fleets() {
        let err =
            RegistrationError::AgentInUse("echo".into(), vec!["fleet-a".into(), "fleet-b".into()]);
        let msg = format!("{err}");
        assert!(msg.contains("echo"));
        assert!(msg.contains("fleet-a"));
        assert!(msg.contains("fleet-b"));
    }

    #[test]
    fn display_remote_passes_through() {
        let err = RegistrationError::Remote("server said no".into());
        assert_eq!(format!("{err}"), "server said no");
    }

    // ========================================================================
    // RegistrationError Display — Fleet variants
    // ========================================================================

    #[test]
    fn display_fleet_duplicate_name() {
        let err = RegistrationError::FleetDuplicateName("my-fleet".into());
        let msg = format!("{err}");
        assert!(msg.contains("my-fleet"));
        assert!(msg.contains("already registered"));
    }

    #[test]
    fn display_fleet_config_mismatch() {
        let err = RegistrationError::FleetConfigMismatch("my-fleet".into());
        let msg = format!("{err}");
        assert!(msg.contains("my-fleet"));
        assert!(msg.contains("different configuration"));
    }

    #[test]
    fn display_fleet_agent_not_registered() {
        let err = RegistrationError::FleetAgentNotRegistered(
            "my-fleet".into(),
            "http://127.0.0.1:9000/agents/missing".into(),
        );
        let msg = format!("{err}");
        assert!(msg.contains("my-fleet"));
        assert!(msg.contains("missing"));
        assert!(msg.contains("not registered"));
    }
}
