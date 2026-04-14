//! `InMemoryRegistry` — in-process Registry implementation with `RwLock`.
//!
//! Used directly for tests and as the read cache inside `PersistentRegistry`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;

use super::{
    ensure_agent_identity, Agent, AgentManifest, Fleet, Job, JobId, JobStatus, Model, ModelType,
    ObjectStorageType, Provider, RegistrationError, Registry, ResourceId, RuntimeType, SecretStore,
    ServiceType, SubmissionId, VectorStorageType,
};

/// Internal state for `InMemoryRegistry`.
struct RegistryState {
    jobs: HashMap<JobId, Job>,
    agents: HashMap<ResourceId, Agent>,
    /// Stored manifests keyed by agent name (ADR 102).
    manifests: HashMap<String, AgentManifest>,
    models: HashMap<ResourceId, Model>,
    fleets: HashMap<String, Fleet>,
    available_runtimes: HashSet<RuntimeType>,
    available_object_storage: HashSet<ObjectStorageType>,
    available_vector_storage: HashSet<VectorStorageType>,
    available_inference_engines: HashSet<Provider>,
    available_embedding_engines: HashSet<Provider>,
}

/// In-memory registry with internal `RwLock` for thread-safe access.
pub struct InMemoryRegistry {
    /// URI where this registry exposes its API.
    registry_id: ResourceId,
    state: RwLock<RegistryState>,
    /// Secret store for agent identity provisioning (ADR 084).
    secret_store: Arc<dyn SecretStore>,
}

impl InMemoryRegistry {
    pub fn new(secret_store: Arc<dyn SecretStore>) -> Self {
        Self {
            registry_id: ResourceId::new("http://127.0.0.1:9000"),
            state: RwLock::new(RegistryState {
                jobs: HashMap::new(),
                agents: HashMap::new(),
                manifests: HashMap::new(),
                models: HashMap::new(),
                fleets: HashMap::new(),
                available_runtimes: HashSet::new(),
                available_object_storage: HashSet::new(),
                available_vector_storage: HashSet::new(),
                available_inference_engines: HashSet::new(),
                available_embedding_engines: HashSet::new(),
            }),
            secret_store,
        }
    }

    /// Internal: check if runtime is available for agent (needs read lock held).
    #[allow(clippy::unused_self)]
    fn select_runtime_internal(&self, agent: &Agent, state: &RegistryState) -> Option<RuntimeType> {
        if state.available_runtimes.contains(&agent.runtime) {
            Some(agent.runtime)
        } else {
            None
        }
    }

    /// Internal: compute agent ID without needing &self for borrow checker.
    fn agent_id_internal(&self, name: &str) -> ResourceId {
        ResourceId::new(format!("{}/agents/{}", self.registry_id.as_str(), name))
    }

    /// Restore a previously-persisted agent without capability validation.
    ///
    /// Used during startup to load agents from disk. Bypasses runtime,
    /// storage, and model checks — persisted agents already passed
    /// validation at registration time.
    pub fn restore_agent(&self, mut agent: Agent) -> Result<(), RegistrationError> {
        let mut state = self.state.write().unwrap();
        let agent_id = self.agent_id_internal(&agent.name);
        if state.agents.contains_key(&agent_id) {
            return Err(RegistrationError::DuplicateName(agent.name.clone()));
        }
        agent.id = agent_id.clone();
        state.agents.insert(agent_id, agent);
        Ok(())
    }

    /// Remove a model by name. Returns true if the model existed.
    pub fn remove_model(&self, name: &str) -> bool {
        let model_id = ResourceId::new(format!("{}/models/{}", self.registry_id.as_str(), name));
        let mut state = self.state.write().unwrap();
        state.models.remove(&model_id).is_some()
    }

    /// Remove an agent by name. Returns true if the agent existed.
    ///
    /// Used by `PersistentRegistry` for cache eviction after disk delete.
    pub fn remove_agent(&self, name: &str) -> bool {
        let agent_id = self.agent_id_internal(name);
        let mut state = self.state.write().unwrap();
        let removed = state.agents.remove(&agent_id).is_some();
        if removed {
            state.manifests.remove(name);
        }
        removed
    }
}

#[async_trait]
impl Registry for InMemoryRegistry {
    fn id(&self) -> ResourceId {
        self.registry_id.clone()
    }

    // --- Agent operations ---

    #[allow(clippy::too_many_lines)]
    async fn register_agent(&self, mut agent: Agent) -> Result<(), RegistrationError> {
        let agent_id = self.agent_id_internal(&agent.name);

        // Phase 1: validate under lock, then drop the guard before await
        {
            let state = self.state.read().unwrap();

            // Idempotency: same config → Ok, different config same name → error
            if let Some(existing) = state.agents.get(&agent_id) {
                if existing.config() == agent.config() {
                    return Ok(());
                }
                return Err(RegistrationError::DuplicateName(agent.name.clone()));
            }

            // Validate runtime is available
            if self.select_runtime_internal(&agent, &state).is_none() {
                return Err(RegistrationError::NoRuntime(ResourceId::new(format!(
                    "runtime:{}",
                    agent.runtime.as_str()
                ))));
            }

            // Validate object storage if declared
            if let Some(ref uri) = agent.object_storage {
                let scheme = uri.scheme().unwrap_or("unknown");
                let storage_type =
                    ObjectStorageType::from_scheme(uri.scheme()).ok_or_else(|| {
                        RegistrationError::UnknownObjectStorageScheme(scheme.to_string())
                    })?;
                if !state.available_object_storage.contains(&storage_type) {
                    return Err(RegistrationError::ObjectStorageUnavailable(storage_type));
                }
            }

            // Validate vector storage if declared
            if let Some(ref uri) = agent.vector_storage {
                let scheme = uri.scheme().unwrap_or("unknown");
                let storage_type =
                    VectorStorageType::from_scheme(uri.scheme()).ok_or_else(|| {
                        RegistrationError::UnknownVectorStorageScheme(scheme.to_string())
                    })?;
                if !state.available_vector_storage.contains(&storage_type) {
                    return Err(RegistrationError::VectorStorageUnavailable(storage_type));
                }
            }

            // Validate all declared models are registered and their engines are available (ADR 094)
            for (model_alias, model_name) in &agent.requirements.models {
                let model = state.models.values().find(|m| m.name == *model_name);
                let Some(model) = model else {
                    return Err(RegistrationError::ModelNotRegistered(
                        model_alias.clone(),
                        model_name.clone(),
                    ));
                };

                match model.model_type {
                    ModelType::Inference => {
                        if !state.available_inference_engines.contains(&model.provider) {
                            return Err(RegistrationError::InferenceEngineUnavailable(
                                model.provider,
                                model.name.clone(),
                            ));
                        }
                        if !agent
                            .requirements
                            .services
                            .contains_key(&ServiceType::Infer)
                        {
                            return Err(RegistrationError::InferenceServiceNotDeclared(
                                model_alias.clone(),
                            ));
                        }
                    }
                    ModelType::Embedding => {
                        if !state.available_embedding_engines.contains(&model.provider) {
                            return Err(RegistrationError::EmbeddingEngineUnavailable(
                                model.provider,
                                model.name.clone(),
                            ));
                        }
                        if !agent
                            .requirements
                            .services
                            .contains_key(&ServiceType::Embed)
                        {
                            return Err(RegistrationError::EmbeddingServiceNotDeclared(
                                model_alias.clone(),
                            ));
                        }
                    }
                }
            }

            // Validate services have corresponding models
            if agent
                .requirements
                .services
                .contains_key(&ServiceType::Infer)
            {
                let has_inference_model = agent.requirements.models.values().any(|name| {
                    state
                        .models
                        .values()
                        .any(|m| m.name == *name && m.model_type == ModelType::Inference)
                });
                if !has_inference_model {
                    return Err(RegistrationError::InferenceServiceWithoutModel);
                }
            }
            if agent
                .requirements
                .services
                .contains_key(&ServiceType::Embed)
            {
                let has_embedding_model = agent.requirements.models.values().any(|name| {
                    state
                        .models
                        .values()
                        .any(|m| m.name == *name && m.model_type == ModelType::Embedding)
                });
                if !has_embedding_model {
                    return Err(RegistrationError::EmbeddingServiceWithoutModel);
                }
            }
        } // guard dropped here — safe to await

        // Phase 2: async identity provisioning (no lock held)
        let identity = ensure_agent_identity(&agent.name, self.secret_store.as_ref())
            .await
            .map_err(|e| RegistrationError::IdentityFailed(e.to_string()))?;
        agent.public_key = Some(identity.public_key.to_vec());

        // Phase 3: re-acquire write lock and insert
        let mut state = self.state.write().unwrap();
        agent.id = agent_id.clone();
        state.agents.insert(agent_id, agent);
        Ok(())
    }

    async fn register_manifest(&self, manifest: AgentManifest) -> Result<Agent, RegistrationError> {
        let agent_id = self.agent_id_internal(&manifest.name);

        // Check idempotency: does an agent with this name already exist?
        {
            let state = self.state.read().unwrap();
            if let Some(stored_manifest) = state.manifests.get(&manifest.name) {
                if *stored_manifest == manifest {
                    // Same manifest → idempotent, return existing agent
                    return Ok(state
                        .agents
                        .get(&agent_id)
                        .expect("agent must exist when manifest matches")
                        .clone());
                }
                return Err(RegistrationError::ConfigMismatch(manifest.name.clone()));
            }
        }

        // New agent: convert manifest → agent, validate via register_agent
        let agent = Agent::from_manifest(manifest.clone())
            .map_err(|e| RegistrationError::Persistence(format!("{e:?}")))?;
        self.register_agent(agent).await?;

        // Store manifest alongside the agent
        let mut state = self.state.write().unwrap();
        state.manifests.insert(manifest.name.clone(), manifest);
        Ok(state
            .agents
            .get(&agent_id)
            .expect("agent must exist after register_agent")
            .clone())
    }

    async fn get_agent(&self, id: &ResourceId) -> Option<Agent> {
        let state = self.state.read().unwrap();
        state.agents.get(id).cloned()
    }

    async fn get_agents(&self) -> Vec<Agent> {
        let state = self.state.read().unwrap();
        state.agents.values().cloned().collect()
    }

    fn select_runtime(&self, agent: &Agent) -> Option<RuntimeType> {
        let state = self.state.read().unwrap();
        self.select_runtime_internal(agent, &state)
    }

    // --- Model operations ---

    async fn register_model(&self, mut model: Model) -> Result<(), RegistrationError> {
        let model_id = self.model_id(&model.name);
        model.id = model_id.clone();
        let mut state = self.state.write().unwrap();

        // Validate that the required engine is available
        match model.model_type {
            ModelType::Inference => {
                if !state.available_inference_engines.contains(&model.provider) {
                    return Err(RegistrationError::InferenceEngineUnavailable(
                        model.provider,
                        model.name,
                    ));
                }
            }
            ModelType::Embedding => {
                if !state.available_embedding_engines.contains(&model.provider) {
                    return Err(RegistrationError::EmbeddingEngineUnavailable(
                        model.provider,
                        model.name,
                    ));
                }
            }
        }

        state.models.insert(model_id, model);
        Ok(())
    }

    fn get_model(&self, name: &str) -> Option<Model> {
        let model_id = self.model_id(name);
        let state = self.state.read().unwrap();
        state.models.get(&model_id).cloned()
    }

    fn get_models(&self) -> Vec<Model> {
        let state = self.state.read().unwrap();
        state.models.values().cloned().collect()
    }

    fn get_model_by_path(&self, path: &ResourceId) -> Option<Model> {
        let state = self.state.read().unwrap();
        state
            .models
            .values()
            .find(|m| &m.model_path == path)
            .cloned()
    }

    fn agent_id(&self, name: &str) -> Option<ResourceId> {
        let id = self.agent_id_internal(name);
        let state = self.state.read().unwrap();
        if state.agents.contains_key(&id) {
            Some(id)
        } else {
            None
        }
    }

    fn model_id(&self, name: &str) -> ResourceId {
        ResourceId::new(format!("{}/models/{}", self.registry_id.as_str(), name))
    }

    fn delete_model(&self, name: &str) -> Result<bool, RegistrationError> {
        let model_id = self.model_id(name);
        let mut state = self.state.write().unwrap();

        let Some(model) = state.models.get(&model_id) else {
            return Ok(false);
        };

        let dependent: Vec<String> = state
            .agents
            .values()
            .filter(|a| a.requirements.models.values().any(|n| n == &model.name))
            .map(|a| a.name.clone())
            .collect();

        if !dependent.is_empty() {
            return Err(RegistrationError::ModelInUse(name.to_string(), dependent));
        }

        state.models.remove(&model_id);
        Ok(true)
    }

    fn delete_agent(&self, name: &str) -> Result<bool, RegistrationError> {
        let agent_id = self.agent_id_internal(name);
        let mut state = self.state.write().unwrap();

        if !state.agents.contains_key(&agent_id) {
            return Ok(false);
        }

        // Check if any fleet references this agent
        let dependent: Vec<String> = state
            .fleets
            .values()
            .filter(|f| f.agents.contains(&agent_id))
            .map(|f| f.name.clone())
            .collect();

        if !dependent.is_empty() {
            return Err(RegistrationError::AgentInUse(name.to_string(), dependent));
        }

        state.agents.remove(&agent_id);
        state.manifests.remove(name);
        Ok(true)
    }

    // --- Fleet operations ---

    fn register_fleet(&self, mut fleet: Fleet) -> Result<(), RegistrationError> {
        let mut state = self.state.write().unwrap();

        // Idempotency: same fleet → Ok, different fleet same name → error
        if let Some(existing) = state.fleets.get(&fleet.name) {
            if existing.agents == fleet.agents && existing.entry == fleet.entry {
                return Ok(());
            }
            return Err(RegistrationError::FleetConfigMismatch(fleet.name.clone()));
        }

        // Validate all referenced agents are registered
        for agent_id in &fleet.agents {
            if !state.agents.contains_key(agent_id) {
                return Err(RegistrationError::FleetAgentNotRegistered(
                    fleet.name.clone(),
                    agent_id.as_str().to_string(),
                ));
            }
        }

        // Validate entry agent is in the agents set
        if !fleet.agents.contains(&fleet.entry) {
            return Err(RegistrationError::FleetAgentNotRegistered(
                fleet.name.clone(),
                fleet.entry.as_str().to_string(),
            ));
        }

        // Assign registry identity
        fleet.id = ResourceId::new(format!(
            "{}/fleets/{}",
            self.registry_id.as_str(),
            fleet.name
        ));
        state.fleets.insert(fleet.name.clone(), fleet);
        Ok(())
    }

    fn get_fleet(&self, name: &str) -> Option<Fleet> {
        let state = self.state.read().unwrap();
        state.fleets.get(name).cloned()
    }

    fn get_fleets(&self) -> Vec<Fleet> {
        let state = self.state.read().unwrap();
        state.fleets.values().cloned().collect()
    }

    // --- Job operations ---

    async fn create_job(
        &self,
        submission_id: SubmissionId,
        agent_id: ResourceId,
        input: String,
    ) -> JobId {
        let id = JobId::new(&self.registry_id);
        let job = Job {
            id: id.clone(),
            submission_id,
            agent_id,
            input,
            status: JobStatus::Pending,
        };
        let mut state = self.state.write().unwrap();
        state.jobs.insert(id.clone(), job);
        id
    }

    async fn get_job(&self, id: &JobId) -> Option<Job> {
        let state = self.state.read().unwrap();
        state.jobs.get(id).cloned()
    }

    async fn update_job_status(&self, id: &JobId, status: JobStatus) {
        let mut state = self.state.write().unwrap();
        if let Some(job) = state.jobs.get_mut(id) {
            job.status = status;
        }
    }

    fn pending_jobs(&self) -> Vec<Job> {
        let state = self.state.read().unwrap();
        state
            .jobs
            .values()
            .filter(|j| j.status == JobStatus::Pending)
            .cloned()
            .collect()
    }

    // --- Capability registration ---

    fn register_runtime(&self, runtime_type: RuntimeType) {
        let mut state = self.state.write().unwrap();
        state.available_runtimes.insert(runtime_type);
    }

    fn register_object_storage(&self, storage_type: ObjectStorageType) {
        let mut state = self.state.write().unwrap();
        state.available_object_storage.insert(storage_type);
    }

    fn register_vector_storage(&self, storage_type: VectorStorageType) {
        let mut state = self.state.write().unwrap();
        state.available_vector_storage.insert(storage_type);
    }

    fn register_inference_engine(&self, engine_type: Provider) {
        let mut state = self.state.write().unwrap();
        state.available_inference_engines.insert(engine_type);
    }

    fn register_embedding_engine(&self, engine_type: Provider) {
        let mut state = self.state.write().unwrap();
        state.available_embedding_engines.insert(engine_type);
    }

    // --- Capability queries ---

    fn has_object_storage(&self, storage_type: ObjectStorageType) -> bool {
        let state = self.state.read().unwrap();
        state.available_object_storage.contains(&storage_type)
    }

    fn has_vector_storage(&self, storage_type: VectorStorageType) -> bool {
        let state = self.state.read().unwrap();
        state.available_vector_storage.contains(&storage_type)
    }

    fn has_inference_engine(&self, engine_type: Provider) -> bool {
        let state = self.state.read().unwrap();
        state.available_inference_engines.contains(&engine_type)
    }

    fn has_embedding_engine(&self, engine_type: Provider) -> bool {
        let state = self.state.read().unwrap();
        state.available_embedding_engines.contains(&engine_type)
    }
}

#[cfg(test)]
mod tests {
    use super::super::secret_store::InMemorySecretStore;
    use super::*;

    fn test_secret_store() -> Arc<dyn SecretStore> {
        Arc::new(InMemorySecretStore::new())
    }

    fn test_agent_id() -> ResourceId {
        ResourceId::new("http://127.0.0.1:9000/agents/test-agent")
    }

    #[tokio::test]
    async fn registry_has_api_endpoint() {
        let registry = InMemoryRegistry::new(test_secret_store());

        // Registry exposes an HTTP API endpoint
        let id = registry.id();
        assert_eq!(id.scheme(), Some("http"));
        assert!(id.as_str().starts_with("http://127.0.0.1:"));
    }

    #[tokio::test]
    async fn job_id_includes_registry_id() {
        let registry = InMemoryRegistry::new(test_secret_store());
        let agent_id = test_agent_id();

        let job_id = registry
            .create_job(SubmissionId::new(), agent_id, "test".to_string())
            .await;

        // JobId format: <registry_id>/jobs/<uuid>
        assert!(job_id.as_str().starts_with(registry.id().as_str()));
        assert!(job_id.as_str().contains("/jobs/"));
    }

    #[tokio::test]
    async fn job_lifecycle() {
        let registry = InMemoryRegistry::new(test_secret_store());
        let agent_id = test_agent_id();

        // Create job
        let job_id = registry
            .create_job(SubmissionId::new(), agent_id.clone(), "hello".to_string())
            .await;

        // Initial state is Pending
        let job = registry.get_job(&job_id).await.unwrap();
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.agent_id, agent_id);
        assert_eq!(job.input, "hello");

        // Update to Running
        registry
            .update_job_status(&job_id, JobStatus::Running)
            .await;
        assert_eq!(
            registry.get_job(&job_id).await.unwrap().status,
            JobStatus::Running
        );

        // Update to Completed
        registry
            .update_job_status(&job_id, JobStatus::Completed("result".to_string()))
            .await;
        assert_eq!(
            registry.get_job(&job_id).await.unwrap().status,
            JobStatus::Completed("result".to_string())
        );
    }

    #[tokio::test]
    async fn pending_jobs_filters_by_status() {
        let registry = InMemoryRegistry::new(test_secret_store());
        let agent_id = test_agent_id();

        let job1 = registry
            .create_job(SubmissionId::new(), agent_id.clone(), "a".to_string())
            .await;
        let job2 = registry
            .create_job(SubmissionId::new(), agent_id.clone(), "b".to_string())
            .await;
        let _job3 = registry
            .create_job(SubmissionId::new(), agent_id.clone(), "c".to_string())
            .await;

        // All three are pending
        assert_eq!(registry.pending_jobs().len(), 3);

        // Mark one as running, one as completed
        registry.update_job_status(&job1, JobStatus::Running).await;
        registry
            .update_job_status(&job2, JobStatus::Completed("done".to_string()))
            .await;

        // Only one pending now
        assert_eq!(registry.pending_jobs().len(), 1);
    }

    // --- Object storage tests ---

    #[tokio::test]
    async fn register_object_storage_types() {
        let registry = InMemoryRegistry::new(test_secret_store());

        // Initially nothing available
        assert!(!registry.has_object_storage(ObjectStorageType::Sqlite));
        assert!(!registry.has_object_storage(ObjectStorageType::InMemory));

        // Register Sqlite
        registry.register_object_storage(ObjectStorageType::Sqlite);
        assert!(registry.has_object_storage(ObjectStorageType::Sqlite));
        assert!(!registry.has_object_storage(ObjectStorageType::InMemory));

        // Register InMemory
        registry.register_object_storage(ObjectStorageType::InMemory);
        assert!(registry.has_object_storage(ObjectStorageType::Sqlite));
        assert!(registry.has_object_storage(ObjectStorageType::InMemory));
    }

    // --- Vector storage tests ---

    #[tokio::test]
    async fn register_vector_storage_types() {
        let registry = InMemoryRegistry::new(test_secret_store());

        // Initially nothing available
        assert!(!registry.has_vector_storage(VectorStorageType::SqliteVec));
        assert!(!registry.has_vector_storage(VectorStorageType::InMemory));

        // Register SqliteVec
        registry.register_vector_storage(VectorStorageType::SqliteVec);
        assert!(registry.has_vector_storage(VectorStorageType::SqliteVec));
        assert!(!registry.has_vector_storage(VectorStorageType::InMemory));

        // Register InMemory
        registry.register_vector_storage(VectorStorageType::InMemory);
        assert!(registry.has_vector_storage(VectorStorageType::SqliteVec));
        assert!(registry.has_vector_storage(VectorStorageType::InMemory));
    }

    // --- Inference engine tests ---

    #[tokio::test]
    async fn register_inference_engine_types() {
        let registry = InMemoryRegistry::new(test_secret_store());

        // Initially nothing available
        assert!(!registry.has_inference_engine(Provider::Ollama));
        assert!(!registry.has_inference_engine(Provider::OpenRouter));

        // Register Ollama
        registry.register_inference_engine(Provider::Ollama);
        assert!(registry.has_inference_engine(Provider::Ollama));
        assert!(!registry.has_inference_engine(Provider::OpenRouter));

        // Register OpenRouter
        registry.register_inference_engine(Provider::OpenRouter);
        assert!(registry.has_inference_engine(Provider::Ollama));
        assert!(registry.has_inference_engine(Provider::OpenRouter));
    }

    // --- Embedding engine tests ---

    #[tokio::test]
    async fn register_embedding_engine_types() {
        let registry = InMemoryRegistry::new(test_secret_store());

        // Initially nothing available
        assert!(!registry.has_embedding_engine(Provider::Ollama));
        assert!(!registry.has_embedding_engine(Provider::OpenRouter));

        // Register Ollama
        registry.register_embedding_engine(Provider::Ollama);
        assert!(registry.has_embedding_engine(Provider::Ollama));
        assert!(!registry.has_embedding_engine(Provider::OpenRouter));

        // Register OpenRouter
        registry.register_embedding_engine(Provider::OpenRouter);
        assert!(registry.has_embedding_engine(Provider::Ollama));
        assert!(registry.has_embedding_engine(Provider::OpenRouter));
    }

    // --- restore_agent tests ---

    fn minimal_agent(name: &str) -> Agent {
        use super::super::{Requirements, RuntimeType};
        use std::collections::HashMap;

        Agent {
            id: Agent::placeholder_id(name),
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

    #[tokio::test]
    async fn restore_agent_bypasses_validation() {
        let registry = InMemoryRegistry::new(test_secret_store());
        // No runtimes, storage, or models registered — register_agent would fail
        let agent = minimal_agent("echo");

        // register_agent should fail (no container runtime registered)
        let result = registry.register_agent(agent.clone()).await;
        assert!(result.is_err());

        // restore_agent should succeed despite no capabilities
        registry.restore_agent(agent).unwrap();

        let agents = registry.get_agents().await;
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "echo");
        // ID should be reassigned by the registry
        assert!(agents[0].id.as_str().contains("/agents/echo"));
    }

    #[tokio::test]
    async fn restore_agent_rejects_duplicate() {
        let registry = InMemoryRegistry::new(test_secret_store());

        registry.restore_agent(minimal_agent("echo")).unwrap();

        let result = registry.restore_agent(minimal_agent("echo"));
        assert!(result.is_err());
    }

    // --- register_manifest tests (ADR 102) ---

    fn minimal_manifest(name: &str) -> AgentManifest {
        use super::super::agent_manifest::RequirementsConfig;

        AgentManifest {
            name: name.to_string(),
            description: format!("{name} agent"),
            source: None,
            runtime: "container".to_string(),
            executable: format!("localhost/{name}:latest"),
            requirements: RequirementsConfig {
                models: HashMap::new(),
                services: HashMap::new(),
                mounts: HashMap::new(),
            },
            prompts: None,
            object_storage: None,
            vector_storage: None,
        }
    }

    #[tokio::test]
    async fn register_manifest_returns_agent_with_registry_id() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let manifest = minimal_manifest("echo");
        let agent = registry.register_manifest(manifest).await.unwrap();

        assert_eq!(agent.name, "echo");
        assert!(agent.id.as_str().contains("/agents/echo"));
        assert!(agent.public_key.is_some());
    }

    #[tokio::test]
    async fn register_manifest_idempotent_same_manifest() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let manifest = minimal_manifest("echo");
        let agent1 = registry.register_manifest(manifest.clone()).await.unwrap();
        let agent2 = registry.register_manifest(manifest).await.unwrap();

        assert_eq!(agent1.id, agent2.id);
        assert_eq!(agent1.name, agent2.name);
    }

    #[tokio::test]
    async fn register_manifest_rejects_different_manifest_same_name() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let manifest1 = minimal_manifest("echo");
        registry.register_manifest(manifest1).await.unwrap();

        let mut manifest2 = minimal_manifest("echo");
        manifest2.description = "different description".to_string();

        let result = registry.register_manifest(manifest2).await;
        assert!(matches!(result, Err(RegistrationError::ConfigMismatch(_))));
    }

    // --- register_agent idempotency ---

    #[tokio::test]
    async fn register_agent_idempotent_same_config() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let agent = minimal_agent("echo");
        registry.register_agent(agent.clone()).await.unwrap();
        registry.register_agent(agent).await.unwrap(); // should succeed
    }

    #[tokio::test]
    async fn register_agent_rejects_different_config_same_name() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let agent1 = minimal_agent("echo");
        registry.register_agent(agent1).await.unwrap();

        let mut agent2 = minimal_agent("echo");
        agent2.executable = "localhost/echo:v2".to_string();

        let result = registry.register_agent(agent2).await;
        assert!(matches!(result, Err(RegistrationError::DuplicateName(_))));
    }

    // --- Fleet registration tests ---

    fn make_fleet(
        registry: &InMemoryRegistry,
        name: &str,
        entry: &str,
        agent_names: &[&str],
    ) -> Fleet {
        let entry_id = registry.agent_id(entry).unwrap();
        let agents: HashSet<ResourceId> = agent_names
            .iter()
            .map(|n| registry.agent_id(n).unwrap())
            .collect();
        Fleet {
            id: Fleet::placeholder_id(name),
            name: name.to_string(),
            entry: entry_id,
            agents,
        }
    }

    #[tokio::test]
    async fn get_fleets_empty_initially() {
        let registry = InMemoryRegistry::new(test_secret_store());
        assert!(registry.get_fleets().is_empty());
    }

    #[tokio::test]
    async fn get_fleet_returns_none_for_unknown() {
        let registry = InMemoryRegistry::new(test_secret_store());
        assert!(registry.get_fleet("nonexistent").is_none());
    }

    #[tokio::test]
    async fn register_fleet_with_valid_agents() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("alpha")).unwrap();
        registry.restore_agent(minimal_agent("beta")).unwrap();

        let fleet = make_fleet(&registry, "my-fleet", "alpha", &["alpha", "beta"]);
        registry.register_fleet(fleet).unwrap();

        let stored = registry.get_fleet("my-fleet").unwrap();
        assert_eq!(stored.name, "my-fleet");
        assert_eq!(stored.agents.len(), 2);
    }

    #[tokio::test]
    async fn register_fleet_assigns_registry_id() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("solo")).unwrap();

        let fleet = make_fleet(&registry, "test", "solo", &["solo"]);
        assert!(fleet.id.as_str().contains("pending-registration"));

        registry.register_fleet(fleet).unwrap();

        let stored = registry.get_fleet("test").unwrap();
        assert!(stored.id.as_str().contains("/fleets/test"));
        assert!(!stored.id.as_str().contains("pending-registration"));
    }

    #[tokio::test]
    async fn register_fleet_validates_agents_exist() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("real")).unwrap();

        // Build a fleet referencing both a real and a fake agent
        let real_id = registry.agent_id("real").unwrap();
        let fake_id = ResourceId::new("http://127.0.0.1:9000/agents/fake");
        let fleet = Fleet {
            id: Fleet::placeholder_id("bad-fleet"),
            name: "bad-fleet".to_string(),
            entry: real_id.clone(),
            agents: HashSet::from([real_id, fake_id]),
        };

        let result = registry.register_fleet(fleet);
        assert!(result.is_err());
        match result.unwrap_err() {
            RegistrationError::FleetAgentNotRegistered(fleet_name, agent_ref) => {
                assert_eq!(fleet_name, "bad-fleet");
                assert!(agent_ref.contains("fake"));
            }
            other => panic!("expected FleetAgentNotRegistered, got: {other}"),
        }
    }

    #[tokio::test]
    async fn register_fleet_validates_entry_in_agents() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("worker")).unwrap();
        registry.restore_agent(minimal_agent("entry-only")).unwrap();

        // Fleet has "worker" in agents but "entry-only" as entry — entry not in agents set
        let entry_id = registry.agent_id("entry-only").unwrap();
        let worker_id = registry.agent_id("worker").unwrap();
        let fleet = Fleet {
            id: Fleet::placeholder_id("bad-entry"),
            name: "bad-entry".to_string(),
            entry: entry_id,
            agents: HashSet::from([worker_id]),
        };

        let result = registry.register_fleet(fleet);
        assert!(result.is_err());
        match result.unwrap_err() {
            RegistrationError::FleetAgentNotRegistered(fleet_name, agent_ref) => {
                assert_eq!(fleet_name, "bad-entry");
                assert!(agent_ref.contains("entry-only"));
            }
            other => panic!("expected FleetAgentNotRegistered, got: {other}"),
        }
    }

    #[tokio::test]
    async fn register_fleet_idempotent_same_config() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("a")).unwrap();

        let fleet1 = make_fleet(&registry, "idempotent", "a", &["a"]);
        let fleet2 = make_fleet(&registry, "idempotent", "a", &["a"]);

        registry.register_fleet(fleet1).unwrap();
        registry.register_fleet(fleet2).unwrap(); // should succeed
    }

    #[tokio::test]
    async fn register_fleet_rejects_config_mismatch_different_entry() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("a")).unwrap();
        registry.restore_agent(minimal_agent("b")).unwrap();

        let fleet1 = make_fleet(&registry, "mismatch", "a", &["a", "b"]);
        registry.register_fleet(fleet1).unwrap();

        // Same name and agents, different entry
        let fleet2 = make_fleet(&registry, "mismatch", "b", &["a", "b"]);
        let result = registry.register_fleet(fleet2);
        assert!(matches!(
            result,
            Err(RegistrationError::FleetConfigMismatch(_))
        ));
    }

    #[tokio::test]
    async fn register_fleet_rejects_config_mismatch_different_agents() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("a")).unwrap();
        registry.restore_agent(minimal_agent("b")).unwrap();
        registry.restore_agent(minimal_agent("c")).unwrap();

        let fleet1 = make_fleet(&registry, "mismatch", "a", &["a", "b"]);
        registry.register_fleet(fleet1).unwrap();

        // Same name and entry, different agent set
        let fleet2 = make_fleet(&registry, "mismatch", "a", &["a", "c"]);
        let result = registry.register_fleet(fleet2);
        assert!(matches!(
            result,
            Err(RegistrationError::FleetConfigMismatch(_))
        ));
    }

    #[tokio::test]
    async fn register_fleet_single_agent() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("solo")).unwrap();

        let fleet = make_fleet(&registry, "solo-fleet", "solo", &["solo"]);
        registry.register_fleet(fleet).unwrap();

        let stored = registry.get_fleet("solo-fleet").unwrap();
        assert_eq!(stored.agents.len(), 1);
        assert_eq!(stored.entry, registry.agent_id("solo").unwrap());
    }

    #[tokio::test]
    async fn register_fleet_multiple_fleets() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("a")).unwrap();
        registry.restore_agent(minimal_agent("b")).unwrap();
        registry.restore_agent(minimal_agent("c")).unwrap();

        let fleet1 = make_fleet(&registry, "fleet-1", "a", &["a", "b"]);
        let fleet2 = make_fleet(&registry, "fleet-2", "b", &["b", "c"]);

        registry.register_fleet(fleet1).unwrap();
        registry.register_fleet(fleet2).unwrap();

        assert_eq!(registry.get_fleets().len(), 2);
        assert!(registry.get_fleet("fleet-1").is_some());
        assert!(registry.get_fleet("fleet-2").is_some());
    }

    #[tokio::test]
    async fn get_fleet_returns_correct_data() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("x")).unwrap();
        registry.restore_agent(minimal_agent("y")).unwrap();

        let fleet = make_fleet(&registry, "lookup-test", "x", &["x", "y"]);
        registry.register_fleet(fleet).unwrap();

        let stored = registry.get_fleet("lookup-test").unwrap();
        assert_eq!(stored.name, "lookup-test");
        assert_eq!(stored.entry, registry.agent_id("x").unwrap());
        assert!(stored.agents.contains(&registry.agent_id("x").unwrap()));
        assert!(stored.agents.contains(&registry.agent_id("y").unwrap()));
    }

    // --- delete_agent tests ---

    #[tokio::test]
    async fn delete_agent_succeeds() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("echo")).unwrap();
        assert_eq!(registry.get_agents().await.len(), 1);

        let deleted = registry.delete_agent("echo").unwrap();
        assert!(deleted);
        assert!(registry.get_agents().await.is_empty());
    }

    #[tokio::test]
    async fn delete_agent_nonexistent_returns_false() {
        let registry = InMemoryRegistry::new(test_secret_store());
        let deleted = registry.delete_agent("nope").unwrap();
        assert!(!deleted);
    }

    #[tokio::test]
    async fn delete_agent_blocked_by_fleet() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.restore_agent(minimal_agent("alpha")).unwrap();
        registry.restore_agent(minimal_agent("beta")).unwrap();

        let fleet = make_fleet(&registry, "my-fleet", "alpha", &["alpha", "beta"]);
        registry.register_fleet(fleet).unwrap();

        let result = registry.delete_agent("alpha");
        assert!(matches!(result, Err(RegistrationError::AgentInUse(_, _))));

        // Agent should still exist
        assert!(registry.get_agent_by_name("alpha").await.is_some());
    }

    #[tokio::test]
    async fn delete_agent_cleans_up_manifest() {
        let registry = InMemoryRegistry::new(test_secret_store());
        registry.register_runtime(RuntimeType::Container);

        let manifest = minimal_manifest("echo");
        registry.register_manifest(manifest).await.unwrap();
        assert!(registry.get_agent_by_name("echo").await.is_some());

        let deleted = registry.delete_agent("echo").unwrap();
        assert!(deleted);
        assert!(registry.get_agent_by_name("echo").await.is_none());

        // Re-registering should work (manifest was cleaned up)
        let manifest2 = minimal_manifest("echo");
        registry.register_manifest(manifest2).await.unwrap();
        assert!(registry.get_agent_by_name("echo").await.is_some());
    }
}
