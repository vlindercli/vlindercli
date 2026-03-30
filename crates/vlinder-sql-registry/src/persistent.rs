//! `PersistentRegistry` — write-through registry with pluggable persistence.
//!
//! Composes `InMemoryRegistry` (fast reads) with any `RegistryRepository`
//! (durable writes). The repository is the source of truth; in-memory is a cache.
//!
//! On construction, loads all models and agents from the repository (fail-fast).
//! On `register_model()`/`register_agent()`, writes to disk first, then updates cache.

use std::sync::Arc;

use crate::RegistryConfig;
use vlinder_core::domain::InMemoryRegistry;
use vlinder_core::domain::{
    Agent, Fleet, Job, JobId, JobStatus, Model, ObjectStorageType, Provider, RegistrationError,
    Registry, RegistryRepository, ResourceId, RuntimeType, SecretStore, SubmissionId,
    VectorStorageType,
};

/// Registry with write-through persistence.
///
/// The `RegistryRepository` is the source of truth. `InMemoryRegistry` is the read cache.
/// All writes go to the repository first (fail-fast), then update the cache.
pub struct PersistentRegistry {
    inner: InMemoryRegistry,
    repo: Arc<dyn RegistryRepository>,
}

impl PersistentRegistry {
    /// Create a persistent registry backed by the given repository.
    ///
    /// Registers engine capabilities from config first, then loads all
    /// existing models from the repository (validating each against available engines).
    /// Fails fast with a clear error on any failure.
    pub fn new(
        repo: Arc<dyn RegistryRepository>,
        config: &RegistryConfig,
        secret_store: Arc<dyn SecretStore>,
    ) -> Result<Self, RegistrationError> {
        let inner = InMemoryRegistry::new(secret_store);

        // Register engine capabilities BEFORE loading models so validation works
        for engine in &config.inference_engines {
            inner.register_inference_engine(*engine);
        }
        for engine in &config.embedding_engines {
            inner.register_embedding_engine(*engine);
        }

        // Load persisted models (each validated against registered engines)
        let models = repo
            .load_models()
            .map_err(|e| RegistrationError::Persistence(format!("failed to load models: {e}")))?;

        for model in models {
            inner.register_model(model)?;
        }

        // Load persisted agents (bypasses validation — capabilities not yet registered)
        let agents = repo
            .load_agents()
            .map_err(|e| RegistrationError::Persistence(format!("failed to load agents: {e}")))?;

        for agent in agents {
            inner.restore_agent(agent)?;
        }

        Ok(Self { inner, repo })
    }
}

// ============================================================================
// Registry trait delegation
// ============================================================================

impl Registry for PersistentRegistry {
    fn id(&self) -> ResourceId {
        self.inner.id()
    }

    // --- Agent operations (write-through for mutations) ---

    fn register_agent(&self, agent: Agent) -> Result<(), RegistrationError> {
        // Validate in-memory first (runtime, storage, model checks), then persist
        self.inner.register_agent(agent.clone())?;

        // Write to disk (agent already validated and cached)
        self.repo
            .save_agent(&agent)
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        Ok(())
    }

    fn agent_id(&self, name: &str) -> Option<ResourceId> {
        self.inner.agent_id(name)
    }

    fn get_agent(&self, id: &ResourceId) -> Option<Agent> {
        self.inner.get_agent(id)
    }

    fn get_agents(&self) -> Vec<Agent> {
        self.inner.get_agents()
    }

    fn select_runtime(&self, agent: &Agent) -> Option<RuntimeType> {
        self.inner.select_runtime(agent)
    }

    // --- Model operations (write-through for mutations) ---

    fn register_model(&self, model: Model) -> Result<(), RegistrationError> {
        // Validate in-memory first (engine availability check), then persist
        self.inner.register_model(model.clone())?;

        // Write to disk (model already validated and cached)
        self.repo
            .save_model(&model)
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        Ok(())
    }

    fn get_model(&self, name: &str) -> Option<Model> {
        self.inner.get_model(name)
    }

    fn get_models(&self) -> Vec<Model> {
        self.inner.get_models()
    }

    fn get_model_by_path(&self, path: &ResourceId) -> Option<Model> {
        self.inner.get_model_by_path(path)
    }

    fn model_id(&self, name: &str) -> ResourceId {
        self.inner.model_id(name)
    }

    fn delete_model(&self, name: &str) -> Result<bool, RegistrationError> {
        // Check if model exists
        let Some(model) = self.inner.get_model(name) else {
            return Ok(false);
        };

        // Check for dependent agents before deleting
        let dependent: Vec<String> = self
            .inner
            .get_agents_requiring_model(&model.name)
            .into_iter()
            .map(|a| a.name)
            .collect();

        if !dependent.is_empty() {
            return Err(RegistrationError::ModelInUse(name.to_string(), dependent));
        }

        // Disk first, then cache
        let deleted = self
            .repo
            .delete_model(name)
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        if deleted {
            self.inner.remove_model(name);
        }

        Ok(deleted)
    }

    fn delete_agent(&self, name: &str) -> Result<bool, RegistrationError> {
        // Check if agent exists
        let Some(_agent) = self.inner.get_agent_by_name(name) else {
            return Ok(false);
        };

        // Check for fleet dependencies before deleting
        let agent_id = self
            .inner
            .agent_id(name)
            .expect("agent_id must exist when get_agent_by_name succeeds");
        let dependent: Vec<String> = self
            .inner
            .get_fleets()
            .into_iter()
            .filter(|f| f.agents.contains(&agent_id))
            .map(|f| f.name)
            .collect();

        if !dependent.is_empty() {
            return Err(RegistrationError::AgentInUse(name.to_string(), dependent));
        }

        // Disk first, then cache
        let deleted = self
            .repo
            .delete_agent(name)
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        if deleted {
            self.inner.remove_agent(name);
        }

        Ok(deleted)
    }

    // --- Fleet operations (delegate to in-memory, persistence deferred) ---

    fn register_fleet(&self, fleet: Fleet) -> Result<(), RegistrationError> {
        self.inner.register_fleet(fleet)
    }

    fn get_fleet(&self, name: &str) -> Option<Fleet> {
        self.inner.get_fleet(name)
    }

    fn get_fleets(&self) -> Vec<Fleet> {
        self.inner.get_fleets()
    }

    // --- Job operations (delegate directly) ---

    fn create_job(
        &self,
        submission_id: SubmissionId,
        agent_id: ResourceId,
        input: String,
    ) -> JobId {
        self.inner.create_job(submission_id, agent_id, input)
    }

    fn get_job(&self, id: &JobId) -> Option<Job> {
        self.inner.get_job(id)
    }

    fn update_job_status(&self, id: &JobId, status: JobStatus) {
        self.inner.update_job_status(id, status);
    }

    fn pending_jobs(&self) -> Vec<Job> {
        self.inner.pending_jobs()
    }

    // --- Capability registration (delegate directly) ---

    fn register_runtime(&self, runtime_type: RuntimeType) {
        self.inner.register_runtime(runtime_type);
    }

    fn register_object_storage(&self, storage_type: ObjectStorageType) {
        self.inner.register_object_storage(storage_type);
    }

    fn register_vector_storage(&self, storage_type: VectorStorageType) {
        self.inner.register_vector_storage(storage_type);
    }

    fn register_inference_engine(&self, engine_type: Provider) {
        self.inner.register_inference_engine(engine_type);
    }

    fn register_embedding_engine(&self, engine_type: Provider) {
        self.inner.register_embedding_engine(engine_type);
    }

    // --- Capability queries (delegate directly) ---

    fn has_object_storage(&self, storage_type: ObjectStorageType) -> bool {
        self.inner.has_object_storage(storage_type)
    }

    fn has_vector_storage(&self, storage_type: VectorStorageType) -> bool {
        self.inner.has_vector_storage(storage_type)
    }

    fn has_inference_engine(&self, engine_type: Provider) -> bool {
        self.inner.has_inference_engine(engine_type)
    }

    fn has_embedding_engine(&self, engine_type: Provider) -> bool {
        self.inner.has_embedding_engine(engine_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use vlinder_core::domain::InMemorySecretStore;
    use vlinder_core::domain::{ModelType, RegistryRepository, Requirements, RuntimeType};
    use vlinder_sql_state::SqliteDagStore;

    fn test_secret_store() -> Arc<dyn SecretStore> {
        Arc::new(InMemorySecretStore::new())
    }

    fn test_config() -> RegistryConfig {
        RegistryConfig {
            inference_engines: vec![Provider::Ollama],
            embedding_engines: vec![Provider::Ollama],
        }
    }

    fn open_repo(db_path: &std::path::Path) -> Arc<SqliteDagStore> {
        Arc::new(SqliteDagStore::open(db_path).unwrap())
    }

    fn open_registry(db_path: &std::path::Path) -> PersistentRegistry {
        let repo = open_repo(db_path);
        PersistentRegistry::new(repo, &test_config(), test_secret_store()).unwrap()
    }

    fn test_model(name: &str) -> Model {
        Model {
            id: Model::placeholder_id(name),
            name: name.to_string(),
            model_type: ModelType::Inference,
            provider: Provider::Ollama,
            model_path: ResourceId::new(format!("ollama://localhost:11434/{name}")),
            digest: String::new(),
        }
    }

    #[test]
    fn open_creates_db_if_not_exists() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        let registry = open_registry(&db_path);
        assert!(registry.get_models().is_empty());
    }

    #[test]
    fn loads_existing_models_on_open() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        // Pre-populate via SqliteDagStore
        {
            let repo = open_repo(&db_path);
            repo.save_model(&test_model("llama3")).unwrap();
        }

        let registry = open_registry(&db_path);
        assert!(registry.get_model("llama3").is_some());
    }

    #[test]
    fn register_model_persists_to_disk() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_registry(&db_path);
        registry.register_model(test_model("phi3")).unwrap();

        // Verify in-memory
        assert!(registry.get_model("phi3").is_some());

        // Verify on disk: open a fresh store and check
        let repo = open_repo(&db_path);
        let models = repo.load_models().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].name, "phi3");
    }

    #[test]
    fn delete_model_removes_from_both() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_registry(&db_path);
        registry.register_model(test_model("phi3")).unwrap();
        assert!(registry.get_model("phi3").is_some());

        let deleted = registry.delete_model("phi3").unwrap();
        assert!(deleted);

        // Gone from in-memory
        assert!(registry.get_model("phi3").is_none());

        // Gone from disk
        let repo = open_repo(&db_path);
        assert!(!repo.model_exists("phi3").unwrap());
    }

    #[test]
    fn delete_nonexistent_model_returns_false() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_registry(&db_path);
        let deleted = registry.delete_model("nope").unwrap();
        assert!(!deleted);
    }

    #[test]
    fn open_fails_fast_on_corrupt_db() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");
        std::fs::write(&db_path, b"not a database").unwrap();

        let result = SqliteDagStore::open(&db_path);
        assert!(result.is_err());
    }

    #[test]
    fn survives_restart() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        // First "session": add a model
        {
            let registry = open_registry(&db_path);
            registry.register_model(test_model("llama3")).unwrap();
        }

        // Second "session": model should be there
        {
            let registry = open_registry(&db_path);
            let model = registry.get_model("llama3");
            assert!(model.is_some(), "model should survive restart");
            assert_eq!(model.unwrap().name, "llama3");
        }
    }

    // --- Agent tests ---

    fn test_agent(name: &str) -> Agent {
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

    /// Open a `PersistentRegistry` with container runtime pre-registered.
    fn open_with_runtime(db_path: &std::path::Path) -> PersistentRegistry {
        let registry = open_registry(db_path);
        registry.register_runtime(RuntimeType::Container);
        registry
    }

    #[test]
    fn register_agent_persists_to_disk() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_with_runtime(&db_path);
        registry.register_agent(test_agent("echo")).unwrap();

        // Verify in-memory
        let agent = registry.get_agent_by_name("echo");
        assert!(agent.is_some());

        // Verify on disk: open a fresh store and check
        let repo = open_repo(&db_path);
        let agents = repo.load_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "echo");
    }

    #[test]
    fn agents_survive_restart() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        // First "session": register an agent
        {
            let registry = open_with_runtime(&db_path);
            registry.register_agent(test_agent("echo")).unwrap();
        }

        // Second "session": agent should be loaded via restore_agent
        {
            let registry = open_registry(&db_path);
            let agent = registry.get_agent_by_name("echo");
            assert!(agent.is_some(), "agent should survive restart");
            assert_eq!(agent.unwrap().name, "echo");
        }
    }

    // --- delete_agent tests ---

    #[test]
    fn delete_agent_removes_from_both() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_with_runtime(&db_path);
        registry.register_agent(test_agent("echo")).unwrap();
        assert!(registry.get_agent_by_name("echo").is_some());

        let deleted = registry.delete_agent("echo").unwrap();
        assert!(deleted);

        // Gone from in-memory
        assert!(registry.get_agent_by_name("echo").is_none());

        // Gone from disk
        let repo = open_repo(&db_path);
        assert!(!repo.agent_exists("echo").unwrap());
    }

    #[test]
    fn delete_nonexistent_agent_returns_false() {
        let temp = tempfile::TempDir::new().unwrap();
        let db_path = temp.path().join("state.db");

        let registry = open_with_runtime(&db_path);
        let deleted = registry.delete_agent("nope").unwrap();
        assert!(!deleted);
    }
}
