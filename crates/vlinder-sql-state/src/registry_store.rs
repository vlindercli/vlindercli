//! `RegistryRepository` implementation for `SqliteDagStore`.
//!
//! Agents and models live in the same `SQLite` database as the DAG,
//! enabling foreign key integrity across all three operational planes.

use diesel::prelude::*;

use crate::dag_store::SqliteDagStore;
use crate::models::{AgentRow, ModelRow, NewAgent, NewModel};
use crate::schema::{agents, models};
use vlinder_core::domain::{
    Agent, Model, RegistryRepository, RepositoryError, StoredAgent, StoredModel,
};

impl RegistryRepository for SqliteDagStore {
    fn save_model(&self, model: &Model) -> Result<(), RepositoryError> {
        let stored = StoredModel::from_model(model);

        let model_type = match stored.model_type {
            vlinder_core::domain::ModelType::Inference => "inference",
            vlinder_core::domain::ModelType::Embedding => "embedding",
        };
        let provider = match stored.provider {
            vlinder_core::domain::Provider::Ollama => "ollama",
            vlinder_core::domain::Provider::OpenRouter => "openrouter",
        };

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        diesel::replace_into(models::table)
            .values(&NewModel {
                name: &stored.name,
                model_type,
                provider,
                model_path: &stored.model_path,
                digest: &stored.digest,
            })
            .execute(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    fn load_models(&self) -> Result<Vec<Model>, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<ModelRow> = models::table
            .select(ModelRow::as_select())
            .load(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        rows.into_iter()
            .map(|r| {
                let model_type: vlinder_core::domain::ModelType =
                    serde_json::from_value(serde_json::Value::String(r.model_type))
                        .map_err(|e| RepositoryError::Serialization(e.to_string()))?;
                let provider: vlinder_core::domain::Provider =
                    serde_json::from_value(serde_json::Value::String(r.provider))
                        .map_err(|e| RepositoryError::Serialization(e.to_string()))?;

                Ok(StoredModel {
                    name: r.name,
                    model_type,
                    provider,
                    model_path: r.model_path,
                    digest: r.digest,
                }
                .to_model())
            })
            .collect()
    }

    fn delete_model(&self, name: &str) -> Result<bool, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let affected = diesel::delete(models::table.filter(models::name.eq(name)))
            .execute(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(affected > 0)
    }

    fn model_exists(&self, name: &str) -> Result<bool, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let count: i64 = models::table
            .filter(models::name.eq(name))
            .count()
            .get_result(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(count > 0)
    }

    fn save_agent(&self, agent: &Agent) -> Result<(), RepositoryError> {
        let stored = StoredAgent::from_agent(agent)?;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        diesel::replace_into(agents::table)
            .values(&NewAgent {
                name: &stored.name,
                description: &stored.description,
                source: stored.source.as_deref(),
                runtime: &stored.runtime,
                executable: &stored.executable,
                image_digest: stored.image_digest.as_deref(),
                object_storage: stored.object_storage.as_deref(),
                vector_storage: stored.vector_storage.as_deref(),
                requirements_json: &stored.requirements_json,
                prompts_json: stored.prompts_json.as_deref(),
                public_key: stored.public_key.as_deref(),
            })
            .execute(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    fn load_agents(&self) -> Result<Vec<Agent>, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<AgentRow> = agents::table
            .select(AgentRow::as_select())
            .load(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        rows.into_iter()
            .map(|r| {
                StoredAgent {
                    name: r.name,
                    description: r.description,
                    source: r.source,
                    runtime: r.runtime,
                    executable: r.executable,
                    image_digest: r.image_digest,
                    object_storage: r.object_storage,
                    vector_storage: r.vector_storage,
                    requirements_json: r.requirements_json,
                    prompts_json: r.prompts_json,
                    public_key: r.public_key,
                }
                .to_agent()
            })
            .collect()
    }

    fn delete_agent(&self, name: &str) -> Result<bool, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let affected = diesel::delete(agents::table.filter(agents::name.eq(name)))
            .execute(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(affected > 0)
    }

    fn agent_exists(&self, name: &str) -> Result<bool, RepositoryError> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let count: i64 = agents::table
            .filter(agents::name.eq(name))
            .count()
            .get_result(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(count > 0)
    }

    fn append_agent_state(
        &self,
        state: &vlinder_core::domain::AgentState,
    ) -> Result<(), RepositoryError> {
        use crate::models::NewAgentState;
        use crate::schema::agent_states;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let updated_at_str = state.updated_at.to_rfc3339();

        diesel::insert_into(agent_states::table)
            .values(&NewAgentState {
                agent_name: state.agent.as_str(),
                state: state.status.as_str(),
                updated_at: &updated_at_str,
                error: state.error.as_deref(),
            })
            .execute(&mut *conn)
            .map_err(|e| RepositoryError::Database(e.to_string()))?;
        Ok(())
    }

    fn get_agent_state(
        &self,
        name: &str,
    ) -> Result<Option<vlinder_core::domain::AgentState>, RepositoryError> {
        use crate::models::AgentStateRow;
        use crate::schema::agent_states;
        use std::str::FromStr;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<AgentStateRow> = agent_states::table
            .filter(agent_states::agent_name.eq(name))
            .order(agent_states::updated_at.desc())
            .select(AgentStateRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| RepositoryError::Database(e.to_string()))?;

        match row {
            None => Ok(None),
            Some(r) => {
                let status = vlinder_core::domain::AgentStatus::from_str(&r.state)
                    .map_err(RepositoryError::Serialization)?;
                let updated_at = chrono::DateTime::parse_from_rfc3339(&r.updated_at)
                    .map_err(|e| RepositoryError::Serialization(e.to_string()))?
                    .with_timezone(&chrono::Utc);
                Ok(Some(vlinder_core::domain::AgentState {
                    agent: vlinder_core::domain::AgentName::new(r.agent_name),
                    status,
                    updated_at,
                    error: r.error,
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use vlinder_core::domain::{
        ImageDigest, ModelType, Prompts, Protocol, Provider, Requirements, ResourceId, RuntimeType,
        ServiceConfig, ServiceType,
    };

    fn test_store() -> SqliteDagStore {
        SqliteDagStore::open(std::path::Path::new(":memory:")).unwrap()
    }

    fn test_model(name: &str) -> Model {
        Model {
            id: Model::placeholder_id(name),
            name: name.to_string(),
            model_type: ModelType::Inference,
            provider: Provider::Ollama,
            model_path: ResourceId::new(format!("ollama://localhost:11434/{name}")),
            digest: format!("sha256:test-digest-{name}"),
        }
    }

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

    // --- Model tests ---

    #[test]
    fn save_and_load_model() {
        let store = test_store();
        let model = test_model("llama3");
        store.save_model(&model).unwrap();

        let models = store.load_models().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].name, "llama3");
        assert_eq!(models[0].provider, Provider::Ollama);
    }

    #[test]
    fn delete_model() {
        let store = test_store();
        store.save_model(&test_model("llama3")).unwrap();
        assert!(store.model_exists("llama3").unwrap());

        let deleted = store.delete_model("llama3").unwrap();
        assert!(deleted);
        assert!(!store.model_exists("llama3").unwrap());
    }

    #[test]
    fn model_upsert_replaces() {
        let store = test_store();
        let mut model = test_model("llama3");
        store.save_model(&model).unwrap();

        model.provider = Provider::OpenRouter;
        store.save_model(&model).unwrap();

        let models = store.load_models().unwrap();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].provider, Provider::OpenRouter);
    }

    // --- Agent tests ---

    #[test]
    fn save_and_load_agent() {
        let store = test_store();
        let agent = test_agent("echo");
        store.save_agent(&agent).unwrap();

        let agents = store.load_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "echo");
        assert_eq!(agents[0].runtime, RuntimeType::Container);
    }

    #[test]
    fn delete_agent() {
        let store = test_store();
        store.save_agent(&test_agent("echo")).unwrap();
        assert!(store.agent_exists("echo").unwrap());

        let deleted = store.delete_agent("echo").unwrap();
        assert!(deleted);
        assert!(!store.agent_exists("echo").unwrap());
    }

    #[test]
    fn agent_upsert_replaces() {
        let store = test_store();
        let mut agent = test_agent("echo");
        store.save_agent(&agent).unwrap();

        agent.description = "Updated description".to_string();
        store.save_agent(&agent).unwrap();

        let agents = store.load_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].description, "Updated description");
    }

    #[test]
    fn full_agent_round_trip() {
        let store = test_store();

        let mut models = HashMap::new();
        models.insert("phi3".to_string(), "phi3:latest".to_string());

        let mut services = HashMap::new();
        services.insert(
            ServiceType::Infer,
            ServiceConfig {
                provider: Provider::Ollama,
                protocol: Protocol::OpenAi,
                models: vec!["phi3:latest".to_string()],
            },
        );

        let agent = Agent {
            id: Agent::placeholder_id("full"),
            name: "full".to_string(),
            description: "Full agent".to_string(),
            source: Some("https://example.com".to_string()),
            runtime: RuntimeType::Container,
            executable: "localhost/full:latest".to_string(),
            image_digest: Some(ImageDigest::parse("sha256:abc123").unwrap()),
            public_key: Some(vec![1, 2, 3, 4]),
            object_storage: Some(ResourceId::new("sqlite:///data/objects.db")),
            vector_storage: Some(ResourceId::new("sqlite:///data/vectors.db")),
            requirements: Requirements {
                models,
                services,
                mounts: HashMap::new(),
            },
            prompts: Some(Prompts {
                intent_recognition: Some("Classify".to_string()),
                query_expansion: None,
                answer_generation: None,
                map_summarize: None,
                reduce_summaries: None,
                direct_summarize: None,
            }),
        };

        store.save_agent(&agent).unwrap();
        let agents = store.load_agents().unwrap();
        assert_eq!(agents.len(), 1);

        let restored = &agents[0];
        assert_eq!(restored.name, "full");
        assert_eq!(restored.source.as_deref(), Some("https://example.com"));
        assert_eq!(
            restored.image_digest.as_ref().map(ImageDigest::as_str),
            Some("sha256:abc123")
        );
        assert_eq!(restored.public_key.as_ref().unwrap(), &[1, 2, 3, 4]);
        assert_eq!(restored.requirements.models.len(), 1);
        assert_eq!(restored.requirements.services.len(), 1);
        assert!(restored.prompts.is_some());
    }

    // --- Agent state tests ---

    #[test]
    fn agent_state_round_trip() {
        let store = test_store();
        store.save_agent(&test_agent("echo")).unwrap();

        let state = vlinder_core::domain::AgentState::registered(
            vlinder_core::domain::AgentName::new("echo"),
        );
        store.append_agent_state(&state).unwrap();

        let loaded = store.get_agent_state("echo").unwrap().unwrap();
        assert_eq!(loaded.agent, state.agent);
        assert_eq!(loaded.status, vlinder_core::domain::AgentStatus::Registered);
        assert!(loaded.error.is_none());
    }

    #[test]
    fn agent_state_transition() {
        let store = test_store();
        store.save_agent(&test_agent("echo")).unwrap();

        let initial = vlinder_core::domain::AgentState::registered(
            vlinder_core::domain::AgentName::new("echo"),
        );
        store.append_agent_state(&initial).unwrap();

        let live = initial.transition(vlinder_core::domain::AgentStatus::Live, None);
        store.append_agent_state(&live).unwrap();

        let loaded = store.get_agent_state("echo").unwrap().unwrap();
        assert_eq!(loaded.status, vlinder_core::domain::AgentStatus::Live);
        assert!(loaded.error.is_none());
    }

    #[test]
    fn agent_state_failed_with_error() {
        let store = test_store();
        store.save_agent(&test_agent("echo")).unwrap();

        let initial = vlinder_core::domain::AgentState::registered(
            vlinder_core::domain::AgentName::new("echo"),
        );
        let failed = initial.transition(
            vlinder_core::domain::AgentStatus::Failed,
            Some("image not found".to_string()),
        );
        store.append_agent_state(&failed).unwrap();

        let loaded = store.get_agent_state("echo").unwrap().unwrap();
        assert_eq!(loaded.status, vlinder_core::domain::AgentStatus::Failed);
        assert_eq!(loaded.error.as_deref(), Some("image not found"));
    }

    #[test]
    fn agent_state_returns_none_for_unknown() {
        let store = test_store();
        let loaded = store.get_agent_state("nonexistent").unwrap();
        assert!(loaded.is_none());
    }
}
