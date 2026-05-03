use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use serde::Serialize;

use super::agent_manifest::{
    AgentManifest, McpProviderConfig, MountConfig, ParseError, PromptsConfig, RequirementsConfig,
    ServiceConfig,
};
use super::image_digest::ImageDigest;
use super::resource_id::ResourceId;
use super::runtime::RuntimeType;
use super::service_type::ServiceType;

/// An agent with resolved paths, ready for execution.
///
/// All paths (executable, model URIs) are resolved to absolute paths at load time.
/// See ADR 020 for the manifest format, ADR 048 for identity model.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Agent {
    pub name: String,
    pub description: String,
    pub source: Option<String>,
    pub requirements: Requirements,
    pub prompts: Option<Prompts>,
    /// Registry-assigned identity: `<registry_id>/agents/<name>`.
    /// Set by the registry during registration.
    pub id: ResourceId,
    /// Which runtime executes this agent.
    pub runtime: RuntimeType,
    /// Executable reference — native to the runtime ecosystem.
    /// Container: OCI image ref (e.g., "localhost/echo-container:latest")
    /// File: absolute path or file:// URI
    pub executable: String,
    /// Content-addressed image digest resolved at registration time (ADR 073).
    /// For container agents: `podman image inspect` → `sha256:...`
    /// None for non-container runtimes or if resolution failed.
    pub image_digest: Option<ImageDigest>,
    /// Ed25519 public key for this agent's identity (ADR 084).
    /// Set by `ensure_agent_identity()` before registration.
    /// None until identity is provisioned.
    pub public_key: Option<Vec<u8>>,
    /// Object storage configuration (optional).
    pub object_storage: Option<ResourceId>,
    /// Vector storage configuration (optional).
    pub vector_storage: Option<ResourceId>,
}

impl Agent {
    /// Create a placeholder ID for agents not yet registered.
    ///
    /// Registry replaces this with `<registry_id>/agents/<name>` during registration.
    pub fn placeholder_id(name: &str) -> ResourceId {
        ResourceId::new(format!("pending-registration://agents/{name}"))
    }

    /// Create an agent directly from TOML content.
    ///
    /// The TOML should contain resolved absolute URIs for `executable` and model paths.
    /// No path resolution is performed - caller is responsible for pre-resolving.
    pub fn from_toml(toml_content: &str) -> Result<Agent, LoadError> {
        let manifest: AgentManifest =
            toml::from_str(toml_content).map_err(|e| LoadError::Parse(e.to_string()))?;
        Self::from_manifest(manifest)
    }

    /// Create an agent from a manifest.
    ///
    /// All paths in the manifest are already resolved to absolute paths.
    /// The `id` field is set to a placeholder. The registry assigns the real
    /// id (`<registry_id>/agents/<name>`) during registration.
    pub fn from_manifest(manifest: AgentManifest) -> Result<Agent, LoadError> {
        let runtime = RuntimeType::from_str(&manifest.runtime)
            .map_err(|_| LoadError::Parse(format!("unknown runtime: {}", manifest.runtime)))?;

        Ok(Agent {
            name: manifest.name.clone(),
            description: manifest.description,
            source: manifest.source,
            requirements: Requirements::from_config(manifest.requirements),
            prompts: manifest.prompts.map(std::convert::Into::into),
            id: Self::placeholder_id(&manifest.name),
            runtime,
            executable: manifest.executable,
            image_digest: None,
            public_key: None,
            object_storage: manifest.object_storage,
            vector_storage: manifest.vector_storage,
        })
    }

    /// Convenience: load agent from a directory path.
    ///
    /// Looks for `agent.toml` manifest in the directory (ADR 020).
    pub fn load(path: &Path) -> Result<Agent, LoadError> {
        let manifest_path = path.join("agent.toml");

        if !manifest_path.exists() {
            return Err(LoadError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("manifest not found: {}", manifest_path.display()),
            )));
        }

        let manifest = AgentManifest::load(&manifest_path)?;
        Self::from_manifest(manifest)
    }

    /// Return a borrowed view of the user-declared configuration.
    ///
    /// Excludes registry-assigned fields (id, `image_digest`, `public_key`)
    /// so two agents with the same declared config compare equal regardless
    /// of registration state.
    pub fn config(&self) -> AgentConfig<'_> {
        AgentConfig {
            name: &self.name,
            description: &self.description,
            runtime: &self.runtime,
            executable: &self.executable,
            requirements: &self.requirements,
            prompts: &self.prompts,
            object_storage: &self.object_storage,
            vector_storage: &self.vector_storage,
        }
    }

    /// Check if this agent declares a model by alias.
    pub fn has_model(&self, alias: &str) -> bool {
        self.requirements.models.contains_key(alias)
    }

    /// Get the registry name for a model alias (ADR 094).
    pub fn model_name(&self, alias: &str) -> Option<&str> {
        self.requirements
            .models
            .get(alias)
            .map(std::string::String::as_str)
    }
}

/// Borrowed view of user-declared agent configuration.
///
/// Used for idempotency checks: two agents with identical configs are the
/// same deployment, even if registry-assigned fields differ.
#[derive(Debug, PartialEq)]
pub struct AgentConfig<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub runtime: &'a RuntimeType,
    pub executable: &'a str,
    pub requirements: &'a Requirements,
    pub prompts: &'a Option<Prompts>,
    pub object_storage: &'a Option<ResourceId>,
    pub vector_storage: &'a Option<ResourceId>,
}

#[derive(Debug)]
pub enum LoadError {
    Io(std::io::Error),
    Parse(String),
}

impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        LoadError::Io(e)
    }
}

impl From<ParseError> for LoadError {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::Io(e) => LoadError::Io(e),
            ParseError::Toml(s) | ParseError::ExecutableNotFound(s) => LoadError::Parse(s),
        }
    }
}

/// Agent runtime requirements (validated)
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Requirements {
    /// Model alias → registry name (ADR 094).
    /// e.g., `inference_model` → "claude-sonnet"
    pub models: HashMap<String, String>,
    pub services: HashMap<ServiceType, ServiceConfig>,
    /// S3 mount declarations (ADR 107).
    pub mounts: HashMap<String, MountConfig>,
    /// MCP provider declarations.
    pub mcp: HashMap<String, McpProviderConfig>,
}

impl Requirements {
    /// Create Requirements from config.
    fn from_config(config: RequirementsConfig) -> Self {
        Requirements {
            models: config.models,
            services: config.services,
            mounts: config.mounts,
            mcp: config.mcp,
        }
    }
}

/// Prompt overrides (validated)
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Prompts {
    pub intent_recognition: Option<String>,
    pub query_expansion: Option<String>,
    pub answer_generation: Option<String>,
    pub map_summarize: Option<String>,
    pub reduce_summaries: Option<String>,
    pub direct_summarize: Option<String>,
}

impl From<PromptsConfig> for Prompts {
    fn from(config: PromptsConfig) -> Self {
        Prompts {
            intent_recognition: config.intent_recognition,
            query_expansion: config.query_expansion,
            answer_generation: config.answer_generation,
            map_summarize: config.map_summarize,
            reduce_summaries: config.reduce_summaries,
            direct_summarize: config.direct_summarize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_agent(toml: &str) -> Result<Agent, LoadError> {
        Agent::from_toml(toml)
    }

    #[test]
    fn from_toml_minimal_container_agent() {
        let agent = make_agent(
            r#"
            name = "echo"
            description = "Echoes input"
            runtime = "container"
            executable = "localhost/echo:latest"

            [requirements]

        "#,
        )
        .unwrap();

        assert_eq!(agent.name, "echo");
        assert_eq!(agent.description, "Echoes input");
        assert_eq!(agent.runtime, RuntimeType::Container);
        assert_eq!(agent.executable, "localhost/echo:latest");
        assert!(agent.prompts.is_none());
        assert!(agent.object_storage.is_none());
        assert!(agent.vector_storage.is_none());
        assert!(agent.image_digest.is_none());
    }

    #[test]
    fn from_toml_with_models() {
        let agent = make_agent(
            r#"
            name = "thinker"
            description = "Thinks"
            runtime = "container"
            executable = "localhost/thinker:latest"

            [requirements.models]
            inference_model = "claude-sonnet"
            embedding_model = "nomic-embed"
        "#,
        )
        .unwrap();

        assert!(agent.has_model("inference_model"));
        assert!(agent.has_model("embedding_model"));
        assert!(!agent.has_model("gpt4"));
        assert_eq!(agent.model_name("inference_model"), Some("claude-sonnet"));
    }

    #[test]
    fn from_toml_with_storage() {
        let agent = make_agent(
            r#"
            name = "noter"
            description = "Takes notes"
            runtime = "container"
            executable = "localhost/noter:latest"
            object_storage = "sqlite:///data/objects.db"
            vector_storage = "sqlite:///data/vectors.db"

            [requirements]

        "#,
        )
        .unwrap();

        assert_eq!(
            agent
                .object_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/objects.db"),
        );
        assert_eq!(
            agent
                .vector_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/vectors.db"),
        );
    }

    #[test]
    fn from_toml_unknown_runtime_fails() {
        let result = make_agent(
            r#"
            name = "bad"
            description = "Bad runtime"
            runtime = "wasm"
            executable = "agent.wasm"

            [requirements]

        "#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn from_toml_invalid_toml_fails() {
        let result = make_agent("not valid toml {{{}}}");
        assert!(result.is_err());
    }

    #[test]
    fn placeholder_id_has_pending_scheme() {
        let id = Agent::placeholder_id("my-agent");
        assert_eq!(id.as_str(), "pending-registration://agents/my-agent");
    }

    #[test]
    fn model_name_returns_none_for_undeclared() {
        let agent = make_agent(
            r#"
            name = "simple"
            description = "No models"
            runtime = "container"
            executable = "localhost/simple:latest"

            [requirements]

        "#,
        )
        .unwrap();

        assert!(agent.model_name("nonexistent").is_none());
    }

    // ========================================================================
    // AgentConfig
    // ========================================================================

    #[test]
    fn config_equal_for_same_declaration() {
        let a = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"
            [requirements]
        "#,
        )
        .unwrap();

        let b = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"
            [requirements]
        "#,
        )
        .unwrap();

        assert_eq!(a.config(), b.config());
    }

    #[test]
    fn config_ignores_registry_assigned_fields() {
        let mut a = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"
            [requirements]
        "#,
        )
        .unwrap();

        let b = a.clone();

        // Simulate registry assigning fields
        a.id = ResourceId::new("http://127.0.0.1:9000/agents/echo");
        a.public_key = Some(vec![1, 2, 3]);

        assert_ne!(a, b); // Agent-level equality sees the difference
        assert_eq!(a.config(), b.config()); // Config-level equality ignores it
    }

    #[test]
    fn config_differs_on_executable() {
        let a = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:v1"
            [requirements]
        "#,
        )
        .unwrap();

        let b = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:v2"
            [requirements]
        "#,
        )
        .unwrap();

        assert_ne!(a.config(), b.config());
    }

    // ========================================================================
    // Mount support (ADR 107)
    // ========================================================================

    #[test]
    fn from_toml_with_mounts() {
        let agent = make_agent(
            r#"
            name = "support"
            description = "Support agent"
            runtime = "container"
            executable = "localhost/support:latest"

            [requirements.mounts.knowledge]
            s3 = "vlinder-support/v0.1.0/"
            path = "/knowledge"
            endpoint = "http://host.containers.internal:4566"
        "#,
        )
        .unwrap();

        assert_eq!(agent.requirements.mounts.len(), 1);
        let mount = &agent.requirements.mounts["knowledge"];
        assert_eq!(mount.s3, "vlinder-support/v0.1.0/");
        assert_eq!(mount.path, "/knowledge");
        assert_eq!(
            mount.endpoint.as_deref(),
            Some("http://host.containers.internal:4566")
        );
    }

    #[test]
    fn from_toml_without_mounts_has_empty_map() {
        let agent = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"

            [requirements]
        "#,
        )
        .unwrap();

        assert!(agent.requirements.mounts.is_empty());
    }

    #[test]
    fn config_differs_on_mounts() {
        let a = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"
            [requirements]
        "#,
        )
        .unwrap();

        let b = make_agent(
            r#"
            name = "echo"
            description = "Echoes"
            runtime = "container"
            executable = "localhost/echo:latest"
            [requirements.mounts.knowledge]
            s3 = "vlinder-support/v0.1.0/"
            path = "/knowledge"
        "#,
        )
        .unwrap();

        assert_ne!(a.config(), b.config());
    }
}
