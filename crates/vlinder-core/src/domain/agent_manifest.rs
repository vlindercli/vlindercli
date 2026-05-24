use std::collections::HashMap;
use std::path::Path;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use super::provider::Provider;

use super::resource_id::ResourceId;
use super::runtime::RuntimeType;
use super::service_type::ServiceType;

/// Agent manifest as read from agent.toml.
///
/// Relative paths (executable, storage URIs) are resolved against
/// the manifest's directory during loading.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentManifest {
    pub name: String,
    pub description: String,
    #[serde(default)]
    pub source: Option<String>,
    /// Runtime type (e.g., "container"). Determines which executor runs the agent.
    pub runtime: String,
    /// Executable reference — native to the runtime ecosystem.
    /// Container: OCI image ref (e.g., "localhost/echo-container:latest")
    /// File paths are resolved to absolute during loading.
    pub executable: String,
    pub requirements: RequirementsConfig,
    /// Object storage resource ID (e.g., `<sqlite:///path/to/objects.db>`)
    #[serde(default)]
    pub object_storage: Option<ResourceId>,
    /// Vector storage resource ID (e.g., `<sqlite:///path/to/vectors.db>`)
    #[serde(default)]
    pub vector_storage: Option<ResourceId>,
}

impl AgentManifest {
    /// Load an agent manifest from a file path.
    ///
    /// Resolves relative paths to absolute:
    /// - `executable` → resolved only for file-based runtimes
    pub fn load(path: &Path) -> Result<AgentManifest, ParseError> {
        let content = std::fs::read_to_string(path)?;
        let mut manifest: AgentManifest = toml::from_str(&content)?;

        // Derive agent directory from manifest path, ensuring it's absolute
        let agent_dir = path
            .parent()
            .unwrap_or(Path::new("."))
            .canonicalize()
            .unwrap_or_else(|_| path.parent().unwrap_or(Path::new(".")).to_path_buf());

        // Resolve executable for file-based runtimes only.
        // Image-based runtimes (container, lambda) pass through as-is.
        let runtime_type = RuntimeType::from_str(&manifest.runtime).ok();
        match runtime_type {
            Some(RuntimeType::Container | RuntimeType::Lambda) => {}
            _ => {
                manifest.executable = resolve_executable_path(&manifest.executable, &agent_dir)?;
            }
        }

        // Models are registry names (ADR 094) — no URI resolution needed.

        // Resolve storage URIs (sqlite:// paths need to be absolute)
        if let Some(ref storage) = manifest.object_storage {
            manifest.object_storage = Some(resolve_storage_uri(storage, &agent_dir));
        }
        if let Some(ref storage) = manifest.vector_storage {
            manifest.vector_storage = Some(resolve_storage_uri(storage, &agent_dir));
        }

        Ok(manifest)
    }
}

/// Resolve a file-based executable path to an absolute file:// URI.
///
/// Only used for non-container runtimes where the executable is a local file.
/// - If already a file:// URI, resolve relative paths
/// - If a bare path, resolve against `agent_dir` and convert to file:// URI
fn resolve_executable_path(executable: &str, agent_dir: &Path) -> Result<String, ParseError> {
    // Already a file:// URI with relative path
    if let Some(path) = executable.strip_prefix("file://") {
        if !Path::new(path).is_absolute() {
            let resolved = agent_dir.join(path);
            if !resolved.exists() {
                return Err(ParseError::ExecutableNotFound(format!(
                    "executable not found: {}",
                    resolved.display()
                )));
            }
            return Ok(format!("file://{}", resolved.display()));
        }
        return Ok(executable.to_string());
    }

    // Already a URI of another scheme — pass through
    if executable.contains("://") {
        return Ok(executable.to_string());
    }

    // Resolve bare path (relative or absolute)
    let exe_path = if Path::new(executable).is_absolute() {
        Path::new(executable).to_path_buf()
    } else {
        agent_dir.join(executable)
    };

    if !exe_path.exists() {
        return Err(ParseError::ExecutableNotFound(format!(
            "executable not found: {}",
            exe_path.display()
        )));
    }

    Ok(format!("file://{}", exe_path.display()))
}

/// Resolve a storage URI, making relative sqlite:// paths absolute.
fn resolve_storage_uri(storage: &ResourceId, agent_dir: &Path) -> ResourceId {
    let uri = storage.as_str();

    // sqlite://path → resolve path relative to agent_dir
    if let Some(path) = uri.strip_prefix("sqlite://") {
        if !Path::new(path).is_absolute() {
            let resolved = agent_dir.join(path);
            return ResourceId::new(format!("sqlite://{}", resolved.display()));
        }
    }

    // Other schemes (memory://, s3://, etc.) pass through unchanged
    storage.clone()
}

#[derive(Debug)]
pub enum ParseError {
    Io(std::io::Error),
    Toml(String),
    ExecutableNotFound(String),
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        ParseError::Io(e)
    }
}

impl From<toml::de::Error> for ParseError {
    fn from(e: toml::de::Error) -> Self {
        ParseError::Toml(e.to_string())
    }
}

/// Mount declaration as declared in agent.toml (ADR 133).
///
/// Declares where inside the agent container the platform's versioned
/// workspace is bind-mounted. The mount is per-(agent, session, branch),
/// CoW-backed by ZFS, and snapshotted on every invoke complete.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MountConfig {
    /// Mount point inside the container (e.g. "/workspaces").
    pub path: String,
}

/// Requirements as declared in agent.toml
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RequirementsConfig {
    /// Model alias → registry name (ADR 094).
    ///
    /// Table form: `inference_model = "claude-sonnet"` (alias differs from name)
    /// Array form: `models = ["claude-sonnet"]` (alias == name)
    #[serde(default, deserialize_with = "deserialize_models")]
    pub models: HashMap<String, String>,
    #[serde(default)]
    pub services: HashMap<ServiceType, ServiceConfig>,
    /// Versioned mount (ADR 133). Absent → agent runs without
    /// a versioned workspace.
    #[serde(default)]
    pub mount: Option<MountConfig>,
    /// MCP provider names (references registered MCP servers by name).
    #[serde(default)]
    pub mcp: Vec<String>,
}

/// Deserialize models from either table or array form.
fn deserialize_models<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ModelsConfig {
        Table(HashMap<String, String>),
        Array(Vec<String>),
    }

    match ModelsConfig::deserialize(deserializer)? {
        ModelsConfig::Table(map) => Ok(map),
        ModelsConfig::Array(names) => Ok(names.into_iter().map(|n| (n.clone(), n)).collect()),
    }
}

/// Service declaration as declared in agent.toml
///
/// Ties together the three dimensions: which service, which provider, which protocol.
/// The service type is the map key. Provider and protocol are typed enums —
/// invalid values fail at parse time.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub provider: Provider,
    pub protocol: Protocol,
    #[serde(default)]
    pub models: Vec<String>,
}

/// Wire protocol — the request/response shape the agent speaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    /// OpenAI-compatible Chat Completion / Embeddings
    OpenAi,
    /// Anthropic Messages API
    Anthropic,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_manifest_with_storage() {
        let toml = r#"
            name = "test-agent"
            description = "Test agent with storage"
            runtime = "container"
            executable = "localhost/test-agent:latest"
            object_storage = "sqlite:///data/objects.db"
            vector_storage = "sqlite:///data/vectors.db"

            [requirements]

        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();

        assert_eq!(manifest.name, "test-agent");
        assert_eq!(manifest.runtime, "container");
        assert_eq!(manifest.executable, "localhost/test-agent:latest");
        assert_eq!(
            manifest
                .object_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/objects.db")
        );
        assert_eq!(
            manifest
                .vector_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("sqlite:///data/vectors.db")
        );
    }

    #[test]
    fn parse_manifest_without_storage() {
        let toml = r#"
            name = "test-agent"
            description = "Test agent without storage"
            runtime = "container"
            executable = "localhost/test-agent:latest"

            [requirements]

        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();

        assert_eq!(manifest.name, "test-agent");
        assert!(manifest.object_storage.is_none());
        assert!(manifest.vector_storage.is_none());
    }

    #[test]
    fn parse_manifest_with_partial_storage() {
        let toml = r#"
            name = "test-agent"
            description = "Test agent with only object storage"
            runtime = "container"
            executable = "localhost/test-agent:latest"
            object_storage = "s3://my-bucket/agents/test"

            [requirements]

        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();

        assert_eq!(
            manifest
                .object_storage
                .as_ref()
                .map(super::super::resource_id::ResourceId::as_str),
            Some("s3://my-bucket/agents/test")
        );
        assert!(manifest.vector_storage.is_none());
    }

    #[test]
    fn parse_manifest_with_service_protocol() {
        let toml = r#"
            name = "finqa"
            description = "Financial Q&A agent"
            runtime = "container"
            executable = "localhost/finqa:latest"

            [requirements.services.infer]
            provider = "openrouter"
            protocol = "anthropic"
            models = ["anthropic/claude-3.5-sonnet"]

            [requirements.services.embed]
            provider = "ollama"
            protocol = "openai"
            models = ["nomic-embed-text:latest"]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();

        let infer = &manifest.requirements.services[&ServiceType::Infer];
        assert_eq!(infer.provider, Provider::OpenRouter);
        assert_eq!(infer.protocol, Protocol::Anthropic);
        assert_eq!(infer.models, vec!["anthropic/claude-3.5-sonnet"]);

        let embed = &manifest.requirements.services[&ServiceType::Embed];
        assert_eq!(embed.provider, Provider::Ollama);
        assert_eq!(embed.protocol, Protocol::OpenAi);
        assert_eq!(embed.models, vec!["nomic-embed-text:latest"]);
    }

    #[test]
    fn storage_resource_id_scheme() {
        let toml = r#"
            name = "test-agent"
            description = "Test"
            runtime = "container"
            executable = "localhost/test-agent:latest"
            object_storage = "memory://test"
            vector_storage = "pinecone://my-index"

            [requirements]

        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();

        assert_eq!(
            manifest.object_storage.as_ref().and_then(|r| r.scheme()),
            Some("memory")
        );
        assert_eq!(
            manifest.vector_storage.as_ref().and_then(|r| r.scheme()),
            Some("pinecone")
        );
    }

    #[test]
    fn invalid_provider_fails_at_parse() {
        let toml = r#"
            name = "bad"
            description = "Bad provider"
            runtime = "container"
            executable = "localhost/bad:latest"

            [requirements.services.infer]
            provider = "banana"
            protocol = "openai"
        "#;

        let result: Result<AgentManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_protocol_fails_at_parse() {
        let toml = r#"
            name = "bad"
            description = "Bad protocol"
            runtime = "container"
            executable = "localhost/bad:latest"

            [requirements.services.infer]
            provider = "openrouter"
            protocol = "grpc-nonsense"
        "#;

        let result: Result<AgentManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn invalid_service_type_fails_at_parse() {
        let toml = r#"
            name = "bad"
            description = "Bad service type"
            runtime = "container"
            executable = "localhost/bad:latest"

            [requirements.services.teleport]
            provider = "openrouter"
            protocol = "openai"
        "#;

        let result: Result<AgentManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    // ========================================================================
    // Model form parsing (ADR 094)
    // ========================================================================

    #[test]
    fn parse_models_table_form() {
        let toml = r#"
            name = "agent"
            description = "Test"
            runtime = "container"
            executable = "localhost/agent:latest"

            [requirements.models]
            inference_model = "claude-sonnet"
            embedding_model = "nomic-embed"
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert_eq!(
            manifest.requirements.models.get("inference_model").unwrap(),
            "claude-sonnet"
        );
        assert_eq!(
            manifest.requirements.models.get("embedding_model").unwrap(),
            "nomic-embed"
        );
    }

    #[test]
    fn parse_models_array_form() {
        let toml = r#"
            name = "agent"
            description = "Test"
            runtime = "container"
            executable = "localhost/agent:latest"

            [requirements]
            models = ["claude-sonnet", "nomic-embed"]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        // Array form: alias == name
        assert_eq!(
            manifest.requirements.models.get("claude-sonnet").unwrap(),
            "claude-sonnet"
        );
        assert_eq!(
            manifest.requirements.models.get("nomic-embed").unwrap(),
            "nomic-embed"
        );
    }

    #[test]
    fn parse_models_empty_default() {
        let toml = r#"
            name = "agent"
            description = "Test"
            runtime = "container"
            executable = "localhost/agent:latest"

            [requirements]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert!(manifest.requirements.models.is_empty());
    }

    // ========================================================================
    // Mount parsing (ADR 133)
    // ========================================================================

    #[test]
    fn parse_manifest_with_mount() {
        let toml = r#"
            name = "support"
            description = "Support agent"
            runtime = "container"
            executable = "localhost/support:latest"

            [requirements.mount]
            path = "/workspaces"
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        let mount = manifest.requirements.mount.as_ref().unwrap();
        assert_eq!(mount.path, "/workspaces");
    }

    #[test]
    fn parse_manifest_without_mount() {
        let toml = r#"
            name = "simple"
            description = "No mount"
            runtime = "container"
            executable = "localhost/simple:latest"

            [requirements]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert!(manifest.requirements.mount.is_none());
    }

    #[test]
    fn parse_manifest_mount_missing_path_fails() {
        let toml = r#"
            name = "bad"
            description = "Missing path"
            runtime = "container"
            executable = "localhost/bad:latest"

            [requirements.mount]
        "#;

        let result: Result<AgentManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn parse_manifest_mount_inline_form() {
        let toml = r#"
            name = "inline"
            description = "Inline mount"
            runtime = "container"
            executable = "localhost/inline:latest"

            [requirements]
            mount = { path = "/workspaces" }
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        let mount = manifest.requirements.mount.as_ref().unwrap();
        assert_eq!(mount.path, "/workspaces");
    }
    // ========================================================================
    // MCP provider parsing (now Vec<String> of names)
    // ========================================================================

    #[test]
    fn parse_manifest_with_mcp_names() {
        let toml = r#"
            name = "mcp-agent"
            description = "MCP agent"
            runtime = "container"
            executable = "localhost/mcp-agent:latest"

            [requirements]
            mcp = ["brave", "jira"]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert_eq!(manifest.requirements.mcp.len(), 2);
        assert_eq!(manifest.requirements.mcp[0], "brave");
        assert_eq!(manifest.requirements.mcp[1], "jira");
    }

    #[test]
    fn parse_manifest_without_mcp() {
        let toml = r#"
            name = "simple"
            description = "No MCP"
            runtime = "container"
            executable = "localhost/simple:latest"

            [requirements]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert!(manifest.requirements.mcp.is_empty());
    }

    #[test]
    fn parse_manifest_single_mcp() {
        let toml = r#"
            name = "mcp-agent"
            description = "Single MCP"
            runtime = "container"
            executable = "localhost/mcp-agent:latest"

            [requirements]
            mcp = ["brave"]
        "#;

        let manifest: AgentManifest = toml::from_str(toml).unwrap();
        assert_eq!(manifest.requirements.mcp, vec!["brave"]);
    }
}
