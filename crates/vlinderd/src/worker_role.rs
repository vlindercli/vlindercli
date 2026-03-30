//! Worker role definitions for distributed mode.
//!
//! When running in distributed mode, each worker process assumes a specific
//! role. The role determines which queues the worker subscribes to and what
//! processing it performs.
//!
//! Workers read their role from the `VLINDER_WORKER_ROLE` environment variable.

use std::fmt;
use std::str::FromStr;

/// Role that a worker process can assume.
///
/// Each role corresponds to a specific service type in the Vlinder architecture.
/// Workers subscribe to queues matching their role and process messages accordingly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerRole {
    /// Registry service - coordinates agents, models, and jobs
    Registry,
    /// Harness service — gRPC interface for CLI agent invocation
    Harness,
    /// Container agent runtime - executes OCI container agents via Podman
    #[cfg(feature = "container")]
    AgentContainer,
    /// Lambda agent runtime - executes agents as AWS Lambda functions
    #[cfg(feature = "lambda")]
    AgentLambda,
    /// Ollama inference service
    #[cfg(feature = "ollama")]
    InferenceOllama,
    /// `OpenRouter` inference service (cloud LLMs)
    #[cfg(feature = "openrouter")]
    InferenceOpenRouter,
    /// `SQLite` object storage service
    #[cfg(feature = "sqlite-kv")]
    StorageObjectSqlite,
    /// SQLite-vec vector storage service
    #[cfg(feature = "sqlite-vec")]
    StorageVectorSqlite,
    /// Secret store service — gRPC interface to the `SecretStore`
    Secret,
    /// State service — gRPC interface to the `DagStore` (ADR 079)
    State,
    /// Catalog service — gRPC interface to model catalogs (Ollama, `OpenRouter`)
    #[cfg(any(feature = "ollama", feature = "openrouter"))]
    Catalog,
    /// Infra plane worker — processes deploy/delete agent messages
    Infra,
    /// DAG git worker — writes messages as git commits for time-travel
    DagGit,
    /// Session viewer HTTP server — read-only conversation browser
    SessionViewer,
}

impl WorkerRole {
    /// Read worker role from `VLINDER_WORKER_ROLE` environment variable.
    ///
    /// Returns None if the env var is not set or has an invalid value.
    pub fn from_env() -> Option<Self> {
        std::env::var("VLINDER_WORKER_ROLE")
            .ok()
            .and_then(|v| v.parse().ok())
    }

    /// Get the environment variable value for this role.
    pub fn as_env_value(&self) -> &'static str {
        match self {
            WorkerRole::Registry => "registry",
            WorkerRole::Harness => "harness",
            #[cfg(feature = "container")]
            WorkerRole::AgentContainer => "agent-container",
            #[cfg(feature = "lambda")]
            WorkerRole::AgentLambda => "agent-lambda",
            #[cfg(feature = "ollama")]
            WorkerRole::InferenceOllama => "inference-ollama",
            #[cfg(feature = "openrouter")]
            WorkerRole::InferenceOpenRouter => "inference-openrouter",
            #[cfg(feature = "sqlite-kv")]
            WorkerRole::StorageObjectSqlite => "storage-object-sqlite",
            #[cfg(feature = "sqlite-vec")]
            WorkerRole::StorageVectorSqlite => "storage-vector-sqlite",
            WorkerRole::Secret => "secret",
            WorkerRole::State => "state",
            #[cfg(any(feature = "ollama", feature = "openrouter"))]
            WorkerRole::Catalog => "catalog",
            WorkerRole::Infra => "infra",
            WorkerRole::DagGit => "dag-git",
            WorkerRole::SessionViewer => "session-viewer",
        }
    }

    /// Get a human-readable description of this role.
    pub fn description(&self) -> &'static str {
        match self {
            WorkerRole::Registry => "Registry service",
            WorkerRole::Harness => "Harness service",
            #[cfg(feature = "container")]
            WorkerRole::AgentContainer => "Container agent runtime",
            #[cfg(feature = "lambda")]
            WorkerRole::AgentLambda => "Lambda agent runtime",
            #[cfg(feature = "ollama")]
            WorkerRole::InferenceOllama => "Ollama inference service",
            #[cfg(feature = "openrouter")]
            WorkerRole::InferenceOpenRouter => "OpenRouter inference service",
            #[cfg(feature = "sqlite-kv")]
            WorkerRole::StorageObjectSqlite => "SQLite object storage",
            #[cfg(feature = "sqlite-vec")]
            WorkerRole::StorageVectorSqlite => "SQLite-vec vector storage",
            WorkerRole::Secret => "Secret store service",
            WorkerRole::State => "State service",
            #[cfg(any(feature = "ollama", feature = "openrouter"))]
            WorkerRole::Catalog => "Catalog service",
            WorkerRole::Infra => "Infra plane worker",
            WorkerRole::DagGit => "DAG git worker",
            WorkerRole::SessionViewer => "Session viewer HTTP server",
        }
    }
}

impl fmt::Display for WorkerRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_env_value())
    }
}

impl FromStr for WorkerRole {
    type Err = ParseWorkerRoleError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "registry" => Ok(WorkerRole::Registry),
            "harness" => Ok(WorkerRole::Harness),
            #[cfg(feature = "container")]
            "agent-container" => Ok(WorkerRole::AgentContainer),
            #[cfg(feature = "lambda")]
            "agent-lambda" => Ok(WorkerRole::AgentLambda),
            #[cfg(feature = "ollama")]
            "inference-ollama" => Ok(WorkerRole::InferenceOllama),
            #[cfg(feature = "openrouter")]
            "inference-openrouter" => Ok(WorkerRole::InferenceOpenRouter),
            #[cfg(feature = "sqlite-kv")]
            "storage-object-sqlite" => Ok(WorkerRole::StorageObjectSqlite),
            #[cfg(feature = "sqlite-vec")]
            "storage-vector-sqlite" => Ok(WorkerRole::StorageVectorSqlite),
            "secret" => Ok(WorkerRole::Secret),
            "state" => Ok(WorkerRole::State),
            #[cfg(any(feature = "ollama", feature = "openrouter"))]
            "catalog" => Ok(WorkerRole::Catalog),
            "infra" => Ok(WorkerRole::Infra),
            "dag-git" => Ok(WorkerRole::DagGit),
            "session-viewer" => Ok(WorkerRole::SessionViewer),
            _ => Err(ParseWorkerRoleError(s.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct ParseWorkerRoleError(String);

impl fmt::Display for ParseWorkerRoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid worker role: {}", self.0)
    }
}

impl std::error::Error for ParseWorkerRoleError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_roles() {
        assert_eq!(
            "registry".parse::<WorkerRole>().unwrap(),
            WorkerRole::Registry
        );
        assert_eq!(
            "harness".parse::<WorkerRole>().unwrap(),
            WorkerRole::Harness
        );
        #[cfg(feature = "container")]
        assert_eq!(
            "agent-container".parse::<WorkerRole>().unwrap(),
            WorkerRole::AgentContainer
        );
        #[cfg(feature = "lambda")]
        assert_eq!(
            "agent-lambda".parse::<WorkerRole>().unwrap(),
            WorkerRole::AgentLambda
        );
        #[cfg(feature = "ollama")]
        assert_eq!(
            "inference-ollama".parse::<WorkerRole>().unwrap(),
            WorkerRole::InferenceOllama
        );
        #[cfg(feature = "sqlite-kv")]
        assert_eq!(
            "storage-object-sqlite".parse::<WorkerRole>().unwrap(),
            WorkerRole::StorageObjectSqlite
        );
        #[cfg(feature = "sqlite-vec")]
        assert_eq!(
            "storage-vector-sqlite".parse::<WorkerRole>().unwrap(),
            WorkerRole::StorageVectorSqlite
        );
        assert_eq!("secret".parse::<WorkerRole>().unwrap(), WorkerRole::Secret);
        assert_eq!("state".parse::<WorkerRole>().unwrap(), WorkerRole::State);
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        assert_eq!(
            "catalog".parse::<WorkerRole>().unwrap(),
            WorkerRole::Catalog
        );
        assert_eq!("dag-git".parse::<WorkerRole>().unwrap(), WorkerRole::DagGit);
        assert_eq!(
            "session-viewer".parse::<WorkerRole>().unwrap(),
            WorkerRole::SessionViewer
        );
    }

    #[test]
    fn parse_invalid_role() {
        assert!("invalid".parse::<WorkerRole>().is_err());
        assert!("".parse::<WorkerRole>().is_err());
        // Removed role names
        assert!("embedding-ollama".parse::<WorkerRole>().is_err());
        assert!("storage-object-memory".parse::<WorkerRole>().is_err());
        assert!("storage-vector-memory".parse::<WorkerRole>().is_err());
        assert!("dag-capture".parse::<WorkerRole>().is_err());
        assert!("dag-sqlite".parse::<WorkerRole>().is_err());
    }

    #[test]
    fn roundtrip_env_value() {
        for role in [
            WorkerRole::Registry,
            WorkerRole::Harness,
            #[cfg(feature = "container")]
            WorkerRole::AgentContainer,
            #[cfg(feature = "lambda")]
            WorkerRole::AgentLambda,
            #[cfg(feature = "ollama")]
            WorkerRole::InferenceOllama,
            #[cfg(feature = "sqlite-kv")]
            WorkerRole::StorageObjectSqlite,
            #[cfg(feature = "sqlite-vec")]
            WorkerRole::StorageVectorSqlite,
            WorkerRole::Secret,
            WorkerRole::State,
            #[cfg(any(feature = "ollama", feature = "openrouter"))]
            WorkerRole::Catalog,
            WorkerRole::Infra,
            WorkerRole::DagGit,
            WorkerRole::SessionViewer,
        ] {
            let env_val = role.as_env_value();
            let parsed: WorkerRole = env_val.parse().unwrap();
            assert_eq!(parsed, role);
        }
    }

    #[test]
    #[cfg(feature = "container")]
    fn from_env_parses_valid_role() {
        // Test the parsing logic directly (avoids env var race conditions)
        let result = "agent-container".parse::<WorkerRole>();
        assert_eq!(result.unwrap(), WorkerRole::AgentContainer);
    }

    #[test]
    fn from_env_rejects_invalid_role() {
        let result = "invalid-role".parse::<WorkerRole>();
        assert!(result.is_err());
    }
}
