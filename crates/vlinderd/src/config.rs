//! Application configuration with env override support.
//!
//! Resolution order (highest priority first):
//! 1. Environment variable: VLINDER_<SECTION>_<KEY>
//! 2. Config file: ~/.vlinder/config.toml
//! 3. Default value
//!
//! The `ConfigLoader` trait allows custom loading strategies for testing
//! or alternative config sources.

use serde::Deserialize;
use std::path::PathBuf;

// ============================================================================
// Backend Enums (compile-time safety for queue + state backends)
// ============================================================================

/// Queue backend selector.
///
/// In production only `Nats` is available. The `Memory` variant exists
/// only in test/test-support builds, so production binaries physically
/// cannot select an in-memory queue.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum QueueBackend {
    Nats,
    #[cfg(any(test, feature = "test-support"))]
    Memory,
}

/// State backend selector (`DagStore`).
///
/// Same gating strategy as `QueueBackend`: `Grpc` in prod, `Memory` in tests.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StateBackend {
    Grpc,
    #[cfg(any(test, feature = "test-support"))]
    Memory,
}

/// Registry backend selector.
///
/// Same gating strategy: `Grpc` in prod (connects to registry service),
/// `Memory` in tests (in-process `InMemoryRegistry`).
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RegistryBackend {
    Grpc,
    #[cfg(any(test, feature = "test-support"))]
    Memory,
}

// ============================================================================
// Config Loader Trait
// ============================================================================

/// Trait for loading configuration.
///
/// Implementations can load from different sources:
/// - `DefaultLoader`: File + env overrides (production)
/// - `TestLoader`: In-memory config (testing)
/// - Future: Remote config services
pub trait ConfigLoader: Send + Sync {
    fn load(&self) -> Config;
}

/// Default loader: file + env overrides.
pub struct DefaultLoader;

impl ConfigLoader for DefaultLoader {
    fn load(&self) -> Config {
        let mut config = Config::load_from_file();
        config.apply_env_overrides();
        config
    }
}

/// Test loader: returns a pre-configured Config.
#[cfg(any(test, feature = "test-support"))]
pub struct TestLoader(pub Config);

#[cfg(any(test, feature = "test-support"))]
impl ConfigLoader for TestLoader {
    fn load(&self) -> Config {
        self.0.clone()
    }
}

// ============================================================================
// Config Struct (Typed)
// ============================================================================

/// Application configuration loaded from ~/.vlinder/config.toml
///
/// `queue` and `state` sections are required — the daemon refuses to start
/// if backends are not explicitly configured.
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub logging: LoggingConfig,
    #[serde(default)]
    pub ollama: OllamaConfig,
    #[serde(default)]
    pub openrouter: OpenRouterConfig,
    pub queue: QueueConfig,
    pub state: StateConfig,
    #[serde(default)]
    pub distributed: DistributedConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// App log level: trace, debug, info, warn, error
    pub level: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OllamaConfig {
    /// Ollama server endpoint
    pub endpoint: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct OpenRouterConfig {
    /// `OpenRouter` API endpoint
    pub endpoint: String,
    /// API key for authentication
    pub api_key: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueueConfig {
    pub backend: QueueBackend,
    #[serde(default = "default_nats_url")]
    pub nats_url: String,
    /// Optional path to a NATS .creds file for authenticated connections (e.g. NGS).
    #[serde(default)]
    pub nats_creds: Option<String>,
}

impl QueueConfig {
    /// Produce the NATS connection config consumed by vlinder-nats.
    ///
    /// Single mapping point — factories and workers call this instead
    /// of reaching into `nats_url/nats_creds` directly.
    pub fn nats_config(&self) -> vlinder_nats::NatsConfig {
        vlinder_nats::NatsConfig {
            url: self.nats_url.clone(),
            creds_file: self.nats_creds.clone(),
            creds_content: None,
        }
    }
}

fn default_nats_url() -> String {
    "nats://localhost:4222".to_string()
}

fn default_registry_backend() -> RegistryBackend {
    RegistryBackend::Grpc
}

#[derive(Debug, Clone, Deserialize)]
pub struct StateConfig {
    pub backend: StateBackend,
}

// ============================================================================
// Distributed Config (ADR 043)
// ============================================================================

/// Configuration for distributed multi-process deployments.
///
/// The daemon spawns separate worker processes for each service type.
/// Workers communicate via NATS queues.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DistributedConfig {
    /// Registry backend selector
    #[serde(default = "default_registry_backend")]
    pub registry_backend: RegistryBackend,
    /// Registry gRPC address for worker coordination
    pub registry_addr: String,
    /// State service gRPC address (ADR 079)
    pub state_addr: String,
    /// Harness gRPC address for CLI→daemon agent invocation
    pub harness_addr: String,
    /// Secret store gRPC address
    pub secret_addr: String,
    /// Catalog service gRPC address
    pub catalog_addr: String,
    /// Worker process counts by type
    pub workers: WorkerCounts,
}

/// Worker process counts for each service type.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct WorkerCounts {
    /// Number of registry worker processes
    pub registry: u32,
    /// Number of harness worker processes
    pub harness: u32,
    /// Agent runtime workers
    pub agent: AgentWorkerCounts,
    /// Inference service workers
    pub inference: InferenceWorkerCounts,
    /// Storage service workers
    pub storage: StorageWorkerCounts,
    /// DAG git worker (singleton recommended — see ADR 078)
    pub dag_git: u32,
    /// Session viewer worker
    pub session_viewer: u32,
}

/// Agent runtime worker counts by backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct AgentWorkerCounts {
    /// Container runtime workers (Podman)
    pub container: u32,
    /// Lambda runtime workers (AWS Lambda)
    pub lambda: u32,
}

/// Inference worker counts by engine.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct InferenceWorkerCounts {
    /// Ollama inference workers
    pub ollama: u32,
    /// `OpenRouter` inference workers
    pub openrouter: u32,
}

/// Storage worker counts by backend.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct StorageWorkerCounts {
    /// Object storage workers
    pub object: ObjectStorageWorkerCounts,
    /// Vector storage workers
    pub vector: VectorStorageWorkerCounts,
}

/// Object storage worker counts by backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ObjectStorageWorkerCounts {
    /// `SQLite` object storage workers
    pub sqlite: u32,
}

/// Vector storage worker counts by backend.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct VectorStorageWorkerCounts {
    /// SQLite-vec vector storage workers
    pub sqlite: u32,
}

// ============================================================================
// Runtime Config (ADR 073)
// ============================================================================

/// Container runtime configuration.
///
/// Controls how the container runtime resolves OCI images and connects to Podman.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RuntimeConfig {
    /// Image resolution policy: "mutable" (default) or "pinned".
    ///
    /// - `mutable`: Uses the tag from `agent.executable` — rebuilt images are
    ///   picked up automatically on next container start. Default for development.
    /// - `pinned`: Uses the content-addressed digest from `agent.image_digest` —
    ///   deterministic execution with the exact image from registration time.
    pub image_policy: String,

    /// Podman socket path: "auto" (probe filesystem), "disabled" (CLI only),
    /// or an absolute path to the socket file.
    pub podman_socket: String,

    /// OCI image reference for the vlinder-podman-sidecar container.
    /// Deployed alongside agent containers in a Podman pod.
    pub sidecar_image: String,

    /// AWS region for Lambda functions (one worker = one region).
    pub lambda_region: String,

    /// Memory allocation for Lambda functions in MB.
    pub lambda_memory_mb: i32,

    /// Execution timeout for Lambda functions in seconds.
    pub lambda_timeout_secs: i32,

    /// VPC subnet IDs for Lambda ENI placement (comma-separated in env).
    pub lambda_vpc_subnet_ids: Vec<String>,

    /// VPC security group IDs for Lambda ENIs (comma-separated in env).
    pub lambda_vpc_security_group_ids: Vec<String>,
}

// ============================================================================
// Defaults
// ============================================================================

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "warn".to_string(),
        }
    }
}

impl Default for OllamaConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://localhost:11434".to_string(),
        }
    }
}

impl Default for OpenRouterConfig {
    fn default() -> Self {
        Self {
            endpoint: "https://openrouter.ai/api/v1".to_string(),
            api_key: String::new(),
        }
    }
}

impl Default for DistributedConfig {
    fn default() -> Self {
        Self {
            registry_backend: default_registry_backend(),
            registry_addr: "http://0.0.0.0:9090".to_string(),
            state_addr: "http://0.0.0.0:9092".to_string(),
            harness_addr: "http://0.0.0.0:9091".to_string(),
            secret_addr: "http://0.0.0.0:9093".to_string(),
            catalog_addr: "http://0.0.0.0:9094".to_string(),
            workers: WorkerCounts::default(),
        }
    }
}

impl Default for WorkerCounts {
    fn default() -> Self {
        Self {
            registry: 1,
            harness: 1,
            agent: AgentWorkerCounts::default(),
            inference: InferenceWorkerCounts::default(),
            storage: StorageWorkerCounts::default(),
            dag_git: 1,
            session_viewer: 1,
        }
    }
}

impl Default for AgentWorkerCounts {
    fn default() -> Self {
        Self {
            container: 1,
            lambda: 0,
        }
    }
}

impl Default for InferenceWorkerCounts {
    fn default() -> Self {
        Self {
            ollama: 1,
            openrouter: 0,
        }
    }
}

impl Default for ObjectStorageWorkerCounts {
    fn default() -> Self {
        Self { sqlite: 1 }
    }
}

impl Default for VectorStorageWorkerCounts {
    fn default() -> Self {
        Self { sqlite: 1 }
    }
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            image_policy: "mutable".to_string(),
            podman_socket: "auto".to_string(),
            sidecar_image: "localhost/vlinder-podman-sidecar:latest".to_string(),
            lambda_region: "us-east-1".to_string(),
            lambda_memory_mb: 512,
            lambda_timeout_secs: 300,
            lambda_vpc_subnet_ids: vec![],
            lambda_vpc_security_group_ids: vec![],
        }
    }
}

// ============================================================================
// Config Implementation
// ============================================================================

impl Config {
    /// Load config using the default loader (file + env).
    pub fn load() -> Self {
        DefaultLoader.load()
    }

    /// Load config using a custom loader.
    pub fn load_with(loader: &dyn ConfigLoader) -> Self {
        loader.load()
    }

    /// Load config from file.
    ///
    /// Panics if the config file exists but is invalid — missing `[queue]` or
    /// `[state]` sections is a fatal misconfiguration.
    /// Panics if no config file exists — create it manually.
    fn load_from_file() -> Self {
        let config_path = config_path();
        assert!(
            config_path.exists(),
            "Config file not found: {}\n\
             Create ~/.vlinder/config.toml manually.\n\
             Required sections: [queue] (backend), [state] (backend)\n\
             See: https://docs.vlindercli.dev/how-to/installation/#bootstrap",
            config_path.display()
        );
        let contents = std::fs::read_to_string(&config_path)
            .unwrap_or_else(|e| panic!("Cannot read {}: {}", config_path.display(), e));
        toml::from_str(&contents)
            .unwrap_or_else(|e| panic!("Invalid config {}: {}", config_path.display(), e))
    }

    /// Construct a Config for tests with memory backends.
    ///
    /// Explicit — not hidden behind Default. Tests that need
    /// different backends should construct Config directly.
    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test() -> Self {
        Self {
            logging: LoggingConfig::default(),
            ollama: OllamaConfig::default(),
            openrouter: OpenRouterConfig::default(),
            queue: QueueConfig {
                backend: QueueBackend::Memory,
                nats_url: "nats://localhost:4222".to_string(),
                nats_creds: None,
            },
            state: StateConfig {
                backend: StateBackend::Memory,
            },
            distributed: DistributedConfig {
                registry_backend: RegistryBackend::Memory,
                ..DistributedConfig::default()
            },
            runtime: RuntimeConfig::default(),
        }
    }

    /// Apply environment variable overrides.
    #[allow(clippy::too_many_lines)]
    fn apply_env_overrides(&mut self) {
        // Logging
        if let Ok(v) = std::env::var("VLINDER_LOGGING_LEVEL") {
            self.logging.level = v;
        }
        // Ollama
        if let Ok(v) = std::env::var("VLINDER_OLLAMA_ENDPOINT") {
            self.ollama.endpoint = v;
        }

        // OpenRouter
        if let Ok(v) = std::env::var("VLINDER_OPENROUTER_ENDPOINT") {
            self.openrouter.endpoint = v;
        }
        if let Ok(v) = std::env::var("VLINDER_OPENROUTER_API_KEY") {
            self.openrouter.api_key = v;
        }

        // Queue
        if let Ok(v) = std::env::var("VLINDER_QUEUE_BACKEND") {
            self.queue.backend = parse_queue_backend(&v);
        }
        if let Ok(v) = std::env::var("VLINDER_QUEUE_NATS_URL") {
            self.queue.nats_url = v;
        }
        if let Ok(v) = std::env::var("VLINDER_QUEUE_NATS_CREDS") {
            self.queue.nats_creds = Some(v);
        }

        // State
        if let Ok(v) = std::env::var("VLINDER_STATE_BACKEND") {
            self.state.backend = parse_state_backend(&v);
        }

        // Distributed
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_REGISTRY_BACKEND") {
            self.distributed.registry_backend = parse_registry_backend(&v);
        }
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_REGISTRY_ADDR") {
            self.distributed.registry_addr = v;
        }
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_STATE_ADDR") {
            self.distributed.state_addr = v;
        }
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_HARNESS_ADDR") {
            self.distributed.harness_addr = v;
        }
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_SECRET_ADDR") {
            self.distributed.secret_addr = v;
        }
        if let Ok(v) = std::env::var("VLINDER_DISTRIBUTED_CATALOG_ADDR") {
            self.distributed.catalog_addr = v;
        }

        // Worker counts (flat env vars for simplicity)
        if let Ok(v) = std::env::var("VLINDER_WORKERS_REGISTRY") {
            self.distributed.workers.registry = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_HARNESS") {
            self.distributed.workers.harness = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_AGENT_CONTAINER") {
            self.distributed.workers.agent.container = v.parse().unwrap_or(0);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_AGENT_LAMBDA") {
            self.distributed.workers.agent.lambda = v.parse().unwrap_or(0);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_INFERENCE_OLLAMA") {
            self.distributed.workers.inference.ollama = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_INFERENCE_OPENROUTER") {
            self.distributed.workers.inference.openrouter = v.parse().unwrap_or(0);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_STORAGE_OBJECT_SQLITE") {
            self.distributed.workers.storage.object.sqlite = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_STORAGE_VECTOR_SQLITE") {
            self.distributed.workers.storage.vector.sqlite = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_DAG_GIT") {
            self.distributed.workers.dag_git = v.parse().unwrap_or(1);
        }
        if let Ok(v) = std::env::var("VLINDER_WORKERS_SESSION_VIEWER") {
            self.distributed.workers.session_viewer = v.parse().unwrap_or(1);
        }
        // Runtime (ADR 073, ADR 077)
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_IMAGE_POLICY") {
            self.runtime.image_policy = v;
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_PODMAN_SOCKET") {
            self.runtime.podman_socket = v;
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_SIDECAR_IMAGE") {
            self.runtime.sidecar_image = v;
        }
        // Lambda runtime
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_LAMBDA_REGION") {
            self.runtime.lambda_region = v;
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_LAMBDA_MEMORY_MB") {
            self.runtime.lambda_memory_mb = v.parse().unwrap_or(512);
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_LAMBDA_TIMEOUT_SECS") {
            self.runtime.lambda_timeout_secs = v.parse().unwrap_or(300);
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_LAMBDA_VPC_SUBNET_IDS") {
            self.runtime.lambda_vpc_subnet_ids = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
        if let Ok(v) = std::env::var("VLINDER_RUNTIME_LAMBDA_VPC_SECURITY_GROUP_IDS") {
            self.runtime.lambda_vpc_security_group_ids = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }

    /// Build tracing `EnvFilter` from config.
    ///
    /// The configured level applies to vlinderd only. External crates
    /// (`async_nats`, tonic, hyper, etc.) are suppressed to warn so they
    /// don't pollute the user's terminal.
    pub fn tracing_filter(&self) -> String {
        format!("warn,vlinderd={}", self.logging.level)
    }
}

// ============================================================================
// Backend parsing helpers
// ============================================================================

fn parse_queue_backend(s: &str) -> QueueBackend {
    match s {
        "nats" => QueueBackend::Nats,
        #[cfg(any(test, feature = "test-support"))]
        "memory" => QueueBackend::Memory,
        other => {
            tracing::warn!(
                value = other,
                "Unknown VLINDER_QUEUE_BACKEND — defaulting to nats"
            );
            QueueBackend::Nats
        }
    }
}

fn parse_registry_backend(s: &str) -> RegistryBackend {
    match s {
        "grpc" => RegistryBackend::Grpc,
        #[cfg(any(test, feature = "test-support"))]
        "memory" => RegistryBackend::Memory,
        other => {
            tracing::warn!(
                value = other,
                "Unknown VLINDER_DISTRIBUTED_REGISTRY_BACKEND — defaulting to grpc"
            );
            RegistryBackend::Grpc
        }
    }
}

fn parse_state_backend(s: &str) -> StateBackend {
    match s {
        "grpc" => StateBackend::Grpc,
        #[cfg(any(test, feature = "test-support"))]
        "memory" => StateBackend::Memory,
        other => {
            tracing::warn!(
                value = other,
                "Unknown VLINDER_STATE_BACKEND — defaulting to grpc"
            );
            StateBackend::Grpc
        }
    }
}

// ============================================================================
// Path helpers
// ============================================================================

/// Path to config file.
pub fn config_path() -> PathBuf {
    vlinder_dir().join("config.toml")
}

/// Base directory for vlinder data (agents, models, storage).
///
/// Resolution order:
/// 1. Env var `VLINDER_DIR` (for project-local or CI)
/// 2. Default: ~/.vlinder
pub fn vlinder_dir() -> PathBuf {
    match std::env::var("VLINDER_DIR") {
        Ok(v) => PathBuf::from(v),
        Err(_) => dirs::home_dir()
            .expect("Could not determine home directory")
            .join(".vlinder"),
    }
}

pub fn agents_dir() -> PathBuf {
    vlinder_dir().join("agents")
}

pub fn models_dir() -> PathBuf {
    vlinder_dir().join("models")
}

pub fn agent_dir(name: &str) -> PathBuf {
    agents_dir().join(name)
}

pub fn agent_manifest_path(name: &str) -> PathBuf {
    agent_dir(name).join(format!("{name}-agent.toml"))
}

pub fn agent_db_path(name: &str) -> PathBuf {
    agent_dir(name).join(format!("{name}.db"))
}

pub fn conversations_dir() -> PathBuf {
    vlinder_dir().join("conversations")
}

pub fn logs_dir() -> PathBuf {
    vlinder_dir().join("logs")
}

pub fn registry_db_path() -> PathBuf {
    vlinder_dir().join("registry.db")
}

pub fn dag_db_path() -> PathBuf {
    vlinder_dir().join("dag.db")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn for_test_gives_memory_backends() {
        let config = Config::for_test();
        assert_eq!(config.queue.backend, QueueBackend::Memory);
        assert_eq!(config.state.backend, StateBackend::Memory);
        assert_eq!(config.logging.level, "warn");
        assert_eq!(config.ollama.endpoint, "http://localhost:11434");
    }

    #[test]
    fn env_override_ollama_endpoint() {
        std::env::set_var("VLINDER_OLLAMA_ENDPOINT", "http://remote:11434");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_OLLAMA_ENDPOINT");

        assert_eq!(config.ollama.endpoint, "http://remote:11434");
    }

    #[test]
    fn test_loader_returns_custom_config() {
        let mut custom = Config::for_test();
        custom.ollama.endpoint = "http://custom:9999".to_string();

        let loader = TestLoader(custom);
        let config = Config::load_with(&loader);

        assert_eq!(config.ollama.endpoint, "http://custom:9999");
    }

    #[test]
    fn for_test_distributed_defaults() {
        let config = Config::for_test();
        assert_eq!(config.distributed.registry_addr, "http://0.0.0.0:9090");
        assert_eq!(config.distributed.harness_addr, "http://0.0.0.0:9091");
        assert_eq!(config.distributed.secret_addr, "http://0.0.0.0:9093");
        assert_eq!(config.distributed.workers.registry, 1);
        assert_eq!(config.distributed.workers.harness, 1);
        assert_eq!(config.distributed.workers.agent.container, 1);
        assert_eq!(config.distributed.workers.inference.ollama, 1);
        assert_eq!(config.distributed.workers.storage.object.sqlite, 1);
        assert_eq!(config.distributed.workers.storage.vector.sqlite, 1);
        assert_eq!(config.distributed.workers.dag_git, 1);
        assert_eq!(config.distributed.workers.session_viewer, 1);
    }

    #[test]
    fn env_override_distributed_registry_addr() {
        std::env::set_var("VLINDER_DISTRIBUTED_REGISTRY_ADDR", "http://remote:9090");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_DISTRIBUTED_REGISTRY_ADDR");

        assert_eq!(config.distributed.registry_addr, "http://remote:9090");
    }

    #[test]
    fn env_override_worker_counts() {
        std::env::set_var("VLINDER_WORKERS_AGENT_CONTAINER", "4");
        std::env::set_var("VLINDER_WORKERS_INFERENCE_OLLAMA", "2");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_WORKERS_AGENT_CONTAINER");
        std::env::remove_var("VLINDER_WORKERS_INFERENCE_OLLAMA");

        assert_eq!(config.distributed.workers.agent.container, 4);
        assert_eq!(config.distributed.workers.inference.ollama, 2);
    }

    #[test]
    fn env_override_queue_backend_to_nats() {
        std::env::set_var("VLINDER_QUEUE_BACKEND", "nats");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_QUEUE_BACKEND");

        assert_eq!(config.queue.backend, QueueBackend::Nats);
    }

    #[test]
    fn env_override_state_backend_to_grpc() {
        std::env::set_var("VLINDER_STATE_BACKEND", "grpc");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_STATE_BACKEND");

        assert_eq!(config.state.backend, StateBackend::Grpc);
    }

    #[test]
    fn lambda_vpc_defaults_to_empty() {
        let config = Config::for_test();
        assert!(config.runtime.lambda_vpc_subnet_ids.is_empty());
        assert!(config.runtime.lambda_vpc_security_group_ids.is_empty());
    }

    #[test]
    fn env_override_lambda_vpc_subnet_ids() {
        std::env::set_var(
            "VLINDER_RUNTIME_LAMBDA_VPC_SUBNET_IDS",
            "subnet-aaa, subnet-bbb",
        );
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_RUNTIME_LAMBDA_VPC_SUBNET_IDS");

        assert_eq!(
            config.runtime.lambda_vpc_subnet_ids,
            vec!["subnet-aaa", "subnet-bbb"]
        );
    }

    #[test]
    fn env_override_lambda_vpc_security_group_ids() {
        std::env::set_var("VLINDER_RUNTIME_LAMBDA_VPC_SECURITY_GROUP_IDS", "sg-xxx");
        let mut config = Config::for_test();
        config.apply_env_overrides();
        std::env::remove_var("VLINDER_RUNTIME_LAMBDA_VPC_SECURITY_GROUP_IDS");

        assert_eq!(config.runtime.lambda_vpc_security_group_ids, vec!["sg-xxx"]);
    }
}
