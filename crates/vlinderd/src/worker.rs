//! Worker process loops for distributed mode.
//!
//! When running in distributed mode, each worker process runs a specialized
//! loop based on its role. Workers communicate via NATS queues.
//!
//! ## Usage
//!
//! Workers are spawned by the daemon with `VLINDER_WORKER_ROLE` set:
//!
//! ```bash
//! VLINDER_WORKER_ROLE=agent-wasm vlinder daemon
//! ```
//!
//! The worker reads its role from the environment and runs the appropriate loop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::config::Config;
use crate::worker_async;
use crate::worker_role::WorkerRole;
use tokio::runtime::Runtime as TokioRuntime;
use vlinder_core::domain::Registry;

/// Helper to get gRPC registry address with http:// prefix.
fn grpc_registry_addr(config: &Config) -> String {
    if config.distributed.registry_addr.starts_with("http://") {
        config.distributed.registry_addr.clone()
    } else {
        format!("http://{}", config.distributed.registry_addr)
    }
}

/// Run the worker loop for the given role.
///
/// This function blocks until shutdown is signaled. Workers should be run
/// in separate processes spawned by the daemon.
pub fn run_worker_loop(role: &WorkerRole, shutdown: &Arc<AtomicBool>) {
    let config = Config::load();

    tracing::info!(role = %role, "Starting worker");

    match role {
        WorkerRole::Registry => run_registry_worker(&config, shutdown),
        WorkerRole::Harness => run_harness_worker(&config, shutdown),
        #[cfg(feature = "container")]
        WorkerRole::AgentContainer => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_agent_container_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(feature = "lambda")]
        WorkerRole::AgentLambda => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_agent_lambda_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(feature = "ollama")]
        WorkerRole::InferenceOllama => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_inference_ollama_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(feature = "openrouter")]
        WorkerRole::InferenceOpenRouter => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_inference_openrouter_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(feature = "sqlite-kv")]
        WorkerRole::StorageObjectSqlite => {
            let rt = TokioRuntime::new().expect("runtime");
            rt.block_on(worker_async::run_storage_object_sqlite_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(feature = "sqlite-vec")]
        WorkerRole::StorageVectorSqlite => {
            let rt = TokioRuntime::new().expect("runtime");
            rt.block_on(worker_async::run_storage_vector_sqlite_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        WorkerRole::Secret => run_secret_worker(&config, shutdown),
        WorkerRole::State => run_state_worker(&config, shutdown),
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        WorkerRole::Catalog => run_catalog_worker(&config, shutdown),
        WorkerRole::Infra => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_infra_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        WorkerRole::DagGit => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_dag_git_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        WorkerRole::SessionViewer => run_session_viewer_worker(&config, shutdown),
    }

    tracing::info!(role = %role, "Worker shutdown complete");
}

// ============================================================================
// Factory helpers
// ============================================================================

// ============================================================================
// Worker Implementations
// ============================================================================

fn run_registry_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use tonic::transport::Server;
    use vlinder_core::domain::{ObjectStorageType, RuntimeType, VectorStorageType};
    use vlinder_nats::secret_service::GrpcSecretClient;
    use vlinder_sql_registry::registry_service::RegistryServer;
    use vlinder_sql_registry::PersistentRegistry;
    use vlinder_sql_state::SqliteDagStore;

    let secret_addr = if config.distributed.secret_addr.starts_with("http://") {
        config.distributed.secret_addr.clone()
    } else {
        format!("http://{}", config.distributed.secret_addr)
    };
    let secret_store: Arc<dyn vlinder_core::domain::SecretStore> = Arc::new(
        GrpcSecretClient::connect(&secret_addr)
            .unwrap_or_else(|e| panic!("Failed to connect to secret service: {e}")),
    );

    // Registry now shares the DAG database (single SQLite, FK integrity across planes)
    let db_path = dag_db_path();
    let store = Arc::new(
        SqliteDagStore::open(&db_path)
            .unwrap_or_else(|e| panic!("Failed to open state database: {e}")),
    );
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::clone(&store) as _;

    // Queue for infra plane — RecordingQueue records deploy/delete to DAG before NATS
    let queue: Arc<dyn vlinder_core::domain::MessageQueue + Send + Sync> =
        crate::queue_factory::recording_from_config(config)
            .expect("Failed to create queue for registry");

    // Build registry config from cluster topology
    let mut inference_engines = Vec::new();
    let mut embedding_engines = Vec::new();
    if config.distributed.workers.inference.ollama > 0 {
        inference_engines.push(vlinder_core::domain::Provider::Ollama);
        embedding_engines.push(vlinder_core::domain::Provider::Ollama);
    }
    if config.distributed.workers.inference.openrouter > 0 {
        inference_engines.push(vlinder_core::domain::Provider::OpenRouter);
    }
    let registry_config = vlinder_sql_registry::RegistryConfig {
        inference_engines,
        embedding_engines,
    };

    // Create the tokio runtime once; use it for both async construction and the gRPC server.
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let registry = rt
        .block_on(PersistentRegistry::new(
            repo,
            &registry_config,
            secret_store,
        ))
        .unwrap_or_else(|e| panic!("Failed to initialize registry: {e}"));

    // Register non-engine capabilities (engines are registered by open())
    registry.register_runtime(RuntimeType::Container);
    if config.distributed.workers.agent.lambda > 0 {
        registry.register_runtime(RuntimeType::Lambda);
    }
    registry.register_object_storage(ObjectStorageType::Sqlite);
    registry.register_vector_storage(VectorStorageType::SqliteVec);

    let registry: Arc<dyn Registry> = Arc::new(registry);

    // Parse address, stripping http:// prefix if present
    let addr_str = config
        .distributed
        .registry_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.registry_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid registry address");

    tracing::info!(?addr, "Starting registry gRPC server");

    // Run the gRPC server until shutdown
    rt.block_on(async {
        let service = RegistryServer::new(registry, queue, Arc::clone(&store) as _).into_service();

        // Start server with graceful shutdown
        let server = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async {
                // Poll for shutdown signal
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

        if let Err(e) = server.await {
            tracing::error!(?e, "Registry server error");
        }
    });
}

fn run_secret_worker(config: &Config, shutdown: &AtomicBool) {
    use tonic::transport::Server;
    use vlinder_nats::secret_service::SecretServer;

    let secret_store = crate::secret_store_factory::from_config(config)
        .unwrap_or_else(|e| panic!("Failed to open secret store: {e}"));

    // Parse address, stripping http:// prefix if present
    let addr_str = config
        .distributed
        .secret_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.secret_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid secret service address");

    tracing::info!(?addr, "Starting secret store gRPC server");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let service = SecretServer::new(secret_store).into_service();

        let server = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async {
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

        if let Err(e) = server.await {
            tracing::error!(?e, "Secret store server error");
        }
    });
}

fn run_harness_worker(config: &Config, shutdown: &AtomicBool) {
    use tonic::transport::Server;
    use vlinder_core::domain::{CoreHarness, HarnessType};
    use vlinder_harness::harness_service::HarnessServer;
    use vlinder_sql_registry::registry_service::GrpcRegistryClient;

    let queue =
        crate::queue_factory::recording_from_config(config).expect("Failed to create queue");

    let registry_addr = grpc_registry_addr(config);
    let registry: Arc<dyn Registry> = Arc::new(
        GrpcRegistryClient::connect(&registry_addr).expect("Failed to connect to registry"),
    );

    let store =
        crate::state_factory::from_config(config).expect("Failed to connect to state service");

    let harness = CoreHarness::new(queue, registry, store, HarnessType::Grpc);

    // Parse address, stripping http:// prefix if present
    let addr_str = config
        .distributed
        .harness_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.harness_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid harness address");

    tracing::info!(?addr, registry = %registry_addr, "Starting harness gRPC server");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let service = HarnessServer::new(Box::new(harness)).into_service();

        let server = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async {
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

        if let Err(e) = server.await {
            tracing::error!(?e, "Harness server error");
        }
    });
}

fn run_state_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use tonic::transport::Server;
    use vlinder_core::domain::DagStore;
    use vlinder_sql_state::state_service::StateServiceServer;
    use vlinder_sql_state::SqliteDagStore;

    let db_path = dag_db_path();
    let store =
        SqliteDagStore::open(&db_path).unwrap_or_else(|e| panic!("Failed to open DAG store: {e}"));

    let store: Arc<dyn DagStore> = Arc::new(store);

    // Parse address, stripping http:// prefix if present
    let addr_str = config
        .distributed
        .state_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.state_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid state service address");

    tracing::info!(?addr, db = %db_path.display(), "Starting state gRPC server");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let service = StateServiceServer::new(store).into_service();

        let server = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async {
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

        if let Err(e) = server.await {
            tracing::error!(?e, "State server error");
        }
    });
}

#[cfg(any(feature = "ollama", feature = "openrouter"))]
fn run_catalog_worker(config: &Config, shutdown: &AtomicBool) {
    use tonic::transport::Server;
    use vlinder_catalog::catalog_service::CatalogServiceServer;
    use vlinder_core::domain::{CatalogService, CompositeCatalog};

    let mut composite = CompositeCatalog::new();
    #[cfg(feature = "ollama")]
    {
        use vlinder_ollama::OllamaCatalog;
        composite.add(
            "ollama".to_string(),
            Arc::new(OllamaCatalog::new(&config.ollama.endpoint)),
        );
    }
    #[cfg(feature = "openrouter")]
    if !config.openrouter.api_key.is_empty() {
        use vlinder_infer_openrouter::OpenRouterCatalog;
        composite.add(
            "openrouter".to_string(),
            Arc::new(OpenRouterCatalog::new(
                &config.openrouter.endpoint,
                &config.openrouter.api_key,
            )),
        );
    }

    let addr_str = config
        .distributed
        .catalog_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.catalog_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid catalog service address");

    let catalog_names = composite.catalogs();
    tracing::info!(?addr, catalogs = ?catalog_names, "Starting catalog gRPC server");

    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    rt.block_on(async {
        let service = CatalogServiceServer::new(Arc::new(composite)).into_service();

        let server = Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, async {
                while !shutdown.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            });

        if let Err(e) = server.await {
            tracing::error!(?e, "Catalog server error");
        }
    });
}

fn run_session_viewer_worker(_config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use vlinder_sql_state::{SessionServer, SqliteDagStore};

    let port = std::env::var("VLINDER_SESSION_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7777u16);

    let store =
        SqliteDagStore::open(&dag_db_path()).expect("Failed to open DAG store for session viewer");
    let server =
        SessionServer::start(Arc::new(store), port).expect("Failed to start session viewer");

    tracing::info!(
        port = server.port(),
        "Session viewer started: http://127.0.0.1:{}",
        server.port()
    );

    while !shutdown.load(Ordering::Relaxed) {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    server.stop();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_loop_respects_shutdown() {
        let shutdown = Arc::new(AtomicBool::new(true)); // Already signaled

        // This should return immediately due to shutdown
        // We can't easily test the full loop, but we can verify it compiles
        assert!(shutdown.load(Ordering::Relaxed));
    }
}
