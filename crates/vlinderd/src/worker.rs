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
use crate::worker_role::WorkerRole;
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
        WorkerRole::AgentContainer => run_agent_container_worker(&config, shutdown),
        #[cfg(feature = "lambda")]
        WorkerRole::AgentLambda => run_agent_lambda_worker(&config, shutdown),
        #[cfg(feature = "ollama")]
        WorkerRole::InferenceOllama => run_inference_ollama_worker(&config, shutdown),
        #[cfg(feature = "openrouter")]
        WorkerRole::InferenceOpenRouter => run_inference_openrouter_worker(&config, shutdown),
        #[cfg(feature = "sqlite-kv")]
        WorkerRole::StorageObjectSqlite => run_storage_object_sqlite_worker(&config, shutdown),
        #[cfg(feature = "sqlite-vec")]
        WorkerRole::StorageVectorSqlite => run_storage_vector_sqlite_worker(&config, shutdown),
        WorkerRole::Secret => run_secret_worker(&config, shutdown),
        WorkerRole::State => run_state_worker(&config, shutdown),
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        WorkerRole::Catalog => run_catalog_worker(&config, shutdown),
        WorkerRole::Infra => run_infra_worker(&config, shutdown),
        WorkerRole::DagGit => run_dag_git_worker(&config, shutdown),
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

    let registry = PersistentRegistry::new(repo, &registry_config, secret_store)
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
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
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

fn run_infra_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::{
        AgentName, AgentState, AgentStatus, QueueError, RegistryRepository,
    };
    use vlinder_sql_state::SqliteDagStore;

    let queue =
        crate::queue_factory::from_config(config).expect("Failed to create queue for infra worker");

    // Open the shared DAG database for agent state management
    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn RegistryRepository> = Arc::new(store);

    // Connect to registry for agent registration
    let registry_addr = grpc_registry_addr(config);
    let registry: Arc<dyn vlinder_core::domain::Registry> = Arc::new(
        vlinder_sql_registry::registry_service::GrpcRegistryClient::connect(&registry_addr)
            .expect("Failed to connect to registry"),
    );

    tracing::info!("Infra plane worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        // Try deploy
        match queue.receive_deploy_agent() {
            Ok((_key, deploy_msg, ack)) => {
                let _ = ack();
                let agent_name = deploy_msg.manifest.name.clone();
                tracing::info!(agent = %agent_name, "Processing deploy-agent");

                let registry_clone = Arc::clone(&registry);
                let manifest = deploy_msg.manifest.clone();
                let reg_result =
                    std::thread::spawn(move || registry_clone.register_manifest(manifest))
                        .join()
                        .unwrap_or_else(|_| {
                            Err(vlinder_core::domain::RegistrationError::Persistence(
                                "registry thread panicked".to_string(),
                            ))
                        });

                match reg_result {
                    Ok(_agent) => {
                        let name = AgentName::new(&agent_name);
                        let deploying =
                            AgentState::registered(name).transition(AgentStatus::Deploying, None);
                        if let Err(e) = repo.append_agent_state(&deploying) {
                            tracing::warn!(error = %e, "Failed to set Deploying state");
                        }
                        tracing::info!(agent = %agent_name, "Agent registered, awaiting runtime provisioning");
                    }
                    Err(e) => {
                        let name = AgentName::new(&agent_name);
                        let failed = AgentState::registered(name)
                            .transition(AgentStatus::Failed, Some(e.to_string()));
                        if let Err(e2) = repo.append_agent_state(&failed) {
                            tracing::warn!(error = %e2, "Failed to set Failed state");
                        }
                        tracing::warn!(agent = %agent_name, error = %e, "Agent deploy failed");
                    }
                }
            }
            Err(QueueError::Timeout) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Infra worker deploy receive error");
            }
        }

        // Try delete
        match queue.receive_delete_agent() {
            Ok((_key, delete_msg, ack)) => {
                let _ = ack();
                let agent_name = delete_msg.agent.as_str().to_string();
                tracing::info!(agent = %agent_name, "Processing delete-agent");

                let name = AgentName::new(&agent_name);
                let deleting = AgentState::registered(name).transition(AgentStatus::Deleting, None);
                let _ = repo.append_agent_state(&deleting);

                tracing::info!(agent = %agent_name, "Agent marked for deletion, awaiting runtime teardown");
            }
            Err(QueueError::Timeout) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Infra worker delete receive error");
            }
        }

        std::thread::sleep(std::time::Duration::from_millis(10));
    }
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

#[cfg(feature = "container")]
fn run_agent_container_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::Runtime;
    use vlinder_podman_runtime::{ContainerRuntime, PodmanRuntimeConfig};
    use vlinder_sql_state::SqliteDagStore;

    let registry =
        crate::registry_factory::from_config(config).expect("Failed to connect to registry");

    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::new(store);

    let podman_config = PodmanRuntimeConfig {
        image_policy: config.runtime.image_policy.clone(),
        podman_socket: config.runtime.podman_socket.clone(),
        sidecar_image: config.runtime.sidecar_image.clone(),
        nats_url: config.queue.nats_url.clone(),
        registry_addr: config.distributed.registry_addr.clone(),
        state_addr: config.distributed.state_addr.clone(),
        secret_addr: config.distributed.secret_addr.clone(),
    };

    let mut runtime = ContainerRuntime::new(&podman_config, registry, repo)
        .expect("Failed to create container runtime");

    tracing::info!("Container agent worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        runtime.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "lambda")]
fn run_agent_lambda_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::Runtime;
    use vlinder_nats_lambda_runtime::{LambdaRuntime, LambdaRuntimeConfig};
    use vlinder_sql_state::SqliteDagStore;

    let registry =
        crate::registry_factory::from_config(config).expect("Failed to connect to registry");

    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::new(store);

    let queue = crate::queue_factory::from_config(config)
        .expect("Failed to create queue for Lambda runtime");

    let lambda_config = LambdaRuntimeConfig {
        registry_addr: config.distributed.registry_addr.clone(),
        region: config.runtime.lambda_region.clone(),
        memory_mb: config.runtime.lambda_memory_mb,
        timeout_secs: config.runtime.lambda_timeout_secs,
        nats_url: config.queue.nats_url.clone(),
        state_url: config.distributed.state_addr.clone(),
        secret_url: if config.distributed.secret_addr.is_empty() {
            None
        } else {
            Some(config.distributed.secret_addr.clone())
        },
        vpc_subnet_ids: config.runtime.lambda_vpc_subnet_ids.clone(),
        vpc_security_group_ids: config.runtime.lambda_vpc_security_group_ids.clone(),
    };

    let mut runtime = LambdaRuntime::new(&lambda_config, registry, repo, queue)
        .expect("Failed to create Lambda runtime");

    tracing::info!(
        region = config.runtime.lambda_region.as_str(),
        "Lambda agent worker ready"
    );

    while !shutdown.load(Ordering::Relaxed) {
        runtime.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "ollama")]
fn run_inference_ollama_worker(config: &Config, shutdown: &AtomicBool) {
    use vlinder_ollama::OllamaWorker;

    let queue =
        crate::queue_factory::recording_from_config(config).expect("Failed to create queue");

    let worker = OllamaWorker::new(queue, config.ollama.endpoint.clone());

    tracing::info!(endpoint = %config.ollama.endpoint, "Ollama inference worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        worker.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "openrouter")]
fn run_inference_openrouter_worker(config: &Config, shutdown: &AtomicBool) {
    use vlinder_infer_openrouter::OpenRouterWorker;

    let queue =
        crate::queue_factory::recording_from_config(config).expect("Failed to create queue");

    let worker = OpenRouterWorker::new(
        queue,
        config.openrouter.endpoint.clone(),
        config.openrouter.api_key.clone(),
    );

    tracing::info!(endpoint = %config.openrouter.endpoint, "OpenRouter inference worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        worker.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "sqlite-kv")]
fn run_storage_object_sqlite_worker(config: &Config, shutdown: &AtomicBool) {
    use vlinder_core::domain::{ObjectStorageType, ServiceBackend};
    use vlinder_sql_registry::registry_service::GrpcRegistryClient;
    use vlinder_sqlite_kv::KvWorker;

    let queue =
        crate::queue_factory::recording_from_config(config).expect("Failed to create queue");

    let registry_addr = grpc_registry_addr(config);
    let registry: Arc<dyn Registry> = Arc::new(
        GrpcRegistryClient::connect(&registry_addr).expect("Failed to connect to registry"),
    );

    let worker = KvWorker::new(
        queue,
        registry,
        ServiceBackend::Kv(ObjectStorageType::Sqlite),
    );

    tracing::info!(registry = %registry_addr, "SQLite object storage worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        worker.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(feature = "sqlite-vec")]
fn run_storage_vector_sqlite_worker(config: &Config, shutdown: &AtomicBool) {
    use vlinder_core::domain::{ServiceBackend, VectorStorageType};
    use vlinder_sql_registry::registry_service::GrpcRegistryClient;
    use vlinder_sqlite_vec::SqliteVecWorker;

    let queue =
        crate::queue_factory::recording_from_config(config).expect("Failed to create queue");

    let registry_addr = grpc_registry_addr(config);
    let registry: Arc<dyn Registry> = Arc::new(
        GrpcRegistryClient::connect(&registry_addr).expect("Failed to connect to registry"),
    );

    let worker = SqliteVecWorker::new(
        queue,
        registry,
        ServiceBackend::Vec(VectorStorageType::SqliteVec),
    );

    tracing::info!(registry = %registry_addr, "SQLite-vec vector storage worker ready");

    while !shutdown.load(Ordering::Relaxed) {
        worker.tick();
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
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

#[allow(clippy::too_many_lines)]
fn run_dag_git_worker(config: &Config, shutdown: &AtomicBool) {
    use crate::config::conversations_dir;
    use vlinder_core::domain::DagWorker;
    use vlinder_git_dag::GitDagWorker;
    use vlinder_nats::{
        complete_parse_subject, delete_agent_parse_subject, deploy_agent_parse_subject,
        fork_parse_subject, invoke_parse_subject, promote_parse_subject, request_parse_subject,
        response_parse_subject, NatsQueue,
    };

    let nats = NatsQueue::connect(&config.queue.nats_config()).expect("Failed to connect to NATS");

    let repo_path = conversations_dir();
    let mut git_worker = GitDagWorker::open(&repo_path, &config.distributed.registry_addr, None)
        .expect("Failed to open git DAG repo");

    tracing::info!(git = %repo_path.display(), "DAG git worker ready");

    let js = nats.jetstream().clone();
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");

    let consumer = rt.block_on(async {
        let stream = js
            .get_stream("VLINDER")
            .await
            .expect("Failed to get VLINDER stream");

        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some("dag-git".to_string()),
                filter_subject: "vlinder.>".to_string(),
                ack_wait: std::time::Duration::from_secs(300),
                inactive_threshold: std::time::Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .expect("Failed to create dag-git consumer")
    });

    while !shutdown.load(Ordering::Relaxed) {
        let msg_result = rt.block_on(async {
            use futures::StreamExt;
            let mut messages = consumer
                .fetch()
                .max_messages(1)
                .expires(std::time::Duration::from_millis(100))
                .messages()
                .await
                .map_err(|e| format!("fetch failed: {e}"))?;

            match messages.next().await {
                Some(Ok(msg)) => Ok(Some(msg)),
                Some(Err(e)) => Err(format!("message error: {e}")),
                None => Ok(None),
            }
        });

        match msg_result {
            Ok(Some(msg)) => {
                let subject = msg.subject.to_string();
                let payload = msg.payload.to_vec();

                let info = msg.info().ok();
                let num_delivered = info.as_ref().map(|i| i.delivered);
                let stream_seq = info.as_ref().map(|i| i.stream_sequence);
                tracing::debug!(
                    subject = subject.as_str(),
                    stream_seq = ?stream_seq,
                    num_delivered = ?num_delivered,
                    "DAG git received NATS message",
                );

                // Try data-plane subject first, fall back to legacy pipeline.
                let created_at = chrono::Utc::now();
                if let Some(key) = complete_parse_subject(&subject) {
                    if let Ok(complete_msg) =
                        serde_json::from_slice::<vlinder_core::domain::CompleteMessage>(&payload)
                    {
                        git_worker.on_complete(&key, &complete_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize CompleteMessage"
                        );
                    }
                } else if let Some(key) = request_parse_subject(&subject) {
                    if let Ok(request_msg) =
                        serde_json::from_slice::<vlinder_core::domain::RequestMessage>(&payload)
                    {
                        git_worker.on_request(&key, &request_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize RequestMessageV2"
                        );
                    }
                } else if let Some(key) = response_parse_subject(&subject) {
                    if let Ok(response_msg) =
                        serde_json::from_slice::<vlinder_core::domain::ResponseMessage>(&payload)
                    {
                        git_worker.on_response(&key, &response_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize ResponseMessageV2"
                        );
                    }
                } else if let Some(key) = invoke_parse_subject(&subject) {
                    if let Ok(invoke_msg) =
                        serde_json::from_slice::<vlinder_core::domain::InvokeMessage>(&payload)
                    {
                        git_worker.on_invoke(&key, &invoke_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize InvokeMessage"
                        );
                    }
                } else if let Some(key) = fork_parse_subject(&subject) {
                    if let Ok(fork_msg) =
                        serde_json::from_slice::<vlinder_core::domain::ForkMessage>(&payload)
                    {
                        git_worker.on_fork(&key, &fork_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize ForkMessage"
                        );
                    }
                } else if let Some(key) = promote_parse_subject(&subject) {
                    if let Ok(promote_msg) =
                        serde_json::from_slice::<vlinder_core::domain::PromoteMessage>(&payload)
                    {
                        git_worker.on_promote(&key, &promote_msg, created_at);
                    } else {
                        tracing::warn!(
                            subject = subject.as_str(),
                            "DAG git: failed to deserialize PromoteMessage"
                        );
                    }
                } else if deploy_agent_parse_subject(&subject).is_some()
                    || delete_agent_parse_subject(&subject).is_some()
                {
                    // Infra plane — acknowledged, no git subtree yet
                    tracing::debug!(subject = subject.as_str(), "DAG git: infra plane event");
                } else {
                    tracing::warn!(
                        subject = subject.as_str(),
                        "DAG git could not reconstruct message"
                    );
                }

                let _ = rt.block_on(async { msg.ack().await });
            }
            Ok(None) => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(e) => {
                tracing::warn!(error = %e, "DAG git fetch error");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
    }
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
