//! Async worker entry points.
//!
//! Each worker runs as a tokio task: a `select!` loop that drives
//! `worker.tick()` until the `CancellationToken` fires.

use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::Config;

#[cfg(feature = "openrouter")]
pub async fn run_inference_openrouter_worker(config: &Config, shutdown: CancellationToken) {
    use vlinder_infer_openrouter::OpenRouterWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let worker = OpenRouterWorker::new(
        queue,
        config.openrouter.endpoint.clone(),
        config.openrouter.api_key.clone(),
    );

    tracing::info!(endpoint = %config.openrouter.endpoint, "OpenRouter inference worker ready");

    Arc::new(worker).run(shutdown).await;
}

#[cfg(feature = "ollama")]
pub async fn run_inference_ollama_worker(config: &Config, shutdown: CancellationToken) {
    use vlinder_ollama::OllamaWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let worker = OllamaWorker::new(queue, config.ollama.endpoint.clone());

    tracing::info!(endpoint = %config.ollama.endpoint, "Ollama inference worker ready");

    Arc::new(worker).run(shutdown).await;
}

#[cfg(feature = "sqlite-kv")]
pub async fn run_storage_object_sqlite_worker(config: &Config, shutdown: CancellationToken) {
    use vlinder_core::domain::{ObjectStorageType, ServiceBackend};
    use vlinder_sqlite_kv::KvWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");
    let worker = KvWorker::new(
        queue,
        registry,
        ServiceBackend::Kv(ObjectStorageType::Sqlite),
    );

    let registry_addr = &config.distributed.registry_addr;
    tracing::info!(registry = %registry_addr, "SQLite object storage worker ready");

    Arc::new(worker).run(shutdown).await;
}

#[cfg(feature = "sqlite-vec")]
pub async fn run_storage_vector_sqlite_worker(config: &Config, shutdown: CancellationToken) {
    use vlinder_core::domain::{ServiceBackend, VectorStorageType};
    use vlinder_sqlite_vec::SqliteVecWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");
    let worker = SqliteVecWorker::new(
        queue,
        registry,
        ServiceBackend::Vec(VectorStorageType::SqliteVec),
    );

    let registry_addr = &config.distributed.registry_addr;
    tracing::info!(registry = %registry_addr, "SQLite-vec vector storage worker ready");

    Arc::new(worker).run(shutdown).await;
}

#[cfg(feature = "container")]
pub async fn run_agent_container_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::Runtime;
    use vlinder_podman_runtime::{ContainerRuntime, PodmanRuntimeConfig};
    use vlinder_sql_state::SqliteDagStore;

    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");

    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::new(store);

    let queue_backend = match config.queue.backend {
        crate::config::QueueBackend::Nats => "nats",
        #[cfg(feature = "amqp")]
        crate::config::QueueBackend::Amqp => "amqp",
        #[cfg(any(test, feature = "test-support"))]
        crate::config::QueueBackend::Memory => "nats",
    };
    let podman_config = PodmanRuntimeConfig {
        image_policy: config.runtime.image_policy.clone(),
        podman_socket: config.runtime.podman_socket.clone(),
        sidecar_image: config.runtime.sidecar_image.clone(),
        queue_backend: queue_backend.to_string(),
        nats_url: config.queue.nats_url.clone(),
        amqp_url: config.queue.amqp_url.clone(),
        registry_addr: config.distributed.registry_addr.clone(),
        state_addr: config.distributed.state_addr.clone(),
        secret_addr: config.distributed.secret_addr.clone(),
    };

    let podman: Box<dyn vlinder_podman_runtime::PodmanClient> = if let Some(path) =
        vlinder_podman_runtime::resolve_socket(&config.runtime.podman_socket)
    {
        tracing::info!(event = "podman.socket", path = %path.display(), "Using Podman socket API");
        Box::new(vlinder_podman_runtime::PodmanApiClient::new(&path))
    } else {
        tracing::info!(event = "podman.cli", "Using Podman CLI");
        Box::new(vlinder_podman_runtime::PodmanCliClient)
    };

    let engine_version = podman.engine_version().await;
    if let Some(ref v) = engine_version {
        tracing::info!(event = "podman.detected", version = %v, "Podman engine detected");
    } else {
        tracing::warn!(
            event = "podman.not_found",
            "Podman not detected — container runtime degraded"
        );
    }

    let mut runtime = ContainerRuntime::new(&podman_config, registry, repo, podman);

    tracing::info!("Container agent worker ready");

    loop {
        tokio::select! {
            _ = runtime.tick() => {}
            () = shutdown.cancelled() => break,
        }
    }

    runtime.shutdown().await;
}

#[allow(clippy::too_many_lines)]
pub async fn run_infra_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::{AgentName, QueueError, ReadinessCheck, RegistryRepository};
    use vlinder_sql_state::SqliteDagStore;

    let queue = crate::queue_factory::from_config_async(config)
        .await
        .expect("Failed to create queue for infra worker");

    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn RegistryRepository> = Arc::new(store);

    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");

    tracing::info!("Infra plane worker ready");

    loop {
        tokio::select! {
            result = queue.receive_deploy_agent() => {
                match result {
                    Ok((_key, deploy_msg, ack)) => {
                        let _ = ack().await;
                        let agent_name = deploy_msg.manifest.name.clone();
                        tracing::info!(agent = %agent_name, "Processing deploy-agent");

                        let manifest = deploy_msg.manifest.clone();
                        let agent_result = vlinder_core::domain::Agent::from_manifest(manifest)
                            .map_err(|e| {
                                vlinder_core::domain::RegistrationError::Persistence(
                                    format!("{e:?}"),
                                )
                            });
                        let reg_result = match agent_result {
                            Ok(agent) => registry.register_agent(agent).await,
                            Err(e) => Err(e),
                        };

                        match reg_result {
                            Ok(()) => {
                                let name = AgentName::new(&agent_name);
                                if let Err(e) = queue.on_agent_deployed(&name).await {
                                    tracing::warn!(agent = %agent_name, error = %e, "Failed to provision agent queues");
                                }
                                tracing::info!(agent = %agent_name, "Agent registered, awaiting runtime provisioning");
                            }
                            Err(e) => {
                                let name = AgentName::new(&agent_name);
                                let check =
                                    ReadinessCheck::pending(name, "registry").failed(e.to_string());
                                let _ = repo.append_readiness_check(&check);
                                tracing::warn!(agent = %agent_name, error = %e, "Agent deploy failed");
                            }
                        }
                    }
                    Err(QueueError::Timeout) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "Infra worker deploy receive error");
                    }
                }
            }
            result = queue.receive_delete_agent() => {
                match result {
                    Ok((_key, delete_msg, ack)) => {
                        let _ = ack().await;
                        let agent_name = delete_msg.agent.as_str().to_string();
                        tracing::info!(agent = %agent_name, "Processing delete-agent");

                        let name = AgentName::new(&agent_name);
                        if let Err(e) = queue.on_agent_deleted(&name).await {
                            tracing::warn!(agent = %agent_name, error = %e, "Failed to deprovision agent queues");
                        }
                        let check = ReadinessCheck::pending(name, "registry").deleted();
                        let _ = repo.append_readiness_check(&check);

                        tracing::info!(agent = %agent_name, "Agent marked for deletion, awaiting runtime teardown");
                    }
                    Err(QueueError::Timeout) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, "Infra worker delete receive error");
                    }
                }
            }
            () = shutdown.cancelled() => break,
        }
    }
}

#[allow(clippy::too_many_lines)]
pub async fn run_dag_git_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::conversations_dir;
    use vlinder_core::domain::{DagWorker, QueueError};
    use vlinder_git_dag::GitDagWorker;
    use vlinder_nats::{
        complete_parse_subject, delete_agent_parse_subject, deploy_agent_parse_subject,
        fork_parse_subject, invoke_parse_subject, promote_parse_subject, request_parse_subject,
        response_parse_subject,
    };

    let queue = crate::queue_factory::from_config_async(config)
        .await
        .expect("Failed to create queue for DAG git worker");

    let repo_path = conversations_dir();
    let mut git_worker = GitDagWorker::open(&repo_path, &config.distributed.registry_addr, None)
        .expect("Failed to open git DAG repo");

    tracing::info!(git = %repo_path.display(), "DAG git worker ready");

    loop {
        tokio::select! {
            result = queue.receive_any() => {
                match result {
                    Ok((subject, payload, ack)) => {
                        tracing::debug!(subject = subject.as_str(), "DAG git received message");

                        let created_at = chrono::Utc::now();
                        if let Some(key) = complete_parse_subject(&subject) {
                            if let Ok(complete_msg) =
                                serde_json::from_slice::<vlinder_core::domain::CompleteMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_complete(&key, &complete_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize CompleteMessage"
                                );
                            }
                        } else if let Some(key) = request_parse_subject(&subject) {
                            if let Ok(request_msg) =
                                serde_json::from_slice::<vlinder_core::domain::RequestMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_request(&key, &request_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize RequestMessageV2"
                                );
                            }
                        } else if let Some(key) = response_parse_subject(&subject) {
                            if let Ok(response_msg) =
                                serde_json::from_slice::<vlinder_core::domain::ResponseMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_response(&key, &response_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize ResponseMessageV2"
                                );
                            }
                        } else if let Some(key) = invoke_parse_subject(&subject) {
                            if let Ok(invoke_msg) =
                                serde_json::from_slice::<vlinder_core::domain::InvokeMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_invoke(&key, &invoke_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize InvokeMessage"
                                );
                            }
                        } else if let Some(key) = fork_parse_subject(&subject) {
                            if let Ok(fork_msg) =
                                serde_json::from_slice::<vlinder_core::domain::ForkMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_fork(&key, &fork_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize ForkMessage"
                                );
                            }
                        } else if let Some(key) = promote_parse_subject(&subject) {
                            if let Ok(promote_msg) =
                                serde_json::from_slice::<vlinder_core::domain::PromoteMessage>(
                                    &payload,
                                )
                            {
                                git_worker.on_promote(&key, &promote_msg, created_at).await;
                            } else {
                                tracing::warn!(
                                    subject = subject.as_str(),
                                    "DAG git: failed to deserialize PromoteMessage"
                                );
                            }
                        } else if deploy_agent_parse_subject(&subject).is_some()
                            || delete_agent_parse_subject(&subject).is_some()
                        {
                            tracing::debug!(subject = subject.as_str(), "DAG git: infra plane event");
                        } else {
                            tracing::warn!(
                                subject = subject.as_str(),
                                "DAG git could not reconstruct message"
                            );
                        }

                        let _ = ack().await;
                    }
                    Err(QueueError::Timeout) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "DAG git fetch error");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
            () = shutdown.cancelled() => break,
        }
    }
}

pub async fn run_state_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use tonic::transport::Server;
    use vlinder_core::domain::DagStore;
    use vlinder_sql_state::state_service::StateServiceServer;
    use vlinder_sql_state::SqliteDagStore;

    let db_path = dag_db_path();
    let store =
        SqliteDagStore::open(&db_path).unwrap_or_else(|e| panic!("Failed to open DAG store: {e}"));
    let store: Arc<dyn DagStore> = Arc::new(store);

    let addr_str = config
        .distributed
        .state_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.state_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid state service address");

    tracing::info!(?addr, db = %db_path.display(), "Starting state gRPC server");

    let service = StateServiceServer::new(store).into_service();
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown.cancelled());
    if let Err(e) = server.await {
        tracing::error!(?e, "State server error");
    }
}

pub async fn run_secret_worker(config: &Config, shutdown: CancellationToken) {
    use tonic::transport::Server;
    use vlinder_nats::secret_service::SecretServer;

    let secret_store = crate::secret_store_factory::from_config_async(config)
        .await
        .unwrap_or_else(|e| panic!("Failed to open secret store: {e}"));

    let addr_str = config
        .distributed
        .secret_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.secret_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid secret service address");

    tracing::info!(?addr, "Starting secret store gRPC server");

    let service = SecretServer::new(secret_store).into_service();
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown.cancelled());
    if let Err(e) = server.await {
        tracing::error!(?e, "Secret store server error");
    }
}

#[cfg(any(feature = "ollama", feature = "openrouter"))]
pub async fn run_catalog_worker(config: &Config, shutdown: CancellationToken) {
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

    let catalog_names = composite.catalogs().await;
    tracing::info!(?addr, catalogs = ?catalog_names, "Starting catalog gRPC server");

    let service = CatalogServiceServer::new(Arc::new(composite)).into_service();
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown.cancelled());
    if let Err(e) = server.await {
        tracing::error!(?e, "Catalog server error");
    }
}

pub async fn run_harness_worker(config: &Config, shutdown: CancellationToken) {
    use tonic::transport::Server;
    use vlinder_core::domain::{CoreHarness, HarnessType};
    use vlinder_harness::harness_service::HarnessServer;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");
    let store = crate::state_factory::from_config_async(config)
        .await
        .expect("Failed to connect to state service");

    let harness = CoreHarness::new(queue, registry, store, HarnessType::Grpc);

    let addr_str = config
        .distributed
        .harness_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.harness_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid harness address");

    tracing::info!(?addr, registry = %config.distributed.registry_addr, "Starting harness gRPC server");

    let service = HarnessServer::new(Box::new(harness)).into_service();
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown.cancelled());
    if let Err(e) = server.await {
        tracing::error!(?e, "Harness server error");
    }
}

#[allow(clippy::too_many_lines)]
pub async fn run_registry_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use tonic::transport::Server;
    use vlinder_core::domain::{ObjectStorageType, Registry, RuntimeType, VectorStorageType};
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
        GrpcSecretClient::connect_async(&secret_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to connect to secret service: {e}")),
    );

    let db_path = dag_db_path();
    let store = Arc::new(
        SqliteDagStore::open(&db_path)
            .unwrap_or_else(|e| panic!("Failed to open state database: {e}")),
    );
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::clone(&store) as _;

    let queue: Arc<dyn vlinder_core::domain::MessageQueue + Send + Sync> =
        crate::queue_factory::recording_from_config_async(config)
            .await
            .expect("Failed to create queue for registry");

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
        .await
        .unwrap_or_else(|e| panic!("Failed to initialize registry: {e}"));

    registry.register_runtime(RuntimeType::Container);
    if config.distributed.workers.agent.lambda > 0 {
        registry.register_runtime(RuntimeType::Lambda);
    }
    registry.register_object_storage(ObjectStorageType::Sqlite);
    registry.register_object_storage(ObjectStorageType::S3);
    registry.register_vector_storage(VectorStorageType::SqliteVec);

    let registry: Arc<dyn vlinder_core::domain::Registry> = Arc::new(registry);

    let addr_str = config
        .distributed
        .registry_addr
        .strip_prefix("http://")
        .unwrap_or(&config.distributed.registry_addr);
    let addr: std::net::SocketAddr = addr_str.parse().expect("Invalid registry address");

    tracing::info!(?addr, "Starting registry gRPC server");

    let service = RegistryServer::new(registry, queue, Arc::clone(&store) as _).into_service();
    let server = Server::builder()
        .add_service(service)
        .serve_with_shutdown(addr, shutdown.cancelled());
    if let Err(e) = server.await {
        tracing::error!(?e, "Registry server error");
    }
}

#[cfg(feature = "lambda")]
pub async fn run_agent_lambda_worker(config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use vlinder_core::domain::Runtime;
    use vlinder_lambda_runtime::{LambdaRuntime, LambdaRuntimeConfig};
    use vlinder_sql_state::SqliteDagStore;

    let queue = crate::queue_factory::from_config_async(config)
        .await
        .expect("Failed to create queue for Lambda runtime");

    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");

    let db_path = dag_db_path();
    let store = SqliteDagStore::open(&db_path)
        .unwrap_or_else(|e| panic!("Failed to open state database: {e}"));
    let repo: Arc<dyn vlinder_core::domain::RegistryRepository> = Arc::new(store);

    let queue_backend = match config.queue.backend {
        crate::config::QueueBackend::Nats => "nats",
        #[cfg(feature = "amqp")]
        crate::config::QueueBackend::Amqp => "amqp",
        #[cfg(any(test, feature = "test-support"))]
        crate::config::QueueBackend::Memory => "nats",
    };

    let lambda_config = LambdaRuntimeConfig {
        registry_addr: config.distributed.registry_addr.clone(),
        region: config.runtime.lambda_region.clone(),
        memory_mb: config.runtime.lambda_memory_mb,
        timeout_secs: config.runtime.lambda_timeout_secs,
        queue_backend: queue_backend.to_string(),
        nats_url: config.queue.nats_url.clone(),
        amqp_url: config.queue.amqp_url.clone(),
        state_url: config.distributed.state_addr.clone(),
        secret_url: if config.distributed.secret_addr.is_empty() {
            None
        } else {
            Some(config.distributed.secret_addr.clone())
        },
        vpc_subnet_ids: config.runtime.lambda_vpc_subnet_ids.clone(),
        vpc_security_group_ids: config.runtime.lambda_vpc_security_group_ids.clone(),
    };

    let mut runtime = LambdaRuntime::new_async(&lambda_config, registry, repo, queue)
        .await
        .expect("Failed to create Lambda runtime");

    tracing::info!(
        region = config.runtime.lambda_region.as_str(),
        "Lambda agent worker ready"
    );

    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => { runtime.tick().await; }
            () = shutdown.cancelled() => break,
        }
    }
    runtime.shutdown().await;
}

pub async fn run_worker_loop(role: crate::worker_role::WorkerRole, shutdown: CancellationToken) {
    use crate::worker_role::WorkerRole;

    let config = Config::load();

    tracing::info!(role = %role, "Starting worker");

    match role {
        WorkerRole::Registry => run_registry_worker(&config, shutdown).await,
        WorkerRole::Harness => run_harness_worker(&config, shutdown).await,
        #[cfg(feature = "container")]
        WorkerRole::AgentContainer => {
            run_agent_container_worker(&config, shutdown).await;
        }
        #[cfg(feature = "lambda")]
        WorkerRole::AgentLambda => run_agent_lambda_worker(&config, shutdown).await,
        #[cfg(feature = "ollama")]
        WorkerRole::InferenceOllama => {
            run_inference_ollama_worker(&config, shutdown).await;
        }
        #[cfg(feature = "openrouter")]
        WorkerRole::InferenceOpenRouter => {
            run_inference_openrouter_worker(&config, shutdown).await;
        }
        #[cfg(feature = "sqlite-kv")]
        WorkerRole::StorageObjectSqlite => {
            run_storage_object_sqlite_worker(&config, shutdown).await;
        }
        #[cfg(feature = "sqlite-vec")]
        WorkerRole::StorageVectorSqlite => {
            run_storage_vector_sqlite_worker(&config, shutdown).await;
        }
        WorkerRole::Secret => run_secret_worker(&config, shutdown).await,
        WorkerRole::State => run_state_worker(&config, shutdown).await,
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        WorkerRole::Catalog => run_catalog_worker(&config, shutdown).await,
        WorkerRole::Infra => run_infra_worker(&config, shutdown).await,
        // DagGit is intentionally absent: GitDagWorker is !Sync (libgit2 raw
        // pointers), so its futures are !Send.  The supervisor runs it via
        // spawn_blocking + new_current_thread instead of tokio::spawn, and
        // calls run_dag_git_worker directly rather than going through this fn.
        WorkerRole::DagGit => {
            unreachable!("DagGit must be run via spawn_blocking — do not pass to run_worker_loop")
        }
        WorkerRole::SessionViewer => run_session_viewer_worker(&config, shutdown).await,
        WorkerRole::Mcp => todo!("MCP worker implementation in commit 6.4"),
    }

    tracing::info!(role = %role, "Worker shutdown complete");
}

pub async fn run_session_viewer_worker(_config: &Config, shutdown: CancellationToken) {
    use crate::config::dag_db_path;
    use vlinder_sql_state::{SessionServer, SqliteDagStore};

    let port = std::env::var("VLINDER_SESSION_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(7777u16);

    let store =
        SqliteDagStore::open(&dag_db_path()).expect("Failed to open DAG store for session viewer");
    let server = SessionServer::start(Arc::new(store), port)
        .await
        .expect("Failed to start session viewer");

    tracing::info!(
        port = server.port(),
        "Session viewer started: http://127.0.0.1:{}",
        server.port()
    );

    shutdown.cancelled().await;
    server.stop().await;
}
