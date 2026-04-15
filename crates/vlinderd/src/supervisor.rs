//! Supervisor - task manager for distributed mode.
//!
//! The Supervisor owns worker tokio tasks. It spawns them based on config,
//! monitors their lifecycle, and terminates them on shutdown.
//!
//! This is purely a task manager — it has no domain objects (no registry,
//! no harness, no queue). Workers are self-contained async loops that connect
//! to NATS and gRPC independently.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;

use crate::config::Config;
use crate::worker_async;
use crate::worker_role::WorkerRole;
#[cfg(any(feature = "ollama", feature = "openrouter"))]
use vlinder_catalog::catalog_service::ping_catalog_service_async;
use vlinder_harness::harness_service::ping_harness_async;
use vlinder_nats::secret_service::ping_secret_service_async;
use vlinder_sql_registry::registry_service::ping_registry_async;
use vlinder_sql_state::state_service::ping_state_service_async;

/// Task manager for distributed worker tasks.
pub struct Supervisor {
    handles: Vec<JoinHandle<()>>,
}

/// Whether a service health check failure should abort startup.
enum HealthCheckPolicy {
    Fatal,
    Warn,
}

/// Ensure an address has the `http://` scheme prefix.
fn ensure_http(addr: &str) -> String {
    if addr.starts_with("http://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}

/// Wait for a gRPC service to become ready, polling with the given ping function.
/// Returns the version if ready, or None if the deadline is exceeded.
async fn wait_for_service<F>(addr: &str, service_name: &str, ping: F, policy: HealthCheckPolicy)
where
    F: Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(u32, u32, u32)>>>>,
{
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut version = None;

    while Instant::now() < deadline {
        if let Some(v) = ping(addr.to_string()).await {
            version = Some(v);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    match (version, policy) {
        (Some((major, minor, patch)), _) => {
            tracing::info!(
                addr = %addr,
                version = %format!("{major}.{minor}.{patch}"),
                "{service_name} is ready"
            );
        }
        (None, HealthCheckPolicy::Fatal) => {
            tracing::error!(addr = %addr, "{service_name} did not become ready within 10s — aborting");
            std::process::exit(1);
        }
        (None, HealthCheckPolicy::Warn) => {
            tracing::warn!(
                addr = %addr,
                "{service_name} did not become ready within 10s"
            );
        }
    }
}

impl Supervisor {
    /// Spawn worker tasks based on config.
    #[allow(clippy::too_many_lines)]
    pub async fn new(config: &Config, shutdown: Arc<AtomicBool>) -> Self {
        let counts = &config.distributed.workers;
        let mut handles: Vec<JoinHandle<()>> = Vec::new();

        macro_rules! spawn_n {
            ($role:expr, $count:expr) => {
                for _ in 0..$count {
                    handles.push(tokio::spawn(worker_async::run_worker_loop(
                        $role,
                        Arc::clone(&shutdown),
                    )));
                }
            };
        }

        // Secret service must start first — registry needs secrets for agent identity.
        spawn_n!(WorkerRole::Secret, 1);
        wait_for_service(
            &ensure_http(&config.distributed.secret_addr),
            "Secret service",
            |a: String| Box::pin(async move { ping_secret_service_async(&a).await }),
            HealthCheckPolicy::Warn,
        )
        .await;

        // State service — must start before registry (registry's RecordingQueue
        // connects to it for DAG recording).
        spawn_n!(WorkerRole::State, 1);
        wait_for_service(
            &ensure_http(&config.distributed.state_addr),
            "State service",
            |a: String| Box::pin(async move { ping_state_service_async(&a).await }),
            HealthCheckPolicy::Warn,
        )
        .await;

        // Registry — depends on secret service and state service.
        spawn_n!(WorkerRole::Registry, counts.registry);
        if counts.registry > 0 {
            wait_for_service(
                &ensure_http(&config.distributed.registry_addr),
                "Registry",
                |a: String| Box::pin(async move { ping_registry_async(&a).await }),
                HealthCheckPolicy::Fatal,
            )
            .await;
        }

        // Catalog service — model catalog queries.
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        {
            spawn_n!(WorkerRole::Catalog, 1);
            wait_for_service(
                &ensure_http(&config.distributed.catalog_addr),
                "Catalog service",
                |a: String| Box::pin(async move { ping_catalog_service_async(&a).await }),
                HealthCheckPolicy::Warn,
            )
            .await;
        }

        // Harness — gRPC bridge for CLI→daemon agent invocation.
        spawn_n!(WorkerRole::Harness, counts.harness);
        if counts.harness > 0 {
            wait_for_service(
                &ensure_http(&config.distributed.harness_addr),
                "Harness",
                |a: String| Box::pin(async move { ping_harness_async(&a).await }),
                HealthCheckPolicy::Fatal,
            )
            .await;
        }

        // Agent runtimes
        #[cfg(feature = "container")]
        spawn_n!(WorkerRole::AgentContainer, counts.agent.container);
        #[cfg(feature = "lambda")]
        spawn_n!(WorkerRole::AgentLambda, counts.agent.lambda);

        // Inference workers
        #[cfg(feature = "ollama")]
        spawn_n!(WorkerRole::InferenceOllama, counts.inference.ollama);
        #[cfg(feature = "openrouter")]
        spawn_n!(WorkerRole::InferenceOpenRouter, counts.inference.openrouter);

        // Storage workers
        #[cfg(feature = "sqlite-kv")]
        spawn_n!(
            WorkerRole::StorageObjectSqlite,
            counts.storage.object.sqlite
        );
        #[cfg(feature = "sqlite-vec")]
        spawn_n!(
            WorkerRole::StorageVectorSqlite,
            counts.storage.vector.sqlite
        );

        // Infra plane worker
        spawn_n!(WorkerRole::Infra, counts.infra);

        // DAG git worker
        spawn_n!(WorkerRole::DagGit, counts.dag_git);

        // Session viewer
        spawn_n!(WorkerRole::SessionViewer, counts.session_viewer);

        tracing::info!(
            task_count = handles.len(),
            "Supervisor started in distributed mode"
        );

        Self { handles }
    }

    /// Abort all worker tasks.
    pub fn shutdown(self) {
        for handle in &self.handles {
            tracing::debug!("Aborting worker task");
            handle.abort();
        }

        tracing::info!("Supervisor shutdown complete");
    }
}
