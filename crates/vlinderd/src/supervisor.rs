//! Supervisor - process manager for distributed mode.
//!
//! The Supervisor owns worker child processes. It spawns them based on config,
//! monitors their lifecycle, and terminates them on shutdown.
//!
//! This is purely a process manager — it has no domain objects (no registry,
//! no harness, no queue). Workers are self-contained processes that connect
//! to NATS and gRPC independently.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::worker_role::WorkerRole;
#[cfg(any(feature = "ollama", feature = "openrouter"))]
use vlinder_catalog::catalog_service::ping_catalog_service;
use vlinder_harness::harness_service::ping_harness;
use vlinder_nats::secret_service::ping_secret_service;
use vlinder_sql_registry::registry_service::ping_registry;
use vlinder_sql_state::state_service::ping_state_service;

/// Process manager for distributed worker processes.
pub struct Supervisor {
    workers: Vec<Child>,
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
fn wait_for_service(
    addr: &str,
    service_name: &str,
    ping: impl Fn(&str) -> Option<(u32, u32, u32)>,
    policy: HealthCheckPolicy,
    workers: &mut [Child],
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut version = None;

    while Instant::now() < deadline {
        if let Some(v) = ping(addr) {
            version = Some(v);
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
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
            tracing::error!(addr = %addr, "{service_name} did not become ready within 10s");
            for child in workers.iter_mut() {
                let _ = child.kill();
            }
            panic!("{service_name} failed to start — aborting distributed mode");
        }
        (None, HealthCheckPolicy::Warn) => {
            tracing::warn!(
                addr = %addr,
                "{service_name} did not become ready within 10s"
            );
        }
    }
}

/// Spawn `count` workers of the given role.
fn spawn_n(workers: &mut Vec<Child>, role: WorkerRole, count: u32) {
    for _ in 0..count {
        if let Some(child) = spawn_worker(role) {
            workers.push(child);
        }
    }
}

impl Supervisor {
    /// Spawn worker processes based on config.
    #[allow(clippy::too_many_lines)]
    pub fn new(config: &Config) -> Self {
        let counts = &config.distributed.workers;
        let mut workers = Vec::new();

        // Secret service must start first — registry needs secrets for agent identity.
        spawn_n(&mut workers, WorkerRole::Secret, 1);
        wait_for_service(
            &ensure_http(&config.distributed.secret_addr),
            "Secret service",
            ping_secret_service,
            HealthCheckPolicy::Warn,
            &mut workers,
        );

        // State service — must start before registry (registry's RecordingQueue
        // connects to it for DAG recording).
        spawn_n(&mut workers, WorkerRole::State, 1);
        wait_for_service(
            &ensure_http(&config.distributed.state_addr),
            "State service",
            ping_state_service,
            HealthCheckPolicy::Warn,
            &mut workers,
        );

        // Registry — depends on secret service and state service.
        spawn_n(&mut workers, WorkerRole::Registry, counts.registry);
        if counts.registry > 0 {
            wait_for_service(
                &ensure_http(&config.distributed.registry_addr),
                "Registry",
                ping_registry,
                HealthCheckPolicy::Fatal,
                &mut workers,
            );
        }

        // Catalog service — model catalog queries.
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        {
            spawn_n(&mut workers, WorkerRole::Catalog, 1);
            wait_for_service(
                &ensure_http(&config.distributed.catalog_addr),
                "Catalog service",
                ping_catalog_service,
                HealthCheckPolicy::Warn,
                &mut workers,
            );
        }

        // Harness — gRPC bridge for CLI→daemon agent invocation.
        spawn_n(&mut workers, WorkerRole::Harness, counts.harness);
        if counts.harness > 0 {
            wait_for_service(
                &ensure_http(&config.distributed.harness_addr),
                "Harness",
                ping_harness,
                HealthCheckPolicy::Fatal,
                &mut workers,
            );
        }

        // Agent runtimes
        #[cfg(feature = "container")]
        spawn_n(
            &mut workers,
            WorkerRole::AgentContainer,
            counts.agent.container,
        );
        #[cfg(feature = "lambda")]
        spawn_n(&mut workers, WorkerRole::AgentLambda, counts.agent.lambda);

        // Inference workers
        #[cfg(feature = "ollama")]
        spawn_n(
            &mut workers,
            WorkerRole::InferenceOllama,
            counts.inference.ollama,
        );
        #[cfg(feature = "openrouter")]
        spawn_n(
            &mut workers,
            WorkerRole::InferenceOpenRouter,
            counts.inference.openrouter,
        );

        // Storage workers
        #[cfg(feature = "sqlite-kv")]
        spawn_n(
            &mut workers,
            WorkerRole::StorageObjectSqlite,
            counts.storage.object.sqlite,
        );
        #[cfg(feature = "sqlite-vec")]
        spawn_n(
            &mut workers,
            WorkerRole::StorageVectorSqlite,
            counts.storage.vector.sqlite,
        );
        #[cfg(feature = "dolt")]
        spawn_n(
            &mut workers,
            WorkerRole::StorageSqlDolt,
            counts.storage.sql.dolt,
        );

        // Infra plane worker
        spawn_n(&mut workers, WorkerRole::Infra, counts.infra);

        // DAG git worker
        spawn_n(&mut workers, WorkerRole::DagGit, counts.dag_git);

        // Session viewer
        spawn_n(
            &mut workers,
            WorkerRole::SessionViewer,
            counts.session_viewer,
        );

        tracing::info!(
            worker_count = workers.len(),
            "Supervisor started in distributed mode"
        );

        Self { workers }
    }

    /// Terminate all workers and wait for them to exit.
    pub fn shutdown(&mut self) {
        for child in &mut self.workers {
            tracing::debug!(pid = child.id(), "Terminating worker");
            let _ = child.kill();
        }

        for child in &mut self.workers {
            let _ = child.wait();
        }

        self.workers.clear();
        tracing::info!("Supervisor shutdown complete");
    }
}

/// Spawn a single worker process with the given role.
fn spawn_worker(role: WorkerRole) -> Option<Child> {
    let exe = std::env::current_exe().ok()?;

    tracing::debug!(role = %role, "Spawning worker");

    match Command::new(exe)
        .env("VLINDER_WORKER_ROLE", role.as_env_value())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(child) => {
            tracing::info!(role = %role, pid = child.id(), "Worker spawned");
            Some(child)
        }
        Err(e) => {
            tracing::error!(role = %role, error = ?e, "Failed to spawn worker");
            None
        }
    }
}
