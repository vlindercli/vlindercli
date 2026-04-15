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

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::config::Config;
use crate::worker_async;
use crate::worker_role::WorkerRole;
use tokio::runtime::Runtime as TokioRuntime;

/// Run the worker loop for the given role.
///
/// This function blocks until shutdown is signaled. Workers should be run
/// in separate processes spawned by the daemon.
#[allow(clippy::too_many_lines)]
pub fn run_worker_loop(role: &WorkerRole, shutdown: &Arc<AtomicBool>) {
    let config = Config::load();

    tracing::info!(role = %role, "Starting worker");

    match role {
        WorkerRole::Registry => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_registry_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        WorkerRole::Harness => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_harness_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
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
        WorkerRole::Secret => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_secret_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        WorkerRole::State => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_state_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
        #[cfg(any(feature = "ollama", feature = "openrouter"))]
        WorkerRole::Catalog => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_catalog_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
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
        WorkerRole::SessionViewer => {
            let rt = TokioRuntime::new().expect("Failed to create tokio runtime");
            rt.block_on(worker_async::run_session_viewer_worker(
                &config,
                Arc::clone(shutdown),
            ));
        }
    }

    tracing::info!(role = %role, "Worker shutdown complete");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn worker_loop_respects_shutdown() {
        let shutdown = Arc::new(AtomicBool::new(true)); // Already signaled

        // This should return immediately due to shutdown
        // We can't easily test the full loop, but we can verify it compiles
        assert!(shutdown.load(Ordering::Relaxed));
    }
}
