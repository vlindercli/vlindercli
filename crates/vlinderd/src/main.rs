//! vlinderd — the Vlinder daemon process.
//!
//! Routes to one of two modes:
//! - Worker: if `VLINDER_WORKER_ROLE` is set
//! - Supervisor: spawns and manages worker processes

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use vlinderd::config::Config;
use vlinderd::supervisor::Supervisor;
use vlinderd::worker_async;
use vlinderd::worker_role::WorkerRole;

#[tokio::main]
async fn main() {
    // Both async-nats (ring) and AWS SDK (aws-lc-rs) activate rustls crypto
    // providers. With both present, rustls can't auto-select — pick ring.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let config = Config::load();
    vlinderd::tracing_setup::init_tracing(&config);

    if let Some(role) = WorkerRole::from_env() {
        run_as_worker(role).await;
    } else {
        run_as_supervisor(&config).await;
    }
}

async fn run_as_worker(role: WorkerRole) {
    tracing::info!(role = %role, "Starting as worker process");

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    ctrlc::set_handler(move || {
        tracing::info!("Received shutdown signal");
        shutdown_clone.store(true, Ordering::Relaxed);
    })
    .expect("Failed to set signal handler");

    worker_async::run_worker_loop(&role, &shutdown).await;
}

async fn run_as_supervisor(config: &Config) {
    tracing::info!("Starting vlinder supervisor (distributed mode)");

    let mut supervisor = Supervisor::new(config).await;

    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_clone = Arc::clone(&shutdown);

    ctrlc::set_handler(move || {
        tracing::info!("Received shutdown signal");
        shutdown_clone.store(true, Ordering::Relaxed);
    })
    .expect("Failed to set signal handler");

    // Wait for shutdown signal
    while !shutdown.load(Ordering::Relaxed) {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    supervisor.shutdown();
    tracing::info!("Supervisor stopped");
}
