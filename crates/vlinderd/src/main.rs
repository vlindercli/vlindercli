//! vlinderd — the Vlinder daemon process.
//!
//! Routes to one of two modes:
//! - Worker: if `VLINDER_WORKER_ROLE` is set
//! - Supervisor: spawns and manages worker processes

use tokio_util::sync::CancellationToken;

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

    let token = CancellationToken::new();
    let token_clone = token.clone();

    ctrlc::set_handler(move || {
        tracing::info!("Received shutdown signal");
        token_clone.cancel();
    })
    .expect("Failed to set signal handler");

    worker_async::run_worker_loop(role, token).await;
}

async fn run_as_supervisor(config: &Config) {
    tracing::info!("Starting vlinder supervisor (distributed mode)");

    let token = CancellationToken::new();
    let token_clone = token.clone();

    ctrlc::set_handler(move || {
        tracing::info!("Received shutdown signal");
        token_clone.cancel();
    })
    .expect("Failed to set signal handler");

    let supervisor = Supervisor::new(config, token.clone()).await;

    // Wait for shutdown signal
    token.cancelled().await;

    supervisor.shutdown();
    tracing::info!("Supervisor stopped");
}
