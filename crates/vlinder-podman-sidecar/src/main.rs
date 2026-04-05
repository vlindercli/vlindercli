//! vlinder-podman-sidecar — standalone binary running alongside agent containers.
//!
//! Deployed as an OCI container in a Podman pod. Reads configuration from
//! environment variables, connects to NATS and the registry, waits for the
//! agent container to be healthy, then enters the dispatch loop.

mod config;
mod dispatch;
mod health;
mod sidecar;

use config::SidecarConfig;
use sidecar::Sidecar;

fn main() {
    // Tracing — filter external crates to warn, show sidecar at info+
    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "warn,vlinder_podman_sidecar=info,vlinder_dolt=info".to_string());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match SidecarConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to parse sidecar config from env");
            std::process::exit(1);
        }
    };

    tracing::info!(
        event = "sidecar.config",
        agent = %config.agent,
        nats_url = %config.nats_url,
        registry_url = %config.registry_url,
        state_url = %config.state_url,
        container_port = config.container_port,
        "Sidecar configuration loaded"
    );

    let sidecar = match Sidecar::new(&config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "Failed to initialize sidecar");
            std::process::exit(1);
        }
    };

    if let Err(e) = sidecar.run() {
        tracing::error!(error = %e, "Sidecar exited with error");
        std::process::exit(1);
    }
}
