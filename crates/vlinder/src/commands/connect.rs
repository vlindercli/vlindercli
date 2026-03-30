//! Shared connection helpers for CLI commands.
//!
//! Provides gRPC client construction for registry, harness, and DAG store.
//! Each helper pings the service first and exits with a clear error on failure.

use std::sync::Arc;

use crate::config::CliConfig;
use vlinder_core::domain::{DagStore, Harness, Registry};
use vlinder_harness::harness_service::{ping_harness, GrpcHarnessClient};
use vlinder_sql_registry::registry_service::{ping_registry, GrpcRegistryClient};
use vlinder_sql_state::state_service::GrpcStateClient;

/// Connect to the registry via gRPC, exiting on failure.
pub fn connect_registry(config: &CliConfig) -> Arc<dyn Registry> {
    let registry_addr = normalize_addr(&config.daemon.registry_addr);

    if ping_registry(&registry_addr).is_none() {
        eprintln!("Cannot reach registry at {registry_addr}. Is the daemon running?");
        std::process::exit(1);
    }

    Arc::new(GrpcRegistryClient::connect(&registry_addr).expect("Failed to connect to registry"))
}

/// Connect to the registry via gRPC, returning None on failure.
pub fn open_registry(config: &CliConfig) -> Option<Arc<dyn Registry>> {
    let registry_addr = normalize_addr(&config.daemon.registry_addr);

    if ping_registry(&registry_addr).is_none() {
        eprintln!("Cannot reach registry at {registry_addr}. Is the daemon running?");
        return None;
    }

    match GrpcRegistryClient::connect(&registry_addr) {
        Ok(client) => Some(Arc::new(client)),
        Err(e) => {
            eprintln!("Failed to connect to registry: {e}");
            None
        }
    }
}

/// Connect to the harness via gRPC, exiting on failure.
pub fn connect_harness(config: &CliConfig) -> Box<dyn Harness> {
    let harness_addr = normalize_addr(&config.daemon.harness_addr);

    if ping_harness(&harness_addr).is_none() {
        eprintln!("Cannot reach harness at {harness_addr}. Is the daemon running?");
        std::process::exit(1);
    }

    Box::new(GrpcHarnessClient::connect(&harness_addr).expect("Failed to connect to harness"))
}

/// Open the `DagStore` via gRPC state service.
pub fn open_dag_store(config: &CliConfig) -> Option<Box<dyn DagStore>> {
    let state_addr = normalize_addr(&config.daemon.state_addr);
    match GrpcStateClient::connect(&state_addr) {
        Ok(client) => Some(Box::new(client)),
        Err(e) => {
            tracing::warn!(error = %e, "Failed to connect to state service, skipping state read");
            None
        }
    }
}

/// Ensure an address has an http:// or https:// scheme.
pub(super) fn normalize_addr(addr: &str) -> String {
    if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    }
}
