//! State factory — wires configuration to concrete `DagStore` implementations.
//!
//! Follows the same pattern as `queue_factory`, `registry_factory`, and
//! `secret_store_factory`.

use std::sync::Arc;

use crate::config::{Config, StateBackend};
use vlinder_core::domain::DagStore;

/// Create a `DagStore` client from configuration.
///
/// Returns `GrpcStateClient` in production. In test builds, `Memory`
/// backend returns an `InMemoryDagStore` (no network required).
pub fn from_config(config: &Config) -> Result<Arc<dyn DagStore>, Box<dyn std::error::Error>> {
    match config.state.backend {
        StateBackend::Grpc => {
            use vlinder_sql_state::state_service::GrpcStateClient;

            let addr = if config.distributed.state_addr.starts_with("http://")
                || config.distributed.state_addr.starts_with("https://")
            {
                config.distributed.state_addr.clone()
            } else {
                format!("http://{}", config.distributed.state_addr)
            };

            let client = GrpcStateClient::connect(&addr)?;
            Ok(Arc::new(client))
        }
        #[cfg(any(test, feature = "test-support"))]
        StateBackend::Memory => {
            use vlinder_core::domain::InMemoryDagStore;
            Ok(Arc::new(InMemoryDagStore::new()))
        }
    }
}

/// Async variant of `from_config` — callable from within a Tokio runtime.
pub async fn from_config_async(
    config: &Config,
) -> Result<Arc<dyn DagStore>, Box<dyn std::error::Error>> {
    match config.state.backend {
        StateBackend::Grpc => {
            use vlinder_sql_state::state_service::GrpcStateClient;

            let addr = if config.distributed.state_addr.starts_with("http://")
                || config.distributed.state_addr.starts_with("https://")
            {
                config.distributed.state_addr.clone()
            } else {
                format!("http://{}", config.distributed.state_addr)
            };

            let client = GrpcStateClient::connect_async(&addr).await?;
            Ok(Arc::new(client))
        }
        #[cfg(any(test, feature = "test-support"))]
        StateBackend::Memory => {
            use vlinder_core::domain::InMemoryDagStore;
            Ok(Arc::new(InMemoryDagStore::new()))
        }
    }
}
