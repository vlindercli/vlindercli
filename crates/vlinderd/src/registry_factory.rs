//! Registry factory — wires configuration to concrete Registry implementations.
//!
//! Follows the same pattern as `queue_factory` and `secret_store::from_config`.

use std::sync::Arc;

use crate::config::{Config, RegistryBackend};
use vlinder_core::domain::Registry;

/// Create a registry client from configuration, callable from within a Tokio runtime.
pub async fn from_config_async(
    config: &Config,
) -> Result<Arc<dyn Registry>, Box<dyn std::error::Error>> {
    match config.distributed.registry_backend {
        RegistryBackend::Grpc => {
            use vlinder_sql_registry::registry_service::GrpcRegistryClient;

            let addr = if config.distributed.registry_addr.starts_with("http://") {
                config.distributed.registry_addr.clone()
            } else {
                format!("http://{}", config.distributed.registry_addr)
            };

            let client = GrpcRegistryClient::connect_async(&addr).await?;
            Ok(Arc::new(client))
        }
        #[cfg(any(test, feature = "test-support"))]
        RegistryBackend::Memory => {
            use vlinder_core::domain::InMemoryRegistry;
            use vlinder_core::domain::InMemorySecretStore;

            let secret_store = Arc::new(InMemorySecretStore::new());
            Ok(Arc::new(InMemoryRegistry::new(secret_store)))
        }
    }
}
