//! Secret store factory — wires configuration to concrete implementations.
//!
//! Follows the same pattern as `queue_factory`.

use crate::config::{Config, QueueBackend};
use std::sync::Arc;
use vlinder_core::domain::{SecretStore, SecretStoreError};
use vlinder_nats::NatsSecretStore;

/// Create a secret store from configuration.
///
/// Requires `backend = Nats` — secrets must persist across processes.
/// In test builds with `Memory` queue backend, returns an `InMemorySecretStore`.
///
/// Reuses the queue's `nats_url` config — same NATS cluster, different bucket.
pub fn from_config(config: &Config) -> Result<Arc<dyn SecretStore>, SecretStoreError> {
    match config.queue.backend {
        QueueBackend::Nats => {
            let store = NatsSecretStore::connect(&config.queue.nats_config())?;
            Ok(Arc::new(store))
        }
        #[cfg(feature = "amqp")]
        QueueBackend::Amqp => {
            // AMQP doesn't provide a secret store — use in-memory for now.
            use vlinder_core::domain::InMemorySecretStore;
            Ok(Arc::new(InMemorySecretStore::new()))
        }
        #[cfg(any(test, feature = "test-support"))]
        QueueBackend::Memory => {
            use vlinder_core::domain::InMemorySecretStore;
            Ok(Arc::new(InMemorySecretStore::new()))
        }
    }
}

/// Async variant of `from_config` — callable from within a Tokio runtime.
pub async fn from_config_async(config: &Config) -> Result<Arc<dyn SecretStore>, SecretStoreError> {
    match config.queue.backend {
        QueueBackend::Nats => {
            let store = NatsSecretStore::connect_async(&config.queue.nats_config()).await?;
            Ok(Arc::new(store))
        }
        #[cfg(feature = "amqp")]
        QueueBackend::Amqp => {
            use vlinder_core::domain::InMemorySecretStore;
            Ok(Arc::new(InMemorySecretStore::new()))
        }
        #[cfg(any(test, feature = "test-support"))]
        QueueBackend::Memory => {
            use vlinder_core::domain::InMemorySecretStore;
            Ok(Arc::new(InMemorySecretStore::new()))
        }
    }
}
