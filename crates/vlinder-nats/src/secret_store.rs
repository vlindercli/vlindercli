//! NATS KV-backed secret store (ADR 083).
//!
//! Provides a sync facade over the async NATS KV client. The tokio runtime
//! is owned internally, so callers use simple blocking APIs.

use std::sync::Arc;

use async_nats::jetstream::{self, kv};
use async_trait::async_trait;
use tokio::runtime::Runtime;

use vlinder_core::domain::{SecretStore, SecretStoreError};

/// NATS KV secret store.
///
/// Sync facade over async internals. Clone is cheap (Arc).
#[derive(Clone)]
pub struct NatsSecretStore {
    inner: Arc<NatsSecretStoreInner>,
}

struct NatsSecretStoreInner {
    /// Kept alive so the NATS client's background connection tasks continue running.
    /// `None` when constructed via `connect_async` — the caller's runtime keeps tasks alive.
    _runtime: Option<Runtime>,
    kv: kv::Store,
}

impl NatsSecretStore {
    /// Connect to a NATS server and create/open the `vlinder-secrets` KV bucket.
    pub fn connect(config: &crate::NatsConfig) -> Result<Self, SecretStoreError> {
        let runtime = Runtime::new()
            .map_err(|e| SecretStoreError::StoreFailed(format!("failed to create runtime: {e}")))?;

        let kv = runtime.block_on(async {
            let client = crate::connect::nats_connect(config)
                .await
                .map_err(SecretStoreError::StoreFailed)?;

            let jetstream = jetstream::new(client);

            let kv = jetstream
                .create_key_value(kv::Config {
                    bucket: "vlinder-secrets".to_string(),
                    history: 1,
                    max_bytes: 10 * 1024 * 1024, // 10 MiB — required by NGS
                    ..Default::default()
                })
                .await
                .map_err(|e| {
                    SecretStoreError::StoreFailed(format!("failed to create KV bucket: {e}"))
                })?;

            Ok::<_, SecretStoreError>(kv)
        })?;

        Ok(Self {
            inner: Arc::new(NatsSecretStoreInner {
                _runtime: Some(runtime),
                kv,
            }),
        })
    }

    /// Async variant of `connect` — callable from within an existing Tokio runtime.
    pub async fn connect_async(config: &crate::NatsConfig) -> Result<Self, SecretStoreError> {
        let client = crate::connect::nats_connect(config)
            .await
            .map_err(SecretStoreError::StoreFailed)?;

        let jetstream = jetstream::new(client);

        let kv = jetstream
            .create_key_value(kv::Config {
                bucket: "vlinder-secrets".to_string(),
                history: 1,
                max_bytes: 10 * 1024 * 1024, // 10 MiB — required by NGS
                ..Default::default()
            })
            .await
            .map_err(|e| {
                SecretStoreError::StoreFailed(format!("failed to create KV bucket: {e}"))
            })?;

        Ok(Self {
            inner: Arc::new(NatsSecretStoreInner { _runtime: None, kv }),
        })
    }
}

#[async_trait]
impl SecretStore for NatsSecretStore {
    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        let value = value.to_vec();
        self.inner
            .kv
            .put(name, value.into())
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?;
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretStoreError> {
        self.inner
            .kv
            .get(name)
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?
            .map(|bytes| bytes.to_vec())
            .ok_or_else(|| SecretStoreError::NotFound(name.to_string()))
    }

    async fn exists(&self, name: &str) -> Result<bool, SecretStoreError> {
        let result = self
            .inner
            .kv
            .get(name)
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?;
        Ok(result.is_some())
    }

    async fn delete(&self, name: &str) -> Result<(), SecretStoreError> {
        self.inner
            .kv
            .delete(name)
            .await
            .map_err(|e| SecretStoreError::DeleteFailed(e.to_string()))
    }
}
