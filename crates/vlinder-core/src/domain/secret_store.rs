//! Secret store trait definition (ADR 083).
//!
//! Named byte-blob storage for secrets (private keys, `NKeys`, API keys).
//! The store does not interpret contents — it stores and retrieves.
//!
//! Naming convention:
//! - `agents/{name}/private-key` — Ed25519 private keys (ADR 084)
//! - `nats/{role}/nkey` — NATS `NKeys` (ADR 085)
//! - `providers/{name}/api-key` — Provider API keys

use std::fmt;

use async_trait::async_trait;

// --- SecretStore Trait ---

/// A store for named secrets (ADR 083).
///
/// Implementations must be thread-safe. Secrets are opaque byte blobs —
/// the store does not interpret, encrypt, or transform the contents.
#[async_trait]
pub trait SecretStore: Send + Sync {
    /// Store a secret. Overwrites if it already exists.
    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretStoreError>;

    /// Retrieve a secret by name.
    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretStoreError>;

    /// Check whether a secret exists.
    async fn exists(&self, name: &str) -> Result<bool, SecretStoreError>;

    /// Delete a secret by name.
    async fn delete(&self, name: &str) -> Result<(), SecretStoreError>;
}

// --- Errors ---

#[derive(Debug)]
pub enum SecretStoreError {
    /// Secret does not exist
    NotFound(String),
    /// Failed to store a secret
    StoreFailed(String),
    /// Failed to delete a secret
    DeleteFailed(String),
}

impl fmt::Display for SecretStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecretStoreError::NotFound(name) => write!(f, "secret not found: {name}"),
            SecretStoreError::StoreFailed(msg) => write!(f, "store failed: {msg}"),
            SecretStoreError::DeleteFailed(msg) => write!(f, "delete failed: {msg}"),
        }
    }
}

impl std::error::Error for SecretStoreError {}

// ============================================================================
// In-Memory Implementation (for testing)
// ============================================================================

/// In-memory secret store for single-process use and testing.
///
/// Secrets are stored in a `HashMap` behind a `Mutex` — same pattern
/// as `InMemoryQueue`. No persistence, no encryption.
pub struct InMemorySecretStore {
    secrets: std::sync::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl InMemorySecretStore {
    pub fn new() -> Self {
        Self {
            secrets: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl Default for InMemorySecretStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SecretStore for InMemorySecretStore {
    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        let mut secrets = self.secrets.lock().unwrap();
        secrets.insert(name.to_string(), value.to_vec());
        Ok(())
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretStoreError> {
        let secrets = self.secrets.lock().unwrap();
        secrets
            .get(name)
            .cloned()
            .ok_or_else(|| SecretStoreError::NotFound(name.to_string()))
    }

    async fn exists(&self, name: &str) -> Result<bool, SecretStoreError> {
        let secrets = self.secrets.lock().unwrap();
        Ok(secrets.contains_key(name))
    }

    async fn delete(&self, name: &str) -> Result<(), SecretStoreError> {
        let mut secrets = self.secrets.lock().unwrap();
        if secrets.remove(name).is_some() {
            Ok(())
        } else {
            Err(SecretStoreError::DeleteFailed(format!(
                "secret not found: {name}"
            )))
        }
    }
}
