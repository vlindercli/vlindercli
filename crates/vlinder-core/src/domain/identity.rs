//! Agent identity: Ed25519 key pair generation and persistence (ADR 084).
//!
//! Every agent gets an Ed25519 key pair:
//! - Private key stored in the `SecretStore` at `agents/{name}/private-key`
//! - Public key carried on the Agent struct and stored in the registry
//!
//! The `ensure_agent_identity` function is idempotent: if a key already exists
//! in the store, it derives the public key from it rather than generating a new pair.

use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;

use super::secret_store::{SecretStore, SecretStoreError};

/// The public half of an agent's Ed25519 identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentIdentity {
    pub public_key: [u8; 32],
}

/// Errors that can occur during identity operations.
#[derive(Debug)]
pub enum IdentityError {
    /// Secret store operation failed.
    Store(SecretStoreError),
    /// Stored key material has invalid length or format.
    InvalidKey(String),
}

impl std::fmt::Display for IdentityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentityError::Store(e) => write!(f, "secret store error: {e}"),
            IdentityError::InvalidKey(msg) => write!(f, "invalid key: {msg}"),
        }
    }
}

impl std::error::Error for IdentityError {}

impl From<SecretStoreError> for IdentityError {
    fn from(e: SecretStoreError) -> Self {
        IdentityError::Store(e)
    }
}

/// Returns the secret store key name for an agent's private key.
///
/// Convention: `agents/{name}/private-key` (see ADR 084, `secret_store.rs`).
pub fn private_key_name(agent_name: &str) -> String {
    format!("agents/{agent_name}/private-key")
}

/// Ensure an agent has an Ed25519 identity.
///
/// Idempotent: if a private key already exists in the store, the public key
/// is derived from it. Otherwise, a new key pair is generated and the private
/// key is stored.
///
/// Returns the public half of the key pair.
pub async fn ensure_agent_identity(
    agent_name: &str,
    store: &dyn SecretStore,
) -> Result<AgentIdentity, IdentityError> {
    let key_name = private_key_name(agent_name);

    if store.exists(&key_name).await? {
        // Derive public key from existing private key
        let private_bytes = store.get(&key_name).await?;
        let seed: [u8; 32] = private_bytes.try_into().map_err(|v: Vec<u8>| {
            IdentityError::InvalidKey(format!("expected 32 bytes, got {}", v.len()))
        })?;
        let signing_key = SigningKey::from_bytes(&seed);
        let public_key = signing_key.verifying_key().to_bytes();
        Ok(AgentIdentity { public_key })
    } else {
        // Generate new key pair
        let signing_key = SigningKey::generate(&mut OsRng);
        let private_bytes = signing_key.to_bytes();
        store.put(&key_name, &private_bytes).await?;
        let public_key = signing_key.verifying_key().to_bytes();
        Ok(AgentIdentity { public_key })
    }
}

#[cfg(test)]
mod tests {
    use super::super::InMemorySecretStore;
    use super::*;

    #[test]
    fn private_key_name_convention() {
        assert_eq!(private_key_name("echo"), "agents/echo/private-key");
        assert_eq!(private_key_name("my-agent"), "agents/my-agent/private-key");
    }

    #[tokio::test]
    async fn generate_stores_private_key() {
        let store = InMemorySecretStore::new();

        let identity = ensure_agent_identity("echo", &store).await.unwrap();

        // Private key should be in the store
        assert!(store.exists("agents/echo/private-key").await.unwrap());

        // Public key is 32 bytes
        assert_eq!(identity.public_key.len(), 32);
    }

    #[tokio::test]
    async fn idempotent_returns_same_public_key() {
        let store = InMemorySecretStore::new();

        let first = ensure_agent_identity("echo", &store).await.unwrap();
        let second = ensure_agent_identity("echo", &store).await.unwrap();

        assert_eq!(first.public_key, second.public_key);
    }

    #[tokio::test]
    async fn different_agents_get_different_keys() {
        let store = InMemorySecretStore::new();

        let alice = ensure_agent_identity("alice", &store).await.unwrap();
        let bob = ensure_agent_identity("bob", &store).await.unwrap();

        assert_ne!(alice.public_key, bob.public_key);
    }

    #[tokio::test]
    async fn derives_public_key_from_stored_private() {
        let store = InMemorySecretStore::new();

        // Manually store a known 32-byte key
        let known_seed = [42u8; 32];
        store
            .put("agents/manual/private-key", &known_seed)
            .await
            .unwrap();

        let identity = ensure_agent_identity("manual", &store).await.unwrap();

        // Verify the derived public key matches what ed25519-dalek produces
        let expected_signing_key = SigningKey::from_bytes(&known_seed);
        let expected_public = expected_signing_key.verifying_key().to_bytes();
        assert_eq!(identity.public_key, expected_public);
    }

    #[tokio::test]
    async fn rejects_invalid_key_length() {
        let store = InMemorySecretStore::new();

        // Store a 16-byte value (invalid — Ed25519 needs exactly 32)
        store
            .put("agents/bad/private-key", &[0u8; 16])
            .await
            .unwrap();

        let result = ensure_agent_identity("bad", &store).await;
        assert!(matches!(result, Err(IdentityError::InvalidKey(_))));
    }
}
