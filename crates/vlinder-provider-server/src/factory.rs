//! Connection factories — production-only, no test backends.
//!
//! The sidecar always connects to real infrastructure: NATS for messaging,
//! gRPC for registry and state. No in-memory alternatives.

use std::sync::Arc;

use vlinder_core::domain::{DagStore, MessageQueue, QueueError, Registry};
use vlinder_core::queue::RecordingQueue;
use vlinder_nats::{NatsConfig, NatsQueue};
use vlinder_sql_registry::registry_service::GrpcRegistryClient;
use vlinder_sql_state::state_service::GrpcStateClient;

/// Resolve NATS connection config from the secret store, falling back to env vars.
///
/// Tries `sidecar.nats.url` from the secret store first. If found, optionally
/// fetches `sidecar.nats.creds` for inline credentials. On any failure or
/// missing secrets, falls back to the provided `fallback_nats_url`.
/// Resolve NATS connection config, optionally fetching credentials from the secret store.
pub fn resolve_nats_config(secret_url: Option<&str>, fallback_nats_url: &str) -> NatsConfig {
    if let Some(secret_url) = secret_url {
        match resolve_from_secrets(secret_url) {
            Some(config) => return config,
            None => {
                tracing::info!(
                    event = "sidecar.nats.fallback",
                    "Secret store did not provide NATS config, using env var"
                );
            }
        }
    }

    NatsConfig {
        url: fallback_nats_url.to_string(),
        creds_file: None,
        creds_content: None,
    }
}

/// Try to build `NatsConfig` from the secret store. Returns None on any failure.
fn resolve_from_secrets(secret_url: &str) -> Option<NatsConfig> {
    use vlinder_core::domain::SecretStore;
    use vlinder_nats::secret_service::GrpcSecretClient;

    let client = match GrpcSecretClient::connect(secret_url) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                event = "sidecar.secret.unreachable",
                error = %e,
                "Could not reach secret store, falling back to env var"
            );
            return None;
        }
    };

    let Ok(nats_url_bytes) = client.get("sidecar.nats.url") else {
        return None;
    };

    let nats_url = String::from_utf8(nats_url_bytes).ok()?;

    let creds_content = client
        .get("sidecar.nats.creds")
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok());

    tracing::info!(
        event = "sidecar.nats.from_secrets",
        url = %nats_url,
        has_creds = creds_content.is_some(),
        "Resolved NATS config from secret store"
    );

    Some(NatsConfig {
        url: nats_url,
        creds_file: None,
        creds_content,
    })
}

/// Queue backend configuration.
pub enum QueueConfig {
    /// NATS with optional secret-store credential resolution.
    Nats(NatsConfig),
}

/// Connect to a queue backend.
pub fn connect(config: &QueueConfig) -> Result<Arc<dyn MessageQueue + Send + Sync>, QueueError> {
    match config {
        QueueConfig::Nats(nats) => Ok(Arc::new(NatsQueue::connect(nats)?)),
    }
}

/// Wrap a queue with synchronous DAG recording.
pub fn with_recording(
    queue: Arc<dyn MessageQueue + Send + Sync>,
    store: Arc<dyn DagStore>,
) -> Arc<dyn MessageQueue + Send + Sync> {
    Arc::new(RecordingQueue::new(queue, store))
}

/// Connect to the State Service via gRPC.
pub fn connect_state(state_url: &str) -> Result<Arc<dyn DagStore>, QueueError> {
    Ok(Arc::new(GrpcStateClient::connect(state_url).map_err(
        |e| QueueError::SendFailed(format!("state service at {state_url} unreachable: {e}")),
    )?))
}

/// Connect to the Registry Service via gRPC.
pub fn connect_registry(
    registry_url: &str,
) -> Result<Arc<dyn Registry>, Box<dyn std::error::Error>> {
    let url = if registry_url.starts_with("http://") || registry_url.starts_with("https://") {
        registry_url.to_string()
    } else {
        format!("http://{registry_url}")
    };
    let client = GrpcRegistryClient::connect(&url)?;
    Ok(Arc::new(client))
}
