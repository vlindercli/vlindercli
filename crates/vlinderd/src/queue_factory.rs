//! Queue factory functions — wires configuration to concrete queue implementations.
//!
//! Extracted from `queue/mod.rs` so that `queue/` can move into vlinder-core
//! without pulling in `config` or `state_service` dependencies.

use std::sync::Arc;

use crate::config::{Config, QueueBackend};
use vlinder_core::domain::{MessageQueue, QueueError};
use vlinder_core::queue::RecordingQueue;
use vlinder_nats::NatsQueue;

/// Create a queue from configuration.
///
/// Returns `NatsQueue` in production. In test builds, `Memory` backend
/// returns an `InMemoryQueue` (no network required).
pub fn from_config(config: &Config) -> Result<Arc<dyn MessageQueue + Send + Sync>, QueueError> {
    let queue: Arc<dyn MessageQueue + Send + Sync> = match config.queue.backend {
        QueueBackend::Nats => Arc::new(NatsQueue::connect(&config.queue.nats_config())?),
        #[cfg(any(test, feature = "test-support"))]
        QueueBackend::Memory => Arc::new(vlinder_core::queue::InMemoryQueue::new()),
    };
    queue.on_cluster_start()?;
    Ok(queue)
}

/// Create a queue with synchronous DAG recording (transactional outbox).
///
/// Wraps the configured queue in a `RecordingQueue` that records
/// DAG nodes into the `DagStore` on every send.
///
/// In production, the `DagStore` is the gRPC State Service.
/// In test builds with `StateBackend::Memory`, uses `InMemoryDagStore`.
pub fn recording_from_config(
    config: &Config,
) -> Result<Arc<dyn MessageQueue + Send + Sync>, QueueError> {
    let inner = from_config(config)?;

    let store = crate::state_factory::from_config(config)
        .map_err(|e| QueueError::SendFailed(format!("state service unreachable: {e}")))?;

    Ok(Arc::new(RecordingQueue::new(inner, store)))
}
