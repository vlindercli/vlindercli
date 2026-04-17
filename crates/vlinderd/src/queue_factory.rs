//! Queue factory functions — wires configuration to concrete queue implementations.
//!
//! Extracted from `queue/mod.rs` so that `queue/` can move into vlinder-core
//! without pulling in `config` or `state_service` dependencies.

use std::sync::Arc;

use crate::config::{Config, QueueBackend};
use vlinder_core::domain::{MessageQueue, QueueError};
use vlinder_core::queue::RecordingQueue;
use vlinder_nats::NatsQueue;

/// Create a queue from configuration (async).
///
/// Returns `NatsQueue` or `AmqpQueue` in production. In test builds, `Memory` backend
/// returns an `InMemoryQueue` (no network required).
pub async fn from_config_async(
    config: &Config,
) -> Result<Arc<dyn MessageQueue + Send + Sync>, QueueError> {
    let queue: Arc<dyn MessageQueue + Send + Sync> = match config.queue.backend {
        QueueBackend::Nats => {
            Arc::new(NatsQueue::connect_async(&config.queue.nats_config()).await?)
        }
        #[cfg(feature = "amqp")]
        QueueBackend::Amqp => {
            Arc::new(vlinder_amqp::AmqpQueue::connect_async(&config.queue.amqp_config()).await?)
        }
        #[cfg(any(test, feature = "test-support"))]
        QueueBackend::Memory => Arc::new(vlinder_core::queue::InMemoryQueue::new()),
    };
    Ok(queue)
}

/// Create a recording queue (with DAG outbox) from configuration (async).
pub async fn recording_from_config_async(
    config: &Config,
) -> Result<Arc<dyn MessageQueue + Send + Sync>, QueueError> {
    use vlinder_sql_state::state_service::GrpcStateClient;

    let inner: Arc<dyn MessageQueue + Send + Sync> = match config.queue.backend {
        QueueBackend::Nats => {
            Arc::new(NatsQueue::connect_async(&config.queue.nats_config()).await?)
        }
        #[cfg(feature = "amqp")]
        QueueBackend::Amqp => {
            Arc::new(vlinder_amqp::AmqpQueue::connect_async(&config.queue.amqp_config()).await?)
        }
        #[cfg(any(test, feature = "test-support"))]
        QueueBackend::Memory => Arc::new(vlinder_core::queue::InMemoryQueue::new()),
    };

    let state_addr = if config.distributed.state_addr.starts_with("http://") {
        config.distributed.state_addr.clone()
    } else {
        format!("http://{}", config.distributed.state_addr)
    };
    let store = GrpcStateClient::connect_async(&state_addr)
        .await
        .map_err(|e| QueueError::SendFailed(format!("state service unreachable: {e}")))?;

    Ok(Arc::new(RecordingQueue::new(inner, Arc::new(store))))
}
