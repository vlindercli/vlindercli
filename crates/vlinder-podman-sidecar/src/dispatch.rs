//! Sidecar's shared substrate handles.
//!
//! After Phase 5.4 the queue-polling dispatch loop is gone; this module
//! only carries the `DispatchContext` struct that the `Sidecar` uses to
//! hold its queue/registry/store/container metadata. The struct is plumbed
//! into the `dispatch_endpoint` HTTP handler via shared `Arc` clones.

use std::sync::Arc;

use vlinder_core::domain::{DagStore, MessageQueue, Registry};

/// Substrate handles shared between `Sidecar::run` and the `/v1/dispatch`
/// HTTP endpoint state.
pub struct DispatchContext {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub store: Arc<dyn DagStore>,
    pub container_port: u16,
}
