//! Dispatch — handles a single agent invocation.
//!
//! Delegates to `vlinder_provider_server::dispatch` for the shared
//! invoke flow, then adds sidecar-specific concerns (health diagnostics,
//! container metadata).

use std::sync::Arc;

use vlinder_core::domain::{
    ContainerId, DataMessageKind, DataRoutingKey, HealthWindow, ImageDigest, ImageRef,
    InvokeMessage, MessageQueue, Registry, RuntimeDiagnostics,
};

use vlinder_provider_server::dispatch as shared;

use crate::health;

/// Everything the dispatch loop needs from the sidecar.
pub struct DispatchContext {
    pub queue: Arc<dyn MessageQueue + Send + Sync>,
    pub registry: Arc<dyn Registry>,
    pub container_port: u16,
    pub container_id: ContainerId,
    pub image_ref: Option<ImageRef>,
    pub image_digest: Option<ImageDigest>,
}

/// Handle a single invocation: dispatch to agent and send complete.
pub fn handle_invoke(
    ctx: &DispatchContext,
    health: &mut HealthWindow,
    key: &DataRoutingKey,
    msg: &InvokeMessage,
) {
    let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        tracing::error!("handle_invoke called with non-Invoke key");
        return;
    };

    match shared::dispatch_invoke(&ctx.queue, &ctx.registry, ctx.container_port, key, msg) {
        Ok(result) => {
            let diagnostics = health::build_diagnostics(
                health,
                ctx.container_port,
                result.duration_ms,
                &ctx.container_id,
                ctx.image_ref.as_ref(),
                ctx.image_digest.as_ref(),
            );
            shared::send_complete(
                ctx.queue.as_ref(),
                key,
                agent,
                result.output,
                result.state,
                diagnostics,
            );
        }
        Err(e) => {
            tracing::warn!(event = "dispatch.error", error = %e, "Dispatch failed");
            shared::send_complete(
                ctx.queue.as_ref(),
                key,
                agent,
                format!("[error] {e}").into_bytes(),
                None,
                RuntimeDiagnostics::placeholder(0),
            );
        }
    }
}
