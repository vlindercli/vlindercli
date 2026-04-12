//! Invoke handler — data-plane logic for provider requests.
//!
//! Owns queue routing and state tracking. No HTTP knowledge —
//! the provider server calls into this after matching routes.

use std::sync::{Arc, RwLock};

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue,
    ProviderRoute, RequestDiagnostics, RequestMessage, SequenceCounter, SessionId, SubmissionId,
};

/// Handles data-plane logic for a single invoke.
///
/// Created per invocation, lives for the duration of the invoke.
/// The provider server owns the HTTP plumbing; this struct owns
/// the queue interactions and state tracking.
pub struct InvokeHandler {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    branch: BranchId,
    submission: SubmissionId,
    session: SessionId,
    agent_id: AgentName,
    state: Arc<RwLock<Option<String>>>,
    sequence: SequenceCounter,
}

impl InvokeHandler {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        branch: BranchId,
        submission: SubmissionId,
        session: SessionId,
        agent_id: AgentName,
        state: Arc<RwLock<Option<String>>>,
    ) -> Self {
        Self {
            queue,
            branch,
            submission,
            session,
            agent_id,
            state,
            sequence: SequenceCounter::new(),
        }
    }

    /// Forward a matched provider request to the message queue.
    pub async fn forward_provider(
        &self,
        route: &ProviderRoute,
        body: Vec<u8>,
        checkpoint: Option<String>,
    ) -> (u16, Vec<u8>) {
        let seq = self.sequence.next();
        let received_at_ms = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX);

        let diagnostics = RequestDiagnostics {
            sequence: seq.as_u32(),
            endpoint: format!("/{}", route.service_backend.service_type().as_str()),
            request_bytes: body.len() as u64,
            received_at_ms,
        };

        let key = DataRoutingKey {
            session: self.session.clone(),
            branch: self.branch,
            submission: self.submission.clone(),
            kind: DataMessageKind::Request {
                agent: self.agent_id.clone(),
                service: route.service_backend,
                operation: route.operation,
                sequence: seq,
            },
        };

        let msg = RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: self.state.read().unwrap().clone(),
            diagnostics,
            payload: body,
            checkpoint,
        };

        match self.queue.call_service(key, msg).await {
            Ok(response) => {
                if let Some(ref new_state) = response.state {
                    *self.state.write().unwrap() = Some(new_state.clone());
                }
                // Unwrap WireResponse envelope (ADR 118) — agent sees the HTTP body,
                // not the wire-format wrapper. Fall back to raw payload if not wrapped.
                if let Ok(wire) = serde_json::from_slice::<vlinder_core::domain::wire::WireResponse>(
                    &response.payload,
                ) {
                    (wire.inner.status().as_u16(), wire.inner.into_body())
                } else {
                    (response.status_code, response.payload.clone())
                }
            }
            Err(e) => (502, format!("queue error: {e}").into_bytes()),
        }
    }
}
