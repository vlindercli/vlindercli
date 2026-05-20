//! Invoke handler — data-plane logic for provider requests.
//!
//! Owns queue routing and state tracking. No HTTP knowledge —
//! the provider server calls into this after matching routes.

use std::sync::{Arc, Mutex, RwLock};

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, MessageQueue,
    ProviderRoute, RequestDiagnostics, RequestMessage, SequenceCounter, SessionId, SubmissionId,
};

/// Routing identifiers that change per incoming invoke. Bundled together
/// so they update atomically — on each new invoke the sidecar resets all
/// three from the invoke's routing key.
///
/// `submission` is per-user-message: a new user turn produces a new
/// submission id. The session-scoped `InvokeHandler` would otherwise stamp
/// requests/responses with the stale submission from when it was constructed.
#[derive(Clone, Debug)]
pub struct RoutingContext {
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub session: SessionId,
}

/// Handles data-plane logic for one agent's session.
///
/// Lives for the duration of the sidecar process. `chain_head` and `routing`
/// reset on each new invoke so the handler's outgoing routing keys reflect
/// the current user turn.
pub struct InvokeHandler {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    routing: Arc<Mutex<RoutingContext>>,
    agent_id: AgentName,
    state: Arc<RwLock<Option<String>>>,
    sequence: SequenceCounter,
    chain_head: Arc<Mutex<DagNodeId>>,
}

impl InvokeHandler {
    pub fn new(
        queue: Arc<dyn MessageQueue + Send + Sync>,
        routing: RoutingContext,
        agent_id: AgentName,
        state: Arc<RwLock<Option<String>>>,
        chain_head: DagNodeId,
    ) -> Self {
        tracing::debug!(
            event = "chain_head_trace",
            site = "InvokeHandler::new",
            session = %routing.session,
            submission = %routing.submission,
            agent = %agent_id.as_str(),
            chain_head_init = %chain_head,
            "InvokeHandler constructed"
        );
        Self {
            queue,
            routing: Arc::new(Mutex::new(routing)),
            agent_id,
            state,
            sequence: SequenceCounter::new(),
            chain_head: Arc::new(Mutex::new(chain_head)),
        }
    }

    /// Return a clone of the `chain_head` `Arc` for sharing with `ProviderServer`.
    pub fn chain_head_arc(&self) -> Arc<Mutex<DagNodeId>> {
        Arc::clone(&self.chain_head)
    }

    /// Return a clone of the `routing` `Arc` for sharing with `ProviderServer`.
    pub fn routing_arc(&self) -> Arc<Mutex<RoutingContext>> {
        Arc::clone(&self.routing)
    }

    /// Returns the current `chain_head` — the DAG id of the last node
    /// in the chain (the invoke's `dag_id`, or the last response's `dag_id`
    /// if any provider calls were made).
    pub fn chain_head(&self) -> DagNodeId {
        self.chain_head.lock().unwrap().clone()
    }

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

        let routing = self.routing.lock().unwrap().clone();
        let key = DataRoutingKey {
            session: routing.session.clone(),
            branch: routing.branch,
            submission: routing.submission.clone(),
            kind: DataMessageKind::Request {
                agent: self.agent_id.clone(),
                service: route.service_backend,
                operation: route.operation,
                sequence: seq,
            },
        };

        let parent = self.chain_head.lock().unwrap().clone();
        tracing::debug!(
            event = "chain_head_trace",
            site = "forward_provider.before_send",
            session = %routing.session,
            submission = %routing.submission,
            agent = %self.agent_id.as_str(),
            seq = seq.as_u32(),
            chain_head_read = %parent,
            "reading chain_head as request dag_parent"
        );
        let msg = RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: parent,
            state: self.state.read().unwrap().clone(),
            diagnostics,
            payload: body,
            checkpoint,
        };

        match self.queue.call_service(key, msg).await {
            Ok(response) => {
                // Advance chain_head to the response dag_id
                *self.chain_head.lock().unwrap() = response.dag_id.clone();
                tracing::debug!(
                    event = "chain_head_trace",
                    site = "forward_provider.after_recv",
                    session = %routing.session,
                    submission = %routing.submission,
                    agent = %self.agent_id.as_str(),
                    seq = seq.as_u32(),
                    chain_head_new = %response.dag_id,
                    "advanced chain_head to response dag_id"
                );
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
