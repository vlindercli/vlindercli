//! `LambdaRuntimeQueue` — wraps a `MessageQueue` to receive invokes from
//! the Lambda Runtime API instead of the queue (ADR 125).
//!
//! `receive_invoke` calls `GET /runtime/invocation/next` and deserializes
//! the `LambdaInvokePayload`. All other methods delegate to the inner queue.

use std::io::Read;
use std::sync::Arc;

use vlinder_core::domain::{
    Acknowledgement, AgentName, CompleteMessage, DataRoutingKey, DeleteAgentMessage,
    DeployAgentMessage, ForkMessage, HarnessType, InfraRoutingKey, InvokeMessage, MessageQueue,
    Operation, PromoteMessage, QueueError, RequestMessage, ResponseMessage, Sequence,
    ServiceBackend, SessionRoutingKey, SessionStartMessage, SubmissionId,
};

use crate::adapter::deserialize_invoke;

/// A `MessageQueue` that receives invokes from the Lambda Runtime API.
///
/// Service calls (request/response) and complete delegate to the inner queue.
/// This is the bridge that lets the lambda adapter use the same dispatch
/// loop as the sidecar.
pub struct LambdaRuntimeQueue {
    inner: Arc<dyn MessageQueue + Send + Sync>,
    runtime_api: String,
    http: ureq::Agent,
}

impl LambdaRuntimeQueue {
    pub fn new(inner: Arc<dyn MessageQueue + Send + Sync>, runtime_api: &str) -> Self {
        Self {
            inner,
            runtime_api: runtime_api.to_string(),
            http: ureq::Agent::new(),
        }
    }
}

impl MessageQueue for LambdaRuntimeQueue {
    // -------------------------------------------------------------------------
    // Invoke — from Lambda Runtime API, not from the queue
    // -------------------------------------------------------------------------

    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        // Lambda functions don't send invokes — the daemon or event source does.
        self.inner.send_invoke(key, msg)
    }

    fn receive_invoke(
        &self,
        _agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let next_url = format!(
            "http://{}/2018-06-01/runtime/invocation/next",
            self.runtime_api,
        );

        let response =
            self.http.get(&next_url).call().map_err(|e| {
                QueueError::ReceiveFailed(format!("GET invocation/next failed: {e}"))
            })?;

        let request_id = response
            .header("Lambda-Runtime-Aws-Request-Id")
            .unwrap_or("unknown")
            .to_string();

        let mut body = Vec::new();
        response.into_reader().read_to_end(&mut body).map_err(|e| {
            QueueError::ReceiveFailed(format!("failed to read invocation body: {e}"))
        })?;

        tracing::info!(
            event = "lambda.invocation",
            request_id = %request_id,
            body_bytes = body.len(),
            "Received Lambda invocation"
        );

        let payload = deserialize_invoke(&body)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize invoke: {e}")))?;

        // Ack posts the response back to the Lambda Runtime API.
        let runtime_api = self.runtime_api.clone();
        let http = self.http.clone();
        let ack: Acknowledgement = Box::new(move || {
            let response_url = format!(
                "http://{runtime_api}/2018-06-01/runtime/invocation/{request_id}/response",
            );
            http.post(&response_url).send_bytes(b"ok").map_err(|e| {
                QueueError::ReceiveFailed(format!("POST invocation response failed: {e}"))
            })?;
            Ok(())
        });

        Ok((payload.key, payload.msg, ack))
    }

    // -------------------------------------------------------------------------
    // Everything else delegates to the inner queue
    // -------------------------------------------------------------------------

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        self.inner.send_complete(key, msg)
    }

    fn receive_complete(
        &self,
        submission: &SubmissionId,
        harness: HarnessType,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        self.inner.receive_complete(submission, harness, agent)
    }

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        self.inner.send_request(key, msg)
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        self.inner.receive_request(service, operation)
    }

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        self.inner.send_response(key, msg)
    }

    fn receive_response(
        &self,
        submission: &SubmissionId,
        agent: &AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        self.inner
            .receive_response(submission, agent, service, operation, sequence)
    }

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        self.inner.send_fork(key, msg)
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        self.inner.send_promote(key, msg)
    }

    fn send_session_start(
        &self,
        key: SessionRoutingKey,
        msg: SessionStartMessage,
    ) -> Result<vlinder_core::domain::BranchId, QueueError> {
        self.inner.send_session_start(key, msg)
    }

    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        self.inner.send_deploy_agent(key, msg)
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        self.inner.send_delete_agent(key, msg)
    }
}
