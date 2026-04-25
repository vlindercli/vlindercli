//! `LambdaInvokeReceiver` — receives invocations from the Lambda Runtime API.
//!
//! The Lambda service delivers invocations via HTTP on 127.0.0.1:9001.
//! This is not a message queue operation — it is AWS Lambda's own
//! invocation mechanism. We name it explicitly so the platform's
//! mechanism is visible in the code.

use std::io::Read;

use vlinder_core::domain::{Acknowledgement, AgentName, DataRoutingKey, InvokeMessage, QueueError};

use crate::adapter::deserialize_invoke;

pub struct LambdaInvokeReceiver {
    runtime_api: String,
    http: ureq::Agent,
}

impl LambdaInvokeReceiver {
    pub fn new(runtime_api: &str) -> Self {
        Self {
            runtime_api: runtime_api.to_string(),
            http: ureq::Agent::new(),
        }
    }

    /// The Runtime API endpoint this receiver is configured for.
    #[allow(dead_code)]
    pub fn runtime_api(&self) -> &str {
        &self.runtime_api
    }

    /// Block until the Lambda Runtime API delivers an invocation.
    ///
    /// Calls `GET /2018-06-01/runtime/invocation/next` on the Runtime API
    /// endpoint (127.0.0.1:9001). This is a blocking long-poll — it returns
    /// when AWS has an invocation ready for this container.
    pub fn receive_invoke(
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
            let result = (|| {
                let response_url = format!(
                    "http://{runtime_api}/2018-06-01/runtime/invocation/{request_id}/response",
                );
                http.post(&response_url).send_bytes(b"ok").map_err(|e| {
                    QueueError::ReceiveFailed(format!("POST invocation response failed: {e}"))
                })?;
                Ok(())
            })();
            Box::pin(async move { result })
        });

        Ok((payload.key, payload.msg, ack))
    }
}
