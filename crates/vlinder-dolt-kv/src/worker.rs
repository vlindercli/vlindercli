//! SQL worker — receives SQL requests from the queue and sends responses back.

use std::sync::Arc;

use vlinder_core::domain::{
    MessageQueue, Operation, RequestMessage, ResponseMessage, ServiceBackend, ServiceDiagnostics,
};

use crate::types::{SqlQueryRequest, SqlQueryResponse};

pub struct SqlWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    service: ServiceBackend,
}

impl SqlWorker {
    pub fn new(queue: Arc<dyn MessageQueue + Send + Sync>, service: ServiceBackend) -> Self {
        Self { queue, service }
    }

    /// Process one message if available. Returns true if processed.
    pub fn tick(&self) -> bool {
        self.try_execute()
    }

    fn try_execute(&self) -> bool {
        match self.queue.receive_request(self.service, Operation::Execute) {
            Ok((request, ack)) => {
                let start = std::time::Instant::now();
                let response_payload = self.handle_execute(&request);
                let duration_ms = start.elapsed().as_millis() as u64;

                let diag = ServiceDiagnostics::storage(
                    self.service.service_type(),
                    self.service.backend_str(),
                    Operation::Execute,
                    response_payload.len() as u64,
                    duration_ms,
                );

                let mut response =
                    ResponseMessage::from_request_with_diagnostics(&request, response_payload, diag);
                response.state = request.state.clone();

                let _ = self.queue.send_response(response);
                let _ = ack();
                true
            }
            Err(_) => false,
        }
    }

    fn handle_execute(&self, request: &RequestMessage) -> Vec<u8> {
        let _req: SqlQueryRequest = match serde_json::from_slice(request.payload.as_slice()) {
            Ok(r) => r,
            Err(e) => return format!("[error] invalid request: {}", e).into_bytes(),
        };

        let resp = SqlQueryResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected: 0,
        };

        serde_json::to_vec(&resp).unwrap_or_else(|e| format!("[error] {}", e).into_bytes())
    }
}
