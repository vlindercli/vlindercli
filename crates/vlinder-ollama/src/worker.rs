//! Ollama worker — receives requests from the queue for inference
//! (Run, Chat, Generate) and embedding (Run on Embed service), POSTs
//! to the appropriate Ollama endpoint, and sends the response back.

use std::sync::Arc;
use std::time::Instant;

use async_openai::types::chat::{CreateChatCompletionRequest, CreateChatCompletionResponse};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use vlinder_core::domain::{
    Acknowledgement, DagNodeId, DataMessageKind, DataRoutingKey, EmbeddingBackendType,
    InferenceBackendType, MessageId, MessageQueue, Operation, QueueError, RequestMessage,
    ResponseMessage, ServiceBackend, ServiceDiagnostics, ServiceMetrics, ServiceType,
};

use crate::types::{
    OllamaChatRequest, OllamaChatResponse, OllamaEmbedRequest, OllamaEmbedResponse,
    OllamaGenerateRequest, OllamaGenerateResponse,
};

/// Result of calling an upstream Ollama endpoint — success or failure,
/// the HTTP response captures everything. Metrics are extracted by the
/// handler because it has the domain knowledge to parse the body.
type HandlerResult = (http::Response<Vec<u8>>, ServiceMetrics);

fn response_key_from_request(req_key: &DataRoutingKey) -> DataRoutingKey {
    let DataMessageKind::Request {
        agent,
        service,
        operation,
        sequence,
    } = &req_key.kind
    else {
        panic!("response_key_from_request called with non-Request key");
    };
    DataRoutingKey {
        session: req_key.session.clone(),
        branch: req_key.branch,
        submission: req_key.submission.clone(),
        kind: DataMessageKind::Response {
            agent: agent.clone(),
            service: *service,
            operation: *operation,
            sequence: *sequence,
        },
    }
}

pub struct OllamaWorker {
    queue: Arc<dyn MessageQueue + Send + Sync>,
    endpoint: String,
    client: reqwest::Client,
}

impl OllamaWorker {
    pub fn new(queue: Arc<dyn MessageQueue + Send + Sync>, endpoint: String) -> Self {
        Self {
            queue,
            endpoint,
            client: reqwest::Client::new(),
        }
    }

    /// Spawn one tokio task per (service, operation) this worker serves and
    /// drive each until `shutdown` fires. Four tasks: three on
    /// `Infer(Ollama)` for `Run`/`Chat`/`Generate` and one on `Embed(Ollama)`
    /// for `Run`.
    ///
    /// Each task loops: `select!` on shutdown vs. `receive_request`; once a
    /// request resolves, processing runs outside the select so the
    /// receive-then-ack path cannot be cancelled mid-flight.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) {
        let mut set: JoinSet<()> = JoinSet::new();

        for op in [Operation::Run, Operation::Chat, Operation::Generate] {
            let me = Arc::clone(&self);
            let shutdown = shutdown.clone();
            set.spawn(async move {
                let service = ServiceBackend::Infer(InferenceBackendType::Ollama);
                loop {
                    let (key, msg, ack) = tokio::select! {
                        () = shutdown.cancelled() => break,
                        result = me.queue.receive_request(service, op) => {
                            match result {
                                Ok(tuple) => tuple,
                                Err(QueueError::Timeout) => continue,
                                Err(e) => {
                                    tracing::warn!(error = %e, operation = ?op, "Ollama worker infer receive error");
                                    continue;
                                }
                            }
                        }
                    };
                    me.process_infer_request(key, msg, ack, op).await;
                }
            });
        }

        {
            let me = Arc::clone(&self);
            let shutdown = shutdown.clone();
            set.spawn(async move {
                let service = ServiceBackend::Embed(EmbeddingBackendType::Ollama);
                let operation = Operation::Run;
                loop {
                    let (key, msg, ack) = tokio::select! {
                        () = shutdown.cancelled() => break,
                        result = me.queue.receive_request(service, operation) => {
                            match result {
                                Ok(tuple) => tuple,
                                Err(QueueError::Timeout) => continue,
                                Err(e) => {
                                    tracing::warn!(error = %e, "Ollama worker embed receive error");
                                    continue;
                                }
                            }
                        }
                    };
                    me.process_embed_request(key, msg, ack).await;
                }
            });
        }

        while set.join_next().await.is_some() {}
    }

    pub async fn process_infer_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
        ack: Acknowledgement,
        operation: Operation,
    ) {
        let start = Instant::now();

        let (http_response, metrics) = match operation {
            Operation::Run => self.handle_openai(&msg.payload).await,
            Operation::Chat => self.handle_chat(&msg.payload).await,
            Operation::Generate => self.handle_generate(&msg.payload).await,
            _ => error_result(400, "unsupported operation"),
        };

        let status_code = http_response.status().as_u16();
        let wire = vlinder_core::domain::wire::WireResponse {
            inner: http_response,
        };
        let response_payload = serde_json::to_vec(&wire).unwrap_or_default();
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        let diag = ServiceDiagnostics {
            service: ServiceType::Infer,
            backend: "ollama".to_string(),
            duration_ms,
            metrics,
        };

        let response_key = response_key_from_request(&key);
        let response = ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: msg.dag_id.clone(),
            correlation_id: msg.id,
            state: msg.state,
            diagnostics: diag,
            payload: response_payload,
            status_code,
            checkpoint: msg.checkpoint,
        };
        let _ = self.queue.send_response(response_key, response).await;
        let _ = ack().await;
    }

    pub async fn process_embed_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
        ack: Acknowledgement,
    ) {
        let start = Instant::now();
        let (http_response, metrics) = self.handle_embed(&msg.payload).await;

        let status_code = http_response.status().as_u16();
        let wire = vlinder_core::domain::wire::WireResponse {
            inner: http_response,
        };
        let response_payload = serde_json::to_vec(&wire).unwrap_or_default();
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        let diag = ServiceDiagnostics {
            service: ServiceType::Embed,
            backend: "ollama".to_string(),
            duration_ms,
            metrics,
        };

        let response_key = response_key_from_request(&key);
        let response = ResponseMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: msg.dag_id.clone(),
            correlation_id: msg.id,
            state: msg.state,
            diagnostics: diag,
            payload: response_payload,
            status_code,
            checkpoint: msg.checkpoint,
        };
        let _ = self.queue.send_response(response_key, response).await;
        let _ = ack().await;
    }

    // ---- OpenAI-compatible: /v1/chat/completions ----

    async fn handle_openai(&self, payload: &[u8]) -> HandlerResult {
        let req: CreateChatCompletionRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return error_result(400, &e.to_string()),
        };

        let model_name = req.model.clone();

        let http_response = match self.call_upstream_raw("/v1/chat/completions", &req).await {
            Ok(r) => r,
            Err(e) => return error_result(500, &e),
        };

        let (ti, to) = serde_json::from_slice::<CreateChatCompletionResponse>(http_response.body())
            .ok()
            .and_then(|r| {
                r.usage
                    .as_ref()
                    .map(|u| (u.prompt_tokens, u.completion_tokens))
            })
            .unwrap_or((0, 0));

        (
            http_response,
            ServiceMetrics::Inference {
                tokens_input: ti,
                tokens_output: to,
                model: model_name,
            },
        )
    }

    // ---- Native: /api/chat ----

    async fn handle_chat(&self, payload: &[u8]) -> HandlerResult {
        let req: OllamaChatRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return error_result(400, &e.to_string()),
        };

        let model_name = req.model.clone();

        let http_response = match self.call_upstream_raw("/api/chat", &req).await {
            Ok(r) => r,
            Err(e) => return error_result(500, &e),
        };

        let (ti, to) = serde_json::from_slice::<OllamaChatResponse>(http_response.body())
            .ok()
            .map_or((0, 0), |r| {
                (r.prompt_eval_count.unwrap_or(0), r.eval_count.unwrap_or(0))
            });

        (
            http_response,
            ServiceMetrics::Inference {
                tokens_input: ti,
                tokens_output: to,
                model: model_name,
            },
        )
    }

    // ---- Native: /api/generate ----

    async fn handle_generate(&self, payload: &[u8]) -> HandlerResult {
        let req: OllamaGenerateRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return error_result(400, &e.to_string()),
        };

        let model_name = req.model.clone();

        let http_response = match self.call_upstream_raw("/api/generate", &req).await {
            Ok(r) => r,
            Err(e) => return error_result(500, &e),
        };

        let (ti, to) = serde_json::from_slice::<OllamaGenerateResponse>(http_response.body())
            .ok()
            .map_or((0, 0), |r| {
                (r.prompt_eval_count.unwrap_or(0), r.eval_count.unwrap_or(0))
            });

        (
            http_response,
            ServiceMetrics::Inference {
                tokens_input: ti,
                tokens_output: to,
                model: model_name,
            },
        )
    }

    // ---- Embed: /api/embed ----

    async fn handle_embed(&self, payload: &[u8]) -> HandlerResult {
        let req: OllamaEmbedRequest = match serde_json::from_slice(payload) {
            Ok(r) => r,
            Err(e) => return error_result(400, &e.to_string()),
        };

        let model_name = req.model.clone();

        let http_response = match self.call_upstream_raw("/api/embed", &req).await {
            Ok(r) => r,
            Err(e) => return error_result(500, &e),
        };

        let dimensions = serde_json::from_slice::<OllamaEmbedResponse>(http_response.body())
            .ok()
            .and_then(|r| {
                r.embeddings
                    .first()
                    .map(|v| u32::try_from(v.len()).unwrap_or(u32::MAX))
            })
            .unwrap_or(0);

        (
            http_response,
            ServiceMetrics::Embedding {
                dimensions,
                model: model_name,
            },
        )
    }

    // ---- HTTP ----

    /// Call the upstream Ollama endpoint and return the full HTTP response.
    async fn call_upstream_raw(
        &self,
        path: &str,
        req: &(impl serde::Serialize + Send + Sync),
    ) -> Result<http::Response<Vec<u8>>, String> {
        let url = format!("{}{}", self.endpoint, path);

        let response = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| e.to_string())?;

        let status = response.status();
        let mut builder = http::Response::builder().status(status);
        for name in response.headers().keys() {
            for value in response.headers().get_all(name) {
                if let Ok(s) = value.to_str() {
                    builder = builder.header(name.as_str(), s);
                }
            }
        }

        let body = response
            .bytes()
            .await
            .map_err(|e| format!("failed to read response body: {e}"))?
            .to_vec();

        builder
            .body(body)
            .map_err(|e| format!("failed to build http::Response: {e}"))
    }
}

/// Build an error result — an HTTP error response with zeroed metrics.
fn error_result(status: u16, message: &str) -> HandlerResult {
    (
        http::Response::builder()
            .status(status)
            .body(error_json(message))
            .expect("building error response with known-valid status"),
        ServiceMetrics::Inference {
            tokens_input: 0,
            tokens_output: 0,
            model: String::new(),
        },
    )
}

/// Build an OpenAI-shaped error JSON payload.
fn error_json(message: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "error": {
            "message": message,
            "type": "server_error",
            "param": null,
            "code": null
        }
    }))
    .expect("JSON serialization cannot fail for static shape")
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{
        AgentName, BranchId, EmbeddingBackendType, InferenceBackendType, RequestDiagnostics,
        RequestMessage, Sequence, ServiceBackend, SessionId, SubmissionId,
    };
    use vlinder_core::queue::InMemoryQueue;

    fn test_request_diag() -> RequestDiagnostics {
        RequestDiagnostics {
            sequence: 0,
            endpoint: String::new(),
            request_bytes: 0,
            received_at_ms: 0,
        }
    }

    fn test_submission() -> SubmissionId {
        SubmissionId::from("sub-test".to_string())
    }

    /// Send an infer request via v2 API and return (service, operation, sequence)
    /// needed to receive the response.
    async fn send_infer_request(
        queue: &Arc<dyn MessageQueue + Send + Sync>,
        operation: Operation,
        payload: Vec<u8>,
        state: Option<String>,
    ) -> (SubmissionId, ServiceBackend, Operation, Sequence) {
        let submission = test_submission();
        let service = ServiceBackend::Infer(InferenceBackendType::Ollama);
        let sequence = Sequence::first();
        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: submission.clone(),
            kind: DataMessageKind::Request {
                agent: AgentName::new("test-agent"),
                service,
                operation,
                sequence,
            },
        };
        let msg = RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state,
            diagnostics: test_request_diag(),
            payload,
            checkpoint: None,
        };
        queue.send_request(key, msg).await.unwrap();
        (submission, service, operation, sequence)
    }

    async fn send_embed_request(
        queue: &Arc<dyn MessageQueue + Send + Sync>,
        payload: Vec<u8>,
        state: Option<String>,
    ) -> (SubmissionId, ServiceBackend, Operation, Sequence) {
        let submission = test_submission();
        let service = ServiceBackend::Embed(EmbeddingBackendType::Ollama);
        let operation = Operation::Run;
        let sequence = Sequence::first();
        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: submission.clone(),
            kind: DataMessageKind::Request {
                agent: AgentName::new("test-agent"),
                service,
                operation,
                sequence,
            },
        };
        let msg = RequestMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state,
            diagnostics: test_request_diag(),
            payload,
            checkpoint: None,
        };
        queue.send_request(key, msg).await.unwrap();
        (submission, service, operation, sequence)
    }

    fn make_worker(queue: &Arc<dyn MessageQueue + Send + Sync>) -> OllamaWorker {
        OllamaWorker::new(Arc::clone(queue), "http://127.0.0.1:1".to_string())
    }

    async fn drain_infer(
        queue: &Arc<dyn MessageQueue + Send + Sync>,
        worker: &OllamaWorker,
        operation: Operation,
    ) {
        let (key, msg, ack) = queue
            .receive_request(
                ServiceBackend::Infer(InferenceBackendType::Ollama),
                operation,
            )
            .await
            .expect("infer request should be available");
        worker.process_infer_request(key, msg, ack, operation).await;
    }

    async fn drain_embed(queue: &Arc<dyn MessageQueue + Send + Sync>, worker: &OllamaWorker) {
        let (key, msg, ack) = queue
            .receive_request(
                ServiceBackend::Embed(EmbeddingBackendType::Ollama),
                Operation::Run,
            )
            .await
            .expect("embed request should be available");
        worker.process_embed_request(key, msg, ack).await;
    }

    // --- Operation::Run (OpenAI-compatible) ---

    #[tokio::test]
    async fn run_rejects_invalid_payload() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let (sub, svc, op, seq) =
            send_infer_request(&queue, Operation::Run, b"not json".to_vec(), None).await;
        drain_infer(&queue, &worker, Operation::Run).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 400);
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn run_echoes_state() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Run,
            serde_json::to_vec(&body).unwrap(),
            Some("xyz".to_string()),
        )
        .await;
        drain_infer(&queue, &worker, Operation::Run).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.state, Some("xyz".to_string()));
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn run_unreachable_endpoint_returns_500() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Run,
            serde_json::to_vec(&body).unwrap(),
            None,
        )
        .await;
        drain_infer(&queue, &worker, Operation::Run).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 500);
        ack().await.unwrap();
    }

    // --- Operation::Chat (/api/chat) ---

    #[tokio::test]
    async fn chat_rejects_invalid_payload() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let (sub, svc, op, seq) =
            send_infer_request(&queue, Operation::Chat, b"not json".to_vec(), None).await;
        drain_infer(&queue, &worker, Operation::Chat).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 400);
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn chat_echoes_state() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Chat,
            serde_json::to_vec(&body).unwrap(),
            Some("abc".to_string()),
        )
        .await;
        drain_infer(&queue, &worker, Operation::Chat).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.state, Some("abc".to_string()));
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn chat_unreachable_endpoint_returns_500() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Chat,
            serde_json::to_vec(&body).unwrap(),
            None,
        )
        .await;
        drain_infer(&queue, &worker, Operation::Chat).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 500);
        ack().await.unwrap();
    }

    // --- Operation::Generate (/api/generate) ---

    #[tokio::test]
    async fn generate_rejects_invalid_payload() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let (sub, svc, op, seq) =
            send_infer_request(&queue, Operation::Generate, b"not json".to_vec(), None).await;
        drain_infer(&queue, &worker, Operation::Generate).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 400);
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn generate_echoes_state() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "prompt": "Why is the sky blue?"
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Generate,
            serde_json::to_vec(&body).unwrap(),
            Some("def".to_string()),
        )
        .await;
        drain_infer(&queue, &worker, Operation::Generate).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.state, Some("def".to_string()));
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn generate_unreachable_endpoint_returns_500() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "llama3.2",
            "prompt": "Why is the sky blue?"
        });
        let (sub, svc, op, seq) = send_infer_request(
            &queue,
            Operation::Generate,
            serde_json::to_vec(&body).unwrap(),
            None,
        )
        .await;
        drain_infer(&queue, &worker, Operation::Generate).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 500);
        ack().await.unwrap();
    }

    // --- Embed (/api/embed) ---

    #[tokio::test]
    async fn embed_rejects_invalid_payload() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let (sub, svc, op, seq) = send_embed_request(&queue, b"not json".to_vec(), None).await;
        drain_embed(&queue, &worker).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 400);
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn embed_echoes_state() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "nomic-embed-text",
            "input": "hello world"
        });
        let (sub, svc, op, seq) = send_embed_request(
            &queue,
            serde_json::to_vec(&body).unwrap(),
            Some("embed-state".to_string()),
        )
        .await;
        drain_embed(&queue, &worker).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.state, Some("embed-state".to_string()));
        ack().await.unwrap();
    }

    #[tokio::test]
    async fn embed_unreachable_endpoint_returns_500() {
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let worker = make_worker(&queue);

        let body = serde_json::json!({
            "model": "nomic-embed-text",
            "input": "hello world"
        });
        let (sub, svc, op, seq) =
            send_embed_request(&queue, serde_json::to_vec(&body).unwrap(), None).await;
        drain_embed(&queue, &worker).await;

        let (_key, response, ack) = queue
            .receive_response(&sub, &AgentName::new("test-agent"), svc, op, seq)
            .await
            .unwrap();
        assert_eq!(response.status_code, 500);
        ack().await.unwrap();
    }
}
