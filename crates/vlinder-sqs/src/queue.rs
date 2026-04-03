//! SQS-backed `MessageQueue` implementation (ADR 125).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

use vlinder_core::domain::{
    Acknowledgement, AgentName, CompleteMessage, DataMessageKind, DataRoutingKey,
    DeleteAgentMessage, DeployAgentMessage, EmbeddingBackendType, ForkMessage, HarnessType,
    InferenceBackendType, InfraRoutingKey, InvokeMessage, MessageQueue, ObjectStorageType,
    Operation, PromoteMessage, QueueError, RequestMessage, ResponseMessage, Sequence,
    ServiceBackend, SessionRoutingKey, SessionStartMessage, SubmissionId, VectorStorageType,
};

use crate::envelope::{self, Envelope};
use crate::routing;

const MAX_RECEIVE_COUNT: &str = "3";
const LONG_POLL_SECONDS: i32 = 1;
const VISIBILITY_TIMEOUT: i32 = 300;

/// SQS queue configuration.
#[derive(Debug, Clone)]
pub struct SqsConfig {
    pub region: String,
    pub queue_prefix: String,
}

/// SQS-backed `MessageQueue`. Sync facade over async internals.
#[derive(Clone)]
pub struct SqsQueue {
    inner: Arc<Inner>,
}

struct Inner {
    runtime: Runtime,
    client: aws_sdk_sqs::Client,
    prefix: String,
    urls: Mutex<HashMap<String, String>>,
}

// ── Construction ────────────────────────────────────────────────────

impl SqsQueue {
    pub fn connect(config: &SqsConfig) -> Result<Self, QueueError> {
        let runtime =
            Runtime::new().map_err(|e| QueueError::SendFailed(format!("tokio runtime: {e}")))?;

        let client = runtime.block_on(async {
            let region = aws_sdk_sqs::config::Region::new(config.region.clone());
            let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region)
                .load()
                .await;
            aws_sdk_sqs::Client::new(&aws_config)
        });

        Ok(Self {
            inner: Arc::new(Inner {
                runtime,
                client,
                prefix: config.queue_prefix.clone(),
                urls: Mutex::new(HashMap::new()),
            }),
        })
    }
}

// ── Internal helpers ────────────────────────────────────────────────

impl SqsQueue {
    fn queue_url(&self, queue_name: &str) -> Result<String, QueueError> {
        {
            let urls = self.inner.urls.lock().expect("urls lock");
            if let Some(url) = urls.get(queue_name) {
                return Ok(url.clone());
            }
        }

        let url = self.inner.runtime.block_on(async {
            self.inner
                .client
                .get_queue_url()
                .queue_name(queue_name)
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("get_queue_url({queue_name}): {e}")))?
                .queue_url
                .ok_or_else(|| QueueError::SendFailed(format!("queue {queue_name} has no URL")))
        })?;

        self.inner
            .urls
            .lock()
            .expect("urls lock")
            .insert(queue_name.to_string(), url.clone());
        Ok(url)
    }

    fn create_queue_with_dlq(&self, queue_name: &str) -> Result<(), QueueError> {
        self.inner.runtime.block_on(async {
            let dlq_name = routing::dlq_name(queue_name);
            let dlq_result = self
                .inner
                .client
                .create_queue()
                .queue_name(&dlq_name)
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("create DLQ {dlq_name}: {e}")))?;

            let dlq_url = dlq_result.queue_url.ok_or_else(|| {
                QueueError::SendFailed(format!("DLQ {dlq_name} created but no URL"))
            })?;

            let dlq_attrs = self
                .inner
                .client
                .get_queue_attributes()
                .queue_url(&dlq_url)
                .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("get DLQ ARN: {e}")))?;

            let dlq_arn = dlq_attrs
                .attributes()
                .and_then(|a| a.get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn))
                .ok_or_else(|| QueueError::SendFailed("DLQ ARN not found".into()))?;

            let redrive = format!(
                r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"{MAX_RECEIVE_COUNT}"}}"#
            );

            let result = self
                .inner
                .client
                .create_queue()
                .queue_name(queue_name)
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
                    &redrive,
                )
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
                    VISIBILITY_TIMEOUT.to_string(),
                )
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("create queue {queue_name}: {e}")))?;

            if let Some(url) = result.queue_url {
                self.inner
                    .urls
                    .lock()
                    .expect("urls lock")
                    .insert(queue_name.to_string(), url);
            }

            Ok(())
        })
    }

    fn delete_queue_pair(&self, queue_name: &str) -> Result<(), QueueError> {
        self.inner.runtime.block_on(async {
            if let Ok(url) = self.resolve_url(queue_name).await {
                let _ = self
                    .inner
                    .client
                    .delete_queue()
                    .queue_url(&url)
                    .send()
                    .await;
            }
            let dlq = routing::dlq_name(queue_name);
            if let Ok(url) = self.resolve_url(&dlq).await {
                let _ = self
                    .inner
                    .client
                    .delete_queue()
                    .queue_url(&url)
                    .send()
                    .await;
            }
            let mut urls = self.inner.urls.lock().expect("urls lock");
            urls.remove(queue_name);
            urls.remove(&dlq);
            Ok(())
        })
    }

    async fn resolve_url(&self, queue_name: &str) -> Result<String, QueueError> {
        self.inner
            .client
            .get_queue_url()
            .queue_name(queue_name)
            .send()
            .await
            .map_err(|e| QueueError::SendFailed(format!("get_queue_url({queue_name}): {e}")))?
            .queue_url
            .ok_or_else(|| QueueError::SendFailed(format!("queue {queue_name} has no URL")))
    }

    fn send_to(&self, queue_name: &str, body: &str) -> Result<(), QueueError> {
        let url = self.queue_url(queue_name)?;
        self.inner.runtime.block_on(async {
            self.inner
                .client
                .send_message()
                .queue_url(&url)
                .message_body(body)
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("send to {queue_name}: {e}")))?;
            Ok(())
        })
    }

    fn receive_one(&self, queue_name: &str) -> Result<(String, Acknowledgement), QueueError> {
        let url = self.queue_url(queue_name)?;
        self.inner.runtime.block_on(async {
            let result = self
                .inner
                .client
                .receive_message()
                .queue_url(&url)
                .max_number_of_messages(1)
                .wait_time_seconds(LONG_POLL_SECONDS)
                .send()
                .await
                .map_err(|e| {
                    QueueError::ReceiveFailed(format!("receive from {queue_name}: {e}"))
                })?;

            let msg = result
                .messages
                .and_then(|mut msgs| {
                    if msgs.is_empty() {
                        None
                    } else {
                        Some(msgs.remove(0))
                    }
                })
                .ok_or(QueueError::Timeout)?;

            let body = msg
                .body
                .ok_or_else(|| QueueError::ReceiveFailed("message has no body".into()))?;
            let receipt = msg
                .receipt_handle
                .ok_or_else(|| QueueError::ReceiveFailed("no receipt handle".into()))?;

            let client = self.inner.client.clone();
            let ack_url = url.clone();
            let ack: Acknowledgement = Box::new(move || {
                // The ack closure may be called from the same tokio runtime
                // context, so use try_current to avoid nesting.
                tokio::runtime::Handle::try_current()
                    .map_err(|e| QueueError::ReceiveFailed(format!("no runtime for ack: {e}")))
                    .and_then(|h| {
                        h.block_on(async {
                            client
                                .delete_message()
                                .queue_url(&ack_url)
                                .receipt_handle(&receipt)
                                .send()
                                .await
                        })
                        .map_err(|e| QueueError::ReceiveFailed(format!("ack failed: {e}")))
                    })?;
                Ok(())
            });

            Ok((body, ack))
        })
    }
}

// ── MessageQueue ────────────────────────────────────────────────────

impl MessageQueue for SqsQueue {
    // Lifecycle

    fn on_cluster_start(&self) -> Result<(), QueueError> {
        self.create_queue_with_dlq(&routing::deploy_queue(&self.inner.prefix))?;
        self.create_queue_with_dlq(&routing::delete_queue(&self.inner.prefix))?;
        self.create_queue_with_dlq(&routing::fork_queue(&self.inner.prefix))?;
        self.create_queue_with_dlq(&routing::promote_queue(&self.inner.prefix))?;
        // Service request queues — one per known backend
        for backend in [
            ServiceBackend::Kv(ObjectStorageType::Sqlite),
            ServiceBackend::Vec(VectorStorageType::SqliteVec),
            ServiceBackend::Infer(InferenceBackendType::Ollama),
            ServiceBackend::Infer(InferenceBackendType::OpenRouter),
            ServiceBackend::Embed(EmbeddingBackendType::Ollama),
        ] {
            self.create_queue_with_dlq(&routing::request_queue(&self.inner.prefix, backend))?;
        }
        tracing::info!(prefix = %self.inner.prefix, "SQS static queues ready");
        Ok(())
    }

    fn on_agent_deployed(&self, agent: &AgentName) -> Result<(), QueueError> {
        self.create_queue_with_dlq(&routing::invoke_queue(&self.inner.prefix, agent))?;
        self.create_queue_with_dlq(&routing::complete_queue(&self.inner.prefix, agent))?;
        self.create_queue_with_dlq(&routing::response_queue(&self.inner.prefix, agent))?;
        tracing::info!(agent = %agent, "SQS agent queues created");
        Ok(())
    }

    fn on_agent_deleted(&self, agent: &AgentName) -> Result<(), QueueError> {
        self.delete_queue_pair(&routing::invoke_queue(&self.inner.prefix, agent))?;
        self.delete_queue_pair(&routing::complete_queue(&self.inner.prefix, agent))?;
        self.delete_queue_pair(&routing::response_queue(&self.inner.prefix, agent))?;
        tracing::info!(agent = %agent, "SQS agent queues deleted");
        Ok(())
    }

    // Data plane — Invoke

    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed("expected Invoke key".into()));
        };
        let queue = routing::invoke_queue(&self.inner.prefix, agent);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let queue = routing::invoke_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<DataRoutingKey, InvokeMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // Data plane — Complete

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let DataMessageKind::Complete { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed("expected Complete key".into()));
        };
        let queue = routing::complete_queue(&self.inner.prefix, agent);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_complete(
        &self,
        _submission: &SubmissionId,
        _harness: HarnessType,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let queue = routing::complete_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<DataRoutingKey, CompleteMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // Data plane — Request

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        let DataMessageKind::Request { service, .. } = &key.kind else {
            return Err(QueueError::SendFailed("expected Request key".into()));
        };
        let queue = routing::request_queue(&self.inner.prefix, *service);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        _operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let queue = routing::request_queue(&self.inner.prefix, service);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<DataRoutingKey, RequestMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // Data plane — Response

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        let DataMessageKind::Response { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed("expected Response key".into()));
        };
        let queue = routing::response_queue(&self.inner.prefix, agent);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_response(
        &self,
        _submission: &SubmissionId,
        agent: &AgentName,
        _service: ServiceBackend,
        _operation: Operation,
        _sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let queue = routing::response_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<DataRoutingKey, ResponseMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // Session plane

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        let queue = routing::fork_queue(&self.inner.prefix);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_fork(
        &self,
    ) -> Result<(SessionRoutingKey, ForkMessage, Acknowledgement), QueueError> {
        let queue = routing::fork_queue(&self.inner.prefix);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<SessionRoutingKey, ForkMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        let queue = routing::promote_queue(&self.inner.prefix);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_promote(
        &self,
    ) -> Result<(SessionRoutingKey, PromoteMessage, Acknowledgement), QueueError> {
        let queue = routing::promote_queue(&self.inner.prefix);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<SessionRoutingKey, PromoteMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_session_start(
        &self,
        _key: SessionRoutingKey,
        _msg: SessionStartMessage,
    ) -> Result<vlinder_core::domain::BranchId, QueueError> {
        Ok(vlinder_core::domain::BranchId::from(1))
    }

    // Infra plane

    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        let queue = routing::deploy_queue(&self.inner.prefix);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        let queue = routing::deploy_queue(&self.inner.prefix);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<InfraRoutingKey, DeployAgentMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        let queue = routing::delete_queue(&self.inner.prefix);
        self.send_to(&queue, &Envelope { key, msg }.to_json()?)
    }

    fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        let queue = routing::delete_queue(&self.inner.prefix);
        let (body, ack) = self.receive_one(&queue)?;
        let env: Envelope<InfraRoutingKey, DeleteAgentMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }
}
