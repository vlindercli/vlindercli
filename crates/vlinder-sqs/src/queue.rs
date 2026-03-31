//! SQS-backed `MessageQueue` implementation (ADR 125).
//!
//! Sync facade over the async AWS SDK, mirroring the `NatsQueue` pattern.
//! An internal tokio runtime bridges the async SQS client to the
//! synchronous `MessageQueue` trait.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::runtime::Runtime;

use vlinder_core::domain::{
    Acknowledgement, AgentName, BranchId, CompleteMessage, DataMessageKind, DataRoutingKey,
    DeleteAgentMessage, DeployAgentMessage, ForkMessage, HarnessType, InfraRoutingKey,
    InvokeMessage, MessageQueue, Operation, PromoteMessage, QueueError, RequestMessage,
    ResponseMessage, Sequence, ServiceBackend, SessionRoutingKey, SessionStartMessage,
    SubmissionId,
};

use crate::routing;

const MAX_RECEIVE_COUNT: &str = "3";
const LONG_POLL_SECONDS: i32 = 1;
const VISIBILITY_TIMEOUT: i32 = 300;

/// SQS queue configuration.
#[derive(Debug, Clone)]
pub struct SqsConfig {
    /// AWS region (e.g., `"eu-west-1"`).
    pub region: String,
    /// Queue name prefix (e.g., `"dev-vlinder"`). Defaults to `"vlinder"`.
    pub queue_prefix: String,
}

/// SQS-backed `MessageQueue`.
///
/// Sync facade over async internals. Clone is cheap (`Arc`).
#[derive(Clone)]
pub struct SqsQueue {
    inner: Arc<SqsQueueInner>,
}

struct SqsQueueInner {
    runtime: Runtime,
    client: aws_sdk_sqs::Client,
    prefix: String,
    /// Cache: queue name → queue URL (SQS operations use URLs, not names).
    queue_urls: Mutex<HashMap<String, String>>,
}

impl SqsQueue {
    /// Connect to SQS using the given config.
    pub fn connect(config: &SqsConfig) -> Result<Self, QueueError> {
        let runtime = Runtime::new()
            .map_err(|e| QueueError::SendFailed(format!("failed to create runtime: {e}")))?;

        let client = runtime.block_on(async {
            let region = aws_sdk_sqs::config::Region::new(config.region.clone());
            let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region)
                .load()
                .await;
            aws_sdk_sqs::Client::new(&aws_config)
        });

        Ok(Self {
            inner: Arc::new(SqsQueueInner {
                runtime,
                client,
                prefix: config.queue_prefix.clone(),
                queue_urls: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Resolve queue name → URL, caching the result.
    fn queue_url(&self, queue_name: &str) -> Result<String, QueueError> {
        {
            let urls = self.inner.queue_urls.lock().expect("queue_urls lock");
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
            .queue_urls
            .lock()
            .expect("queue_urls lock")
            .insert(queue_name.to_string(), url.clone());
        Ok(url)
    }

    /// Create an SQS queue with a paired DLQ. Idempotent.
    fn create_queue_with_dlq(&self, queue_name: &str) -> Result<(), QueueError> {
        self.inner.runtime.block_on(async {
            // 1. Create DLQ
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
                QueueError::SendFailed(format!("DLQ {dlq_name} created but no URL returned"))
            })?;

            // 2. Get DLQ ARN (needed for redrive policy)
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

            // 3. Create main queue with redrive policy pointing to DLQ
            let redrive_policy = format!(
                r#"{{"deadLetterTargetArn":"{dlq_arn}","maxReceiveCount":"{MAX_RECEIVE_COUNT}"}}"#
            );

            let result = self
                .inner
                .client
                .create_queue()
                .queue_name(queue_name)
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::RedrivePolicy,
                    &redrive_policy,
                )
                .attributes(
                    aws_sdk_sqs::types::QueueAttributeName::VisibilityTimeout,
                    VISIBILITY_TIMEOUT.to_string(),
                )
                .send()
                .await
                .map_err(|e| QueueError::SendFailed(format!("create queue {queue_name}: {e}")))?;

            // Cache the URL
            if let Some(url) = result.queue_url {
                self.inner
                    .queue_urls
                    .lock()
                    .expect("queue_urls lock")
                    .insert(queue_name.to_string(), url);
            }

            Ok(())
        })
    }

    /// Delete an SQS queue and its paired DLQ. Idempotent.
    fn delete_queue_with_dlq(&self, queue_name: &str) -> Result<(), QueueError> {
        self.inner.runtime.block_on(async {
            // Delete main queue
            if let Ok(url) = self.resolve_url_async(queue_name).await {
                let _ = self
                    .inner
                    .client
                    .delete_queue()
                    .queue_url(&url)
                    .send()
                    .await;
            }
            // Delete DLQ
            let dlq = routing::dlq_name(queue_name);
            if let Ok(url) = self.resolve_url_async(&dlq).await {
                let _ = self
                    .inner
                    .client
                    .delete_queue()
                    .queue_url(&url)
                    .send()
                    .await;
            }
            // Evict from cache
            let mut urls = self.inner.queue_urls.lock().expect("queue_urls lock");
            urls.remove(queue_name);
            urls.remove(&dlq);
            Ok(())
        })
    }

    /// Async helper: resolve queue name → URL without caching.
    async fn resolve_url_async(&self, queue_name: &str) -> Result<String, QueueError> {
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

    /// Send a JSON-serialized envelope to the named queue.
    fn send_to_queue(&self, queue_name: &str, body: &str) -> Result<(), QueueError> {
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

    /// Receive one message from the named queue (long poll).
    ///
    /// Returns the deserialized body and an `Acknowledgement` closure that
    /// calls `DeleteMessage` on the receipt handle.
    fn receive_from_queue(
        &self,
        queue_name: &str,
    ) -> Result<(String, Acknowledgement), QueueError> {
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

            let receipt_handle = msg
                .receipt_handle
                .ok_or_else(|| QueueError::ReceiveFailed("message has no receipt handle".into()))?;

            // Build ack closure: deletes the message from SQS
            let client = self.inner.client.clone();
            let ack_url = url.clone();
            let ack: Acknowledgement = Box::new(move || {
                // We need a runtime to call the async delete. Use a fresh one
                // since the FnOnce may be called outside our runtime context.
                tokio::runtime::Handle::try_current()
                    .map_err(|e| QueueError::ReceiveFailed(format!("no runtime for ack: {e}")))
                    .and_then(|handle| {
                        handle
                            .block_on(async {
                                client
                                    .delete_message()
                                    .queue_url(&ack_url)
                                    .receipt_handle(&receipt_handle)
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

// ============================================================================
// Envelope: wraps routing key + message body for SQS transport
// ============================================================================

/// SQS messages carry both the routing key and the message in a JSON envelope.
/// NATS encodes the routing key in the subject; SQS has no equivalent, so we
/// bundle it in the body.
mod envelope {
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    pub(super) struct Envelope<K, M> {
        pub key: K,
        pub msg: M,
    }

    impl<K: Serialize, M: Serialize> Envelope<K, M> {
        pub fn to_json(&self) -> Result<String, super::QueueError> {
            serde_json::to_string(self)
                .map_err(|e| super::QueueError::SendFailed(format!("serialize envelope: {e}")))
        }
    }

    pub(super) fn from_json<K, M>(body: &str) -> Result<Envelope<K, M>, super::QueueError>
    where
        K: for<'de> Deserialize<'de>,
        M: for<'de> Deserialize<'de>,
    {
        serde_json::from_str(body)
            .map_err(|e| super::QueueError::ReceiveFailed(format!("deserialize envelope: {e}")))
    }
}

// ============================================================================
// MessageQueue implementation
// ============================================================================

impl MessageQueue for SqsQueue {
    fn on_cluster_start(&self) -> Result<(), QueueError> {
        // Infra plane — one queue per message type
        self.create_queue_with_dlq(&routing::deploy_queue(&self.inner.prefix))?;
        self.create_queue_with_dlq(&routing::delete_queue(&self.inner.prefix))?;
        // Session plane — one queue per message type
        self.create_queue_with_dlq(&routing::fork_queue(&self.inner.prefix))?;
        self.create_queue_with_dlq(&routing::promote_queue(&self.inner.prefix))?;
        // Service request queues are created per backend. The known set at
        // startup is small — additional backends create queues lazily.
        Ok(())
    }

    fn on_agent_deployed(&self, agent: &AgentName) -> Result<(), QueueError> {
        self.create_queue_with_dlq(&routing::invoke_queue(&self.inner.prefix, agent))?;
        self.create_queue_with_dlq(&routing::complete_queue(&self.inner.prefix, agent))?;
        self.create_queue_with_dlq(&routing::response_queue(&self.inner.prefix, agent))?;
        Ok(())
    }

    fn on_agent_deleted(&self, agent: &AgentName) -> Result<(), QueueError> {
        self.delete_queue_with_dlq(&routing::invoke_queue(&self.inner.prefix, agent))?;
        self.delete_queue_with_dlq(&routing::complete_queue(&self.inner.prefix, agent))?;
        self.delete_queue_with_dlq(&routing::response_queue(&self.inner.prefix, agent))?;
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Data plane — Invoke
    // -------------------------------------------------------------------------

    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let DataMessageKind::Invoke { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed(
                "send_invoke: expected Invoke key".into(),
            ));
        };
        let queue_name = routing::invoke_queue(&self.inner.prefix, agent);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let queue_name = routing::invoke_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<DataRoutingKey, InvokeMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // -------------------------------------------------------------------------
    // Data plane — Complete
    // -------------------------------------------------------------------------

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let DataMessageKind::Complete { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed(
                "send_complete: expected Complete key".into(),
            ));
        };
        let queue_name = routing::complete_queue(&self.inner.prefix, agent);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_complete(
        &self,
        _submission: &SubmissionId,
        _harness: HarnessType,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let queue_name = routing::complete_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<DataRoutingKey, CompleteMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // -------------------------------------------------------------------------
    // Data plane — Request
    // -------------------------------------------------------------------------

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        let DataMessageKind::Request { service, .. } = &key.kind else {
            return Err(QueueError::SendFailed(
                "send_request: expected Request key".into(),
            ));
        };
        let queue_name = routing::request_queue(&self.inner.prefix, *service);
        // Lazily ensure the request queue exists
        self.create_queue_with_dlq(&queue_name)?;
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        expected_operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let queue_name = routing::request_queue(&self.inner.prefix, service);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<DataRoutingKey, RequestMessage> = envelope::from_json(&body)?;
        // Client-side filter: operation must match. If not, reject (let it redeliver).
        if let DataMessageKind::Request { operation, .. } = &env.key.kind {
            if *operation != expected_operation {
                // Don't ack — let visibility timeout redeliver to another consumer
                return Err(QueueError::Timeout);
            }
        }
        Ok((env.key, env.msg, ack))
    }

    // -------------------------------------------------------------------------
    // Data plane — Response
    // -------------------------------------------------------------------------

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        let DataMessageKind::Response { ref agent, .. } = key.kind else {
            return Err(QueueError::SendFailed(
                "send_response: expected Response key".into(),
            ));
        };
        let queue_name = routing::response_queue(&self.inner.prefix, agent);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_response(
        &self,
        _submission: &SubmissionId,
        agent: &AgentName,
        _service: ServiceBackend,
        _operation: Operation,
        _sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let queue_name = routing::response_queue(&self.inner.prefix, agent);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<DataRoutingKey, ResponseMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    // -------------------------------------------------------------------------
    // Session plane
    // -------------------------------------------------------------------------

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        let queue_name = routing::fork_queue(&self.inner.prefix);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_fork(
        &self,
    ) -> Result<(SessionRoutingKey, ForkMessage, Acknowledgement), QueueError> {
        let queue_name = routing::fork_queue(&self.inner.prefix);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<SessionRoutingKey, ForkMessage> = envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        let queue_name = routing::promote_queue(&self.inner.prefix);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_promote(
        &self,
    ) -> Result<(SessionRoutingKey, PromoteMessage, Acknowledgement), QueueError> {
        let queue_name = routing::promote_queue(&self.inner.prefix);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<SessionRoutingKey, PromoteMessage> =
            envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_session_start(
        &self,
        _key: SessionRoutingKey,
        _msg: SessionStartMessage,
    ) -> Result<BranchId, QueueError> {
        // Fire-and-forget — RecordingQueue handles persistence.
        Ok(BranchId::from(1))
    }

    // -------------------------------------------------------------------------
    // Infra plane
    // -------------------------------------------------------------------------

    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        let queue_name = routing::deploy_queue(&self.inner.prefix);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        let queue_name = routing::deploy_queue(&self.inner.prefix);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<InfraRoutingKey, DeployAgentMessage> =
            envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        let queue_name = routing::delete_queue(&self.inner.prefix);
        let body = envelope::Envelope { key, msg }.to_json()?;
        self.send_to_queue(&queue_name, &body)
    }

    fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        let queue_name = routing::delete_queue(&self.inner.prefix);
        let (body, ack) = self.receive_from_queue(&queue_name)?;
        let env: envelope::Envelope<InfraRoutingKey, DeleteAgentMessage> =
            envelope::from_json(&body)?;
        Ok((env.key, env.msg, ack))
    }
}
