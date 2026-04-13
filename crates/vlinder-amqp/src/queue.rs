//! AMQP 0-9-1 `MessageQueue` implementation using `lapin`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use tokio::runtime::Runtime;

use vlinder_core::domain::{
    Acknowledgement, AgentName, CompleteMessage, DataMessageKind, DataRoutingKey,
    DeleteAgentMessage, DeployAgentMessage, ForkMessage, HarnessType, InfraRoutingKey,
    InvokeMessage, MessageQueue, Operation, PromoteMessage, QueueError, RequestMessage,
    ResponseMessage, Sequence, ServiceBackend, SessionMessageKind, SessionRoutingKey,
    SessionStartMessage, SubmissionId,
};

use crate::connect::{AmqpConfig, EXCHANGE_NAME};
use crate::routing;

/// AMQP 0-9-1 queue using a topic exchange.
///
/// Sync facade over async `lapin`. Clone is cheap (Arc).
#[derive(Clone)]
pub struct AmqpQueue {
    inner: Arc<AmqpQueueInner>,
}

struct AmqpQueueInner {
    runtime: Runtime,
    channel: Channel,
    #[allow(dead_code)]
    connection: Connection,
    #[allow(dead_code)]
    consumers: Mutex<HashMap<String, lapin::Consumer>>,
}

impl AmqpQueue {
    /// Connect to an AMQP broker and declare the topic exchange.
    pub fn connect(config: &AmqpConfig) -> Result<Self, QueueError> {
        let runtime = Runtime::new()
            .map_err(|e| QueueError::SendFailed(format!("failed to create runtime: {e}")))?;

        let (connection, channel) = runtime.block_on(async {
            let conn = Connection::connect(&config.url, ConnectionProperties::default())
                .await
                .map_err(|e| QueueError::SendFailed(format!("AMQP connect failed: {e}")))?;

            let ch = conn
                .create_channel()
                .await
                .map_err(|e| QueueError::SendFailed(format!("AMQP channel failed: {e}")))?;

            ch.exchange_declare(
                EXCHANGE_NAME,
                ExchangeKind::Topic,
                ExchangeDeclareOptions {
                    durable: true,
                    ..ExchangeDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| QueueError::SendFailed(format!("exchange declare failed: {e}")))?;

            Ok::<_, QueueError>((conn, ch))
        })?;

        Ok(Self {
            inner: Arc::new(AmqpQueueInner {
                runtime,
                channel,
                connection,
                consumers: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Get or create a consumer for the given binding pattern.
    ///
    /// Declares an exclusive auto-delete queue, binds it to the exchange
    /// with the binding pattern, and starts consuming. Consumers are cached
    /// by binding pattern.
    async fn get_or_create_consumer(&self, binding: &str) -> Result<lapin::Consumer, QueueError> {
        {
            let consumers = self.inner.consumers.lock().unwrap();
            if let Some(consumer) = consumers.get(binding) {
                return Ok(consumer.clone());
            }
        }

        let queue_name = routing::binding_to_queue_name(binding);

        self.inner
            .channel
            .queue_declare(
                &queue_name,
                QueueDeclareOptions {
                    exclusive: true,
                    auto_delete: true,
                    ..QueueDeclareOptions::default()
                },
                FieldTable::default(),
            )
            .await
            .map_err(|e| QueueError::ReceiveFailed(format!("queue declare failed: {e}")))?;

        self.inner
            .channel
            .queue_bind(
                &queue_name,
                EXCHANGE_NAME,
                binding,
                QueueBindOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| QueueError::ReceiveFailed(format!("queue bind failed: {e}")))?;

        let consumer = self
            .inner
            .channel
            .basic_consume(
                &queue_name,
                &queue_name,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .map_err(|e| QueueError::ReceiveFailed(format!("basic_consume failed: {e}")))?;

        {
            let mut consumers = self.inner.consumers.lock().unwrap();
            consumers.insert(binding.to_string(), consumer.clone());
        }

        Ok(consumer)
    }

    /// Fetch one message from a binding pattern, returning the routing key,
    /// payload, and an ack closure.
    fn fetch_one(&self, binding: &str) -> Result<(String, Vec<u8>, Acknowledgement), QueueError> {
        self.inner.runtime.block_on(async {
            let mut consumer = self.get_or_create_consumer(binding).await?;

            let delivery = tokio::time::timeout(Duration::from_millis(100), consumer.next())
                .await
                .map_err(|_| QueueError::Timeout)?
                .ok_or(QueueError::Timeout)?
                .map_err(|e| QueueError::ReceiveFailed(format!("consumer error: {e}")))?;

            let routing_key = delivery.routing_key.to_string();
            let payload = delivery.data.clone();
            let delivery_tag = delivery.delivery_tag;
            let channel = self.inner.channel.clone();
            let handle = self.inner.runtime.handle().clone();

            let ack_fn: Acknowledgement = Box::new(move || {
                handle.block_on(async {
                    channel
                        .basic_ack(delivery_tag, BasicAckOptions::default())
                        .await
                        .map_err(|e| QueueError::ReceiveFailed(format!("ack failed: {e}")))
                })
            });

            Ok((routing_key, payload, ack_fn))
        })
    }

    /// Publish a JSON-serialized message to the topic exchange.
    async fn publish(
        &self,
        routing_key: &str,
        message_id: &str,
        payload: &[u8],
    ) -> Result<(), QueueError> {
        let properties = BasicProperties::default()
            .with_message_id(message_id.into())
            .with_content_type("application/json".into())
            .with_delivery_mode(2); // persistent

        self.inner
            .channel
            .basic_publish(
                EXCHANGE_NAME,
                routing_key,
                BasicPublishOptions::default(),
                payload,
                properties,
            )
            .await
            .map_err(|e| QueueError::SendFailed(format!("publish failed: {e}")))?
            .await
            .map_err(|e| QueueError::SendFailed(format!("publish confirm failed: {e}")))?;

        Ok(())
    }
}

#[async_trait]
impl MessageQueue for AmqpQueue {
    fn on_cluster_start(&self) -> Result<(), QueueError> {
        tracing::info!(exchange = EXCHANGE_NAME, "AMQP topic exchange ready");
        Ok(())
    }

    fn on_agent_deployed(&self, agent: &AgentName) -> Result<(), QueueError> {
        tracing::debug!(agent = %agent, "AMQP: agent deployed — bindings created on first consume");
        Ok(())
    }

    fn on_agent_deleted(&self, agent: &AgentName) -> Result<(), QueueError> {
        tracing::debug!(agent = %agent, "AMQP: agent deleted — queues auto-delete on disconnect");
        Ok(())
    }

    async fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let rk = routing::invoke_routing_key(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize invoke: {e}")))?;
        self.publish(&rk, msg.id.as_str(), &payload).await
    }

    async fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let binding = routing::invoke_binding(agent);
        let (rk, payload, ack) = self.fetch_one(&binding)?;
        let key = routing::invoke_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid invoke routing key: {rk}"))
        })?;
        let msg: InvokeMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize invoke: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_complete(
        &self,
        submission: &SubmissionId,
        harness: HarnessType,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let binding = routing::complete_binding(submission, agent, harness);
        let (rk, payload, ack) = self.fetch_one(&binding)?;
        let key = routing::complete_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid complete routing key: {rk}"))
        })?;
        let msg: CompleteMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize complete: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_request(
        &self,
        service: ServiceBackend,
        operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let binding = routing::request_binding(service, operation);
        let (rk, payload, ack) = self.fetch_one(&binding)?;
        let key = routing::request_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid request routing key: {rk}"))
        })?;
        let msg: RequestMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize request: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_response(
        &self,
        submission: &SubmissionId,
        agent: &AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let binding = routing::response_binding(submission, agent, service, operation, sequence);
        let (rk, payload, ack) = self.fetch_one(&binding)?;
        let key = routing::response_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid response routing key: {rk}"))
        })?;
        let msg: ResponseMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize response: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_fork(
        &self,
    ) -> Result<(SessionRoutingKey, ForkMessage, Acknowledgement), QueueError> {
        let binding = routing::fork_binding();
        let (rk, payload, ack) = self.fetch_one(binding)?;
        let key = routing::fork_parse(&rk)
            .ok_or_else(|| QueueError::ReceiveFailed(format!("invalid fork routing key: {rk}")))?;
        let msg: ForkMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize fork: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_promote(
        &self,
    ) -> Result<(SessionRoutingKey, PromoteMessage, Acknowledgement), QueueError> {
        let binding = routing::promote_binding();
        let (rk, payload, ack) = self.fetch_one(binding)?;
        let key = routing::promote_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid promote routing key: {rk}"))
        })?;
        let msg: PromoteMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize promote: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        let binding = routing::deploy_agent_binding();
        let (rk, payload, ack) = self.fetch_one(binding)?;
        let key = routing::deploy_agent_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid deploy routing key: {rk}"))
        })?;
        let msg: DeployAgentMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize deploy: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        let binding = routing::delete_agent_binding();
        let (rk, payload, ack) = self.fetch_one(binding)?;
        let key = routing::delete_agent_parse(&rk).ok_or_else(|| {
            QueueError::ReceiveFailed(format!("invalid delete routing key: {rk}"))
        })?;
        let msg: DeleteAgentMessage = serde_json::from_slice(&payload)
            .map_err(|e| QueueError::ReceiveFailed(format!("deserialize delete: {e}")))?;
        Ok((key, msg, ack))
    }

    async fn receive_any(&self) -> Result<(String, Vec<u8>, Acknowledgement), QueueError> {
        let binding = "vlinder.#";
        let (rk, payload, ack) = self.fetch_one(binding)?;
        Ok((rk, payload, ack))
    }

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let DataMessageKind::Complete {
            ref agent, harness, ..
        } = key.kind
        else {
            return Err(QueueError::SendFailed("expected Complete key".into()));
        };
        let rk = routing::complete_routing_key(
            &key.session,
            key.branch,
            &key.submission,
            agent,
            harness,
        );
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize complete: {e}")))?;
        self.inner
            .runtime
            .block_on(self.publish(&rk, msg.id.as_str(), &payload))
    }

    async fn send_request(
        &self,
        key: DataRoutingKey,
        msg: RequestMessage,
    ) -> Result<(), QueueError> {
        let DataMessageKind::Request {
            ref agent,
            service,
            operation,
            sequence,
        } = key.kind
        else {
            return Err(QueueError::SendFailed("expected Request key".into()));
        };
        let rk = routing::request_routing_key(
            &key.session,
            key.branch,
            &key.submission,
            agent,
            service,
            operation,
            sequence,
        );
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize request: {e}")))?;

        let properties = BasicProperties::default()
            .with_message_id(msg.id.as_str().into())
            .with_content_type("application/json".into())
            .with_delivery_mode(2); // persistent

        self.inner
            .channel
            .basic_publish(
                EXCHANGE_NAME,
                &rk,
                BasicPublishOptions::default(),
                &payload,
                properties,
            )
            .await
            .map_err(|e| QueueError::SendFailed(format!("publish failed: {e}")))?
            .await
            .map_err(|e| QueueError::SendFailed(format!("publish confirm failed: {e}")))?;

        Ok(())
    }

    async fn send_response(
        &self,
        key: DataRoutingKey,
        msg: ResponseMessage,
    ) -> Result<(), QueueError> {
        let DataMessageKind::Response {
            ref agent,
            service,
            operation,
            sequence,
        } = key.kind
        else {
            return Err(QueueError::SendFailed("expected Response key".into()));
        };
        let rk = routing::response_routing_key(
            &key.session,
            key.branch,
            &key.submission,
            agent,
            service,
            operation,
            sequence,
        );
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize response: {e}")))?;
        self.publish(&rk, msg.id.as_str(), &payload).await
    }

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Fork { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Fork kind".into()));
        };
        let rk = routing::fork_routing_key(&key, agent_name);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize fork: {e}")))?;
        self.inner
            .runtime
            .block_on(self.publish(&rk, msg.id.as_str(), &payload))
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Promote { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Promote kind".into()));
        };
        let rk = routing::promote_routing_key(&key, agent_name, msg.branch_id);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize promote: {e}")))?;
        self.inner
            .runtime
            .block_on(self.publish(&rk, msg.id.as_str(), &payload))
    }

    fn send_session_start(
        &self,
        _key: SessionRoutingKey,
        _msg: SessionStartMessage,
    ) -> Result<vlinder_core::domain::BranchId, QueueError> {
        Ok(vlinder_core::domain::BranchId::from(1))
    }

    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        let rk = routing::deploy_agent_routing_key(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize deploy: {e}")))?;
        self.inner
            .runtime
            .block_on(self.publish(&rk, key.submission.as_str(), &payload))
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        let rk = routing::delete_agent_routing_key(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize delete: {e}")))?;
        self.inner
            .runtime
            .block_on(self.publish(&rk, key.submission.as_str(), &payload))
    }
}
