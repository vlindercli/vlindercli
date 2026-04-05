//! AMQP 0-9-1 `MessageQueue` implementation using `lapin`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, ExchangeKind,
};
use tokio::runtime::Runtime;

use vlinder_core::domain::{
    Acknowledgement, AgentName, CompleteMessage, DataMessageKind, DataRoutingKey,
    DeleteAgentMessage, DeployAgentMessage, ForkMessage, InfraRoutingKey, InvokeMessage,
    MessageQueue, PromoteMessage, QueueError, RequestMessage, ResponseMessage, SessionMessageKind,
    SessionRoutingKey, SessionStartMessage,
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

    /// Publish a JSON-serialized message to the topic exchange.
    fn publish(
        &self,
        routing_key: &str,
        message_id: &str,
        payload: &[u8],
    ) -> Result<(), QueueError> {
        self.inner.runtime.block_on(async {
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
        })
    }
}

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

    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let rk = routing::invoke_routing_key(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize invoke: {e}")))?;
        self.publish(&rk, msg.id.as_str(), &payload)
    }

    fn receive_invoke(
        &self,
        _agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        Err(QueueError::Timeout) // step 3
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
        self.publish(&rk, msg.id.as_str(), &payload)
    }

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
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
        self.publish(&rk, msg.id.as_str(), &payload)
    }

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
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
        self.publish(&rk, msg.id.as_str(), &payload)
    }

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Fork { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Fork kind".into()));
        };
        let rk = routing::fork_routing_key(&key, agent_name);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize fork: {e}")))?;
        self.publish(&rk, msg.id.as_str(), &payload)
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Promote { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Promote kind".into()));
        };
        let rk = routing::promote_routing_key(&key, agent_name, msg.branch_id);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize promote: {e}")))?;
        self.publish(&rk, msg.id.as_str(), &payload)
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
        self.publish(&rk, key.submission.as_str(), &payload)
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        let rk = routing::delete_agent_routing_key(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize delete: {e}")))?;
        self.publish(&rk, key.submission.as_str(), &payload)
    }
}
