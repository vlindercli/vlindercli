//! NATS-backed message queue with `JetStream` durability (ADR 044).
//!
//! Provides a sync facade over the async NATS client. The runtime is owned
//! internally, so callers use simple blocking APIs while getting async I/O.
//!
//! Uses typed messages exclusively for full observability.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::{self, consumer, stream};
type PullConsumer = async_nats::jetstream::consumer::Consumer<consumer::pull::Config>;
use async_nats::jetstream::message::Message as JetStreamMessage;
use futures::StreamExt;
use tokio::runtime::Runtime;

use std::str::FromStr;

use vlinder_core::domain::{
    Acknowledgement, AgentName, BranchId, CompleteMessage, DataMessageKind, DataRoutingKey,
    DeleteAgentMessage, DeployAgentMessage, ForkMessage, HarnessType, InfraMessageKind,
    InfraRoutingKey, InvokeMessage, MessageQueue, Operation, PromoteMessage, QueueError,
    RequestMessage, ResponseMessage, RuntimeType, Sequence, ServiceBackend, ServiceType, SessionId,
    SessionMessageKind, SessionRoutingKey, SessionStartMessage, SubmissionId,
};

/// NATS queue with `JetStream` durability.
///
/// Sync facade over async internals. Clone is cheap (Arc).
#[derive(Clone)]
pub struct NatsQueue {
    inner: Arc<NatsQueueInner>,
}

struct NatsQueueInner {
    runtime: Runtime,
    client: async_nats::Client,
    jetstream: jetstream::Context,
    consumers: Mutex<HashMap<String, PullConsumer>>,
}

impl NatsQueue {
    /// Connect to a NATS server using the given config.
    ///
    /// Creates the VLINDER stream if it doesn't exist.
    pub fn connect(config: &crate::NatsConfig) -> Result<Self, QueueError> {
        let runtime = Runtime::new()
            .map_err(|e| QueueError::SendFailed(format!("failed to create runtime: {e}")))?;

        let (client, jetstream) = runtime.block_on(async {
            let client = crate::connect::nats_connect(config)
                .await
                .map_err(QueueError::SendFailed)?;

            let jetstream = jetstream::new(client.clone());

            // Ensure stream exists
            Self::ensure_stream(&jetstream).await?;

            Ok::<_, QueueError>((client, jetstream))
        })?;

        Ok(Self {
            inner: Arc::new(NatsQueueInner {
                runtime,
                client,
                jetstream,
                consumers: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// Ensure the VLINDER stream exists.
    async fn ensure_stream(jetstream: &jetstream::Context) -> Result<(), QueueError> {
        let config = stream::Config {
            name: "VLINDER".to_string(),
            subjects: vec!["vlinder.>".to_string()],
            retention: stream::RetentionPolicy::Limits,
            max_age: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_bytes: 100 * 1024 * 1024,                   // 100 MiB — required by NGS
            ..Default::default()
        };

        // get_or_create_stream creates if missing, returns existing if present
        jetstream
            .get_or_create_stream(config)
            .await
            .map_err(|e| QueueError::SendFailed(format!("failed to create stream: {e}")))?;

        Ok(())
    }

    /// Escape hatch: access the async client directly.
    pub fn async_client(&self) -> &async_nats::Client {
        &self.inner.client
    }

    /// Escape hatch: access `JetStream` context directly.
    pub fn jetstream(&self) -> &jetstream::Context {
        &self.inner.jetstream
    }

    /// Get or create a named consumer for the given filter.
    ///
    /// Consumers are cached by filter pattern to avoid creating ephemeral
    /// consumers on every poll — which floods the NATS server and causes
    /// messages to get stuck in "pending ack" limbo on abandoned consumers.
    async fn get_or_create_consumer(&self, filter: &str) -> Result<PullConsumer, QueueError> {
        // Check cache first
        {
            let consumers = self.inner.consumers.lock().unwrap();
            if let Some(consumer) = consumers.get(filter) {
                return Ok(consumer.clone());
            }
        }

        // Create a named consumer — name derived from filter for uniqueness
        let name = filter_to_consumer_name(filter);
        let stream = self
            .inner
            .jetstream
            .get_stream("VLINDER")
            .await
            .map_err(|e| QueueError::ReceiveFailed(e.to_string()))?;

        // TODO: inactive_threshold is hardcoded — see ADR 053 for adaptive timeout strategy
        let consumer = stream
            .create_consumer(consumer::pull::Config {
                name: Some(name),
                filter_subject: filter.to_string(),
                ack_wait: Duration::from_secs(300),
                inactive_threshold: Duration::from_secs(300),
                ..Default::default()
            })
            .await
            .map_err(|e| QueueError::ReceiveFailed(e.to_string()))?;

        // Cache it
        {
            let mut consumers = self.inner.consumers.lock().unwrap();
            consumers.insert(filter.to_string(), consumer.clone());
        }

        Ok(consumer)
    }

    /// Fetch one message from a subject filter, returning the message and an ack closure.
    ///
    /// If the fetch fails with a 503 (consumer GC'd by server), evicts the
    /// stale consumer from cache, recreates it, and retries once.
    async fn fetch_one(
        &self,
        filter: &str,
    ) -> Result<
        (
            JetStreamMessage,
            Box<dyn FnOnce() -> Result<(), QueueError> + Send>,
        ),
        QueueError,
    > {
        match self.try_fetch_one(filter).await {
            Ok(result) => Ok(result),
            Err(QueueError::ReceiveFailed(ref msg)) if msg.contains("503") => {
                tracing::warn!(filter = filter, "consumer stale (503), recreating");
                self.evict_consumer(filter);
                self.try_fetch_one(filter).await
            }
            Err(e) => Err(e),
        }
    }

    /// Attempt a single fetch from the consumer for this filter.
    async fn try_fetch_one(
        &self,
        filter: &str,
    ) -> Result<
        (
            JetStreamMessage,
            Box<dyn FnOnce() -> Result<(), QueueError> + Send>,
        ),
        QueueError,
    > {
        let consumer = self.get_or_create_consumer(filter).await?;

        let mut messages = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_millis(100))
            .messages()
            .await
            .map_err(|e| QueueError::ReceiveFailed(e.to_string()))?;

        let js_msg = messages
            .next()
            .await
            .ok_or(QueueError::Timeout)?
            .map_err(|e| QueueError::ReceiveFailed(e.to_string()))?;

        // Wrap message for ack closure
        let js_msg_for_ack: Arc<Mutex<Option<JetStreamMessage>>> =
            Arc::new(Mutex::new(Some(js_msg.clone())));
        let handle = self.inner.runtime.handle().clone();

        let ack_fn: Box<dyn FnOnce() -> Result<(), QueueError> + Send> = Box::new(move || {
            if let Some(msg) = js_msg_for_ack.lock().unwrap().take() {
                handle.block_on(async {
                    msg.ack()
                        .await
                        .map_err(|e| QueueError::ReceiveFailed(format!("ack failed: {e}")))
                })
            } else {
                Ok(())
            }
        });

        Ok((js_msg, ack_fn))
    }

    /// Remove a cached consumer so the next fetch recreates it.
    fn evict_consumer(&self, filter: &str) {
        let mut consumers = self.inner.consumers.lock().unwrap();
        consumers.remove(filter);
    }
}

impl MessageQueue for NatsQueue {
    fn on_cluster_start(&self) -> Result<(), QueueError> {
        // Stream is already created in connect(). Log its state for observability.
        self.inner.runtime.block_on(async {
            match self.inner.jetstream.get_stream("VLINDER").await {
                Ok(mut stream) => {
                    let info = stream.info().await;
                    match info {
                        Ok(info) => {
                            tracing::info!(
                                stream = "VLINDER",
                                messages = info.state.messages,
                                bytes = info.state.bytes,
                                consumers = info.state.consumer_count,
                                "NATS stream ready"
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                stream = "VLINDER",
                                error = %e,
                                "NATS stream exists but failed to query info"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        stream = "VLINDER",
                        error = %e,
                        "NATS stream not found — ensure_stream may have failed at connect time"
                    );
                }
            }
        });
        Ok(())
    }

    fn on_agent_deployed(&self, agent: &AgentName) -> Result<(), QueueError> {
        // NATS subjects are implicit — no queues to create.
        // The agent's subjects (invoke, complete, response) will be created
        // on first publish. Log for traceability.
        tracing::debug!(
            agent = %agent,
            "NATS: agent deployed — subjects will be created on first message"
        );
        Ok(())
    }

    fn on_agent_deleted(&self, agent: &AgentName) -> Result<(), QueueError> {
        // NATS subjects don't need explicit deletion — messages expire
        // via stream retention policy (max_age / max_bytes).
        tracing::debug!(
            agent = %agent,
            "NATS: agent deleted — subjects will expire via retention policy"
        );
        Ok(())
    }

    fn send_invoke(&self, key: DataRoutingKey, msg: InvokeMessage) -> Result<(), QueueError> {
        let subject = invoke_subject(&key);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize invoke: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn receive_invoke(
        &self,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, InvokeMessage, Acknowledgement), QueueError> {
        let filter = invoke_filter(agent);

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(&filter).await?;

            let subject = js_msg.subject.as_str();
            let key = invoke_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid invoke subject: {subject}"))
            })?;

            let msg: InvokeMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize invoke: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }

    fn send_complete(&self, key: DataRoutingKey, msg: CompleteMessage) -> Result<(), QueueError> {
        let DataMessageKind::Complete { agent, harness } = &key.kind else {
            return Err(QueueError::SendFailed(
                "send_complete: expected Complete key".into(),
            ));
        };
        let subject = complete_subject(&key.session, key.branch, &key.submission, agent, *harness);
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize complete: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn receive_complete(
        &self,
        submission: &SubmissionId,
        harness: HarnessType,
        agent: &AgentName,
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let filter = complete_filter(submission, agent, harness);

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(&filter).await?;

            let subject = js_msg.subject.as_str();
            let key = complete_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid complete subject: {subject}"))
            })?;

            let msg: CompleteMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize complete: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }

    fn send_request(&self, key: DataRoutingKey, msg: RequestMessage) -> Result<(), QueueError> {
        let DataMessageKind::Request {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            return Err(QueueError::SendFailed(
                "send_request: expected Request key".into(),
            ));
        };
        let subject = request_subject(
            &key.session,
            key.branch,
            &key.submission,
            agent,
            *service,
            *operation,
            *sequence,
        );
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize request: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn receive_request(
        &self,
        service: ServiceBackend,
        operation: Operation,
    ) -> Result<(DataRoutingKey, RequestMessage, Acknowledgement), QueueError> {
        let filter = request_filter(service, operation);

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(&filter).await?;

            let subject = js_msg.subject.as_str();
            let key = request_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid request subject: {subject}"))
            })?;

            let msg: RequestMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize request: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }

    fn send_response(&self, key: DataRoutingKey, msg: ResponseMessage) -> Result<(), QueueError> {
        let DataMessageKind::Response {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            return Err(QueueError::SendFailed(
                "send_response: expected Response key".into(),
            ));
        };
        let subject = response_subject(
            &key.session,
            key.branch,
            &key.submission,
            agent,
            *service,
            *operation,
            *sequence,
        );
        let payload = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize response: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, payload.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn receive_response(
        &self,
        submission: &SubmissionId,
        agent: &AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let filter = response_filter(submission, agent, service, operation, sequence);

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(&filter).await?;

            let subject = js_msg.subject.as_str();
            let key = response_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid response subject: {subject}"))
            })?;

            let msg: ResponseMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize response: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }

    fn send_fork(&self, key: SessionRoutingKey, msg: ForkMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Fork { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Fork kind".into()));
        };
        let subject = fork_subject(&key, agent_name);
        let body = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize fork: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, body.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;
            Ok(())
        })
    }

    fn send_promote(&self, key: SessionRoutingKey, msg: PromoteMessage) -> Result<(), QueueError> {
        let SessionMessageKind::Promote { ref agent_name } = key.kind else {
            return Err(QueueError::SendFailed("expected Promote kind".into()));
        };
        let subject = promote_subject(&key, agent_name, msg.branch_id);
        let body = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize promote: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, body.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;
            Ok(())
        })
    }

    fn send_session_start(
        &self,
        _key: SessionRoutingKey,
        _msg: SessionStartMessage,
    ) -> Result<BranchId, QueueError> {
        // Session start is fire-and-forget — no NATS message needed.
        // RecordingQueue handles persistence before this is called.
        Ok(BranchId::from(1))
    }

    fn send_deploy_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeployAgentMessage,
    ) -> Result<(), QueueError> {
        let subject = deploy_agent_subject(&key);
        let body = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize deploy_agent: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, body.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;
            Ok(())
        })
    }

    fn send_delete_agent(
        &self,
        key: InfraRoutingKey,
        msg: DeleteAgentMessage,
    ) -> Result<(), QueueError> {
        let subject = delete_agent_subject(&key);
        let body = serde_json::to_vec(&msg)
            .map_err(|e| QueueError::SendFailed(format!("serialize delete_agent: {e}")))?;

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", msg.id.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, body.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;
            Ok(())
        })
    }

    fn receive_deploy_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeployAgentMessage, Acknowledgement), QueueError> {
        let filter = "vlinder.infra.v1.*.deploy-agent";

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(filter).await?;

            let subject = js_msg.subject.as_str();
            let key = deploy_agent_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid deploy-agent subject: {subject}"))
            })?;

            let msg: DeployAgentMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize deploy-agent: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }

    fn receive_delete_agent(
        &self,
    ) -> Result<(InfraRoutingKey, DeleteAgentMessage, Acknowledgement), QueueError> {
        let filter = "vlinder.infra.v1.*.delete-agent";

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(filter).await?;

            let subject = js_msg.subject.as_str();
            let key = delete_agent_parse_subject(subject).ok_or_else(|| {
                QueueError::ReceiveFailed(format!("invalid delete-agent subject: {subject}"))
            })?;

            let msg: DeleteAgentMessage = serde_json::from_slice(&js_msg.payload)
                .map_err(|e| QueueError::ReceiveFailed(format!("deserialize delete-agent: {e}")))?;

            Ok((key, msg, ack_fn))
        })
    }
}

// ============================================================================
// Data-plane invoke subject (ADR 121)
//
// Format: vlinder.data.v1.{session}.{branch}.{submission}.invoke.{harness}.{runtime}.{agent}
// Positions: 0      1    2  3         4        5            6      7         8         9
//
// All three operations derive from this single format definition.
// ============================================================================

const INVOKE_PREFIX: &str = "vlinder.data.v1";
const INVOKE_KIND: &str = "invoke";
const INVOKE_SEGMENT_COUNT: usize = 10;

/// Build a NATS subject from a `DataRoutingKey` for invoke.
fn invoke_subject(key: &DataRoutingKey) -> String {
    use vlinder_core::domain::DataMessageKind;
    let DataMessageKind::Invoke {
        harness,
        runtime,
        agent,
    } = &key.kind
    else {
        panic!("invoke_subject called with non-Invoke key");
    };
    format!(
        "{INVOKE_PREFIX}.{}.{}.{}.{INVOKE_KIND}.{harness}.{runtime}.{agent}",
        key.session, key.branch, key.submission
    )
}

/// Parse a NATS subject back into a `DataRoutingKey` for invoke.
pub fn invoke_parse_subject(subject: &str) -> Option<DataRoutingKey> {
    use vlinder_core::domain::DataMessageKind;
    let s: Vec<&str> = subject.split('.').collect();
    if s.len() != INVOKE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != INVOKE_KIND {
        return None;
    }

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Invoke {
            harness: HarnessType::from_str(s[7]).ok()?,
            runtime: RuntimeType::from_str(s[8]).ok()?,
            agent: AgentName::new(s[9]),
        },
    })
}

/// NATS wildcard filter for receiving invoke messages for a specific agent.
fn invoke_filter(agent: &AgentName) -> String {
    format!("{INVOKE_PREFIX}.*.*.*.{INVOKE_KIND}.*.*.{}", agent.as_str())
}

// ============================================================================
// Complete subject format (ADR 121)
//
// vlinder.data.v1.{session}.{branch}.{sub}.complete.{agent}.{harness}
// ============================================================================

const COMPLETE_PREFIX: &str = "vlinder.data.v1";
const COMPLETE_KIND: &str = "complete";
const COMPLETE_SEGMENT_COUNT: usize = 9;

/// Build a NATS subject for a complete message.
fn complete_subject(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    harness: HarnessType,
) -> String {
    format!("{COMPLETE_PREFIX}.{session}.{branch}.{submission}.{COMPLETE_KIND}.{agent}.{harness}",)
}

/// Parse a NATS subject back into a `DataRoutingKey` for complete.
pub fn complete_parse_subject(subject: &str) -> Option<DataRoutingKey> {
    use vlinder_core::domain::DataMessageKind;
    let s: Vec<&str> = subject.split('.').collect();
    if s.len() != COMPLETE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != COMPLETE_KIND {
        return None;
    }

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Complete {
            agent: AgentName::new(s[7]),
            harness: HarnessType::from_str(s[8]).ok()?,
        },
    })
}

/// NATS filter for receiving complete messages for a specific submission and agent.
fn complete_filter(submission: &SubmissionId, agent: &AgentName, harness: HarnessType) -> String {
    format!(
        "{COMPLETE_PREFIX}.*.*.{}.{COMPLETE_KIND}.{}.{}",
        submission.as_str(),
        agent.as_str(),
        harness.as_str()
    )
}

// ============================================================================
// Request subject format (ADR 121)
//
// vlinder.data.v1.{session}.{branch}.{sub}.request.{agent}.{service}.{backend}.{op}.{seq}
// ============================================================================

const REQUEST_PREFIX: &str = "vlinder.data.v1";
const REQUEST_KIND: &str = "request";
const REQUEST_SEGMENT_COUNT: usize = 12;

/// Build a NATS subject for a data-plane request message.
fn request_subject(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{REQUEST_PREFIX}.{session}.{branch}.{submission}.{REQUEST_KIND}.{agent}.{}.{}.{}.{}",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

/// Parse a NATS subject back into a `DataRoutingKey` for request.
///
/// Subject: `vlinder.data.v1.{session}.{branch}.{sub}.request.{agent}.{svc_type}.{backend}.{op}.{seq}`
/// Index:    0       1    2   3         4        5     6       7       8          9         10   11
pub fn request_parse_subject(subject: &str) -> Option<DataRoutingKey> {
    use vlinder_core::domain::DataMessageKind;
    let s: Vec<&str> = subject.split('.').collect();
    if s.len() != REQUEST_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != REQUEST_KIND {
        return None;
    }

    let service = ServiceBackend::from_parts(ServiceType::from_str(s[8]).ok()?, s[9])?;
    let operation: Operation = s[10].parse().ok()?;
    let sequence = Sequence::from(s[11].parse::<u32>().ok()?);

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Request {
            agent: AgentName::new(s[7]),
            service,
            operation,
            sequence,
        },
    })
}

/// NATS wildcard filter for receiving request messages for a service+operation.
fn request_filter(service: ServiceBackend, operation: Operation) -> String {
    format!(
        "{REQUEST_PREFIX}.*.*.*.{REQUEST_KIND}.*.{}.{}.{}.>",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
    )
}

// ============================================================================
// Response subject format (ADR 121)
//
// vlinder.data.v1.{session}.{branch}.{sub}.response.{agent}.{svc_type}.{backend}.{op}.{seq}
// ============================================================================

const RESPONSE_PREFIX: &str = "vlinder.data.v1";
const RESPONSE_KIND: &str = "response";
const RESPONSE_SEGMENT_COUNT: usize = 12;

/// Build a NATS subject for a data-plane response message.
fn response_subject(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{RESPONSE_PREFIX}.{session}.{branch}.{submission}.{RESPONSE_KIND}.{agent}.{}.{}.{}.{}",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

/// Parse a NATS subject back into a `DataRoutingKey` for response.
///
/// Subject: `vlinder.data.v1.{session}.{branch}.{sub}.response.{agent}.{svc_type}.{backend}.{op}.{seq}`
/// Index:    0       1    2   3         4        5     6        7       8          9         10   11
pub fn response_parse_subject(subject: &str) -> Option<DataRoutingKey> {
    use vlinder_core::domain::DataMessageKind;
    let s: Vec<&str> = subject.split('.').collect();
    if s.len() != RESPONSE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != RESPONSE_KIND {
        return None;
    }

    let service = ServiceBackend::from_parts(ServiceType::from_str(s[8]).ok()?, s[9])?;
    let operation: Operation = s[10].parse().ok()?;
    let sequence = Sequence::from(s[11].parse::<u32>().ok()?);

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Response {
            agent: AgentName::new(s[7]),
            service,
            operation,
            sequence,
        },
    })
}

/// NATS wildcard filter for receiving response messages for a specific submission+service+operation.
fn response_filter(
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{RESPONSE_PREFIX}.*.*.{}.{RESPONSE_KIND}.{}.{}.{}.{}.{}",
        submission.as_str(),
        agent.as_str(),
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

/// Build a NATS subject for a fork message.
fn fork_subject(key: &SessionRoutingKey, agent_name: &AgentName) -> String {
    format!(
        "vlinder.{}.1.{}.fork.{}",
        key.session, key.submission, agent_name
    )
}

/// Build a NATS subject for a promote message.
fn promote_subject(key: &SessionRoutingKey, agent_name: &AgentName, branch_id: BranchId) -> String {
    format!(
        "vlinder.{}.{}.{}.promote.{}",
        key.session, branch_id, key.submission, agent_name
    )
}

/// Parse a fork NATS subject. Returns `SessionRoutingKey` if the subject matches.
pub fn fork_parse_subject(subject: &str) -> Option<SessionRoutingKey> {
    let s: Vec<&str> = subject.split('.').collect();
    // vlinder.{session}.{branch}.{submission}.fork.{agent}
    if s.len() != 6 || s[0] != "vlinder" || s[4] != "fork" {
        return None;
    }
    Some(SessionRoutingKey {
        session: SessionId::try_from(s[1].to_string()).ok()?,
        submission: SubmissionId::from(s[3].to_string()),
        kind: SessionMessageKind::Fork {
            agent_name: AgentName::new(s[5]),
        },
    })
}

/// Parse a promote NATS subject. Returns `SessionRoutingKey` if the subject matches.
pub fn promote_parse_subject(subject: &str) -> Option<SessionRoutingKey> {
    let s: Vec<&str> = subject.split('.').collect();
    // vlinder.{session}.{branch}.{submission}.promote.{agent}
    if s.len() != 6 || s[0] != "vlinder" || s[4] != "promote" {
        return None;
    }
    Some(SessionRoutingKey {
        session: SessionId::try_from(s[1].to_string()).ok()?,
        submission: SubmissionId::from(s[3].to_string()),
        kind: SessionMessageKind::Promote {
            agent_name: AgentName::new(s[5]),
        },
    })
}

// ============================================================================
// Infra-plane subjects (ADR 121)
//
// Format: vlinder.infra.v1.{submission}.deploy-agent
// Positions: 0      1     2  3            4
// ============================================================================

/// Build a NATS subject for a deploy-agent message.
fn deploy_agent_subject(key: &InfraRoutingKey) -> String {
    format!("vlinder.infra.v1.{}.deploy-agent", key.submission)
}

/// Build a NATS subject for a delete-agent message.
fn delete_agent_subject(key: &InfraRoutingKey) -> String {
    format!("vlinder.infra.v1.{}.delete-agent", key.submission)
}

/// Parse a deploy-agent NATS subject. Returns `InfraRoutingKey` if the subject matches.
pub fn deploy_agent_parse_subject(subject: &str) -> Option<InfraRoutingKey> {
    let s: Vec<&str> = subject.split('.').collect();
    // vlinder.infra.v1.{submission}.deploy-agent
    if s.len() != 5
        || s[0] != "vlinder"
        || s[1] != "infra"
        || s[2] != "v1"
        || s[4] != "deploy-agent"
    {
        return None;
    }
    Some(InfraRoutingKey {
        submission: SubmissionId::from(s[3].to_string()),
        kind: InfraMessageKind::DeployAgent,
    })
}

/// Parse a delete-agent NATS subject. Returns `InfraRoutingKey` if the subject matches.
pub fn delete_agent_parse_subject(subject: &str) -> Option<InfraRoutingKey> {
    let s: Vec<&str> = subject.split('.').collect();
    // vlinder.infra.v1.{submission}.delete-agent
    if s.len() != 5
        || s[0] != "vlinder"
        || s[1] != "infra"
        || s[2] != "v1"
        || s[4] != "delete-agent"
    {
        return None;
    }
    Some(InfraRoutingKey {
        submission: SubmissionId::from(s[3].to_string()),
        kind: InfraMessageKind::DeleteAgent,
    })
}

/// Derive a stable consumer name from a filter pattern.
///
/// NATS consumer names must be alphanumeric + dash/underscore.
/// We replace dots and wildcards with underscores.
fn filter_to_consumer_name(filter: &str) -> String {
    filter.replace('.', "_").replace('*', "W").replace('>', "G")
}

// ============================================================================
// Tests — data-plane subject serialization (ADR 121)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionId {
        SessionId::try_from("00000000-0000-4000-8000-000000000001".to_string()).unwrap()
    }
    fn timeline() -> BranchId {
        BranchId::from(1)
    }
    fn submission() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }
    fn agent() -> AgentName {
        AgentName::new("echo")
    }

    // ========================================================================
    // Invoke subject format (ADR 121)
    // ========================================================================

    #[test]
    fn data_invoke_subject_format() {
        let key = DataRoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: vlinder_core::domain::DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: agent(),
            },
        };
        assert_eq!(
            invoke_subject(&key),
            format!(
                "vlinder.data.v1.{}.{}.{}.invoke.cli.container.echo",
                session(),
                timeline(),
                submission()
            ),
        );
    }

    #[test]
    fn data_invoke_subject_round_trips() {
        let key = DataRoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: vlinder_core::domain::DataMessageKind::Invoke {
                harness: HarnessType::Grpc,
                runtime: RuntimeType::Container,
                agent: agent(),
            },
        };
        let subject = invoke_subject(&key);
        let recovered =
            invoke_parse_subject(&subject).unwrap_or_else(|| panic!("failed to parse: {subject}"));
        assert_eq!(recovered, key);
    }

    #[test]
    fn data_invoke_filter_matches_agent() {
        let filter = invoke_filter(&agent());
        assert_eq!(filter, "vlinder.data.v1.*.*.*.invoke.*.*.echo");
    }

    #[test]
    fn data_invoke_parse_rejects_old_format() {
        let old = format!(
            "vlinder.{}.{}.{}.invoke.cli.container.echo",
            session(),
            timeline(),
            submission()
        );
        assert!(invoke_parse_subject(&old).is_none());
    }

    #[test]
    fn data_invoke_parse_rejects_wrong_segment_count() {
        assert!(invoke_parse_subject("vlinder.data.v1").is_none());
        assert!(invoke_parse_subject("").is_none());
    }

    #[test]
    fn data_invoke_parse_rejects_wrong_version() {
        let subject = format!(
            "vlinder.data.v2.{}.{}.{}.invoke.cli.container.echo",
            session(),
            timeline(),
            submission()
        );
        assert!(invoke_parse_subject(&subject).is_none());
    }

    // ========================================================================
    // Infra plane subject format (ADR 121)
    // ========================================================================

    #[test]
    fn infra_deploy_agent_subject_format() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeployAgent,
        };
        assert_eq!(
            deploy_agent_subject(&key),
            format!("vlinder.infra.v1.{}.deploy-agent", submission()),
        );
    }

    #[test]
    fn infra_deploy_agent_subject_round_trips() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeployAgent,
        };
        let subject = deploy_agent_subject(&key);
        let parsed = deploy_agent_parse_subject(&subject).expect("should parse");
        assert_eq!(parsed.submission, key.submission);
        assert_eq!(parsed.kind, InfraMessageKind::DeployAgent);
    }

    #[test]
    fn infra_delete_agent_subject_format() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeleteAgent,
        };
        assert_eq!(
            delete_agent_subject(&key),
            format!("vlinder.infra.v1.{}.delete-agent", submission()),
        );
    }

    #[test]
    fn infra_delete_agent_subject_round_trips() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeleteAgent,
        };
        let subject = delete_agent_subject(&key);
        let parsed = delete_agent_parse_subject(&subject).expect("should parse");
        assert_eq!(parsed.submission, key.submission);
        assert_eq!(parsed.kind, InfraMessageKind::DeleteAgent);
    }

    #[test]
    fn infra_deploy_agent_parse_rejects_invalid() {
        assert!(deploy_agent_parse_subject("vlinder.infra.v1.sub.delete-agent").is_none());
        assert!(deploy_agent_parse_subject("vlinder.data.v1.sub.deploy-agent").is_none());
        assert!(deploy_agent_parse_subject("vlinder.infra.v2.sub.deploy-agent").is_none());
        assert!(deploy_agent_parse_subject("").is_none());
    }

    #[test]
    fn infra_delete_agent_parse_rejects_invalid() {
        assert!(delete_agent_parse_subject("vlinder.infra.v1.sub.deploy-agent").is_none());
        assert!(delete_agent_parse_subject("vlinder.data.v1.sub.delete-agent").is_none());
        assert!(delete_agent_parse_subject("").is_none());
    }
}
