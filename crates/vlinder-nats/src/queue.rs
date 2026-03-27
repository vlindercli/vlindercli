//! NATS-backed message queue with `JetStream` durability (ADR 044).
//!
//! Provides a sync facade over the async NATS client. The runtime is owned
//! internally, so callers use simple blocking APIs while getting async I/O.
//!
//! Uses typed messages exclusively for full observability.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_nats::jetstream::{self, consumer, stream};
type PullConsumer = async_nats::jetstream::consumer::Consumer<consumer::pull::Config>;
use async_nats::jetstream::message::Message as JetStreamMessage;
use futures::StreamExt;
use tokio::runtime::Runtime;

use std::str::FromStr;

use vlinder_core::domain::{
    Acknowledgement, AgentName, BranchId, CompleteMessage, DagNodeId, DataMessageKind,
    DataRoutingKey, HarnessType, InvokeMessage, MessageId, MessageQueue, Operation, QueueError,
    RepairMessage, RequestMessage, ResponseMessage, RoutingKey, RoutingKind, RuntimeType, Sequence,
    ServiceBackend, ServiceType, SessionId, SubmissionId,
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
    ) -> Result<(DataRoutingKey, CompleteMessage, Acknowledgement), QueueError> {
        let filter = complete_filter(submission, harness);

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
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    ) -> Result<(DataRoutingKey, ResponseMessage, Acknowledgement), QueueError> {
        let filter = response_filter(submission, service, operation, sequence);

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

    fn send_repair(&self, msg: RepairMessage) -> Result<(), QueueError> {
        let subject = routing_key_to_subject(&msg.routing_key());

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("msg-id", msg.id.as_str());
            headers.insert("protocol-version", msg.protocol_version.as_str());
            headers.insert("branch-id", msg.branch.to_string());
            headers.insert("submission-id", msg.submission.as_str());
            headers.insert("session-id", msg.session.as_str());
            headers.insert("agent-id", msg.agent_name.as_str());
            headers.insert("harness", msg.harness.as_str());
            headers.insert("dag-parent", msg.dag_parent.as_str());
            headers.insert("checkpoint", msg.checkpoint.as_str());
            headers.insert("service", msg.service.service_type().as_str());
            headers.insert("backend", msg.service.backend_str());
            headers.insert("operation", msg.operation.as_str());
            headers.insert("sequence", msg.sequence.to_string());
            if let Some(ref state) = msg.state {
                headers.insert("state", state.as_str());
            }

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, msg.payload.into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn receive_repair(
        &self,
        agent: &AgentName,
    ) -> Result<(RepairMessage, Acknowledgement), QueueError> {
        // Build filter: vlinder.{session}.{branch}.{submission}.repair.{harness}.{agent}
        let filter = format!("vlinder.*.*.*.repair.*.{}", agent.as_str());

        self.inner.runtime.block_on(async {
            let (js_msg, ack_fn) = self.fetch_one(&filter).await?;

            let headers = js_msg
                .headers
                .as_ref()
                .ok_or_else(|| QueueError::ReceiveFailed("missing headers".to_string()))?;

            let msg = RepairMessage {
                id: MessageId::from(get_header(headers, "msg-id")?),
                protocol_version: get_header(headers, "protocol-version").unwrap_or_default(),
                branch: get_header(headers, "branch-id")
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .map_or(BranchId::from(1), BranchId::from),
                submission: SubmissionId::from(get_header(headers, "submission-id")?),
                session: SessionId::try_from(get_header(headers, "session-id")?)
                    .map_err(QueueError::ReceiveFailed)?,
                agent_name: AgentName::new(get_header(headers, "agent-id")?),
                harness: HarnessType::from_str(&get_header(headers, "harness")?)
                    .map_err(|_| QueueError::ReceiveFailed("unknown harness type".to_string()))?,
                dag_parent: DagNodeId::from(get_header(headers, "dag-parent")?),
                checkpoint: get_header(headers, "checkpoint")?,
                service: ServiceBackend::from_parts(
                    ServiceType::from_str(&get_header(headers, "service")?).map_err(|_| {
                        QueueError::ReceiveFailed("unknown service type".to_string())
                    })?,
                    &get_header(headers, "backend")?,
                )
                .ok_or_else(|| QueueError::ReceiveFailed("invalid service/backend".to_string()))?,
                operation: Operation::from_str(&get_header(headers, "operation")?)
                    .map_err(|_| QueueError::ReceiveFailed("unknown operation".to_string()))?,
                sequence: Sequence::from(
                    get_header(headers, "sequence")?.parse::<u32>().unwrap_or(1),
                ),
                payload: js_msg.payload.to_vec(),
                state: get_header(headers, "state").ok(),
            };

            Ok((msg, ack_fn))
        })
    }

    fn send_fork(&self, msg: vlinder_core::domain::ForkMessage) -> Result<(), QueueError> {
        let subject = routing_key_to_subject(&RoutingKey {
            session: msg.session.clone(),
            branch: msg.branch,
            submission: msg.submission.clone(),
            kind: RoutingKind::Fork {
                agent_name: msg.agent_name.clone(),
            },
        });

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("msg-id", msg.id.as_str());
            headers.insert("protocol-version", msg.protocol_version.as_str());
            headers.insert("session-id", msg.session.as_str());
            headers.insert("branch-name", msg.branch_name.as_str());
            headers.insert("fork-point", msg.fork_point.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, "".into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn send_promote(&self, msg: vlinder_core::domain::PromoteMessage) -> Result<(), QueueError> {
        let subject = routing_key_to_subject(&RoutingKey {
            session: msg.session.clone(),
            branch: msg.branch,
            submission: msg.submission.clone(),
            kind: RoutingKind::Promote {
                agent_name: msg.agent_name.clone(),
            },
        });

        self.inner.runtime.block_on(async {
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("msg-id", msg.id.as_str());
            headers.insert("protocol-version", msg.protocol_version.as_str());
            headers.insert("session-id", msg.session.as_str());

            self.inner
                .jetstream
                .publish_with_headers(subject, headers, "".into())
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?
                .await
                .map_err(|e| QueueError::SendFailed(e.to_string()))?;

            Ok(())
        })
    }

    fn send_session_start(
        &self,
        _msg: vlinder_core::domain::SessionStartMessage,
    ) -> Result<vlinder_core::domain::BranchId, QueueError> {
        // NatsQueue doesn't create branches — RecordingQueue wraps this
        // and returns the real branch ID.
        Ok(vlinder_core::domain::BranchId::from(1))
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

/// NATS wildcard filter for receiving complete messages for a specific submission.
fn complete_filter(submission: &SubmissionId, harness: HarnessType) -> String {
    format!(
        "{COMPLETE_PREFIX}.*.*.{}.{COMPLETE_KIND}.*.{}",
        submission.as_str(),
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
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{RESPONSE_PREFIX}.*.*.{}.{RESPONSE_KIND}.*.{}.{}.{}.{}",
        submission.as_str(),
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

/// Serialize a routing key to a NATS subject string (ADR 096 §8).
///
/// This is the single point of truth for `RoutingKey` → NATS subject
/// serialization. Injectivity: distinct routing keys produce distinct
/// subjects (verified by tests in `routing_key.rs`).
fn routing_key_to_subject(key: &RoutingKey) -> String {
    let prefix = format!("vlinder.{}.{}.{}", key.session, key.branch, key.submission);
    let suffix = match &key.kind {
        RoutingKind::Repair { harness, agent } => {
            format!("repair.{harness}.{agent}")
        }
        RoutingKind::Fork { agent_name } => {
            format!("fork.{agent_name}")
        }
        RoutingKind::Promote { agent_name } => {
            format!("promote.{agent_name}")
        }
    };
    format!("{prefix}.{suffix}")
}

/// Parse a NATS subject back into a `RoutingKey`.
///
/// Inverse of `routing_key_to_subject`. Returns `None` for subjects
/// that don't match the `vlinder.{session}.{branch}.{submission}.{type}...` format.
pub fn subject_to_routing_key(subject: &str) -> Option<RoutingKey> {
    let s: Vec<&str> = subject.split('.').collect();
    if s.len() < 5 || s[0] != "vlinder" {
        return None;
    }

    let session = SessionId::try_from(s[1].to_string()).ok()?;
    let branch = BranchId::from(s[2].parse::<i64>().unwrap_or(0));
    let submission = SubmissionId::from(s[3].to_string());

    let kind = match s[4] {
        "repair" if s.len() == 7 => Some(RoutingKind::Repair {
            harness: HarnessType::from_str(s[5]).ok()?,
            agent: AgentName::new(s[6]),
        }),
        "fork" if s.len() == 6 => Some(RoutingKind::Fork {
            agent_name: AgentName::new(s[5]),
        }),
        "promote" if s.len() == 6 => Some(RoutingKind::Promote {
            agent_name: AgentName::new(s[5]),
        }),
        _ => None,
    };

    kind.map(|kind| RoutingKey {
        session,
        branch,
        submission,
        kind,
    })
}

/// Serialize a `RepairMessage` into NATS headers (sans payload).
#[allow(dead_code)]
pub fn repair_to_nats_headers(msg: &RepairMessage) -> HashMap<String, String> {
    let mut h = HashMap::new();
    h.insert("msg-id".to_string(), msg.id.as_str().to_string());
    h.insert("protocol-version".to_string(), msg.protocol_version.clone());
    h.insert("session-id".to_string(), msg.session.as_str().to_string());
    h.insert("dag-parent".to_string(), msg.dag_parent.to_string());
    h.insert("checkpoint".to_string(), msg.checkpoint.clone());
    h.insert(
        "service".to_string(),
        msg.service.service_type().as_str().to_string(),
    );
    h.insert("backend".to_string(), msg.service.backend_str().to_string());
    h.insert("operation".to_string(), msg.operation.as_str().to_string());
    h.insert("sequence".to_string(), msg.sequence.to_string());
    if let Some(ref state) = msg.state {
        h.insert("state".to_string(), state.clone());
    }
    h
}

/// Reconstruct typed message headers from a `RoutingKey` and NATS header map.
///
/// The `RoutingKey` provides the message type discriminant and routing fields
/// (agent, harness, service, etc.) — already parsed by `subject_to_routing_key`.
/// The header map provides session-scoped fields (msg-id, session-id, state,
/// diagnostics) that aren't part of routing.
pub fn from_nats_headers<S: BuildHasher>(
    key: &RoutingKey,
    headers: &HashMap<String, String, S>,
) -> Option<vlinder_core::domain::ObservableMessageHeaders> {
    use vlinder_core::domain::{MessageDetails, ObservableMessageHeaders};

    let id = MessageId::from(headers.get("msg-id")?.clone());
    let protocol_version = headers.get("protocol-version").cloned().unwrap_or_default();
    let state = headers.get("state").cloned();

    let details = match &key.kind {
        RoutingKind::Repair { .. } => {
            let dag_parent = DagNodeId::from(headers.get("dag-parent").cloned()?);
            let checkpoint = headers.get("checkpoint").cloned()?;
            let service = ServiceBackend::from_parts(
                ServiceType::from_str(headers.get("service")?).ok()?,
                headers.get("backend")?,
            )?;
            let operation = Operation::from_str(headers.get("operation")?).ok()?;
            let sequence = Sequence::from(headers.get("sequence")?.parse::<u32>().ok()?);
            Some(MessageDetails::Repair {
                dag_parent,
                checkpoint,
                service,
                operation,
                sequence,
            })
        }
        RoutingKind::Fork { .. } => {
            let branch_name = headers.get("branch-name").cloned()?;
            let fork_point = DagNodeId::from(headers.get("fork-point").cloned()?);
            Some(MessageDetails::Fork {
                branch_name,
                fork_point,
            })
        }
        RoutingKind::Promote { .. } => Some(MessageDetails::Promote),
    };

    details.map(|details| ObservableMessageHeaders {
        id,
        protocol_version,
        state,
        routing_key: key.clone(),
        details,
    })
}

/// Derive a stable consumer name from a filter pattern.
///
/// NATS consumer names must be alphanumeric + dash/underscore.
/// We replace dots and wildcards with underscores.
fn filter_to_consumer_name(filter: &str) -> String {
    filter.replace('.', "_").replace('*', "W").replace('>', "G")
}

/// Extract a header value from NATS headers.
fn get_header(headers: &async_nats::HeaderMap, key: &str) -> Result<String, QueueError> {
    headers
        .get(key)
        .map(std::string::ToString::to_string)
        .ok_or_else(|| QueueError::ReceiveFailed(format!("missing header: {key}")))
}

// ============================================================================
// Tests — subject serialization injectivity (ADR 096 §9)
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
    fn timeline_alt() -> BranchId {
        BranchId::from(2)
    }
    fn submission() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }
    fn submission_alt() -> SubmissionId {
        SubmissionId::from("sub-2".to_string())
    }
    fn agent() -> AgentName {
        AgentName::new("echo")
    }
    fn agent_alt() -> AgentName {
        AgentName::new("pensieve")
    }

    // ========================================================================
    // Format sanity — subjects have the expected shape
    // ========================================================================

    #[test]
    fn repair_subject_format() {
        let key = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        assert_eq!(
            routing_key_to_subject(&key),
            format!(
                "vlinder.{}.{}.{}.repair.cli.echo",
                session(),
                timeline(),
                submission()
            ),
        );
    }

    #[test]
    fn fork_subject_format() {
        let key = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        };
        assert_eq!(
            routing_key_to_subject(&key),
            format!(
                "vlinder.{}.{}.{}.fork.echo",
                session(),
                timeline(),
                submission()
            ),
        );
    }

    #[test]
    fn promote_subject_format() {
        let key = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent(),
            },
        };
        assert_eq!(
            routing_key_to_subject(&key),
            format!(
                "vlinder.{}.{}.{}.promote.echo",
                session(),
                timeline(),
                submission()
            ),
        );
    }

    // ========================================================================
    // Injectivity — distinct routing keys produce distinct subjects
    // ========================================================================

    /// Helper: assert two different routing keys serialize to different subjects.
    fn assert_injective(a: &RoutingKey, b: &RoutingKey) {
        assert_ne!(a, b, "precondition: keys must differ");
        assert_ne!(
            routing_key_to_subject(a),
            routing_key_to_subject(b),
            "injectivity violated: {a:?} and {b:?} mapped to same subject",
        );
    }

    #[test]
    fn repair_injective_by_timeline() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline_alt(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        assert_injective(&a, &b);
    }

    #[test]
    fn repair_injective_by_submission() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission_alt(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        assert_injective(&a, &b);
    }

    #[test]
    fn repair_injective_by_harness() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Web,
                agent: agent(),
            },
        };
        assert_injective(&a, &b);
    }

    #[test]
    fn repair_injective_by_agent() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent_alt(),
            },
        };
        assert_injective(&a, &b);
    }

    #[test]
    fn fork_injective_by_agent() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent_alt(),
            },
        };
        assert_injective(&a, &b);
    }

    #[test]
    fn promote_injective_by_agent() {
        let a = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent(),
            },
        };
        let b = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent_alt(),
            },
        };
        assert_injective(&a, &b);
    }

    // ========================================================================
    // Cross-variant injectivity — different message types never collide
    // ========================================================================

    #[test]
    fn repair_and_fork_subjects_differ() {
        let repair = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        };
        let fork = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        };
        assert_injective(&repair, &fork);
    }

    #[test]
    fn fork_and_promote_subjects_differ() {
        let fork = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        };
        let promote = RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent(),
            },
        };
        assert_injective(&fork, &promote);
    }

    // ========================================================================
    // Round-trip: routing_key_to_subject → subject_to_routing_key
    // ========================================================================

    fn assert_round_trips(key: &RoutingKey) {
        let subject = routing_key_to_subject(key);
        let recovered = subject_to_routing_key(&subject)
            .unwrap_or_else(|| panic!("failed to parse subject: {subject}"));
        assert_eq!(&recovered, key, "round-trip failed for subject: {subject}");
    }

    #[test]
    fn repair_round_trips() {
        assert_round_trips(&RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Repair {
                harness: HarnessType::Cli,
                agent: agent(),
            },
        });
    }

    #[test]
    fn fork_round_trips() {
        assert_round_trips(&RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        });
    }

    #[test]
    fn promote_round_trips() {
        assert_round_trips(&RoutingKey {
            session: session(),
            branch: timeline(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent(),
            },
        });
    }

    #[test]
    fn subject_to_routing_key_rejects_garbage() {
        assert!(subject_to_routing_key("not-a-subject").is_none());
        assert!(subject_to_routing_key("").is_none());
        assert!(subject_to_routing_key("vlinder.1.sub.unknown.stuff").is_none());
    }

    #[test]
    fn subject_to_routing_key_rejects_too_few_segments() {
        assert!(subject_to_routing_key("vlinder.1.sub").is_none());
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
}
