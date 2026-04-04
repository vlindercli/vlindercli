# ADR 125: SQS Queue Backend

**Status:** Draft

## Context

NATS JetStream is the only production queue backend. It works well but requires babysitting a queue cluster — an operational burden that doesn't pay for itself in AWS-native deployments where Lambda is the primary runtime.

The system needs an AWS-native queue option where the queue is managed infrastructure, not a self-hosted service. Amazon SQS is the obvious candidate: fully managed, pay-per-use, native Lambda integration via event source mappings.

### Constraints

1. **Strict ordering and durability** — the system constructs Merkle DAGs from messages. The RecordingQueue writes DAG nodes to the DagStore synchronously before publishing, so the DAG's parent chain is resolved from the database, not from queue ordering. The queue's job is correct message delivery, not global ordering.

2. **No crate explosion** — sidecars must not be tightly coupled to the queue implementation. Adding SQS must not create a crate for every `{queue, runtime}` combination. The `MessageQueue` trait already decouples transport from consumers; the factory function is the only coupling point.

3. **One queue per cluster** — a deployment chooses NATS or SQS at provisioning time. No mixed-queue clusters.

### Why the MessageQueue Trait Needs to Evolve

NATS gives things for free that SQS requires explicitly:

- **Subject-based routing**: NATS creates virtual consumers via wildcard filters on one stream. SQS requires discrete queues per consumer pattern.
- **Implicit queue lifecycle**: NATS subjects exist as soon as you publish to them. SQS queues must be explicitly created and deleted.
- **Consumer auto-GC**: NATS consumers expire after inactivity. SQS queues persist until deleted.

The trait should formalize these responsibilities so any backend knows what it must provide.

### Why the DagStore Trait Needs an Idempotency Guard

All queue backends provide at-least-once delivery. When a consumer crashes between processing and acknowledgement, the message is redelivered. For expensive operations (LLM calls), redundant processing is wasteful. The DagStore — where evidence of prior processing lives — is the right place for a consumer-side idempotency check.

## Decision

### 1. Queue Topology: Discrete SQS Queues

Map NATS's single-stream-with-filters model to discrete SQS queues — one per consumer pattern.

**Static queues** (created at daemon startup via `ensure_infrastructure`):

| Queue | Messages | Consumer |
|---|---|---|
| `{prefix}-infra` | DeployAgent, DeleteAgent | Infra worker |
| `{prefix}-session` | Fork, Promote, SessionStart | Session worker |
| `{prefix}-request-{svc}-{backend}` | RequestMessage | Service worker |

**Dynamic queues** (created at deploy, deleted at undeploy via `provision_agent` / `deprovision_agent`):

| Queue | Messages | Consumer |
|---|---|---|
| `{prefix}-invoke-{agent}` | InvokeMessage | Agent's sidecar |
| `{prefix}-complete-{agent}` | CompleteMessage | Harness |
| `{prefix}-response-{agent}` | ResponseMessage | Sidecar (durable mode) |

3 static + 3 per agent. Queue prefix is configurable, defaulting to `"vlinder"`.

**Routing change from NATS**: responses route to the agent's queue (`{prefix}-response-{agent}`), not to a submission-scoped subject. The agent name is already in the `DataRoutingKey`. The sidecar does lightweight client-side filtering by `(submission, service, operation, sequence)` — trivial since it processes one invoke at a time.

### 2. Standard Queues, Not FIFO

Standard SQS queues, not FIFO. Rationale:

- **Ordering isn't the queue's job**: RecordingQueue handles DAG ordering via synchronous DagStore writes. The queue is a delivery mechanism.
- **Cost**: Standard is $0.40/M vs FIFO $0.50/M. At LLM-bottlenecked volumes the absolute difference is negligible, but there's no reason to pay more for a guarantee the application doesn't need from the queue.
- **Throughput ceiling**: Standard is unlimited. FIFO is 300 msg/s per message group (3000 with high-throughput). Irrelevant now, but no reason to take a ceiling.
- **Duplicate delivery**: Standard is at-least-once. The DagStore's `INSERT OR IGNORE` on content-addressed hashes already handles duplicate DAG node insertion. The new `exists_in_submission` idempotency guard prevents redundant processing.

### 3. Dead Letter Queues

Every SQS queue gets a paired DLQ:

```
{prefix}-invoke-{agent}     → {prefix}-invoke-{agent}-dlq
{prefix}-infra              → {prefix}-infra-dlq
...
```

Redrive policy: `maxReceiveCount = 3`. After 3 failed processing attempts, the message moves to the DLQ instead of cycling forever. This is free safety — maps to ADR 053's staleness recovery concept, but with a permanent record of poison messages instead of consumer recreation.

DLQs are created alongside their parent queue in `provision_agent` / `ensure_infrastructure`.

### 4. MessageQueue Trait: Lifecycle Methods

Add lifecycle methods with default no-op implementations. NATS no-ops them (subjects are implicit). SQS implements them.

```rust
pub trait MessageQueue {
    /// Ensure cluster-level queue infrastructure exists.
    ///
    /// Called once at daemon startup.
    ///
    /// NATS: creates the VLINDER JetStream stream (already idempotent).
    /// SQS:  creates infra, session, and per-service request queues + DLQs.
    fn ensure_infrastructure(&self) -> Result<(), QueueError> {
        Ok(())
    }

    /// Provision agent-scoped queues.
    ///
    /// Called by the infra worker when processing DeployAgentMessage.
    /// Must be idempotent.
    ///
    /// NATS: no-op.
    /// SQS:  creates invoke, complete, and response queues + DLQs for this agent.
    fn provision_agent(&self, agent: &AgentName) -> Result<(), QueueError> {
        Ok(())
    }

    /// Deprovision agent-scoped queues.
    ///
    /// Called by the infra worker when processing DeleteAgentMessage.
    /// Must be idempotent.
    ///
    /// NATS: no-op.
    /// SQS:  deletes the agent's queues and their DLQs.
    fn deprovision_agent(&self, agent: &AgentName) -> Result<(), QueueError> {
        Ok(())
    }

    // ... existing send/receive methods unchanged ...
}
```

### 5. MessageQueue Trait: Delivery Guarantee Declaration

```rust
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeliveryGuarantee {
    /// Messages may be delivered more than once.
    /// Consumers MUST tolerate duplicates.
    AtLeastOnce,
    /// Each message is delivered exactly once to exactly one consumer.
    ExactlyOnce,
}

pub trait MessageQueue {
    /// The delivery guarantee this backend provides.
    ///
    /// NATS JetStream: AtLeastOnce.
    /// SQS Standard:   AtLeastOnce.
    /// SQS FIFO:       ExactlyOnce.
    fn delivery_guarantee(&self) -> DeliveryGuarantee {
        DeliveryGuarantee::AtLeastOnce
    }
}
```

### 6. DagStore Trait: Idempotency Guard

```rust
pub trait DagStore: Send + Sync {
    /// Check whether a node of the given type already exists for this
    /// submission on this branch.
    ///
    /// Enables consumer-side idempotency. Before doing expensive work
    /// on a received message, check if the expected downstream result
    /// already exists in the DAG:
    ///
    /// - Sidecar receives Invoke  → check for Complete
    /// - Worker receives Request  → check for Response
    ///
    /// If true, the consumer should ack and skip processing.
    ///
    /// SQL implementation: SELECT EXISTS(... WHERE submission_id = ?
    ///   AND branch_id = ? AND message_type = ?)
    fn exists_in_submission(
        &self,
        submission: &SubmissionId,
        branch: BranchId,
        message_type: MessageType,
    ) -> Result<bool, String> {
        Ok(false)
    }
}
```

Consumer pattern:

```rust
let (key, msg, ack) = queue.receive_invoke(&agent)?;
if store.exists_in_submission(&key.submission, key.branch, MessageType::Complete)? {
    ack()?;
    continue;
}
// ... expensive LLM call ...
```

### 7. Acknowledgement Mapping

NATS `msg.ack()` maps to SQS `DeleteMessage(receipt_handle)`. The `Acknowledgement` closure captures the receipt handle and queue URL:

```rust
let ack: Acknowledgement = Box::new(move || {
    client.delete_message(&queue_url, &receipt_handle)?;
    Ok(())
});
```

Semantics are identical: after acknowledgement, the message is permanently removed. Before acknowledgement, it reappears after the visibility timeout (SQS) or ack_wait (NATS).

### 8. Message Deduplication

NATS uses `Nats-Msg-Id` header for publish-side dedup in JetStream.

SQS Standard has no publish-side dedup. This is acceptable because:
- DAG node insertion is idempotent (`INSERT OR IGNORE` on content-addressed hash)
- The `exists_in_submission` guard prevents redundant processing
- Duplicate delivery is rare in practice with Standard SQS

If a future deployment needs stronger guarantees, switching to FIFO queues with `MessageDeduplicationId = msg.id` is a configuration change, not an architectural one.

### 9. Request-Reply Pattern

The `call_service` pattern (send request, poll for response) changes routing but not semantics:

- **Send request**: publish to `{prefix}-request-{svc}-{backend}` queue. Include the agent name as a message attribute so the service worker knows which response queue to write to.
- **Receive response**: poll `{prefix}-response-{agent}` queue. Client-side filter by `(submission, service, operation, sequence)`.

The service worker reads from the request queue, processes, and writes the response to `{prefix}-response-{agent}` (derived from the agent name in the message attributes).

Since the sidecar processes one invoke at a time and service calls are sequential within an invoke, the response queue has at most one pending message. Client-side filtering is trivial.

### 10. Lambda-Native Integration

With SQS, Lambda functions are triggered directly by event source mappings instead of NATS polling:

**Current (NATS):**
```
vlinder-lambda-runtime polls NATS for invokes →
calls Lambda API with payload →
Lambda starts, adapter connects BACK to NATS (requires VPC) →
adapter sends Complete to NATS
```

**With SQS:**
```
Harness publishes invoke to SQS queue →
SQS event source mapping triggers Lambda natively →
Lambda receives invoke as SQS event payload →
Lambda sends Complete to SQS complete queue
```

This eliminates:
- The NATS connection from inside Lambda (no VPC networking for queue access)
- The `vlinder-lambda-runtime` polling loop
- Cold-start latency for NATS client initialization inside Lambda

Deploy flow creates an SQS event source mapping (`CreateEventSourceMapping` API) that wires `{prefix}-invoke-{agent}` → Lambda function. Batch size = 1 (each invoke triggers one LLM call).

The Lambda adapter still needs an SQS client for the ProviderServer's service calls (sending requests to `{prefix}-request-{svc}-{backend}`, polling `{prefix}-response-{agent}`).

### 11. Sidecar Decoupling

Feature-gate queue backends in `vlinder-provider-server`:

```toml
# vlinder-provider-server/Cargo.toml
[features]
default = ["nats"]
nats = ["vlinder-nats"]
sqs = ["vlinder-sqs"]
```

`factory::connect_queue` switches on config:

```rust
pub fn connect_queue(config: &QueueConnectionConfig, ...) -> Result<Arc<dyn MessageQueue + Send + Sync>> {
    let inner: Arc<dyn MessageQueue + Send + Sync> = match config {
        #[cfg(feature = "nats")]
        QueueConnectionConfig::Nats { .. } => Arc::new(NatsQueue::connect(..)?),
        #[cfg(feature = "sqs")]
        QueueConnectionConfig::Sqs { .. } => Arc::new(SqsQueue::connect(..)?),
    };
    Ok(Arc::new(RecordingQueue::new(inner, store)))
}
```

Sidecars compile with the feature they need. No new crates per `{queue, runtime}` combination.

Env vars generalize from NATS-specific to queue-agnostic:

```
VLINDER_QUEUE_BACKEND=sqs        (or "nats")
VLINDER_SQS_REGION=eu-west-1     (SQS only)
VLINDER_SQS_QUEUE_PREFIX=dev-vlinder  (SQS only)
VLINDER_NATS_URL=nats://...      (NATS only)
```

### 12. Crate Structure

One new crate:

```
crates/vlinder-sqs/
├── Cargo.toml       # aws-sdk-sqs, vlinder-core, tokio, serde_json
├── src/
│   ├── lib.rs
│   ├── queue.rs     # SqsQueue: impl MessageQueue
│   ├── connect.rs   # SQS client initialization
│   ├── naming.rs    # queue_prefix + routing_key → queue name
│   └── dlq.rs       # DLQ creation and redrive policy
```

### 13. Configuration

```toml
# config.toml
[queue]
backend = "sqs"
sqs_region = "eu-west-1"
sqs_queue_prefix = "dev-vlinder"  # default: "vlinder"
```

Or via environment:

```bash
VLINDER_QUEUE_BACKEND=sqs
VLINDER_QUEUE_SQS_REGION=eu-west-1
VLINDER_QUEUE_SQS_QUEUE_PREFIX=dev-vlinder
```

### 14. Infrastructure Provisioning

Static SQS queues and IAM permissions are provisioned by Terraform (test-infra). Dynamic queues are created by vlinderd at agent deploy time.

**Terraform additions** (`sqs.tf`):
- Static queues: infra, session, per-service request queues + DLQs
- IAM: EC2 role gains `sqs:*` on `arn:aws:sqs:{region}:{account}:{prefix}-*`
- IAM: Lambda execution roles gain `sqs:SendMessage` + `sqs:DeleteMessage` on their agent-scoped queues

**vlinderd additions** (infra worker):
- `provision_agent`: creates invoke, complete, response queues + DLQs
- For Lambda agents: creates event source mapping (invoke queue → Lambda function)
- `deprovision_agent`: deletes agent queues, DLQs, and event source mapping

### 15. AWS Credentials

Native to each runtime:
- **EC2 (vlinderd)**: IAM instance profile (already provisioned by test-infra)
- **Lambda**: execution role (already provisioned by vlinderd at deploy time)
- **Podman sidecar on EC2**: inherits instance profile credentials via `host.containers.internal` metadata proxy

No surprises, sensible defaults. The `aws-sdk-sqs` crate's default credential chain handles all three cases.

## Consequences

**Positive:**
- AWS-native deployment without self-hosted queue infrastructure
- Lambda functions triggered natively by SQS — no VPC networking for queue access
- `MessageQueue` trait formalized with lifecycle and delivery guarantee contracts
- `DagStore` gains explicit idempotency guard — consumer-side dedup for expensive operations
- DLQs provide permanent record of poison messages
- No crate explosion — feature-gated backends behind existing trait

**Negative:**
- Two queue backends to maintain (NATS and SQS)
- SQS has no subject-based routing — discrete queues mean explicit lifecycle management
- Response routing changes from subject-scoped to agent-scoped (minor semantic shift)
- Standard SQS has no publish-side dedup (mitigated by DagStore idempotency)

## What Changes vs What Doesn't

**Unchanged:**
- `MessageQueue` trait send/receive signatures
- `RecordingQueue` transactional outbox pattern
- `DagStore` / DagNode / Merkle chain (completely queue-agnostic)
- Message types (InvokeMessage, RequestMessage, etc.)
- Routing key types (DataRoutingKey, SessionRoutingKey, InfraRoutingKey)
- Sidecar dispatch logic

**Changed:**
- `MessageQueue` trait gains lifecycle + delivery_guarantee methods (default no-ops)
- `DagStore` trait gains `exists_in_submission` idempotency guard (default false)
- `queue_factory.rs` gains Sqs branch
- `config.rs` gains `QueueBackend::Sqs` + SQS config fields
- `pool.rs` injects queue-backend-agnostic env vars
- `provider-server/factory.rs` feature-gates NATS/SQS
- Infra worker calls `provision_agent` / `deprovision_agent`
- Lambda runtime: SQS variant uses event source mapping

## Known Bugs (discovered during E2E testing 2026-04-03)

Three compounding bugs cause the system to return stale responses and accumulate thousands of unprocessed messages in SQS queues.

### Bug 1: Lambda Adapter Never Acks Invoke (refactor/03-lambda-runtime-queue, #55)

The lambda adapter's `dispatch_loop` in `main.rs` receives `(key, invoke, _ack)` from `receive_invoke` but never calls `_ack`. For `LambdaRuntimeQueue`, the ack closure POSTs the invocation response to the Lambda Runtime API (`/invocation/{request_id}/response`). Without it, the Lambda runtime thinks the invocation timed out and retries — each retry produces another complete message. This is a **redelivery amplifier**.

With NATS, the sidecar's `receive_invoke` ack deletes the message from the JetStream consumer. The lambda adapter historically didn't need it because the Lambda Runtime API itself managed delivery. But `LambdaRuntimeQueue` wraps the Runtime API behind `receive_invoke`, making ack semantically required.

The podman sidecar (`sidecar.rs:94`) correctly calls `let _ = ack();` immediately after receiving the invoke.

**Fix**: Call `ack()` after `send_complete` in the lambda adapter's dispatch loop.

### Bug 2: `receive_complete` Ignores Submission Filter (sqs/03-sqs-crate, #60)

```rust
fn receive_complete(
    &self,
    _submission: &SubmissionId,  // IGNORED
    _harness: HarnessType,       // IGNORED
    agent: &AgentName,
) -> Result<...> {
    let queue = routing::complete_queue(&self.inner.prefix, agent);
    let (body, ack) = self.receive_one(&queue)?;
    // returns whatever message is next — no filtering
}
```

The harness calls `receive_complete(&submission, harness, &agent)` in a busy-loop, expecting to receive the completion for its specific submission. With NATS, subject-based routing (`VLINDER.data.complete.{submission}.{agent}`) provides server-side filtering. With SQS, the per-agent complete queue contains completions for ALL submissions. The SQS implementation returns whichever message arrives first.

**Consequence**: The harness receives a stale complete from a previous submission (e.g., the first "buy milk" response) instead of the current one. It acks and returns it — so the user sees the same response every time. The correct complete for the current submission remains in the queue, unconsumed.

**Fix options**:
1. **Client-side filter + requeue**: After `receive_one`, check if the envelope's `key.submission` matches. If not, let the visibility timeout expire (don't ack) so the message reappears for a future consumer. Continue polling. Risk: messages cycle through visibility timeouts repeatedly.
2. **Client-side filter + ack stale**: After `receive_one`, if submission doesn't match, ack and discard (the submitter is gone). This loses the message if a slow consumer is still waiting. Only safe if the system guarantees one consumer per submission.
3. **SQS message group + receive with filter**: Use message attributes on send, and `MessageAttributeName` filtering on receive. SQS doesn't support server-side content-based filtering on `ReceiveMessage`, so this doesn't work.

Likely correct approach: option 2 (ack stale), since `receive_complete` is called by the harness which owns the submission lifecycle. If the submission's harness isn't polling, the complete is orphaned anyway.

### Bug 3: `receive_response` Ignores All Filters (sqs/03-sqs-crate, #60)

```rust
fn receive_response(
    &self,
    _submission: &SubmissionId,  // IGNORED
    agent: &AgentName,
    _service: ServiceBackend,    // IGNORED
    _operation: Operation,       // IGNORED
    _sequence: Sequence,         // IGNORED
) -> Result<...> {
    let queue = routing::response_queue(&self.inner.prefix, agent);
    let (body, ack) = self.receive_one(&queue)?;
    // returns whatever message is next — no filtering
}
```

During `call_service`, the sidecar/adapter sends a request and then busy-loops on `receive_response` expecting a response matching `(submission, service, operation, sequence)`. With NATS, subject routing provides exact-match delivery. With SQS, the per-agent response queue has responses from all submissions and service calls mixed together.

**Consequence**: `send_and_wait` receives a response from a previous invocation or different service call, acks it, and returns the wrong data to the agent. The correct response sits in the queue and becomes invisible for the 300s visibility timeout. Observed: 2419 messages in-flight on the response queue.

**Fix**: Same pattern as Bug 2 — client-side filter after `receive_one`. For responses, the filter must match `(submission, service, operation, sequence)` from the envelope's routing key. Non-matching messages should NOT be acked (let visibility timeout expire and redeliver to a future poll). Unlike completions, responses are actively waited on by a specific `call_service` invocation, so discarding non-matching ones would break other concurrent calls.

### Cascade

Bug 1 is the amplifier — Lambda retries produce duplicate invocations, each generating more completions and service call responses. Bug 2 causes the harness to consume stale completions. Bug 3 corrupts service call data during agent execution. Together, they explain the observed behavior: every user message returns "buy milk" (the first-ever completion), while queues accumulate hundreds of orphaned messages.

### RecordingQueue Delegation (also discovered 2026-04-03)

`RecordingQueue` inherited the default no-op implementations of `on_cluster_start`, `on_agent_deployed`, and `on_agent_deleted`. Since the infra worker holds a `RecordingQueue` wrapping the real queue, SQS lifecycle calls never reached the inner `SqsQueue` — agent queues were never created. Fixed by delegating lifecycle methods through `RecordingQueue` to `self.inner`.

## Future: Per-Worker Queue Creation

Service request queues (e.g., `dev-vlinder-request-kv-sqlite`) are currently hardcoded in `on_cluster_start` with all known backends. This couples SQS infrastructure to service worker configuration — adding a new backend requires updating `on_cluster_start`.

Cleaner: each service worker creates its own request queue at startup via a new trait method like `on_worker_start(service: ServiceBackend)`. The worker knows which backend it handles. NATS no-ops it. SQS creates the queue + DLQ. Adding a backend means adding a worker, not editing queue infrastructure code.
