# ADR 125: SQS Queue Backend

**Status:** Discarded

## Context

NATS JetStream is the only production queue backend. We investigated Amazon SQS as an AWS-native alternative to eliminate the operational burden of running a NATS cluster for Lambda-based deployments.

A full implementation was built (6 PRs), deployed, and E2E tested.

## Decision

Discarded. NATS is simpler, cheaper, and correct by default. SQS solves an operational burden that doesn't exist yet and introduces implementation complexity that NATS avoids entirely.

## Why

The `MessageQueue` trait requires server-side, exact-match filtering on receive: `receive_complete(submission, agent)` must return only the complete for that specific submission. Concurrent submissions to the same agent are the common case.

NATS provides this for free via subject-based routing — each filter parameter maps to a subject token. No configuration, no resource provisioning, no lifecycle management.

SQS alone cannot do this. SQS queues are provisioned resources — creating and deleting them takes seconds, has rate limits (80/s), and a 60-second deletion cooldown. Per-submission queues are impractical at the rate submissions arrive. To work around this, a more practical design could be per-agent queues, but then `ReceiveMessage` returns any message in the queue with no content-based filtering. 

SQS can be made to work with additional AWS services, but each approach has significant caveats:

| Alternative | Problem |
|---|---|
| Per-submission SQS queues | `CreateQueue` ~1s latency, 80/s rate limit, 60s deletion cooldown |
| Client-side filter + ack stale | Destroys messages for concurrent consumers |
| Client-side filter + visibility timeout | 300s redelivery delay, DLQ after 3 cycles |
| DynamoDB mailbox | Polling (no push), reimplements queue semantics on a KV store |
| SNS fan-out with subscription filters | Per-submission subscription lifecycle, adds latency and complexity |
| Amazon MQ (managed RabbitMQ) | **Viable** — satisfies routing contract, AWS-managed. Worth exploring. |

Every approach adds latency, lifecycle complexity, and moving parts to approximate what NATS provides natively: subject tokens are free strings, not provisioned resources. Publishing to `VLINDER.data.complete.{submission}.{agent}` creates the routing path implicitly. Subscribing to it delivers only matching messages, server-side, with no prior setup. The entire lifecycle — creation, filtering, cleanup — is handled by the broker as a consequence of addressing.

E2E testing confirmed this: three bugs discovered under concurrent submissions, two of which (`receive_complete` and `receive_response` returning wrong submissions' messages) are structural consequences of SQS's lack of subject routing, not implementation mistakes.

## Evidence

Kani proof harnesses in `vlinder-core::domain::message_queue` verify the routing contract formally:

- **`filtered_queue_satisfies_routing_contract`** — VERIFIED: a subject-routed backend never violates the contract.
- **`unfiltered_queue_violates_routing_contract`** — VERIFIED (panics): an unfiltered backend can return the wrong submission's message.

## What Survived

1. **Lifecycle methods** on `MessageQueue` — `on_cluster_start`, `on_agent_deployed`, `on_agent_deleted`.
2. **Idempotency guard** on `DagStore` — `exists_in_submission` for consumer-side dedup.
3. **Routing contract assertions and Kani proofs**: formal verification that the trait requires subject-based routing.
4. **Lambda adapter ack fix**: a real dispatch loop bug discovered during this work, fixed in refactor/03.
