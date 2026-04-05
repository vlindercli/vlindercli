# ADR 126: AMQP 0-9-1 Queue Backend

**Status:** Draft

## Context

ADR 125 investigated Amazon SQS as an AWS-native queue backend and discarded it. SQS lacks server-side content-based filtering — the `MessageQueue` trait requires exact-match filtering on `receive_complete` and `receive_response` under concurrent submissions. Kani proofs formally verify that unfiltered backends violate the routing contract.

The investigation also revealed:

- AWS has no serverless equivalent of NATS's subject-based routing. SQS, SNS, and EventBridge all require provisioned resources for fine-grained filtering.
- Azure Service Bus (AMQP 1.0) has the same problem — subscriptions with filter rules are provisioned resources, not implicit addresses.
- AMQP 1.0 is a wire protocol only. Routing semantics are broker-specific, so "one AMQP 1.0 crate for all clouds" is not achievable.

NATS remains the default for self-hosted deployments. The remaining need: a managed queue backend for customers who don't want to operate a NATS cluster, available across AWS, Azure, and GCP.

## Decision

Add `vlinder-amqp`, a new `MessageQueue` implementation using the AMQP 0-9-1 protocol. The preferred managed service is **LavinMQ by 84codes via CloudAMQP**.

### Why AMQP 0-9-1

AMQP 0-9-1 defines routing at the protocol level, not the broker level. Topic exchanges with routing keys are part of the spec. Any compliant broker provides implicit, server-side, exact-match filtering — the same property that makes NATS work and SQS fail.

Routing key: `complete.{submission}.{agent}` — maps directly from the NATS subject `VLINDER.data.complete.{submission}.{agent}`. No resource provisioning per submission.

### Why LavinMQ / CloudAMQP

| Criteria | LavinMQ |
|---|---|
| Protocol | AMQP 0-9-1 — routing contract satisfied by spec |
| License | Apache 2.0 |
| Multi-cloud | AWS, Azure, GCP via CloudAMQP marketplace |
| Managed | 84codes operates it — provisioning, patching, failover |
| Pricing | Free tier (2M msg/mo, 40 connections), $19/mo (20M msg/mo, 200 connections) — 2x the limits of RabbitMQ plans at the same price |
| Fallback | RabbitMQ (same protocol, same crate works unchanged) |

84codes is an independent Swedish company. They built LavinMQ as a lighter alternative to RabbitMQ (Crystal vs Erlang, disk-first vs memory-first) after operating thousands of RabbitMQ clusters.

### Provider preference

First-party cloud services are preferred when available. On AWS, **Amazon MQ (RabbitMQ)** is first-party — native to the customer's account, billed through AWS, covered by AWS SLA. CloudAMQP (LavinMQ) is the preferred option for cloud-agnostic deployments or where first-party support is unavailable (Azure, GCP). The crate is provider-agnostic — same connection string format, same protocol.

### Why not RabbitMQ directly

RabbitMQ is MPL 2.0 owned by Broadcom. It works — same protocol, same crate. But Broadcom's acquisition history makes license stability uncertain. LavinMQ (Apache 2.0) is the preferred default for CloudAMQP deployments. If Broadcom relicenses RabbitMQ, the crate works unchanged against LavinMQ or any AMQP 0-9-1 broker.

### Alternatives considered

| Alternative | Outcome |
|---|---|
| Amazon SQS | Discarded (ADR 125) — no server-side filtering |
| Azure Service Bus (AMQP 1.0) | Provisioned subscriptions, same resource problem as SQS |
| Amazon MQ (ActiveMQ, AMQP 1.0) | Works on AWS only. Azure/GCP have no managed equivalent |
| Amazon MQ (RabbitMQ) | Works but AWS-only. CloudAMQP covers all three clouds |
| Self-hosted NATS on K8s | Works everywhere but defeats the "managed" goal |

## Consequences

**Positive:**
- Multi-cloud managed queue without self-hosted infrastructure
- Routing contract satisfied by protocol spec, not broker implementation
- One crate (`vlinder-amqp`) works against LavinMQ, RabbitMQ, or any AMQP 0-9-1 broker
- Apache 2.0 licensed default, protocol-level portability as insurance
- Free tier available for development

**Negative:**
- Two queue backends to maintain (NATS and AMQP)
- Third-party managed service dependency (84codes / CloudAMQP)
- AMQP 0-9-1 is heavier than NATS protocol — more connection ceremony

## Crate structure

```
crates/vlinder-amqp/
├── Cargo.toml       # lapin (AMQP 0-9-1 client), vlinder-core
└── src/
    ├── lib.rs
    └── queue.rs     # AmqpQueue: impl MessageQueue
```

Rust client: `lapin` — mature, async, well-maintained AMQP 0-9-1 library.

## Routing mapping

| NATS subject | AMQP 0-9-1 routing key |
|---|---|
| `VLINDER.data.v1.{session}.{branch}.{submission}.invoke.{harness}.{runtime}.{agent}` | `invoke.{submission}.{agent}` |
| `VLINDER.data.v1.{session}.{branch}.{submission}.complete.{agent}.{harness}` | `complete.{submission}.{agent}` |
| `VLINDER.data.v1.{session}.{branch}.{submission}.request.{agent}.{service}.{op}.{seq}` | `request.{submission}.{agent}.{service}.{op}.{seq}` |
| `VLINDER.data.v1.{session}.{branch}.{submission}.response.{agent}.{service}.{op}.{seq}` | `response.{submission}.{agent}.{service}.{op}.{seq}` |

One topic exchange per cluster. Consumers bind with exact routing keys. The broker filters server-side.
