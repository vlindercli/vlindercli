# ADR 126: AMQP 0-9-1 Queue Backend

**Status:** Accepted

## Context

ADR 125 investigated Amazon SQS as an AWS-native queue backend and discarded it. SQS lacks server-side content-based filtering — the `MessageQueue` trait requires exact-match filtering on `receive_complete` and `receive_response` under concurrent submissions. Kani proofs formally verify that unfiltered backends violate the routing contract.

The investigation also revealed:

- AWS has no serverless equivalent of NATS's subject-based routing. SQS, SNS, and EventBridge all require provisioned resources for fine-grained filtering.
- Azure Service Bus (AMQP 1.0) has the same problem — subscriptions with filter rules are provisioned resources, not implicit addresses.
- AMQP 1.0 is a wire protocol only. Routing semantics are broker-specific, so "one AMQP 1.0 crate for all clouds" is not achievable.

NATS remains the default for self-hosted deployments. The remaining need: a managed queue backend for customers who don't want to operate a NATS cluster, available across AWS, Azure, and GCP.

## Decision

Add `vlinder-amqp`, a new `MessageQueue` implementation using the AMQP 0-9-1 protocol.

### Why AMQP 0-9-1

AMQP 0-9-1 defines routing at the protocol level, not the broker level. Topic exchanges with routing keys are part of the spec. Any compliant broker provides implicit, server-side filtering — the same property that makes NATS work and SQS fail. No resource provisioning per submission.

### Routing

AMQP routing keys mirror NATS subjects exactly — same dot-delimited format, same segments. One topic exchange per cluster. Each consumer gets an exclusive auto-delete queue bound with wildcard patterns. The broker filters server-side.

### Provider preference

First-party cloud services are preferred when available. On AWS, **Amazon MQ (RabbitMQ)** is first-party — native to the customer's account, billed through AWS, covered by AWS SLA. Validated with full e2e testing.

**CloudAMQP (LavinMQ)** is the preferred option for cloud-agnostic deployments or where first-party support is unavailable (Azure, GCP). The crate is provider-agnostic — same connection string format, same protocol.

### Alternatives considered

| Alternative | Outcome |
|---|---|
| Amazon SQS | Discarded (ADR 125) — no server-side filtering |
| Azure Service Bus (AMQP 1.0) | Provisioned subscriptions, same resource problem as SQS |
| Amazon MQ (ActiveMQ, AMQP 1.0) | Works on AWS only. Azure/GCP have no managed equivalent |
| Amazon MQ (RabbitMQ) | Validated — e2e tested. AWS-only; CloudAMQP covers all three clouds |
| Self-hosted NATS on K8s | Works everywhere but defeats the "managed" goal |

## Consequences

**Positive:**
- Multi-cloud managed queue without self-hosted infrastructure
- Routing contract satisfied by protocol spec, not broker implementation
- One crate works against LavinMQ, RabbitMQ, or any AMQP 0-9-1 broker

**Negative:**
- Two queue backends to maintain (NATS and AMQP)
- AMQP 0-9-1 is heavier than NATS protocol — more connection ceremony
