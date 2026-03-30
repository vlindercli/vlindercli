# ADR 121: Operational Planes

**Status:** Accepted

## Context

The platform has three distinct operational concerns:

1. **Data plane** — agent execution. Invoke, request, response, complete. DAG-recorded. Session-scoped. Latency-sensitive.

2. **Session plane** — compensating transactions. Fork, promote, session start. Corrective actions on top of the immutable execution record. Session-scoped. Deliberate, not real-time.

3. **Infra plane** — provisioning. Deploy, delete. Changes what agents exist and how they're provisioned. Cluster-scoped. Async lifecycle with state tracking.

## Decision

### 1. Each plane owns its routing key

The plane determines the address shape. No wrapper enum — each plane's routing key is used directly by its queue methods.

- **Data plane** — `DataRoutingKey { session, branch, submission, kind }`. Session-scoped.
- **Session plane** — `SessionRoutingKey { session, submission, kind }`. Session-scoped, no branch.
- **Infra plane** — `InfraRoutingKey { submission, kind }`. Cluster-scoped, no session.

Submission ID is present on all three planes — it correlates every CLI action to its outcome.

### 2. NATS subject hierarchy by plane

| Plane | Format | Example |
|---|---|---|
| Data | `vlinder.data.v1.{session}.{branch}.{sub}.{type}...` | `vlinder.data.v1.abc.1.sub.invoke.cli.container.echo` |
| Session | `vlinder.{session}.{sub}.{type}.{agent}` | `vlinder.abc.sub.fork.echo` |
| Infra | `vlinder.infra.v1.{sub}.{type}` | `vlinder.infra.v1.sub.deploy-agent` |

Consumers subscribe to plane-specific prefixes or `vlinder.>` for everything.

### 3. CQRS — writes go through the queue

All write operations go through the message queue. The CLI sends commands via gRPC to a service that enqueues. Workers process asynchronously.

- **Data/session plane**: CLI → Harness (gRPC) → RecordingQueue → NATS → workers
- **Infra plane**: CLI → Registry (gRPC) → RecordingQueue → NATS → infra worker → runtime worker

The RecordingQueue records every message to the DagStore before forwarding to NATS (transactional outbox). Read operations go directly to the store.

### 4. Infra plane shares the same store

All three planes share one database. Infra plane nodes live in the same `dag_nodes` chain index as data/session nodes, with `session_id` and `branch_id` nullable (infra nodes are cluster-scoped). This enables foreign key integrity — agent IDs are FK targets for session and data plane tables.

### 5. Agent lifecycle state machine

`AgentState` is a domain object separate from `Agent` (which stays a pure manifest). State is tracked in `agent_states` with FK to `agents(name)`.

```
Deploying → Live
    ↓
  Failed

Live → Deleting → Deleted
```

The infra worker sets intent (`Deploying` / `Deleting`). The runtime worker (container/Lambda) does the actual provisioning or teardown and transitions to the terminal state (`Live` / `Failed` / `Deleted`).

### 6. Registry is the infra read model

The `Registry` trait is to the infra plane what `DagStore` is to the data/session planes — a query interface. Infra writes go through the queue. The registry stays as the read-side interface.

### 7. Wire format

Consistent across all three planes:

- **Subject** — routing. Parsed without touching the payload.
- **Headers** — NATS concerns only (`Nats-Msg-Id` for dedup). No domain data.
- **Payload** — `serde_json::to_vec(&FooMessage)`. Self-contained.

### 8. Infra messages are self-sufficient

Infra plane message payloads carry all context needed to act on the message — agent name lives in the payload, not the routing key. This enables replay from the DAG without reconstructing routing context.

## Consequences

- The plane is the top-level discriminant — code that handles one plane doesn't see the others
- Each plane is independently subscribable at the NATS level
- Retention and delivery guarantees can differ per plane
- Agent deployment is async with observable lifecycle state
- All three planes share one database with FK integrity across planes
- Registry is a read model — vlinder eventually runs on vlinder
