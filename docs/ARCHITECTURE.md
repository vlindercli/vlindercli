# Architecture

Vlinder is a local-first control plane for AI agents. Agents run as OCI containers or AWS Lambda functions. Every side effect is recorded in a content-addressed Merkle DAG for time-travel debugging.

## Operational Planes

All operations fall into one of three planes (ADR 121):

| Plane | Scope | Messages | Purpose |
|-------|-------|----------|---------|
| **Data** | Session | Invoke, Complete, Request, Response | Agent execution |
| **Session** | Session | Fork, Promote, SessionStart | Compensating transactions |
| **Infra** | Cluster | DeployAgent, DeleteAgent | Provisioning |

Each plane has its own routing key shape, NATS subject prefix, and typed DAG tables. All share the same database and recording pipeline.

## Write Path (CQRS)

All writes go through the message queue. No service writes directly to the store.

```
CLI
 |
 v
gRPC service (Harness or Registry)
 |
 v
RecordingQueue
 |-- record to DagStore (transactional outbox)
 |-- forward to NATS
 |
 v
Worker (picks up from NATS, does the actual work)
```

- **Data/session plane**: CLI -> Harness -> RecordingQueue -> NATS -> sidecar/runtime
- **Infra plane**: CLI -> Registry -> RecordingQueue -> NATS -> infra worker (sets intent via AgentState) ... runtime worker (acts on next tick)

Reads go directly to the store (Registry for agents, DagStore for conversation state).

## Crate Map

### Domain (`vlinder-core`)

The protocol specification. Types, traits, no infrastructure.

| Path | What |
|------|------|
| `domain/routing_key.rs` | `DataRoutingKey`, `SessionRoutingKey`, `InfraRoutingKey` |
| `domain/message/` | Typed message structs (InvokeMessage, ForkMessage, DeployAgentMessage, etc.) |
| `domain/message_queue.rs` | `MessageQueue` trait — send/receive for all three planes |
| `domain/dag.rs` | `DagStore` trait, `DagWorker` trait, `DagNode`, `MessageType`, `Plane` |
| `domain/registry.rs` | `Registry` trait — read model for agents, models, fleets |
| `domain/registry_repository.rs` | `RegistryRepository` trait — persistence for agents, models, agent state |
| `domain/agent.rs` | `Agent` — resolved manifest, ready for execution |
| `domain/agent_state.rs` | `AgentState`, `AgentStatus` — deployment lifecycle |
| `queue/recording.rs` | `RecordingQueue` — transactional outbox (record to DagStore + forward) |
| `queue/in_memory.rs` | `InMemoryQueue` — test-only, no NATS required |

### Storage (`vlinder-sql-state`)

Single SQLite database, Diesel ORM. Implements `DagStore` and `RegistryRepository`.

| Table | Plane | Purpose |
|-------|-------|---------|
| `dag_nodes` | All | Chain index (hash, parent, type, timestamp) |
| `invoke_nodes` | Data | Invoke payload + routing |
| `complete_nodes` | Data | Complete payload + routing |
| `request_nodes` | Data | Request payload + routing |
| `response_nodes` | Data | Response payload + routing |
| `fork_nodes` | Session | Fork payload |
| `promote_nodes` | Session | Promote payload |
| `deploy_agent_nodes` | Infra | Deploy manifest |
| `delete_agent_nodes` | Infra | Delete target |
| `agents` | Infra (read model) | Agent definitions |
| `models` | Infra (read model) | Model definitions |
| `agent_states` | Infra | Deployment lifecycle (FK to agents) |
| `sessions` | Session (read model) | Conversation sessions |
| `branches` | Session (read model) | Timeline branches |

`dag_nodes.session_id` and `branch_id` are nullable — infra plane nodes are cluster-scoped.

### Queue (`vlinder-nats`)

NATS JetStream implementation of `MessageQueue`. Owns subject builders and parsers.

| Subject | Plane | Parser |
|---------|-------|--------|
| `vlinder.data.v1.{s}.{b}.{sub}.invoke...` | Data | `invoke_parse_subject` |
| `vlinder.{s}.{sub}.fork.{agent}` | Session | `fork_parse_subject` |
| `vlinder.infra.v1.{sub}.deploy-agent` | Infra | `deploy_agent_parse_subject` |

Wire format: JSON payload, `Nats-Msg-Id` header for dedup.

### Registry (`vlinder-sql-registry`)

gRPC service wrapping the `Registry` trait. Holds queue for infra plane commands.

| Component | Role |
|-----------|------|
| `RegistryServer` | gRPC server — queries + infra command ingestion |
| `GrpcRegistryClient` | Client — implements `Registry` trait via gRPC |
| `PersistentRegistry` | Write-through cache (in-memory + RegistryRepository) |

### Harness (`vlinder-harness`)

gRPC service for CLI agent invocation. Owns data/session plane command ingestion.

### Runtimes

| Crate | Runtime | Provisioning |
|-------|---------|-------------|
| `vlinder-podman-runtime` | Container (Podman) | Starts/stops pods based on `AgentState` |
| `vlinder-lambda-runtime` | Lambda (AWS) | Creates/deletes functions based on `AgentState` |

Both check `AgentState` on each tick: `Deploying` -> provision -> `Live`/`Failed`, `Deleting` -> teardown -> `Deleted`.

### Daemon (`vlinderd`)

Supervisor that spawns worker processes. Wires dependencies and manages startup order.

Startup: Secret -> State -> Registry -> Catalog -> Harness -> runtimes -> Infra -> DagGit -> SessionViewer

### Other

| Crate | Purpose |
|-------|---------|
| `vlinder` | CLI binary |
| `vlinder-git-dag` | Git projection of the DAG (data/session planes only) |
| `vlinder-podman-sidecar` | In-pod sidecar mediating queue <-> agent container |
| `vlinder-provider-server` | HTTP server inside sidecar for agent service calls |
| `vlinder-lambda-adapter` | Lambda entrypoint adapter |
| `vlinder-catalog` | Model catalog service (Ollama, OpenRouter) |

## Agent Lifecycle

Deploy and delete are two-phase: the infra worker sets intent via `AgentState`, the runtime worker acts on it. They don't communicate directly — the handoff is through the shared database.

**Deploy:**

```
vlinder agent deploy
    |
    v
Registry gRPC -> RecordingQueue -> NATS
                                     |
                                     v
                              Infra worker
                              (validates manifest, registers agent)
                              writes AgentState: Deploying
                                     .
                                     . (no direct call — shared DB)
                                     .
                              Runtime worker (container/Lambda)
                              (sees Deploying on next tick, starts pod or creates function)
                              writes AgentState: Live (or Failed)
```

**Delete:**

```
vlinder agent delete
    |
    v
Registry gRPC -> RecordingQueue -> NATS
                                     |
                                     v
                              Infra worker
                              writes AgentState: Deleting
                                     .
                                     . (no direct call — shared DB)
                                     .
                              Runtime worker
                              (sees Deleting on next tick, stops pod, deletes from registry)
                              writes AgentState: Deleted
```

CLI polls `GetAgentState` and prints status transitions as they happen.
