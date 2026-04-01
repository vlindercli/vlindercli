# Module Contracts

Invariants, error guarantees, and implementation notes for heavily-used modules. Method signatures are in [DOMAIN_MODEL.md](DOMAIN_MODEL.md); this document records what the code *actually guarantees* on top of those signatures.

If a contract and the code disagree, the code is authoritative and this document is stale.

---

## MessageQueue

**Location:** `crates/vlinder-core/src/domain/message_queue.rs`

**Invariants:**
- The `Acknowledgement` closure returned by `receive_*` must be called exactly once. Calling it confirms processing to the broker (NATS: acks the JetStream message). Not calling it causes the message to be redelivered.
- Routing is deterministic: the same routing key always produces the same NATS subject.
- Message type enums are exhaustive — adding a new message type requires a new variant, which produces compile errors at all match sites. This is intentional.
- Plane separation is type-enforced: `DataRoutingKey`, `SessionRoutingKey`, and `InfraRoutingKey` are distinct types. Passing the wrong key type to a send method is a compile error.

**Error conditions:**
- `QueueError::SendFailed` — NATS publish failed (connection lost, subject invalid)
- `QueueError::ReceiveFailed` — consumer subscription failed
- `receive_*` has no built-in timeout — callers are responsible for cancellation

**Implementations:**
- `InMemoryQueue` — test double only; single-process `VecDeque` per subject
- `RecordingQueue` — decorator; see [RecordingQueue](#recordingqueue) below
- `NatsQueue` — production; NATS JetStream with a sync facade over async tokio

---

## DagStore

**Location:** `crates/vlinder-core/src/domain/dag.rs`

**Invariants:**
- **Append-only.** No update or delete operations exist on the trait. Nodes are permanent once inserted.
- **Parent-before-child.** A node's `parent_hash` must exist in the store before the node is inserted. Session root nodes use a zero-hash sentinel as `parent_hash` — the only exception.
- **Content-addressed identity.** `hash = SHA-256(payload || parent_hash || message_type || diagnostics)`. Same inputs always produce the same hash. Two nodes with the same hash are identical.
- **Sessions are isolated islands by default.** Normal execution produces no cross-session parent edges. Cross-session edges (repair/fork) are represented via `dag_parent` in the invoke message — they are explicit. *Note: `RecordingQueue` does not yet pass `dag_parent` when recording repair invokes — see [KNOWN_GAPS.md item 1](KNOWN_GAPS.md).*
- Infra plane nodes (`DeployAgent`, `DeleteAgent`) have `session_id = NULL` — they are cluster-scoped, not session-scoped.

**Error conditions:**
- `DagStoreError::AlreadyExists` — inserting a hash that already exists is silently accepted (idempotent insert)
- `DagStoreError::ParentNotFound` — parent hash does not exist in the store
- `DagStoreError::StorageError` — SQLite or gRPC failure
- Read methods return `Result<Option<T>>`: `Ok(None)` = does not exist; `Err(...)` = storage failure. Do not conflate the two.

**Implementations:**
- `InMemoryDagStore` — test double; no persistence across restarts. *Known deviation: some read methods return `Option<T>` rather than `Result<Option<T>>`, masking storage errors in unit tests — see [KNOWN_GAPS.md item 5](KNOWN_GAPS.md).*
- `SqliteDagStore` (`vlinder-sql-state`) — production; Diesel ORM; schema in `vlinder-sql-state/migrations/`
- `GrpcStateClient` (`vlinder-sql-state`) — remote client; implements `DagStore` via gRPC calls to `SqliteDagStore`

---

## RecordingQueue

**Location:** `crates/vlinder-core/src/queue/recording.rs`

A decorator over any `MessageQueue` that records every sent message as a DAG node before forwarding it.

**Critical invariant — ordering:**
> DAG insert happens **before** forwarding to the inner queue. If a worker crashes between receiving and processing a message, the node is already in the DAG. On replay, the system finds the node without re-executing side effects.

**Chain cache:**
- Per-process map of `(session_id, branch_id)` → latest DAG node hash, used to compute `parent_hash` for each new node
- Cache misses fall back to `DagStore::latest_node_hash()` (SQL query)
- Not shared across processes — safe because each session's data plane runs through one harness process at a time (ADR 052)

**Error conditions:**
- If DAG insert fails, `send_*` returns `Err` and the message is **not** forwarded — prevents "sent but not recorded" split-brain
- If the forward to the inner queue fails after a successful DAG insert, the node exists in the DAG with no children (orphaned). This is detectable but not automatically recovered

---

## Registry

**Location:** `crates/vlinder-core/src/domain/registry.rs`

**Invariants:**
- Agent names are globally unique. Re-registering an existing name is an upsert — the previous record is replaced.
- Manifests are validated at registration time. An invalid manifest returns `Err` immediately and is never stored.
- Job status transitions are: `Pending` → `Running` → `Completed(String)` | `Failed(String)`. `Completed` and `Failed` are terminal — status cannot change after reaching either.
- `select_runtime` only returns `RuntimeType` values that have been registered as available capabilities. If no worker has advertised a given runtime, it returns `Err`.

**Error conditions:**
- `RegistrationError::InvalidManifest` — manifest failed validation
- `RegistrationError::NotFound` — agent or model name does not exist
- `RegistrationError::NoRuntimeAvailable` — `select_runtime` found no matching registered capability

**Implementations:**
- `InMemoryRegistry` — test double; `HashMap` in memory
- `PersistentRegistry` — write-through: in-memory cache backed by `SqliteRegistryRepository`
- `GrpcRegistryClient` — remote client; implements `Registry` via gRPC

---

## Harness

**Location:** `crates/vlinder-core/src/domain/harness.rs`

**Invariants:**
- **Invoke is idempotent per `SubmissionId`.** `SubmissionId` is content-addressed: `SHA-256(payload || session_id || parent_submission)` (ADR 081). Replaying the same submission returns the cached result without re-executing.
- **Fork does not modify the source branch.** It creates a new branch from the state at `source_state`. The source branch remains unchanged.
- **Promote invalidates the branch.** After a successful promote, the branch ID is no longer valid for invocation.
- **All operations produce DAG nodes** via `RecordingQueue`. There is no harness operation that bypasses the DAG.
- A timeline is sealed after a fork — no new invokes on the forked-from branch until the fork is either promoted or abandoned.

**Error conditions:**
- `HarnessError::AgentNotFound` — agent name not in registry at invoke time
- `HarnessError::SessionNotFound` — session ID does not exist
- `HarnessError::BranchSealed` — invoke attempted on a sealed timeline
- `HarnessError::QueueError(...)` — underlying queue failure propagated up

**Implementations:**
- `CoreHarness` — canonical; owns session management, submission chaining, state tracking
- `GrpcHarnessClient` — remote client; implements `Harness` via gRPC
- Sidecar implementation (`vlinder-podman-sidecar`) — in-container; calls provider server over HTTP

---

## SecretStore

**Location:** `crates/vlinder-core/src/domain/secret_store.rs`

**Invariants:**
- Secrets are never logged. No `tracing::*` call in any implementation includes secret values.
- Secrets are never written to the DAG. They do not appear in `DagNode.payload` or `DagNode.diagnostics`.
- `get` returns `Err(SecretStoreError::NotFound)` for missing names — not `Ok(None)`. Callers must handle this error variant explicitly.
- `put` is idempotent: writing the same name twice overwrites silently.
- Names follow a hierarchical convention (`providers/{provider}/api-key`, `agents/{name}/identity-key`). The trait does not enforce this, but consumers depend on it.

**Error conditions:**
- `SecretStoreError::NotFound` — name does not exist (returned by `get` and `delete`)
- `SecretStoreError::StorageError` — NATS KV or gRPC failure

**Implementations:**
- `InMemorySecretStore` — test double; `HashMap` in memory
- `NatsSecretStore` — production; NATS KV bucket
- `GrpcSecretClient` — remote client for workers that connect via gRPC rather than directly to NATS

---

## RoutingKey

**Location:** `crates/vlinder-core/src/domain/routing_key.rs`

Three types, one per operational plane (ADR 121): `DataRoutingKey`, `SessionRoutingKey`, `InfraRoutingKey`.

**Invariants:**
- Routing is deterministic: the same key struct always serializes to the same NATS subject string.
- Subject components are NATS-safe (alphanumeric or hyphenated). ID types enforce this at construction.
- Plane separation is compile-time: passing a `DataRoutingKey` to `send_deploy_agent` is a type error.
- `DataMessageKind`, `SessionMessageKind`, and `InfraMessageKind` variants are exhaustive — adding a new message type causes compile errors at all match sites.

**Keeping parsers in sync:** NATS consumers subscribe using wildcards and parse subjects back into routing keys. The subject parsers in `vlinder-nats` (`invoke_parse_subject`, etc.) are the inverse of the key serializers. They must be updated whenever key struct fields change.
