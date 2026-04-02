# TODO

Working memory — not a backlog. Git history captures what's done.

---

# Next: Dogfood observability via durable agents

Build agents that exercise more service types in durable mode. Use the
platform's own observability (DAG, conversations, state store) to
diagnose failures. Let the gaps drive what to build next.

## AgentHealth — sliding window diagnostics

`agent_health.rs` exists as a seed (readiness check). Grow it into:

- Background thread polls `/health` on an interval
- Each check produces a `HealthSnapshot` (timestamp, latency, status code)
- Sliding window: `VecDeque<HealthSnapshot>`, evict by age
- Sidecar reads the window slice for each invocation's duration
- Slice embeds into `RuntimeDiagnostics` on the messages already being sent
- DAG commits capture container health alongside agent behavior
- `AgentHealth` exposes `is_healthy()` — sidecar checks before dispatching

**Future evolution:**
- `HealthSnapshot` aligns with OpenTelemetry semantic conventions
- `AgentHealth` becomes a client for a second sidecar container
- Health sidecar emits OTLP to NATS, DAG workers are just another consumer
- Commercial tier: DAG-aware observability UI (the thing Grafana can't do)

**Key insight:** `RuntimeDiagnostics` and `ServiceDiagnostics` already
exist as placeholders. This is how they get real data — not by
introspecting after the fact, but by continuously observing.

## dag_parent — canonical node identity (in progress on dag-parent-invoke branch)

`dag_parent` on `InvokeMessage` is the same concept as `parent_hash` on `DagNode`.
Both mean "which node is the parent." `dag_parent` is set by the caller;
`parent_hash` is computed by `RecordingQueue`. When `dag_parent` is present,
`RecordingQueue` should use it as the `parent_hash` instead of its chain cache.

Key decisions from discussion:
- `hash_dag_node()` produces the canonical domain identity for DAG nodes
- Git OIDs and SQL PKs are store-internal — domain uses canonical hash only
- Per-session Merkle chains are correct; cross-session lineage is a git projection concern
- Normal first invoke: `dag_parent` is empty → root of new chain
- Repair invoke: `dag_parent` points to the node being forked from
- GitDagWorker must store canonical hash as `hash` file in message subtree
- GitDagWorker must maintain `canonical_hash → git_oid` map for parent resolution
- `apply_dag_parent()` shelling out to `git rev-parse HEAD` is wrong — remove it

Sessions as islands in git:
- Each session's first commit is an orphan (no parent) — no artificial cross-session chaining
- GitDagWorker creates/updates `refs/sessions/<session_id>` as the chain grows
- Repair forks are the only cross-session edges (dag_parent points to another session's node)
- `git log --all --graph` shows the islands
- Drop `last_commit` carry-over between sessions in GitDagWorker

RecordingQueue chain cache is per-process but safe:
- Messages within a session are sequential by protocol (user is the lock)
- RecordingQueue inserts into SQL *before* forwarding to NATS
- Next process's cache miss queries SQL, which already has the latest node

Remaining work:
- [ ] Fix GitDagWorker: store `hash` file, build canonical→oid map, resolve dag_parent via map
- [ ] Fix GitDagWorker: orphan commits per session, per-session refs, drop cross-session `last_commit`
- [x] Delete `apply_dag_parent()` — git shelling is wrong
- [x] Wire `dag_parent` into `RecordingQueue.record()` so it overrides chain cache parent
- [x] Revert GitDagWorker dag_parent changes — defer git projection to later

## Sidecar refactoring (in progress on step-function-execution-mode branch)

Top-down ordering done. `agent_health.rs` extracted. Remaining:
- Deduplicate setup between `handle_invoke` and `handle_repair`
- Break up `run_checkpoint_loop`'s "call" arm
- Consolidate diagnostics + reply sending

Do this after AgentHealth has real data — the diagnostics shape drives
the refactoring.

## Compensating transactions (future, no doors closed)

Promote publishes a timeline diff to a NATS subject. The diff shows
which commits are abandoned vs promoted. Subscribers handle
domain-specific undo logic. The platform's only job is to emit the
event with full context. Content-addressed DAG makes the diff trivial
— it's a PR UX.

---

# Pending ADRs

## MVP blocker

| ADR | Decision | Pri | Complexity | Notes |
|-----|----------|-----|------------|-------|
|     | Lambda + SQS runtime | | | | | 
| 106 | Replace sqlite-vec with S3 vector | 1 | High | sqlite-vec can't do filtered vector search. pgvector supports full SQL alongside distance operators. |
| 098 | Time travel trait | 1 | High | Formalize time travel as a trait (log, route, checkout, repair, promote). |
| 100 | Storage traits as time travel contract | 1 | High | ObjectStorage + VectorStorage + DagWorker + StateStore define what's captured. |
| 072 | Storage snapshot contract | 3 | Medium | Formalizes what a snapshot captures. |
| 088 | Timeline UX | 1 | Medium | User-facing timeline commands and display. |
| 097 | Content-addressed identity | 2 | Medium | Replace random UUIDs with deterministic hashing. |
| 099 | Log as revertable states | 2 | Medium | Log entries must be identifiable, descriptive, revertable. |
| Services use in-memory state that doesn't survive restarts | `registry_memory.rs`, provider workers, harness | 1 | High | Registry, harness, and several provider workers store state in in-memory HashMaps. Moving to EC2 makes this a real problem — a restart loses all registered agents, sessions, etc. Each service needs a backing database and persistent repository implementation. |
| 084 | Container identity | 3 | Low | Identity model for containers. |
| 085 | Service identity | 3 | Low | Identity model for services. |
| 025 | Explicit timeouts | 3 | Low | Timeout declarations in manifest. |


### Good to have
| ADR | Decision | Pri | Complexity | Notes |
|-----|----------|-----|------------|-------|
| 027 | Manifest dependencies | 3 | Low | Agent dependency declarations. |
| 053 | Consumer staleness recovery | 2 | Medium | Recover from stale NATS consumers. |
| 089 | Submission branches | 2 | Medium | Git branch per submission. |


---

# ADR 121 Migration Progress

## Data plane — complete

All four data-plane message types migrated to typed tables + DataRoutingKey:
Invoke, Complete, Request, Response. Verified e2e on local (podman) and
AWS (Lambda + NATS + EC2). Sidecar constructs CompleteMessage + DataRoutingKey
directly (legacy DelegateReplyMessage intermediary removed).

## Peer-to-peer delegation — removed (ADR 124)

Broken by cross-sidecar consumer collision (response_filter wildcard).
Diagnosed via council fleet e2e: nonce mismatch deadlock when multiple
advisors share a submission. Entire peer-to-peer delegation path deleted.
Harness-mediated delegation (ADR 124) will replace it after session plane
and infra plane are clean.

## Session plane — next

Repair, Fork, Promote still go through ObservableMessage → insert_node →
dag_nodes blob path. This is the last dual-write: insert_node writes to
dag_nodes (blob) AND insert_typed_node writes to repair_nodes/fork_nodes/
promote_nodes. Once session-plane messages have typed record_* methods
(same pattern as data plane), insert_node and the blob columns become
dead code. dag_nodes stays for chain/timeline/snapshot index.

## Infra plane — blocks AWS e2e

Agent deploy returns "Deployed" before Lambda is actually ready.
Need async deploy lifecycle: submitted → deploying → ready → failed.
Without this, AWS e2e requires manual Lambda status checks from console.
This is the infra plane from ADR 121 — agent-scoped, not session-scoped.

## After cleanup: Harness-mediated delegation (ADR 124)

Session plane migration gets battle-tested during delegation work.
Infra plane unblocks reliable AWS testing. Only then is the codebase
clean enough to build delegation properly.

# Pending: Message type simplification (refactor-nats-headers branch)

WIP branch has state removed from all message types, correlation_id
removed from ResponseMessage, nonce replaced with Sequence on
delegation. Does not compile tests. Needs the following before landing:

- [ ] Wire state hash through queue for DAG node snapshot (transactional outbox in worker)
- [ ] Fix tests broken by state/correlation_id/nonce removal
- [ ] Remove `status_code` from ResponseMessage (now in WireResponse)
- [ ] Remove `checkpoint` from messages (deprecated)
- [ ] Add `dag_parent` to every message (not just Invoke/Fork)
- [ ] Compose `ObservableMessageHeaders` with `RoutingKey` (collapses `from_nats_headers`)
- [ ] Convert KV and vector workers to wire-format payloads
- [ ] Remove WireResponse fallback in provider server once all workers converted

Key design insight: state hash belongs on the DAG node (ADR 116), not
the message. The worker owns the atomicity of state mutation + message
send. The DAG node is a projection of the message stream. The hash
travels through the queue so the projector can build the snapshot.

# Pending: ADRs 119, 120 (drafted, not accepted)

- ADR 119: Dolt as content-addressed state store. `vlinder-dolt` crate
  with `postgres-protocol` provider host. Agent brings own Doltgres.
  Validates whether teams want time travel and repair.
- ADR 120: Provider plugin contract. Sidecar becomes plugin host. Each
  crate brings its own protocol listener and lifecycle.

# Tech debt

| What | Where | Pri | Complexity | Why it matters |
|------|-------|-----|------------|----------------|
| Async deploy: agent registration returns "submitted", runtime reports "ready" | CLI + registry + runtimes | 1 | High | `vlinder agent deploy` returns immediately with "submitted". The runtime reconciles asynchronously — for Lambda this means IAM role + function creation can take seconds. Need: agent status in registry (submitted → deploying → ready → failed), CLI polling or event stream, `vlinder agent status <name>` command. Without this, deploy lies — says "Deployed" before the function exists. |
| Monolithic config file | `~/.vlinder/config.toml`, `vlinderd/src/config.rs` | 2 | Medium | Everything lives in one `config.toml` even though we have separate structs internally (QueueConfig, RuntimeConfig, DistributedConfig, etc.). On-disk structure should mirror the domain: e.g. `config.d/` directory with per-concern files, or at minimum split NATS/runtime/provider credentials out of the main file. Credentials (API keys, creds files) especially shouldn't share a file with worker counts. |
| Install script out of date | `scripts/install.sh` | 1 | Medium | Missing `[state]` in config, wrong worker section names, invalid manifest syntax, dead `[[mounts]]`, image ref mismatch. See ADR 059. |
| Provider crates are hard-wired | `vlinderd/Cargo.toml`, `vlinder-sidecar/Cargo.toml`, `vlinder-podman-runtime/Cargo.toml` | 1 | High | All provider crates are unconditional dependencies. Use Cargo features to make them opt-in at compile time. |
| vlinder-core mixes protocol with implementations | `vlinder-core/src/` | 0 | High | Should be a pure protocol crate. NatsQueue/NatsSecretStore extracted to vlinder-nats. Still contains InMemoryQueue, InMemoryRegistry, RecordingQueue, and provider-leaked concepts (ObjectStorageType, Operation enum). |
| Flat `Operation` enum doesn't scale per-provider | `vlinder-core/src/domain/operation.rs` | 1 | High | Operation is a global union (Get/Put/Run/etc). Each provider should define its own operation vocabulary. Current flat enum leaks provider-specific concepts into shared routing. |
| Agent SDK templates use stale sidecar action protocol | `../vlinder-agent-*/vlinder.py` | 0 | Medium | The SDK returns actions for the sidecar to execute. Now that agents call provider hostnames directly, the SDK is a middleman with no purpose. Delete and update templates. |
| Lambda architecture hardcoded to arm64 | `vlinder-lambda-runtime/src/lambda_client.rs` | 2 | Low | `create_function` sets `Architecture::Arm64` unconditionally. Should be configurable via `LambdaRuntimeConfig` (or inferred from the ECR image manifest) so x86_64 images work too. |
| Sidecar Dockerfile pulls in vlinder-lambda-runtime (and transitively AWS SDK) | `crates/vlinder-podman-sidecar/Dockerfile` | 1 | Medium | The sidecar has no business depending on AWS. It's only included because the Dockerfile must list every workspace crate for cargo to resolve the workspace graph. Fix: Cargo feature flags (see row above) so the sidecar build excludes lambda-runtime entirely. Also: Dockerfile still references old path `vlinder-lambda-runtime`, needs updating to `vlinder-lambda-runtime`. |
| Dead pod detection — ensure_containers only starts missing pods, doesn't detect crashed ones yet | `vlinder-podman-runtime/src/pool.rs` | 2 | Medium | |
| `ServiceDiagnostics::placeholder()` | `domain/diagnostics.rs` | 1 | High | Fabricates fake diagnostics. Needs design: what to capture, how it flows to DAG nodes, user optionality. Diagnostics are a core selling point. |
| `RuntimeDiagnostics::placeholder()` + `ContainerId::unknown()` | `domain/` | 1 | High | Masks missing data. Same design scope as ServiceDiagnostics — part of the observability story. (Renamed from ContainerDiagnostics; RuntimeInfo is now a tagged enum — domain model is runtime-agnostic.) |
| `ObservableMessage` duplicates `RoutingKey` fields | `vlinder-core/src/domain/message/observable.rs` | 2 | Medium | Tracked in refactor-nats-headers branch. Compose with `RoutingKey` instead of repeating fields per variant. Collapses `from_nats_headers` (240→~40 lines) and `assemble()`. |
| RwLock/Mutex `.unwrap()` in non-test code | `registry_memory.rs`, `provider_server.rs` | 2 | Medium | Poisoned lock = process crash. Low probability but not production-safe. |
| Custom Podman VM image with s3fs-fuse baked in | `scripts/install.sh` | 1 | Medium | Ship `ghcr.io/vlindercli/vlinder-machine-os` so `podman machine init --image` gives users a VM with all deps. Eliminates runtime provisioning (Ignition, rpm-ostree). |
| Integration test for end-to-end pod lifecycle with real Podman + NATS | `vlinderd/tests/container_runtime_tests.rs` | 2 | High | |
| Linux rootless networking validation (host.containers.internal) | `vlinder-podman-runtime/src/podman_client.rs` | 3 | Medium | |

---

# MVP Roadmap
- [ ]
- [ ] templates repo
- [ ] finqa demo
- [ ] Docs site
- [ ] Blog
- [ ] Socials
