# ADR 131: Runtime as Message Handler

**Status:** Draft

## Context

While implementing S3 Files session plane support (ADR 130, step 9), we discovered that the Runtime trait has no contract for handling session plane messages (fork, promote). Digging deeper, the contract is missing for all three planes. And operations that span multiple workers (deploy, fork) have no coordination mechanism.

### What runtimes do today

Both `ContainerRuntime` (Podman) and `LambdaRuntime` implement `Runtime::tick()` as a reconciliation loop:

**Infra plane:** `ensure_containers()` / `ensure_functions()` polls the Registry to discover agents, diffs against local state, deploys/undeploys. This bypasses the infra plane queue messages (`DeployAgentMessage`, `DeleteAgentMessage`) that already exist.

**Data plane:** Podman delegates entirely to the sidecar inside the container — the sidecar subscribes to the queue and handles invocations. The runtime has no visibility. Lambda bridges because Lambda's execution model requires it (`dispatch_invocations` polls the queue and calls `invoke_function`). Neither handles `CompleteMessage`.

**Session plane:** Neither runtime handles fork or promote. Today this works because sqlite-kv state travels on the DAG envelope — no infrastructure changes needed on fork. S3 Files breaks this assumption: fork requires CopyObject + access point creation + Lambda config switch.

### The sync tick loop ordering bug (observed)

The infra worker's tick loop processes deploy then delete in each iteration:

```rust
while !shutdown {
    match queue.receive_deploy_agent() { ... }  // always tried first
    match queue.receive_delete_agent() { ... }  // always tried second
    sleep(10ms);
}
```

This creates a message ordering bug. When the CLI does an "idempotent delete" (agent already deleted), it sees `Deleted` from previous readiness checks and exits immediately — but its NATS message is still queued. If a new deploy arrives before the infra worker consumes the stale delete, the worker processes the deploy first (because it tries deploy before delete), then the stale delete in the same iteration, poisoning the readiness state:

```
 Row 9:  registry=ready    @ 37.940  ← deploy processed
 Row 10: container=pending @ 37.940  ← deploy processed
 Row 11: registry=deleted  @ 37.942  ← stale delete processed 2ms later
 Row 12: container=ready   @ 38.372  ← container runtime finishes deploy
 Row 13: container=deleted @ 48.732  ← container runtime processes stale delete
```

The CLI deploy loop sees `Deleted`/`Deleting` instead of `Live` and hangs forever. This is not fixable with guards in the handler — the deploy re-registers the agent, so by the time the stale delete runs the agent exists again. The fix requires FIFO processing of all infra messages on a single stream, which requires async.

Validated by e2e testing: the infra stress tests in `sample-agents-fleets` (commit `8579d74`) reliably reproduce this. The tests are parked on the `infra-stress-tests` branch, blocked on this ADR's async migration.

### The coordination problem

Operations span multiple workers. Deploy requires:
1. **Registry**: register manifest, validate capabilities
2. **Queue backend**: provision agent-scoped queues
3. **Runtime**: create compute (pod, Lambda function)

Fork (S3 Files) requires:
1. **S3 Files**: copy files, create access point
2. **Runtime**: switch Lambda config

These workers are independent — they can run in parallel. But the operation isn't complete until ALL workers finish. Today this is faked as a sequential pipeline: the infra worker sets `Deploying`, the runtime polls the Registry for `Deploying` agents. This works but doesn't scale to N workers and conflates ordering with coordination.

### Gaps exposed

1. **No session plane handlers.** Fork/promote require infrastructure work that varies by backend.
2. **Infra plane bypasses the queue.** Runtimes poll the Registry instead of receiving messages.
3. **Data plane is inconsistent.** Podman delegates to sidecar. Lambda bridges. No common contract.
4. **No multi-worker coordination.** Deploy and fork are sagas with no coordination mechanism.
5. **The Runtime trait hides the contract.** `tick()` is opaque. No way to know what a runtime handles.

## Decision

### 1. Universal message handler contract

The Runtime trait declares async handlers for every message type on every plane. Default implementations are no-ops. Each runtime overrides the handlers it needs. Every worker in the system follows this same shape.

For every `receive_X` on `MessageQueue`, there is an `on_X` on `Runtime`. One-to-one correspondence.

Handlers are `async` (via `#[async_trait]`) from day one. `tick()` remains sync during migration, bridging to async handlers via `block_on`. This is the seam for the eventual move to `tokio::select!`.

`MessageQueue` gets async receive methods (`receive_X_async`) alongside the sync versions. Default impls delegate to sync. Backends override with truly async implementations when ready. Once all callers use async, sync methods are removed.

### 2. Condition-based coordination (barrier pattern)

Operations that span multiple workers use conditions on the shared resource. Inspired by Kubernetes conditions pattern, adapted to our architecture.

**How it works:**

1. The **Registry** validates the manifest and determines which workers need to participate — it already validates runtime, storage, inference, embedding, queue capabilities during `register_agent`.

2. The Registry creates **conditions** for each required worker: `registry: pending`, `queues: pending`, `compute: pending`. The conditions are declared upfront before the message goes to workers.

3. The deploy message goes out via fan-out (each worker gets its own queue copy).

4. Each worker does its part independently and marks its condition `ready` on the Registry.

5. The Registry uses a **barrier**: when a worker marks its condition ready, the Registry atomically checks if all conditions are now met. If yes, it transitions the aggregate state (e.g., `Deploying` → `Live`). The last worker to finish — whichever that is — triggers the transition.

**Why the Registry owns this:**

- It already knows the full topology (registered runtimes, storage backends, engines)
- It already validates "can this agent be deployed?" — the conditions are exactly the capabilities that passed validation
- Workers don't need to know about each other — they only know the Registry
- No upfront participant count in workers, no late-registration race
- Condition creation and validation are atomic — if validation fails, no conditions are created

**The barrier transaction:**

```
Worker completes its part:
  BEGIN TRANSACTION
    SET condition[worker] = ready
    SELECT COUNT(*) FROM conditions WHERE operation=X AND status='pending'
    IF 0 pending → SET aggregate status = Live (or Ready)
  COMMIT
```

No polling. No coordinator process. No ordering assumptions. SQLite transactions guarantee exactly one worker sees "all met" and triggers the transition.

**Same mechanism for all operations:**

| Operation | Conditions | Aggregate state |
|-----------|-----------|----------------|
| Deploy | registry, queues, compute | `Deploying` → `Live` |
| Delete | compute, queues, registry | `Deleting` → `Deleted` |
| Fork (S3 Files) | files_copied, access_point, lambda_switched | `creating` → `ready` |
| Promote | files_copied, lambda_switched | `promoting` → `ready` |

### 3. Strangler fig migration

New handlers coexist with `tick()` during migration. As each plane migrates, reconciliation code shrinks until `tick()` is removed.

**Phase 1 (done):** Add all async handlers as no-ops. Add async receive methods to `MessageQueue`. Both runtimes annotated with `#[async_trait]`. Wire `dispatch_messages` into `tick()` for both runtimes.

**Phase 2 (in progress):** Migrate infra plane. `on_deploy_agent` / `on_delete_agent` do real work. Deploy uses condition-based coordination. Infra worker responsibilities absorbed into the condition model. Health check retains orphan cleanup and crash recovery.

**Phase 3:** Migrate session plane. `on_fork` / `on_promote` use condition-based coordination. S3 Files operations flow through naturally.

**Phase 4:** Migrate data plane. `on_invoke` replaces `dispatch_invocations` (Lambda) and sidecar dispatch (Podman).

**Phase 5:** Remove `tick()`, remove sync receive methods. Runtime is purely async message handlers.

### 4. Every worker is this trait

Every worker in the system has the same shape:

- **Inference worker**: handles `on_request` / `on_response`
- **Storage worker**: handles `on_request` / `on_response`
- **Git DAG worker**: handles all messages as projections
- **Runtime**: handles deploy/invoke/fork/promote
- **Infra worker**: absorbed into condition model (registry + queue provisioning become conditions)

Today each is a custom loop with ad-hoc message parsing. The Runtime trait is the first formalization. When we unify workers, this trait becomes the universal worker interface.

### 5. Health check

`tick()` retains a health check role even after handlers take over:

- **Orphan cleanup**: compute running but not in registry → stop
- **Crash recovery**: agent in `Live` state but compute not running → restart

These are reconciliation concerns that don't have messages — they detect drift between desired and actual state. The health check is what remains of `tick()` after all planes are migrated.

## Current status

### Prerequisite: registry-conditions (ready to merge)

Branch `registry-conditions/06-remove-agent-state` is validated (unit tests + e2e pass). Implements readiness checks, derived status, barrier-based state transitions. The AgentState dead code is removed. Ready to merge to main.

### Dependency chain

1. **Merge registry-conditions to main** — gate: Lambda e2e validation on EC2
2. **Async traits on MessageQueue** (this ADR, phase 1) — unblocks FIFO message processing
3. **Async infra worker** (this ADR, phase 2) — fixes the tick loop ordering bug
4. **Control/session/data plane signatures on MessageQueue** — unblocks S3 Files
5. **S3 Files integration** (ADR 130)

### Branches in progress

- `runtime-handlers/01-queue-wiring`: async handler no-ops + queue wiring (3 commits)
- `runtime-handlers/02-infra-plane`: Podman deploy/delete via handlers (1 commit on top)
- `infra-stress-tests` (sample-agents-fleets): parked, blocked on async migration

## Pre-merge fixes for registry-conditions

Code review of PRs #68–#72 (steps 01–05) and step 06 identified issues to fix before merging to main. Organized by which stacked diff branch should carry the fix.

### Step 02 (readiness-checks) — ✅ done

- ~~**No index on `readiness_checks`.**~~ Added `(agent_name, worker, updated_at)` index in DDL.
- ~~**N+1 query in `get_derived_status_inner`.**~~ Collapsed into single self-join query via `latest_checks_inner` (`MAX(id)` per worker).
- ~~**Duplicated derivation logic.**~~ Extracted shared `derive_status()` in `vlinder-core/domain/readiness.rs`, used by both `InMemoryDagStore` and `SqliteDagStore`. (Moved from step 03 — natural fit alongside the query refactor.)

### Step 03 (derived-status) — ✅ done

- ~~**`ensure_deleted` no-ops when function not in local map.**~~ Now marks readiness check as `Deleted` even when the function isn't locally tracked. Conditional teardown only if function exists.
- ~~**`ensure_deployed` appends a ready check every tick.**~~ Removed the re-append — returns early without writing when function already deployed.
- ~~**Error reason lost from deploy/delete failure messages.**~~ Added `get_derived_status_with_error` to `RegistryRepository`. gRPC `GetAgentState` now returns the error from the latest `Failed` readiness check. CLI shows actual error on failure.
- **`ListAgents` calls `get_derived_status` per agent.** N mutex acquisitions + N×(1+W) queries. Deferred — perf optimization, not a correctness issue.

### Step 05 (pod-liveness) — ✅ done

- ~~**`is_pod_live` checks existence, not running state.**~~ API client now parses `State` field from JSON response. CLI client uses `--format {{.State}}`. Both check for `"Running"`.
- ~~**No test for crash recovery path.**~~ Added `crashed_pod_is_recreated` test with `AtomicBool`-controlled mock.

### Step 06 (remove-agent-state) — ✅ done

- ~~**`agent_states` table DDL and Diesel schema still present.**~~ Removed `CREATE TABLE`, index, `diesel::table!`, `joinable!`, and `allow_tables_to_appear_in_same_query!` entries.
- ~~**`readiness_checks` not cleaned up on `delete_agent`.**~~ `delete_agent` now deletes `readiness_checks` rows before deleting the agent.

## Consequences

- Runtime trait is the definitive contract for what a runtime handles
- Operations that span multiple workers have explicit coordination via conditions
- The Registry is the coordination point — validates, declares conditions, owns the barrier
- Workers are decoupled — each only knows the Registry, not other workers
- New runtimes/workers get the full contract from day one
- The infra worker's responsibilities (registry registration, queue provisioning) become conditions rather than a separate sequential step
- `tick()` has a clear migration path to elimination
- The same condition mechanism works for deploy, delete, fork, promote, and future multi-worker operations
