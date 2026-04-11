# ADR 131: Runtime as Message Handler

**Status:** Draft

## Context

While implementing S3 Files session plane support (ADR 130, step 9), we discovered that the Runtime trait has no contract for handling session plane messages (fork, promote). Digging deeper, the contract is missing for all three planes. And operations that span multiple workers (deploy, fork) have no coordination mechanism.

### What runtimes do today

Both `ContainerRuntime` (Podman) and `LambdaRuntime` implement `Runtime::tick()` as a reconciliation loop:

**Infra plane:** `ensure_containers()` / `ensure_functions()` polls the Registry to discover agents, diffs against local state, deploys/undeploys. This bypasses the infra plane queue messages (`DeployAgentMessage`, `DeleteAgentMessage`) that already exist.

**Data plane:** Podman delegates entirely to the sidecar inside the container — the sidecar subscribes to the queue and handles invocations. The runtime has no visibility. Lambda bridges because Lambda's execution model requires it (`dispatch_invocations` polls the queue and calls `invoke_function`). Neither handles `CompleteMessage`.

**Session plane:** Neither runtime handles fork or promote. Today this works because sqlite-kv state travels on the DAG envelope — no infrastructure changes needed on fork. S3 Files breaks this assumption: fork requires CopyObject + access point creation + Lambda config switch.

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

## Consequences

- Runtime trait is the definitive contract for what a runtime handles
- Operations that span multiple workers have explicit coordination via conditions
- The Registry is the coordination point — validates, declares conditions, owns the barrier
- Workers are decoupled — each only knows the Registry, not other workers
- New runtimes/workers get the full contract from day one
- The infra worker's responsibilities (registry registration, queue provisioning) become conditions rather than a separate sequential step
- `tick()` has a clear migration path to elimination
- The same condition mechanism works for deploy, delete, fork, promote, and future multi-worker operations
