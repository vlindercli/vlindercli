# Known Gaps and Fragility Flags

Incomplete areas and fragility points as of 2026-04-01. Each entry notes severity and whether it is tracked in TODO.md. Remove entries when resolved — stale fragility flags are worse than none.

---

## High Severity

### 1. `dag_parent` not wired through `RecordingQueue`

**What:** Cross-session repair invokes carry a `dag_parent` field in `InvokeMessage` pointing to the prior-session node this repair branches from. `RecordingQueue` resolves parent hashes from its in-process chain cache (latest node in the current session/branch). It does not read `dag_parent` from the message.

**Impact:** Repair invokes are inserted into the DAG without their cross-session parent edge. The time-travel graph is incorrect for repaired sessions — you cannot follow lineage back through a repair to the original failure.

**Tracked:** Yes — TODO.md, branch `dag-parent-invoke`.

**Fix:** `RecordingQueue::send_invoke` must check `msg.dag_parent` and use it as `parent_hash` when present, bypassing the chain cache.

---

### 2. `GitDagWorker` lacks canonical hash → git OID mapping

**What:** `GitDagWorker` writes conversation history as git commits. To set correct parent commit relationships for cross-session repair edges, it must map DAG node hashes to git OIDs. This mapping is not stored — the worker cannot reliably resolve which git OID corresponds to a given DAG hash.

**Impact:** The git projection of cross-session repair timelines is incorrect. The SQL DAG store is unaffected — only the git projection is wrong.

**Tracked:** Yes — TODO.md, same branch as item 1.

**Fix:** Store the hash → OID map (e.g., a `hash` file per commit or a git notes ref). Resolve parent OID via the map.

---

## Medium Severity

### 3. Worker dispatch stubs are `unimplemented!()` / `todo!()`

**What:** Several tick-loop workers have placeholder dispatch logic that panics at runtime:

| Worker | File | Status |
|--------|------|--------|
| `OllamaWorker` | `vlinder-ollama/src/worker.rs` | Partial — some paths stub |
| `SqliteVecWorker` | `vlinder-sqlite-vec/src/worker.rs` | Stub |
| `OpenRouterWorker` | `vlinder-infer-openrouter/src/worker.rs` | Partial |

**Impact:** An agent requesting OpenRouter inference or vector storage will cause the worker to panic. The panic propagates as a failed `ResponseMessage` — the agent receives an error, not a hang. Feature-gate guards reduce blast radius, but the condition is reachable in a configured system.

**Tracked:** Implied by TODO.md (step-function work), not explicitly listed as blocking.

---

### 4. Worker process restart not automatic

**What:** The supervisor spawns workers and health-checks them at startup. If a worker crashes after startup, the supervisor does not restart it — the `AtomicBool` shutdown propagates only on clean SIGINT.

**Impact:** A single worker crash takes that capability offline for the daemon's lifetime with no alert. Subsequent requests to that worker block indefinitely or time out.

**Tracked:** Not tracked.

---

## Low Severity

### 5. `InMemoryDagStore` return types diverge from the trait

**What:** The `DagStore` trait defines read methods as `Result<Option<T>, DagStoreError>`. Some methods in `InMemoryDagStore` may return `Option<T>` directly, not wrapping in `Result`. *This was inferred during audit and not confirmed by reading the implementation — verify before acting on it.*

**Impact:** If true: callers tested against the in-memory double may not handle `Err` cases. The SQLite implementation returns `Err` on storage failures; the in-memory double would silently return `None`. Bugs masked in unit tests would surface in integration tests.

**Tracked:** Not tracked.

---

### 6. Registry gRPC server has unimplemented `send_request`

**What:** `vlinder-sql-registry/src/registry_service/server.rs` — the `send_request` method returns `Err(QueueError::SendFailed("send_request not implemented"))`.

**Impact:** Any caller routing a request through the registry gRPC service's queue path will receive an error. The CLI does not currently use this path (reads go via `GrpcRegistryClient`; writes go via the infra queue), so the impact is latent.

**Tracked:** Not tracked.

---

### 7. Health check timeout is hardcoded

**What:** The supervisor waits up to 10 seconds for each service to become ready. This is hardcoded in `vlinderd/src/supervisor.rs` — not configurable via `~/.vlinder/config.toml` or environment variable.

**Impact:** Slow machines or containers with large model initialization may fail startup even though the service would eventually become ready, producing a misleading startup error.

**Tracked:** Not tracked.
