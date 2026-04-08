# ADR 129: S3 Object Storage Backend

**Status:** Draft

Phase 2 of the three-step PoC defined in ADR 127. Adds an S3 backend for object storage.

## Context

Vlinder's object storage today is `vlinder-sqlite-kv`. Each session has its own pair of SQLite files: `objects.db` (a flat KV table for current state) and `state.db` (a content-addressed values/snapshots/state_commits store for time travel, per ADR 055). The DAG, fork, and promote semantics work today and are exercised end-to-end by the todoapp test script.

This ADR adds a new S3-backed storage worker. New feature work for production storage targets the S3 backend; `vlinder-sqlite-kv` stays as it is today and continues to serve as the local/offline option. The two backends are not maintained in lockstep.

## Decision

A new worker, `vlinder-s3`, implements the storage trait against S3. The architecture and queue plumbing are unchanged from the existing pattern: sidecar → queue → worker → backend.

### Agent-facing API (v1)

Start with the four operations sqlite-kv already exposes, plus metadata. Iterate from there.

- `get(path)` → content + metadata
- `put(path, content, metadata)`
- `delete(path)`
- `list(prefix, pagination)`

What is explicitly NOT in v1:
- copy / move / info / exists / bulk operations
- Signed URLs
- Streaming / resumable uploads
- Public URLs
- User-defined access rules
- Buckets / namespaces (paths only)
- Image transformations
- Time-travel reads in the agent API
- Cross-session or cross-branch reads

These can be added later when a concrete use case demands them, without breaking what exists.

### Worker internal model

Git-like content-addressed storage on S3. Single tier — no separate materialized current-state cache.

#### Why single tier (vs two tier)

`vlinder-sqlite-kv` today has two layers per session:

- **`objects.db`** — flat `files(path, content)` table, holds the current state of each path. Reads hit one indexed lookup. Writes overwrite via `INSERT OR REPLACE`. No history of its own.
- **`state.db`** — content-addressed `state_values` / `state_snapshots` / `state_commits` (the values/snapshots/state_commits model from ADR 055). This is where time travel actually lives. Append-only.

Every agent write hits both layers. Reads hit only `objects.db` because it's faster. Fork/checkout walks `state.db` to find the target snapshot, then rewrites `objects.db` to match.

The S3 worker collapses this into a single tier — only the content-addressed layer exists. There is no flat user-path-keyed S3 layout mirroring current state. Reads walk `HEAD → commit → tree → blob` (3-4 S3 GETs cold). Writes create new objects and update `refs/heads/<branch>`. Fork is a single S3 PUT updating a HEAD ref to point at a historical commit hash — no rewrite of any materialized state, no copy of any data.

The trade-off accepted:

| | Two-tier (sqlite-kv style) | Single-tier (chosen) |
|---|---|---|
| Read latency, cold | 1 lookup | 3-4 S3 GETs |
| Read latency, warm | 1 lookup | 1 S3 GET (internal nodes cached) |
| Writes per operation | Both layers | One layer |
| Fork cost | O(file count) — rewrite the materialized layer | O(1) — one PUT to update HEAD |
| Failure modes | Two layers can drift if a write partially fails | Single source of truth, nothing to keep in sync |
| Crash recovery | Validate the materialized layer matches history | Just open the HEAD ref |

The cold-read penalty is the main cost. Two mitigations make it acceptable in practice:

- **Internal objects (commits, trees) are immutable and small.** The worker can cache them in memory aggressively with no invalidation logic. After warmup, a typical read becomes "cache hit on commit + cache hit on tree + one S3 GET for the blob" — essentially the same as the two-tier read path.
- **S3 Express keeps even cold reads cheap.** ~5-15 ms per GET means a cold 4-GET walk is ~20-60 ms. Not free, but bounded.

If profiling later shows specific access patterns hitting the cold-cache cost (e.g., new worker startup with no cache), we can add a materialized current-state layer as an optimization without changing the contract or the storage trait. The history layer stays canonical; the materialized layer becomes a derived cache. Same migration shape as flat-trees → nested-trees: simple now, optimization later when real workload data shows it's needed.

#### Worked example: todoapp access pattern

Three `add` invocations followed by a `list`. Each `add` writes one file (`/todos/<id>.json`). The `list` reads all of them.

**Two-tier (sqlite-kv today)**, per `add`:

```
objects.db: 1 INSERT OR REPLACE
state.db:   1 state_value + 1 state_snapshot + 1 state_commit
            = 3 inserts
Total: 4 SQLite writes, sub-millisecond
```

The `list` is one indexed lookup on `objects.db` for the prefix, then 3 indexed `SELECT`s by path. 4 SQLite reads, sub-millisecond.

**Single-tier (S3 git-like)**, per `add`:

```
PUT objects/<blob-hash>           ← new content
GET HEAD, GET commit, GET tree    ← walk current state (cached after turn 1)
PUT objects/<new-tree-hash>       ← updated tree
PUT objects/<new-commit-hash>     ← new commit
PUT refs/heads/<branch>           ← move HEAD
Total: 4 PUTs + 0-3 GETs depending on cache
```

After turn 1, the worker caches the current commit and tree. Turns 2 and 3 hit the cache for HEAD/commit/tree and only do the 4 PUTs (~40ms each turn on S3 Express). Cold turn 1 is ~70ms because of the GET walk.

The `list` resolves entirely from cache for HEAD/commit/tree, then does 0-3 GETs for the leaf blobs depending on whether they're still cached from being recently written.

**What the comparison shows**:

- **Per-op work is the same shape in both**: hash content, build a tree, build a commit, move HEAD. Two-tier does 4 inserts; single-tier does 4 PUTs. The number of operations matches.
- **Per-op latency is dramatically different**: SQLite is microseconds, S3 is milliseconds. For sub-second latency budgets per agent op, S3 is fine; for tight loops it's not.
- **Cache dynamics are similar**: two-tier uses `objects.db` as a fast lookup table for the latest state. Single-tier uses an in-memory cache of immutable commits/trees as the same kind of fast path. Both reduce read latency by having a hot view.
- **System-level properties differ where it counts**: Fork is `O(file count)` SQL inserts in the two-tier model (rewriting `objects.db`) versus one S3 PUT in the single-tier model (just moving a HEAD ref). Multi-region replication, worker statelessness across restarts, and the lack of any materialized layer to keep in sync all favor the single-tier model for the kind of system Vlinder is.

For the typical Vlinder workload (agent operations spaced by LLM calls of hundreds of milliseconds), the per-op storage latency is in the noise. The system-level properties — fork-as-pointer-update, statelessness, no two-layer sync — are what makes simpler win.

```
s3://vlinder/sessions/<session>/
    objects/<hash>            ← content-addressed: blobs, trees, commits
    refs/heads/<branch>       ← small mutable object holding the current commit hash
```

- **Blob** = file content, hashed by `SHA(content)`
- **Tree** = directory listing, `{path → blob-hash}`, hashed by `SHA(serialized listing)`. Flat in v1; nested trees-of-trees as a future bounded migration when the per-session file count justifies it.
- **Commit** = `tree-hash + parent-commit + metadata`, hashed by `SHA(self)`
- **HEAD ref** = small mutable object (the only mutable thing) holding the current commit hash for a branch

Same content always has the same hash → automatic deduplication. Unchanged subtrees share storage across commits and across branches.

### Write path (per agent operation)

1. Hash the content → `<blob-hash>`
2. `PutObject objects/<blob-hash>` (skip if it already exists)
3. Read current branch HEAD → current commit hash
4. Read current commit → current tree hash, read current tree
5. Update the tree with `path → <blob-hash>` (also write the metadata into the tree entry)
6. Hash the new tree → `<new-tree-hash>`, `PutObject objects/<new-tree-hash>`
7. Build new commit (`new-tree-hash`, parent = current commit), hash it, `PutObject`
8. Update `refs/heads/<branch>` to the new commit hash (CAS via S3 conditional write)

Pattern C: every write creates a commit. Mid-invocation forks aren't a thing — invocation boundaries get marked separately. Boundary-marking mechanism is an implementation detail of the worker (separate ref namespace, commit metadata field, or tag — to be picked during implementation).

### Read path

1. Read HEAD → current commit hash
2. Read commit → tree hash
3. Read tree → find `path → <blob-hash>`
4. Read blob → return bytes (and metadata from the tree entry)

3-4 S3 GETs cold, one cached. With aggressive caching of immutable objects (commits, trees, blobs), warm reads are essentially the cost of a single S3 GET for the leaf blob.

### Fork / promote

These are session-plane operations driven by Vlinder's existing CLI. The worker doesn't need new semantics:

- **Fork** at a historical DAG node: create a new HEAD ref pointing at the commit hash from that DAG node. No data copy, no rewrite. Constant-time.
- **Promote**: rename the new branch to main (or whatever the canonical name is), seal the previous main. Both branches' HEADs still exist. Both stay reachable.
- **No discard semantics**: branches are sealed, not deleted. No object becomes unreachable. No GC needed.

### Fork addressability contract

A fork target is an invocation. The state at a fork target is the complete bundle of files written by all invocations up to and including that one. Files within an invocation are atomic from a fork standpoint — they share the invocation's identifier and cannot be addressed more granularly.

Concretely:

- An invocation that writes `/a.json`, `/b.json`, and `/c.json` produces one fork-addressable identifier (the boundary commit hash recorded against the Complete message)
- Operator can fork from that identifier → new branch has all three files at the values they had after the invocation
- Operator CANNOT fork to "after `/a.json` was written but before `/b.json`" — that intermediate state is not addressable from the fork API
- Per-write commits exist internally for crash durability (Pattern C), but they are not user-addressable; only the boundary commits are
- Per-file granularity is preserved for **reads** (the agent can `get(/a.json)` independently), but not for **fork targeting**

The agent author controls fork granularity by structuring how their main loop maps work to invocations. If they want fork-targetable boundaries between two logical operations, those operations need to be in separate invocations. If they pack multiple logical operations into one invocation, the invocation is the smallest fork unit. The granularity is theirs to control by code structure, not by an API knob.

### Agent-author burden vs worker internals

The agent author's mental model is "I have a key-value store with metadata; my data is private to me; I write a normal agent loop." Almost everything else is hidden in the worker.

| Agent author must know | Worker concern (hidden) |
|---|---|
| The four operations (`get`, `put`, `delete`, `list`) | Storage substrate (S3, SQLite, anything else) |
| Path conventions (free-form, hierarchical via `/`) | Content addressing (blobs, trees, commits) |
| Metadata field exists if they want it | Hashing, tree construction, commit chain |
| Their data is isolated to their session/branch | Caching strategy for immutable internals |
| Invocation structure controls fork granularity | Pattern C (commit per write, marked at boundaries) |
| | The boundary marker mechanism |
| | S3 conditional writes / HEAD CAS / atomicity |
| | Fork/repair/promote mechanics |
| | Deduplication |
| | GC story (none needed) |
| | Multi-region replication |

The only architectural concept that bleeds into agent code is **invocation structure**: the agent author should think about what an invocation represents in their domain so that fork-from-n-turns-ago aligns with operator intent. This is implicit in how they write their main loop, not exposed as an API knob. Everything else is the worker's job.

### What's the same as today

- The DAG structure and existing message types (`invoke`, `complete`, `request`, `response`, `fork_nodes`, `promote_nodes`, etc.)
- Session-plane CLI (`vlinder session fork`, `vlinder session promote`)
- Per-session/branch isolation
- Fork-from-historical at invocation boundaries
- The agent-facing semantics (the agent always sees "now", time travel is an operator concern)

### What's different from today

- A new storage substrate (S3) is available alongside `vlinder-sqlite-kv`
- The trait gains metadata support (the existing sqlite-kv backend doesn't have this and doesn't need to)
- The S3 worker is content-addressed end-to-end on S3, no materialized view
- New features land on the S3 backend; sqlite-kv stays at its current capability

## Local development and tests

- **Production**: real S3 (or S3 Express in the same region as the worker)
- **Local dev with internet**: real S3 against a dev account
- **Local/offline dev**: `vlinder-sqlite-kv` continues to serve this role
- **Tests**: MinIO in a container (s3-compatible) for integration tests against the S3 worker; the existing sqlite-kv tests stay where they are

## Storage cost story

- S3 cost grows monotonically with unique content (deduplicated across writes and branches)
- No GC needed in normal operation because Vlinder has no discard semantics — every commit is reachable from some branch HEAD forever
- Lifecycle policies can be applied later for cost management (move very-old objects to cheaper tiers, or expire entire prefixes for explicitly deleted sessions) — not needed for correctness

## What's NOT in this ADR (deferred to follow-ups)

- **S3 Vectors**: vector storage is its own concern, will get its own ADR
- **Signed URLs, streaming, copy/move, bulk ops**: deferred until use cases demand them
- **Multi-region HADR**: the architecture supports it via queue fan-out, but the implementation is a separate piece of work
- **Object format on S3** (JSON vs binary for trees/commits): implementation detail to be settled when writing the worker
- **Caching strategy** for hot trees/commits/HEADs in the worker: implementation detail
- **The exact mechanism for marking invocation boundaries** in Pattern C: implementation detail

