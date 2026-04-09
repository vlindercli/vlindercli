# ADR 129: S3-Backed Agent Storage

**Status:** Draft

Originally scoped as a content-addressed S3 storage worker (Phase 2 of ADR 127). Evolved through three phases of PoC validation. The current direction is S3 Files with per-branch access points — no custom storage protocol, no content addressing, no sidecar-driven copy.

## Context

Vlinder's object storage today is `vlinder-sqlite-kv`. Each session has its own pair of SQLite files: `objects.db` (flat KV for current state) and `state.db` (content-addressed values/snapshots/state_commits for time travel, per ADR 055). The DAG, fork, and promote semantics work today and are exercised end-to-end by the todoapp test script.

This ADR tracks the evolution of the S3-backed production storage design through three phases.

## Phase 1: Content-addressed S3 worker (implemented, superseded)

A `vlinder-s3` crate implementing a git-like content-addressed model: blobs, trees, commits, with SHA-256 hashing. The agent talked HTTP to `s3.vlinder.local`, the storage worker resolved requests through a commit chain, and state traveled on the message envelope (`msg.state`).

**What was built** (12 stacked-diff branches, `s3/01-skeleton` through `s3/12-vlinderd-aws-wiring`). E2e validated against real S3 with the todoapp. Fork worked via envelope-based state. 147 unit tests.

**Why superseded:** The agent needed Vlinder-specific HTTP client code. Every read/write was a queue round-trip. The content-addressed layer duplicated what S3 versioning already provides.

## Phase 2: S3 versioning + boto3 copy (validated, superseded)

Discovered during the S3 Files PoC: S3 versioning (required by S3 Files) returns a `VersionId` on every `put_object`. This IS the per-invocation identifier — the same thing we spent days chasing through Turso's `replication_index` (ADR 128).

The model: sidecar does `put_object` (commit) and `download_file(VersionId)` (checkout) around each invocation. Agent writes to `/tmp` or a mount. Manifest tracks multi-file state.

**What was validated** (s3-files-poc repo):

1. **SQLite (OLTP):** seed, read, write, checkpoint, commit/checkout via VersionId. Time travel and fork work.
2. **S3 versioning as commit hash:** Single key, many versions. `VersionId` is the commit hash. No snapshot prefix needed.
3. **DuckDB (OLAP):** Aggregation queries, inserts, checkpoint, commit/checkout. Time travel validated.
4. **Multi-file workspace:** Manifest model — walk workspace, `put_object` each file, collect `(path, VersionId)` into manifest. Manifest's `VersionId` is the commit hash.

**Why superseded:** The sidecar copied files on every commit/checkout. For small files this was fine; for large files it scaled linearly with size. Branching required content addressing for deduplication, which reintroduced the complexity we were trying to avoid. And S3 Files was already syncing the files to S3 — the sidecar was reimplementing what the mount does for free.

## Phase 3: S3 Files with per-branch access points (current direction)

### The insight

S3 Files access points have a configurable `root_directory`. Lambda's `file-system-configs` can be updated via `update-function-configuration`. Switching an access point takes ~7 seconds.

This means each branch can be a separate S3 prefix, each with its own access point. The mount always shows the active branch's files. Branch switching = access point switch. No file copy needed for checkout.

### How it works

**S3 layout:**
```
s3://bucket/agent-prefix/
    branches/
        main/                 ← access point A, root=/branches/main
            state.db
            config.json
        fork-1/               ← access point B, root=/branches/fork-1
            state.db
            config.json
```

**Same branch (common case, zero overhead):**
1. Invoke arrives
2. Agent reads/writes on mount — files are at `/mnt/s3files/state.db`
3. S3 Files auto-syncs writes to `branches/main/state.db` in S3
4. Complete. No sidecar work for commit — S3 Files IS the commit.

**Branch switch (fork, ~7 seconds):**
1. `CopyObject` files from `branches/main/*` to `branches/fork-1/*` (server-side, same bucket)
2. Create access point for `fork-1` with `root_directory=/branches/fork-1`
3. `update-function-configuration` to point at the new access point (~7s)
4. Next invocation sees fork-1's files on the mount

**Agent side:** Opens `/mnt/s3files/state.db`. Doesn't know which branch is mounted.

### S3 Files sync characteristics (measured)

- **Sync direction:** Mount → S3 is "within minutes" (AWS documentation). Measured ~65 seconds in eu-west-1.
- **Sync delay is constant:** 1KB and 10MB both sync in ~65 seconds. Not proportional to file size. It's a fixed-interval background job.
- **Not triggered by close/fsync:** `close()`, `fsync()`, `fsync(dir)` — none trigger immediate sync. The sync runs on its own schedule.
- **NFS close-to-open consistency:** Writes by one Lambda invocation are visible to subsequent invocations on the same mount immediately. The ~65s delay is only for the mount-to-S3-API sync.
- **S3 Files creates new versions:** Auto-synced files appear as new S3 object versions. Delete markers for deleted files. Versioning is built in.

### Why this is the right model

| Concern | Phase 1 (HTTP worker) | Phase 2 (boto3 copy) | Phase 3 (S3 Files + access points) |
|---|---|---|---|
| Agent persistence API | HTTP to `s3.vlinder.local` | File I/O + sidecar copies | File I/O, no sidecar involvement |
| Agent SDK | Required | None | None |
| Commit mechanism | Commit chain (blobs/trees) | boto3 `put_object` → VersionId | S3 Files auto-sync (~65s) |
| Checkout mechanism | Envelope state threading | boto3 `download_file(VersionId)` | Access point switch (~7s) |
| Fork | Envelope state threading | Copy + new VersionId | `CopyObject` + new access point |
| Per-write overhead | HTTP round-trip + 4 S3 PUTs | Direct file I/O | Direct file I/O |
| Commit overhead | None (inline) | boto3 upload per file | None (auto-sync) |
| Branch switch cost | None (envelope carries state) | boto3 download per file | ~7s access point switch |
| Storage deduplication | Content-addressed (SHA-256) | None (VersionId per PUT) | None (copy per branch) |
| Supported formats | Opaque bytes via HTTP | Any file | Any file |
| Infra | Storage worker process | `/tmp` only | S3 Files (VPC, mount target, access points) |

### What stays

- Per-agent storage URI: `object_storage = "s3://bucket/prefix"` in the agent manifest
- The DAG tracking state per invocation
- `vlinder-sqlite-kv` as the local/offline backend
- Session-plane fork/promote semantics
- Agent code writes `sqlite3.connect("/mnt/storage/state.db")` and gets persistence, time travel, and fork

### What's removed

- The `vlinder-s3` crate (blobs, trees, commits, ObjectStore, S3Worker, etc.)
- The `s3.vlinder.local` provider hostname
- The storage worker process
- Sidecar-driven commit/checkout (the sidecar doesn't copy files anymore)
- Manifest model (no manifests — the branch folder IS the state)

### What's new (to be implemented)

- Per-branch S3 prefix layout under the agent's storage path
- Per-branch S3 Files access point creation
- Lambda `update-function-configuration` for branch switching
- `CopyObject` for fork (copy branch prefix to new prefix)
- S3 Files infrastructure in the Lambda runtime (VPC, mount target, security group, S3 gateway endpoint)

## Agent-author burden vs platform internals

| Agent author must know | Platform concern (hidden) |
|---|---|
| Their files live at a mount path | S3 Files mount, access points |
| Any file-based storage works (SQLite, DuckDB, JSON, etc.) | Per-branch prefixes in S3 |
| Invocation structure controls fork granularity | Access point switching on branch change |
| | `CopyObject` for fork |
| | VPC, mount targets, security groups |

## Local development

- **Lambda (production):** S3 Files mount with per-branch access points
- **Podman (local):** `vlinder-sqlite-kv` continues to serve this role. Agent code is identical — it opens files at a mount path.

## Storage cost

Each branch has a full copy of every file. A 100MB database across 5 branches = 500MB. At $0.023/GB/month = $0.01/month. S3 storage is cheap enough that per-branch duplication is acceptable without content addressing.

For agents with very large state (GB+) and many branches, S3 lifecycle policies can expire old branch prefixes. The DAG knows which branches are reachable.

## Open questions

- **Access point creation latency.** Creating an access point takes a few seconds. For fork operations, this is a one-time cost. Need to measure in production.
- **Access point limits.** How many access points per file system? If there's a limit, branches need to be cleaned up.
- **Sync timing variability.** We measured ~65s in eu-west-1. Does this vary by region, file size pattern, or bucket activity? AWS documents "within minutes."
- **Cold start with mount.** Lambda cold starts with VPC + S3 Files mount take several seconds. Warm invocations are fast. Acceptable for agent workloads gated by LLM latency.
- **S3 Express One Zone.** Faster latency but doesn't support versioning. Not compatible with this model. Revisit if Express adds versioning.
- **Cloud portability.** Phase 2 (boto3 copy + VersionId) works on any cloud with versioned object storage. Phase 3 (S3 Files + access points) is AWS-specific. Phase 2 remains the portable fallback.

## What's NOT in this ADR (deferred)

- **S3 Vectors**: vector storage is its own concern
- **Multi-region**: S3 Cross-Region Replication + mount targets per region. Architecture supports it; implementation is separate.
- **Version lifecycle**: S3 lifecycle policies for expiring unreachable branches.
