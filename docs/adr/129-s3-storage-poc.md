# ADR 129: S3-Backed Agent Storage

**Status:** Draft

Originally scoped as a content-addressed S3 storage worker (Phase 2 of ADR 127). Evolved through three phases of PoC validation. The current direction uses S3 Files for the hot path (agent reads/writes via mount) and a manifest of S3 VersionIds for time travel (fork from historical invocations).

## Context

Vlinder's object storage today is `vlinder-sqlite-kv`. Each session has its own pair of SQLite files: `objects.db` (flat KV for current state) and `state.db` (content-addressed values/snapshots/state_commits for time travel, per ADR 055). The DAG, fork, and promote semantics work today and are exercised end-to-end by the todoapp test script.

This ADR tracks the evolution of the S3-backed production storage design through three phases.

## Phase 1: Content-addressed S3 worker (implemented, superseded)

A `vlinder-s3` crate implementing a git-like content-addressed model: blobs, trees, commits, with SHA-256 hashing. The agent talked HTTP to `s3.vlinder.local`, the storage worker resolved requests through a commit chain, and state traveled on the message envelope (`msg.state`).

**What was built** (12 stacked-diff branches, `s3/01-skeleton` through `s3/12-vlinderd-aws-wiring`). E2e validated against real S3 with the todoapp. Fork worked via envelope-based state. 147 unit tests.

**Why superseded:** The agent needed Vlinder-specific HTTP client code. Every read/write was a queue round-trip. The content-addressed layer duplicated what S3 versioning already provides.

## Phase 2: S3 versioning + boto3 copy (validated, superseded)

Discovered during the S3 Files PoC: S3 versioning (required by S3 Files) returns a `VersionId` on every `put_object`. The sidecar did `put_object` (commit) and `download_file(VersionId)` (checkout) around each invocation.

**What was validated** (s3-files-poc repo): SQLite, DuckDB, multi-file workspaces — all with time travel and fork via VersionId.

**Why superseded:** The sidecar copied files on every commit/checkout. S3 Files was already syncing the same files — the sidecar was reimplementing what the mount does for free. And for large files, the copy scaled linearly with size while S3 Files sync is constant (~65s).

## Phase 3: S3 Files mount + manifest for time travel (current direction)

### Two concerns, two mechanisms

1. **Hot path (reads/writes during invocation):** S3 Files mount. Agent reads/writes files natively. Zero sidecar involvement. NFS close-to-open consistency ensures sequential invocations on the same mount see each other's writes immediately.

2. **Time travel (fork from historical invocations):** Manifest of S3 VersionIds captured after each invocation. The mount always shows "now." The manifest records what "now" looked like at each invocation boundary so it can be restored later.

The mount gives us the fast path. The manifest gives us time travel. Both are needed. Neither replaces the other.

### S3 layout

```
s3://bucket/agent-prefix/
    branches/
        main/                   ← access point A, root=/branches/main
            state.db            ← S3 versioning tracks every synced version
            config.json
        fork-1/                 ← access point B, root=/branches/fork-1
            state.db
            config.json
```

### How it works

**Normal invocation (common case, zero sidecar work for reads/writes):**

```
Invoke arrives
    │
    ▼  Agent reads/writes on mount at /mnt/s3files/
    │  (S3 Files mount is already showing the right branch)
    │
    ▼  Agent finishes
    │
    ▼  RECORD: wait for S3 Files sync (~65s)
    │          list-object-versions on branch prefix
    │          capture {path: VersionId} as manifest
    │          store manifest on DAG Complete node as state
    │
Complete
```

The ~65s wait happens AFTER the agent returns its response. The response goes back to the user immediately. The manifest capture is async work between invocations.

**Fork from historical invocation:**

```
Fork command targets invocation N
    │
    ▼  Read manifest from invocation N's DAG node
    │  (has {state.db: "ver-abc", config.json: "ver-def"})
    │
    ▼  CopyObject each file at its historical VersionId
    │  from branches/main/ to branches/fork-1/
    │  (server-side copy, same bucket)
    │
    ▼  Create access point for fork-1
    │  root_directory = /branches/fork-1
    │
    ▼  update-function-configuration (~7s)
    │  Lambda now mounts fork-1's files
    │
    ▼  Next invocation sees the fork point's state
```

**Same-branch resumption:** No work. The mount already shows the right branch via NFS close-to-open consistency.

**Branch switch (resume a different existing branch):**

`update-function-configuration` to the target branch's access point (~7s). No file copies — the mount shows the target branch's files directly.

### S3 Files sync characteristics (measured, eu-west-1)

- **Sync latency:** ~65 seconds, constant regardless of file size (1KB and 10MB both sync in ~65s)
- **Sync trigger:** Background process on a fixed interval. NOT triggered by `close()`, `fsync()`, or any application-level operation
- **NFS consistency:** Close-to-open. Writes by one Lambda are visible to subsequent invocations on the same mount immediately. The ~65s delay is only for mount-to-S3-API visibility.
- **Versioning:** S3 Files creates new S3 object versions when it syncs. Deletes create delete markers. Built in.
- **AWS documentation:** "within minutes" for export sync. Our measurement is consistent with this.

### Access point switching (measured)

- `update-function-configuration` to change the access point takes **~7 seconds**
- Different access points with different `root_directory` values show completely different files on the same mount path
- The Lambda sees the new files on the next invocation after the switch

### What the manifest records

After each invocation, once S3 Files has synced:

```json
{
    "state.db": "ver-abc123",
    "config.json": "ver-def456"
}
```

Each entry maps a file path to the S3 VersionId that was current at that invocation boundary. This is stored on the DAG's Complete node as `state`. The manifest is the minimum information needed to fork from this point — it tells `CopyObject` which version of each file to copy.

For invocations that didn't change any files (read-only): the sidecar stat-diffs the mount before and after. If no files changed, the previous manifest is carried forward. No sync wait needed.

### Why this is the right model

| Concern | Phase 1 (HTTP worker) | Phase 2 (boto3 copy) | Phase 3 (mount + manifest) |
|---|---|---|---|
| Agent reads/writes | HTTP round-trip per op | File I/O on /tmp | File I/O on mount |
| Agent SDK | Required | None | None |
| Per-write overhead | HTTP + queue + 4 S3 PUTs | None (local I/O) | None (NFS) |
| Commit | Inline (every write) | boto3 upload per file | S3 Files auto-sync + capture VersionIds |
| Checkout (same branch) | Envelope state | boto3 download per file | Nothing (mount already correct) |
| Checkout (branch switch) | Envelope state | boto3 download per file | Access point switch (~7s) |
| Fork | Envelope state | Copy + new VersionId | CopyObject at historical VersionIds + new access point |
| Large file handling | Same as small files | Scales linearly | Mount handles it (NFS page-level) |
| Storage dedup | Content-addressed (SHA-256) | None | None (per-branch copies) |
| Infra | Storage worker process | /tmp only | S3 Files (VPC, mount target, access points) |

### What stays

- Per-agent storage URI: `object_storage = "s3://bucket/prefix"` in the agent manifest
- The DAG tracking state per invocation (now a manifest of VersionIds)
- `vlinder-sqlite-kv` as the local/offline backend
- Session-plane fork/promote semantics
- Agent code writes `sqlite3.connect("/mnt/storage/state.db")` and gets persistence, time travel, and fork

### What's removed

- The `vlinder-s3` crate (blobs, trees, commits, ObjectStore, S3Worker, etc.)
- The `s3.vlinder.local` provider hostname
- The storage worker process
- Sidecar-driven file upload/download on the hot path

### What's new (to be implemented)

- S3 Files infrastructure provisioning (file system, mount target, VPC, access points)
- Per-branch S3 prefix layout under the agent's storage path
- Per-branch access point creation and Lambda config switching
- Manifest capture after invocation (wait for sync, list-object-versions, record on DAG)
- Stat-diff before/after invocation to detect changes (skip manifest capture on read-only invocations)
- `CopyObject` at historical VersionIds for fork
- Promote mechanics (copy or swap branch prefix)

## Agent-author burden vs platform internals

| Agent author must know | Platform concern (hidden) |
|---|---|
| Their files live at a mount path | S3 Files mount, access points, VPC |
| Any file-based storage works (SQLite, DuckDB, JSON, etc.) | Per-branch prefixes in S3 |
| Invocation structure controls fork granularity | Manifest capture and VersionId tracking |
| | Access point switching on branch change |
| | CopyObject at historical VersionIds for fork |
| | S3 Files sync timing (~65s) |

## Local development

- **Lambda (production):** S3 Files mount with per-branch access points, manifest for time travel
- **Podman (local):** `vlinder-sqlite-kv` continues to serve this role. Agent code is identical — it opens files at a mount path.

## Storage cost

Each branch has a full copy of every file. S3 versioning retains synced versions within each branch. At $0.023/GB/month, per-branch duplication is cheap for typical agent workloads (KB–MB databases). S3 lifecycle policies can expire old branches and old versions within branches.

## Open questions

- **Access point limits.** How many per file system? Branches need cleanup if capped.
- **Manifest capture timing.** The ~65s sync wait between invocations. For conversational agents (messages minutes apart), invisible. For rapid-fire automation, could be a bottleneck. Measure in real workloads.
- **Cold start with mount.** Lambda cold starts with VPC + S3 Files mount. Acceptable for LLM-gated workloads.
- **Cloud portability.** S3 Files + access points is AWS-specific. Phase 2 (boto3 copy + VersionId) remains the portable fallback for Azure/GCP.

## What's NOT in this ADR (deferred)

- **S3 Vectors**: vector storage is its own concern
- **Multi-region**: S3 Cross-Region Replication + mount targets per region
- **Version lifecycle**: S3 lifecycle policies for expiring unreachable branches and old versions
- **Content-addressed deduplication**: for agents with many branches sharing identical large files. Deferred until storage cost data shows it's needed.
