# ADR 129: S3-Backed Agent Storage

**Status:** Draft

Originally scoped as a content-addressed S3 storage worker (Phase 2 of ADR 127). The design was implemented and validated end-to-end, then superseded by a fundamentally simpler approach discovered during the PoC: S3 Files + S3 versioning.

## Context

Vlinder's object storage today is `vlinder-sqlite-kv`. Each session has its own pair of SQLite files: `objects.db` (flat KV for current state) and `state.db` (content-addressed values/snapshots/state_commits for time travel, per ADR 055). The DAG, fork, and promote semantics work today and are exercised end-to-end by the todoapp test script.

This ADR tracks the evolution of the S3-backed production storage design through three phases: the original content-addressed worker, its implementation and e2e validation, and the S3 Files discovery that superseded it.

## Phase 1: Content-addressed S3 worker (implemented, superseded)

A `vlinder-s3` crate implementing a git-like content-addressed model: blobs, trees, commits, with SHA-256 hashing. The agent talked HTTP to `s3.vlinder.local`, the storage worker resolved requests through a commit chain, and state traveled on the message envelope (`msg.state`).

**What was built** (12 stacked-diff branches, `s3/01-skeleton` through `s3/12-vlinderd-aws-wiring`):

- Object model: `Blob`, `Tree`, `Commit`, `ObjectHash` types with deterministic serialization
- `S3Client` trait + `InMemoryS3Client` test fake + `AwsS3Client` backed by `aws-sdk-s3`
- `S3ClientFactory` for per-agent-per-bucket client caching
- `ObjectStore` and `S3Storage` with per-agent key prefixes parsed from `object_storage` URI
- `S3Worker` consuming the queue with `Registry` lookup per request
- `WireResponse` envelope (ADR 118) matching the ollama/openrouter pattern
- Provider-server host registration with dispatch fix for the `ObjectStorageType::S3` variant
- vlinderd supervisor wiring with `WorkerRole::StorageObjectS3`
- Sidecar hostname injection (`s3.vlinder.local`)

**What was validated:**

- E2e against real S3 (todoapp: add items, list, read back) ✓
- Fork via envelope-based state (`msg.state` carries the commit hash) ✓
- Per-agent bucket isolation via `object_storage = "s3://bucket/prefix"` ✓
- 147 unit tests across the crate ✓

**Why it was superseded:**

The content-addressed HTTP worker added a custom storage protocol between the agent and the platform. The agent had to use `s3.vlinder.local` HTTP endpoints with specific request/response shapes. This meant:

- Agent authors needed Vlinder-specific client code (`kv_get`, `kv_put`)
- Every read/write was an HTTP round-trip through the queue
- The content-addressed layer (blobs, trees, commits) duplicated what S3 versioning already provides
- Fork required envelope-based state threading — correct but complex

## Phase 2: S3 Files + S3 versioning (validated, current direction)

AWS announced S3 Files (April 2026): NFS mounts backed by S3 buckets, available on Lambda/EC2/ECS/EKS. This changes the storage model fundamentally.

### The insight

S3 Files requires bucket versioning. Every `put_object` returns a `VersionId`. The `VersionId` IS the per-invocation identifier — the same thing we spent days chasing through Turso's `replication_index` (ADR 128) and bottomless's `(generation, frame_number)`. AWS hands it to you as a response header on every PUT.

### How it works

**Agent side:** The agent mounts its storage as a local directory. It opens SQLite, reads JSON, writes files — normal file I/O. No SDK, no HTTP client, no Vlinder-specific code.

**Sidecar side (commit/checkout around each invocation):**

```
Invoke arrives (msg.state = parent VersionId)
    │
    ▼ CHECKOUT: s3.download_file(key, mount_path, VersionId=parent)
    │
    ▼ DISPATCH: POST /invoke to agent container
    │
    ▼ Agent runs, reads/writes files on the mount
    │
    ▼ COMMIT: s3.put_object(key, mount_path) → new VersionId
    │
Complete (response.state = new VersionId)
```

**Fork:** `checkout(fork_point_version_id)` before the forked invocation. The agent doesn't know a fork happened.

### What was validated (s3-files-poc repo)

Four rounds of validation on Lambda with an S3 Files mount in eu-west-1:

1. **SQLite (OLTP):** `sqlite3.connect("/mnt/s3files/workspace/state.db")` — seed, read, write, checkpoint, commit via `put_object`, checkout via `download_file(VersionId)`. Time travel and fork both work.

2. **S3 versioning as commit hash:** Single key, many versions. `VersionId` from `put_object` is the commit hash. No snapshot prefix, no copy operations. Checkout downloads a specific version.

3. **DuckDB (OLAP):** Single-file analytical database on the same mount. Aggregation queries (`GROUP BY`, `SUM`), inserts, checkpoint, commit/checkout all work. Time travel validated.

4. **Multi-file workspace:** Manifest model for directories of files. Commit walks the workspace, `put_object` each file, collects `(path, VersionId)` pairs into a manifest. Manifest's `VersionId` is the commit hash. Checkout restores exact state including file additions and deletions across timelines.

### Key findings

- **S3 Files sync is not instant (~1 min).** Commit/checkout must happen inside the Lambda via boto3, not from an external process via `aws s3 cp`. The sidecar is the natural place for this.
- **Lambda in VPC needs an S3 gateway endpoint.** Without it, boto3 calls to S3 hang.
- **SQLite defaults to `journal_mode=delete` on S3 Files.** Fine for single-writer agent workloads.
- **DuckDB requires bundling in the Lambda zip (~38MB).** Runtime pip install too slow over VPC.

## Decision

The production S3 storage backend for Vlinder will use S3 Files + S3 versioning, not the content-addressed HTTP worker.

### What this means

| Concern | Content-addressed worker (Phase 1) | S3 Files (Phase 2) |
|---|---|---|
| Agent persistence API | HTTP to `s3.vlinder.local` | Normal file I/O on mounted directory |
| Agent SDK requirement | Yes (kv_get/kv_put) | None |
| Time travel mechanism | Commit chain (blobs/trees/commits) | S3 `VersionId` |
| Fork mechanism | Envelope state threading | `checkout(VersionId)` before invocation |
| Storage worker | `S3Worker` consuming queue | None — sidecar does commit/checkout |
| Per-write overhead | HTTP round-trip + hash + 4 S3 PUTs | Direct file I/O (NFS) |
| Supported formats | Opaque bytes via HTTP API | Any file: SQLite, DuckDB, Parquet, JSON, anything |
| Content addressing | Custom (SHA-256 blobs/trees) | S3 versioning (AWS-managed) |
| Deduplication | Automatic via content addressing | S3 versioning (per-version storage) |

### What stays from Phase 1

- Per-agent storage URI: `object_storage = "s3://bucket/prefix"` in the agent manifest
- The DAG tracking which state (now `VersionId` instead of commit hash) to restore from
- `vlinder-sqlite-kv` as the local/offline backend
- Session-plane fork/promote semantics (unchanged — just the state identifier format changes)
- The `ObjectStorageType::S3` variant and the provider-server dispatch fix

### What's removed

- The `vlinder-s3` crate (blobs, trees, commits, ObjectStore, RefStore, S3Storage, S3Worker, S3ClientFactory, AwsS3Client)
- The `s3.vlinder.local` provider hostname and WireResponse storage envelope
- The storage worker process (`WorkerRole::StorageObjectS3`)
- The `http` dep, `sha2`/`hex` for content addressing, `md-5` for ETag computation

The `s3/01-skeleton` through `s3/12-vlinderd-aws-wiring` branches remain unmerged as reference for the wire path design decisions.

### What's new (to be implemented)

- S3 Files mount configuration in the Lambda adapter and Podman runtime
- Sidecar commit/checkout logic (~20 lines: checkpoint + `put_object` / `download_file`)
- `VersionId` as the state identifier on DAG nodes (replaces the commit hash string)
- Manifest model for agents with multiple files

## Agent-author burden vs platform internals

| Agent author must know | Platform concern (hidden) |
|---|---|
| Their files live at a mount path | S3 Files mount setup |
| Any file-based storage works (SQLite, DuckDB, JSON, etc.) | Commit/checkout lifecycle |
| Invocation structure controls fork granularity | `VersionId` tracking on the DAG |
| | S3 versioning, lifecycle policies |
| | Mount target provisioning, VPC endpoints |
| | Checkpoint before commit (for databases) |

The agent author writes `sqlite3.connect("/mnt/storage/state.db")` and gets persistence, time travel, and fork — without knowing any of it exists.

## Local development

- **Lambda (production):** S3 Files mount, real S3, sidecar does commit/checkout
- **Podman (local):** `vlinder-sqlite-kv` continues to serve this role. The agent code is the same — it opens files at a mount path. The difference is what provides the mount: S3 Files in production, a local directory volume in Podman.

## What's NOT in this ADR (deferred)

- **S3 Vectors**: vector storage is its own concern
- **Incremental snapshots**: for agents with large databases that change few rows per invocation, uploading the full `.db` on every commit is wasteful. WAL-level shipping (like bottomless) or rsync-style diffs could help. Deferred until workload data shows it's needed.
- **Multi-region**: S3 Cross-Region Replication + S3 Files mount targets per region. The architecture supports it; implementation is separate.
- **Version lifecycle**: S3 lifecycle policies for expiring unreachable versions. The DAG knows which versions are reachable.
