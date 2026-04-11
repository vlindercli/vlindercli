# ADR 130: S3 Files Implementation Plan

**Status:** Draft

Step-by-step plan for integrating S3 Files into vlindercli, based on the validated PoC (ADR 129). Each step is a stacked diff branch, independently reviewable and testable.

## Prerequisites

The s3-files-poc validated:
- SQLite/DuckDB on S3 Files mount (NFS) ✓
- S3 versioning → VersionId as commit hash ✓
- Manifest capture (list-object-versions after sync) ✓
- CopyObject at historical VersionId with POSIX metadata for fork ✓
- Access point switching for branch isolation (~7s) ✓
- Stat-diff for change detection ✓
- Full loop: write → commit → fork → access point switch → read fork state ✓

## Steps

### Step 1: S3 Files domain types in vlinder-core

Add configuration types for S3 Files storage.

**Changes:**
- `ObjectStorageType::S3Files` variant in `storage.rs`
- `S3FilesConfig` struct: file system ID, bucket, mount path, region
- `ResourceId` parses `s3files://bucket/prefix` URIs (or reuse `s3://` with scheme detection)

**Tests:** Unit tests for URI parsing, type construction, serialization.

**No infra, no AWS calls.** Pure types.

### Step 2: S3 Files infrastructure provisioning

New module that creates the AWS resources needed for an agent's S3 Files storage. Called during agent deployment when `object_storage` uses the S3 Files scheme.

**What it creates:**
- S3 bucket with versioning enabled (or validates existing)
- S3 Files file system (`aws s3files create-file-system`)
- VPC, subnet, security group with NFS port 2049 (or reuses existing)
- S3 VPC gateway endpoint
- Mount target in the subnet
- Initial access point for `branches/main/` with POSIX user 1000:1000

**Changes:**
- New module in `vlinder-lambda-runtime` (or a new `vlinder-s3-files` crate)
- Uses `aws-sdk-s3files` (if it exists) or raw AWS API calls
- Idempotent: skip resources that already exist
- Outputs: file system ID, access point ARN, mount target ID

**Tests:** Integration test against real AWS (gated by env var like the OpenRouter tests). Verify file system creation, mount target availability, access point creation.

### Step 3: Lambda creation with S3 Files mount

Modify Lambda function creation to attach the S3 Files mount when the agent declares S3 Files storage.

**Changes:**
- `vlinder-lambda-runtime`: add `file-system-configs` to `create-function` call
- Wire the access point ARN and `/mnt/s3files` mount path
- Add S3 Files client permissions to the Lambda execution role (`s3files:ClientMount`, `s3files:ClientWrite`)
- Add `s3:ListBucketVersions` to the role (needed for manifest capture)

**Tests:** Deploy a test Lambda with the mount, invoke it, verify the mount path exists.

### Step 4: Stat-diff utility

Small utility module for detecting workspace changes between invocations.

**Interface:**
```rust
pub struct WorkspaceSnapshot {
    pub files: BTreeMap<String, FileStat>,
}

pub struct FileStat {
    pub mtime_ns: u64,
    pub size: u64,
}

impl WorkspaceSnapshot {
    pub fn capture(workspace_path: &Path) -> Self;
    pub fn diff(&self, other: &Self) -> WorkspaceDiff;
}

pub struct WorkspaceDiff {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}
```

**Changes:** New module, either in the Lambda adapter or a small shared crate.

**Tests:** Unit tests with tempdir — create files, snapshot, modify, snapshot again, verify diff.

### Step 5: Manifest capture

After each invocation, capture the current file VersionIds and store them on the DAG.

**Flow:**
1. Stat-diff detects changes
2. If no changes: carry forward previous manifest. Done.
3. If changes: for each changed file, boto3 `put_object` from the mount to S3 → capture VersionId
4. For unchanged files: carry forward VersionId from previous manifest
5. Build manifest: `{path: VersionId, ...}`
6. Store manifest JSON as `state` on the DAG Complete node

**Why boto3 upload instead of waiting for sync:** S3 Files sync takes ~65s and aggregates writes. boto3 upload is instant and gives a per-invocation VersionId. Only changed files are uploaded (stat-diff tells us which). Unchanged files keep their existing VersionIds.

**Changes:**
- Lambda adapter: after dispatch, call stat-diff, upload changed files, build manifest
- DAG Complete node: `state` field carries the manifest JSON string

**Tests:**
- Write a file on mount, capture manifest, verify VersionId is present
- Write two files, change one, capture again — only changed file gets new VersionId
- Read-only invocation — manifest is carried forward, no uploads

### Step 6: Fork via CopyObject + access point

Session-plane fork operation: materialize a historical manifest into a new branch prefix, create an access point, switch the Lambda config.

**Flow:**
1. Read manifest from target DAG node's `state` field
2. Create branch prefix in S3: `branches/<fork-name>/`
3. Create `workspace/` directory marker with POSIX metadata (040755, uid 1000, gid 1000)
4. For each file in manifest: `CopyObject` at historical VersionId to `branches/<fork-name>/workspace/<path>` with POSIX metadata (0100644, uid 1000, gid 1000)
5. Create access point with `root_directory=/branches/<fork-name>`, posix user 1000:1000
6. `update-function-configuration` to mount the new access point (~7s)

**Changes:**
- Session-plane fork handler: new S3 Files fork implementation alongside existing sqlite-kv fork
- Access point management: create, list, delete

**Tests:**
- Seed data, commit, write more, commit, fork from first commit
- Verify forked Lambda reads correct historical state
- Verify write on fork succeeds (POSIX permissions correct)
- Verify main branch is unaffected after fork

### Step 7: Branch switch

When resuming a session on a different branch than what's currently mounted, switch the access point.

**Flow:**
1. Look up the target branch's access point ARN
2. `update-function-configuration` (~7s)
3. Next invocation sees the target branch's files

**Changes:**
- Session-plane handler: detect when the target branch differs from the currently mounted branch
- Track current mounted branch per Lambda function (in the registry or a config store)

**Tests:** Switch between two branches, verify correct data on each.

### Step 8: Promote

Promote a branch to main.

**Flow (option A — swap access point):**
1. Main's access point root → rename to `branches/old-main/`
2. Fork's access point root → rename to `branches/main/`
3. Or: just update the registry to treat fork's access point as the new "main"

**Flow (option B — copy files):**
1. CopyObject all files from the promoted branch to `branches/main/` with POSIX metadata
2. Switch Lambda back to main's access point

**Changes:** Session-plane promote handler.

**Tests:** Fork, write on fork, promote, verify main has the fork's data.

### Step 9: E2e with todoapp

Run the existing todoapp e2e test script against the S3 Files backend.

**Changes:**
- Agent manifest: `object_storage = "s3files://bucket/prefix"` (or `s3://` with detection)
- Agent code: `sqlite3.connect("/mnt/s3files/workspace/state.db")` instead of HTTP client
- Test script: same flow (add items, list, fork, verify fork, promote)

**Tests:** The todoapp e2e script passes with S3 Files storage, including fork and promote.

## Dependencies

```
1 (types)
├── 2 (infra provisioning)
│   └── 3 (Lambda with mount)
│       └── 4 (stat-diff)
│           └── 5 (manifest capture)
│               └── 6 (fork)
│                   └── 7 (branch switch)
│                       └── 8 (promote)
│                           └── 9 (e2e)
```

Linear chain. Each step builds on the previous.

## What does NOT change

- `vlinder-sqlite-kv` — local/Podman dev path stays as-is
- DAG structure — existing message types, node format
- Session-plane CLI — `vlinder session fork`, `vlinder session promote` (same commands, different backend)
- Queue routing for inference/embedding — unaffected
- Agent code for non-storage operations — unaffected

## Risk log

| Risk | Impact | Mitigation |
|---|---|---|
| S3 Files sync aggregates writes (~65s) | Can't rely on sync for per-invocation VersionIds | boto3 upload for changed files (step 5) |
| CopyObject without POSIX metadata → readonly | Fork fails | Always set file-owner/group/permissions metadata (step 6) |
| Access point limit (10,000 per file system) | Can't create unlimited branches | Cleanup old branch access points; one file system per agent |
| Lambda config update on branch switch (~7s) | Latency on fork/switch | One-time cost per switch, not per invocation |
| S3 Files only on AWS | No Azure/GCP portability | Phase 2 (boto3 copy + VersionId) is the portable fallback |
| aws-sdk for s3files may not exist in Rust | Can't use typed SDK | Use raw AWS API calls via reqwest/hyper, or use the AWS CLI from Rust |
