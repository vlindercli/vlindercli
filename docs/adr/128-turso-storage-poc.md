# ADR 128: SQLite-Based Production Storage

**Status:** Deferred

Originally scoped as the Turso PoC (Phase 1 of ADR 127). Investigation broadened into the question of whether Vlinder should run a SQLite-based production storage backend at all. Conclusion: deferred. Findings recorded for future evaluation.

## Context

Vlinder currently uses local SQLite for all persistence (DagStore, object storage, vector storage). Phase 1 of ADR 127's PoC plan investigated whether a SQLite-based production backend was the right answer.

The storage trait contract this would have populated is the one proposed in ADRs 097–100, which are still drafts.

## What was investigated

1. Turso Cloud as the managed libSQL service
2. Litestream as a WAL shipping tool
3. Verneuil as a Rust-native VFS-level page snapshot tool
4. Self-hosting libsql-server

## Findings

### Turso Cloud

- The PITR API (`seed.timestamp`) is timestamp-based. There is no way to address the state right after a specific transaction.
- The PITR docs note a possible 15-second gap in the data immediately preceding the timestamp, complicating timestamp-based addressing further.
- Most active development is happening in the Rust rewrite (`tursodb`). Turso Cloud runs the older `libsql-server`.
- **Empirically**, in our PoC against cloud Turso, the `replication_index` field in Hrana execute responses (`POST /v3/pipeline`) comes back as `null` on every operation we tested (CREATE TABLE, INSERT, SELECT). The field exists in the response schema; cloud just doesn't populate it.

#### What the addressability story actually looks like

Turso's own production architecture blog ([How Turso Cloud Keeps Your Data Durable and Safe](https://turso.tech/blog/how-does-the-turso-cloud-keep-your-data-durable-and-safe)) describes the storage layer in their own terms:

- The database file is segmented into 128 KB chunks
- "The collection of all the segments that comprise a database file is called a _generation_"
- Writes go to S3 Express synchronously: "Only after the data is safely stored in S3 Express, is the transaction acknowledged to the user"
- PITR works by walking generations + WAL fragments: "find the latest generation before the specific timestamp we want to restore to, the WAL fragments written after that generation up to the specific timestamp, and the database can then be restored to that point"

So at the storage layer, cloud Turso's restoration mechanism already operates on `(generation, WAL fragment)` granularity. The timestamp parameter on the public PITR API is translated internally into "find the right generation and walk WAL fragments forward." **The fine-grained addressability exists at the storage layer.** The public API just hides it behind a timestamp wrapper.

Independently, the `libsql-server` open-source code (`libsql-server/src/hrana/result_builder.rs:249`) populates `replication_index: self.last_frame_no` from the current WAL frame number via `get_current_frame_no` (`connection_core.rs:259`). So the protocol field IS designed to carry per-write identifiers — the open-source server populates it; cloud Turso does not, for reasons we don't know.

#### Discord conversation outcome

We asked Turso about this on their [Discord](https://discord.com/channels/933071162680958986/1491143335388119130). The initial response was: "Transactions don't have an identifier as such." Suggested workarounds:

- An application-level transaction counter (orders writes logically, but does not enable per-transaction restore — the counter is data, not a restoration target)
- Binary-search PITR (converges within the 15-second window but cannot get finer than that)

Neither workaround addresses the actual gap. We followed up referencing the blog post, the bottomless source, and the empirical `replication_index: null` finding, asking specifically whether the precise per-write identifier could be exposed through the public API — the response capture point would naturally be the `POST /v3/pipeline` response (where `replication_index` already exists in the schema) and the restore consumption point would be `seed.replication_index` on the database create endpoint, alongside the existing `seed.timestamp`.

**Conclusion as of this ADR's writing:** the addressability exists at the storage layer; the public API does not surface it. Whether Turso will expose it in the future is open. For our current needs, the gap is real and the deferral stands.

### Litestream

- The library API is loop-shaped — `store.Open(ctx)` starts background replication goroutines, with no exposed primitive for "ship pending state now and return."
- Restore is timestamp-based. No transaction-boundary primitive.

### Verneuil

- Production-tested at Backtrace, but the crate is effectively dormant: last release was version 0.6.4 on February 23, 2022. Single-vendor, pre-1.0. Too risky to bet on as a library dependency.

### Self-hosting libsql-server

- Not an option at the project's current stage. Operating a database server (durability, monitoring, backup, restart handling) is too much ongoing work for the value.

## Why deferred

No clean off-the-shelf option fits, and self-hosting or hand-rolling replication code is not justified at the project's current stage. 
