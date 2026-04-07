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

### Litestream

- The library API is loop-shaped — `store.Open(ctx)` starts background replication goroutines, with no exposed primitive for "ship pending state now and return."
- Restore is timestamp-based. No transaction-boundary primitive.

### Verneuil

- Production-tested at Backtrace, but the crate is effectively dormant: last release was version 0.6.4 on February 23, 2022. Single-vendor, pre-1.0. Too risky to bet on as a library dependency.

### Self-hosting libsql-server

- Not an option at the project's current stage. Operating a database server (durability, monitoring, backup, restart handling) is too much ongoing work for the value.

## Why deferred

No clean off-the-shelf option fits, and self-hosting or hand-rolling replication code is not justified at the project's current stage. 
