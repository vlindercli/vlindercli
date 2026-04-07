# ADR 128: Turso Storage PoC

**Status:** Draft

## Context

Phase 1 of the three-step PoC defined in ADR 127. The storage trait contract this PoC will exercise is the one proposed in ADRs 097–100 (still drafts).

## What Turso is

- Managed libSQL (a fork of SQLite)
- HTTP API for queries
- Database branching via platform API
- Point-in-time restore
- Embedded replicas (local SQLite synced to cloud)
- Turso explicitly markets branching for AI agent workspaces; their reference customer (Adaptive Computer One) runs hundreds to thousands of ephemeral branches per agent task
- Branches are independent after fork; no automatic merge

## Goal

Evaluate Turso as production storage for Vlinder.

## Status

Not started.
