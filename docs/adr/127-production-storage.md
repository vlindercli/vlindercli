# ADR 127: Production Storage Beyond Local SQLite

**Status:** Draft

## Context

Vlinder currently uses local SQLite for all persistence: DagStore, object storage, and vector storage. This is fine for local mode but insufficient for production deployments.

Production storage needs to:
- Preserve time travel (fork, restore to a historical state)
- Use managed infrastructure where possible
- Provide a usable API surface to agents

ADRs 097–100 propose the content-addressed identity model and the storage trait contract for time travel. Those ADRs are still drafts. This ADR and the PoCs it kicks off are how we plan to validate and finalize that contract — by seeing what survives contact with real backends.

## Options considered

Candidates discussed:

- **Dolt** — MySQL-compatible database with git-style versioning
- **DoltgreSQL** — Postgres-compatible Dolt frontend
- **Postgres + PostgREST**
- **Hasura on Postgres**
- **Neon** — Postgres with branching
- **Aurora** — clone, PITR
- **Turso** — managed libSQL with database branching
- **DynamoDB**
- **MongoDB Atlas**
- **S3** — object storage with native versioning
- **S3 Vectors** — managed vector database built into S3

## Decision

Run a three-step PoC. Each step is evaluated in light of what was learned from the previous one.

1. **Turso** (ADR 128) — first. Closest to what we already have (libSQL is a SQLite fork).
2. **S3 + S3 Vectors** (ADR 129) — second. A different shape from a relational store.
3. **More powerful but more complex options** — third. Evaluated only after the first two are understood. Candidates in this class include Dolt, Aurora, Neon, and others. The specific shortlist for step 3 will be informed by what we learn in steps 1 and 2.

## Status

Brainstorming complete. Phase 1 not yet started.
