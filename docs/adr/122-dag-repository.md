# ADR 122: SQL Representation of the Vlinder Protocol

**Status:** Accepted

## Context

The vlinder protocol captures every agent interaction as a content-addressed node in a Merkle DAG. The protocol defines typed messages across three operational planes (ADR 121). The SQL schema should reflect the protocol's type structure: explicit fields per message type, extensible without modifying existing tables, and referential integrity on the chain.

## Decision

### Typed tables per message type

Each message type gets its own table with domain-specific fields. The `dag_nodes` table is the chain index. Typed tables FK to `dag_nodes` for the Merkle chain.

| Plane | Tables |
|-------|--------|
| Data | `invoke_nodes`, `complete_nodes`, `request_nodes`, `response_nodes` |
| Session | `fork_nodes`, `promote_nodes` |
| Infra | `deploy_agent_nodes`, `delete_agent_nodes` |

### Chain index is plane-agnostic

`dag_nodes` stores: `hash` (PK), `parent_hash` (nullable, self-referential FK), `message_type`, `created_at`, `protocol_version`, `snapshot`. Session-scoped fields (`session_id`, `submission_id`, `branch_id`) are nullable — infra plane nodes are cluster-scoped and have no session.

### Referential integrity

- `parent_hash REFERENCES dag_nodes(hash)` — the database enforces the Merkle chain. Root nodes use NULL.
- `session_id REFERENCES sessions(id)` — data/session plane nodes reference valid sessions.
- `branch_id REFERENCES branches(id)` — data/session plane nodes reference valid branches.
- All typed tables FK to `dag_nodes(hash)` via `dag_hash`.
- `agent_states.agent_name REFERENCES agents(name)` — state log references valid agents.

### Diesel ORM

All queries use Diesel:

- **Schema** (`schema.rs`) — `table!` macros declare column names and types. Compile-time validation.
- **Models** (`models.rs`) — `Queryable`/`Insertable` structs map rows to Rust types.
- **FK relationships** — `joinable!` macros verify joins at compile time.

### Read models in the same database

Registry data (agents, models) and session data (sessions, branches) live alongside the DAG in one SQLite database. Agent state (`agent_states`) is an append-only log tracking deployment lifecycle transitions.

## Consequences

- Schema mirrors the protocol's type structure across all three planes
- Compile-time query validation — no positional params, no string SQL
- New message types = new typed table + Diesel model, no changes to existing tables
- Merkle chain integrity enforced by the database
- Single database with FK integrity across planes
