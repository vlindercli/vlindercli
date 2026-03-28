# ADR 122: SQL Representation of the Vlinder Protocol

**Status:** In Progress

## Context

The vlinder protocol captures every agent interaction as a content-addressed node in a Merkle DAG. The protocol defines typed messages — Invoke, Request, Response, Complete, Fork, Promote — each with distinct fields. They share common structure: session, branch, submission, and a hash that chains them.

The SQL schema should reflect the protocol's type structure:

- Each message type's fields are explicit and enforced
- New message types extend the schema without modifying existing tables
- Queries can filter and join on type-specific fields
- The database enforces referential integrity (parent chain)

## Decision

### Typed tables per message type

Each message type gets its own table with domain-specific fields. The `dag_nodes` table is the chain index (hash, parent_hash, branch, session, created_at). Typed tables FK to `dag_nodes` for the Merkle chain.

Tables: `invoke_nodes`, `complete_nodes`, `request_nodes`, `response_nodes`, `fork_nodes`, `promote_nodes`.

### Diesel ORM for type safety

All surviving queries use Diesel instead of raw rusqlite:

- **Schema** (`schema.rs`) — `table!` macros declare column names and types. Compile-time validation.
- **Models** (`models.rs`) — `Queryable`/`Insertable` structs map rows to Rust types. No positional params.
- **FK relationships** — `joinable!` macros let Diesel verify joins at compile time.

### Dual connection (strangler fig)

`SqliteDagStore` holds both a rusqlite `Connection` (for legacy `dag_nodes` reads that are dying) and a Diesel `SqliteConnection` (for everything else). When the session-plane migration eliminates the legacy reads, rusqlite is removed entirely.

## Progress

### Complete

- [x] Diesel schema and models for all tables
- [x] Data-plane reads: `get_invoke_node`, `get_complete_node`, `get_request_node`, `get_response_node`
- [x] Data-plane writes: `insert_invoke_node`, `insert_complete_node`, `insert_request_node`, `insert_response_node`
- [x] Branch CRUD: `create_branch`, `get_branch`, `get_branch_by_name`, `get_branches_for_session`, `rename_branch`, `seal_branch`
- [x] Session CRUD: `create_session`, `get_session`, `get_session_by_name`, `update_session_default_branch`

### Remaining (dies with session-plane migration)

- `dag_nodes` reads: `get_node`, `get_node_by_prefix`, `get_session_nodes`, `get_children`, `get_nodes_by_submission`, `latest_node_on_branch`
- `dag_nodes` legacy write: `insert_node` (used by fork/promote)
- `insert_typed_node` (session-plane dual-write)
- `row_to_dag_node` (rusqlite row mapper)
- `list_sessions` (complex aggregate query on `dag_nodes`)

These use `DagNode.message: Option<SessionPlane>` which requires the `message_blob` column. Once fork/promote have typed recording methods, `message_blob` and all the above become dead code.

### Future: referential integrity

`dag_nodes.parent_hash` should FK to `dag_nodes.hash`. The DAG worker writes in NATS stream order; the DB verifies the chain is valid. Two independent systems agreeing on correctness. This is straightforward once rusqlite is removed and Diesel owns all schema creation.

## Consequences

- Schema mirrors the protocol's type structure
- Compile-time query validation via Diesel — no positional params, no string SQL for surviving queries
- New message types = new typed table + Diesel model, no changes to existing tables
- Referential integrity on the Merkle chain (once rusqlite is removed)
- rusqlite removal is mechanical — migrate session-plane, delete `conn` field
