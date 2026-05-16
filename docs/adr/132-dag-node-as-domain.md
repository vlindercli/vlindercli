# ADR 132: DAG node as domain entity; consumers project from the chain

**Status:** Draft

## Context

vlinder's data model today carries parallel hierarchies:

- Queue-message types: `InvokeMessage`, `CompleteMessage`, `RequestMessage`, `ResponseMessage`, `RequestV2`, `ResponseV2`, `ForkMessage`, `PromoteMessage`, etc. Each carries `dag_id`, `dag_parent`, a payload, and a `diagnostics` block.
- LLM-message types: `Message::User`, `Message::Agent`, `Message::Tool`, `Message::System`. Bare payload, no ids.
- DAG-level: `DagNode`, with a discriminator over the queue-message variants above. Persisted by `DagStore`.

Three problems motivate this ADR.

### Problem 1: aggregation creates O(N²) storage and a fragile contract

`InvokeMessage.history: Vec<Message>` carries the cumulative conversation. Each new turn writes a fresh Invoke that embeds *all* prior messages plus the current input. Storage grows quadratically in turn count. The aggregation is also load-bearing for the runtime protocol — the executor relies on receiving the full history embedded in the Invoke it gets over NATS.

### Problem 2: parallel hierarchies for the same data

The same conversation turn is represented at three levels:

- A `DagNode` of type `Invoke` (persistence layer).
- An `InvokeMessage` payload (queue/wire layer).
- Several `Message::*` entries inside that payload (LLM layer).

Adding a new consumer (resume rendering, agentic memory, external API) requires deciding which of these three to read from, or building yet another type that projects from one of them.

### Problem 3: multi-writer parent_id integrity

Multiple components insert DAG nodes — `CoreHarness`, `RecordingQueue`, the git worker, MCP workers. Each computes `parent_id` from its own observation of chain state. Past sessions (`project_dag_as_source_of_truth.md`, parent-selection-across-writers, chain_head wiring) have debugged invariant violations in this code path. The `DagStore` trait accepts whatever `parent_id` the caller passes; it does not verify.

### Pretext: TUI resume rendering

A specific in-flight use case — the TUI must hydrate prior turns when resuming a session — surfaced these problems. The `Harness` trait exposes no method to fetch conversation history. The data exists in the DAG. The question "what does the fetch method return" forced the question "what is the actual domain type?"

## Decision

The DAG node is the only domain entity in vlinder. The chain — a node and its lineage walked through `parent_id` — is the authoritative, complete record of *what happened*. Every other type in the system that represents a message, event, or transition is either:

- A **projection** of a single DAG node onto a consumer's view (pure function, single-node), or
- A **query** that walks a chain rooted at a DAG node and assembles a **read model** for the consumer (walk + combine, multi-node).

Projections and queries are owned by consumers (or by shared modules consumers can use), not by the domain. "Aggregate root" in this ADR refers to the DDD concept — the DAG node as the entry point through which a query reaches the cluster of state it needs. It is not synonymous with "aggregation" as an operation.

### Load-bearing invariants

1. **Parent existence.** `parent_id` shall point to a node that exists in the store. The `DagStore` enforces this with an existence check on insert; nodes with non-existent parents are rejected. Logical correctness of parent *selection* (head advancement under concurrency) is the writer's responsibility, not the substrate's.

2. **Chain meaningfulness.** Every node is a valid stopping point. The chain is meaningful at every node, including mid-flight nodes (e.g., a `Request` whose `Response` has not arrived). Any practical inability to act on a mid-flight node (resume execution, restore container) is a tooling limit, not a domain compromise.

3. **No authoritative copies.** Consumers do not hold authoritative copies. A consumer may cache projections or query results, but the chain is the tie-breaker when views disagree.

### Aggregate root

A `DagNode` is the aggregate root for the slice of state reachable through it. Queries that need "the world at point N" use the node as their entry point and walk backward (and, in branched views, forward) via `parent_id` to assemble the read model they need.

### Storage shape

- DAG nodes carry their variant payload inline. Today's queue-message types (`InvokeMessage`, etc.) are the variant payload. They are not separate domain types; they are the in-node representation.
- Heavy artifacts that don't fit inline (container snapshots, large object payloads) live in content-addressed external stores, referenced from the DAG node via a `stateref`. The DAG is the authoritative log; supporting stores are reachable from it.
- Aggregated forms (e.g., the cumulative `Vec<Message>` carried by the latest Invoke) are permitted as *compaction* — a materialized cumulative view carried by the most recent state-bearing node. They are derived, not authoritative; the chain remains the source of truth.

### Memory is not a system

There is no separate memory store, no memory algorithm in the substrate. Memory is a *query* over the chain — a caller (LLM, agent, UI) walks the chain (or a subset) and assembles whatever read model makes sense. The bitter lesson applies: the substrate captures, the retrieval shapes follow consumer demand.

This does not forbid grouping memory-shaped query helpers under a memory-themed trait or module. It forbids a parallel substrate.

### Harness traversal surface

Consumers access the chain through two `Harness` methods:

```rust
async fn latest_nodes(
    &self,
    session_id: &SessionId,
    branch_id: BranchId,
    n: u32,
) -> Result<Vec<DagNode>, String>;

async fn get_node(
    &self,
    node_id: &DagNodeId,
) -> Result<Option<DagNode>, String>;
```

`latest_nodes` is the entry-point batch read — one round trip for "the tail of length n." `get_node` is pointer-chase: each `DagNode` carries its `parent_id`, so consumers chain calls to walk backwards. This is sufficient for the agent container (fetch latest, walk as needed) and the TUI (initial transcript + scroll-up = one more). Additional retrieval methods are added when concrete consumer demand surfaces.

### Query efficiency is a DagStore property

The harness trait surface commits to a contract, not a performance profile. The efficiency of any specific query for producing any specific read model is a property of the `DagStore` implementation. SQL is sufficient for current workloads. If a future workload makes a particular query class load-bearing (fast traversal of long chains, similarity search, windowed scans), the response is open — augmenting or replacing the `DagStore` backing (graph DB, columnar store, hybrid) is one path; evolving the harness trait or adding new methods is another. Neither is foreclosed by this ADR. The only commitments that are: the DAG node is the domain entity, and the chain is authoritative. Everything above that — trait shape, backing storage — remains open to revision under new information.

## Consequences

### Immediate

- `Harness` trait gains `latest_nodes` and `get_node`. Every existing `Harness` impl decides what to return (loud `Err` for fakes without data; real reads for `CoreHarness` delegating to `DagStore`).
- The TUI resume rendering use case (in flight in a separate handoff) lands cleanly on top of `latest_nodes` + `get_node`. The TUI projects DagNode → renderable line in its own code.
- The agent container, when re-architected to fetch its prompt from the harness instead of receiving it embedded in NATS messages, uses the same two methods.

### Preserved as compaction

- `InvokeMessage.history` continues to exist for now as the materialized cumulative array. It is reframed: not an authoritative aggregation, but a compaction on the latest state-bearing node. Reading the latest Invoke gives the prompt in O(1). The chain is still the source of truth; the aggregation is just a cache the writer maintains.

### Deferred

- Specific fields of the unified DagNode payload — defer until projection demands surface them.
- Migration of the parallel hierarchies (`InvokeMessage` and the rest) into pure projections — defer; strangler-fig is the default approach when it happens.
- Compaction nodes beyond the existing latest-Invoke-carries-cumulative-array pattern (e.g., summary nodes that telescope long chains) — defer until O(N) walks become a real bottleneck for a real consumer.
- Stronger enforcement of parent_id correctness (CAS against chain head, fence tokens) — defer; the existence-of-parent contract is what is committed here.

### Not committed

- This ADR does not commit to where projection and query functions physically live (methods on `DagNode`, sibling module, per-consumer crate). Decide per case.
- It does not commit to a memory retrieval surface beyond stating that memory access is a query pattern over the chain. Specific memory methods land when demand appears.
- It does not commit to a snapshot store implementation. Container staterefs are content-addressed references; the store backing them is an implementation detail (likely the existing object/blob storage abstraction).

## Alternatives considered

- **Keep parallel hierarchies, add a transcript-fetch method.** Rejected: this is the path that motivated the ADR. It adds a fourth type for the same data and does not address the underlying problem.
- **Unify `Message` and queue-message types into one parameterized type with a discriminator.** Equivalent to what `DagNode` already is. The ADR's contribution is recognizing that `DagNode` already is the unified type, not creating a new one.
- **Push projections onto the trait (return `Vec<Message>` from `Harness`).** Rejected because it locks consumers into the daemon's view of what they need. Returning `DagNode` keeps consumer-side projection flexibility.
- **CAS-based parent_id enforcement (writer carries expected_head, store rejects on mismatch).** Considered, deferred. The existence-of-parent contract is the minimum that prevents orphan chains; CAS layers on top of it later if writer-bug-class incidents continue.

## Open questions

- The fields of the unified DagNode payload — what is in the variant payload, what is referenced externally. Probably resolved on a per-variant basis as consumers surface demand.
- Whether `get_node` becomes a batched variant (`get_nodes(ids) -> Vec<DagNode>`) when consumer round-trip cost becomes visible. Defer until measured.
- How branched views (multiple children of one parent) interact with consumer traversal. Today's two methods handle the linear case; forward traversal (`children_of`) is not in this ADR's surface and is added when needed.
