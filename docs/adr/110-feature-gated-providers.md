# ADR 110: Feature-Gated Crate Dependencies

**Status:** Accepted

## Context

All provider crates, runtime crates, and gRPC server implementations were
unconditional dependencies. The CLI binary pulled in rusqlite, async-nats,
tiny_http, and all server code despite only needing thin gRPC clients.
vlinderd pulled in AWS SDK even for container-only deployments. The sidecar
included every provider regardless of what the agent needed.

## Decision

Feature-gate dependencies within existing crates. No crate splits — client
and server code stay co-located next to the proto definition.

### Client/server gates on service crates

Heavy dependencies move behind feature flags. The CLI opts in to client-only
features:

| Crate | Features | Heavy deps behind gate |
|-------|----------|----------------------|
| vlinder-harness | `client`, `server` | — |
| vlinder-sql-registry | `client`, `server` | rusqlite |
| vlinder-sql-state | `client`, `server` | rusqlite, tiny_http |
| vlinder-nats | `queue`, `secret-store`, `secret-client` | async-nats |
| vlinder-catalog | `client`, `server` | — |

### Provider and runtime gates

**vlinder-podman-runtime** — provider dependencies removed entirely.
HOSTNAME constants moved to vlinder-core.

**vlinder-provider-server** — four optional provider features:
`ollama`, `openrouter`, `sqlite-kv`, `sqlite-vec`. All on by default.

**vlinderd** — six optional features: four providers plus `container`
(vlinder-podman-runtime) and `lambda` (vlinder-lambda-runtime).
All on by default.

## Consequences

- Default features preserve current behavior — nothing breaks
- CLI binary drops rusqlite, tiny_http, async-nats, and all server code
- Slim daemon/sidecar/adapter binaries become possible
- CI can produce targeted binary variants per deployment mode (connects to #32)
