# Multi-stage build for vlinder-podman-sidecar.
# Build context: workspace root (sidecar depends on sibling crates).
#
# Only the 10 crates in the sidecar's dependency chain get real Cargo.toml
# files. The remaining 8 workspace members get minimal stubs (no deps) so
# Cargo can resolve the workspace without triggering feature unification
# on shared dependencies like tonic.
#
# Usage:
#   podman build -t localhost/vlinder-podman-sidecar:latest -f crates/vlinder-podman-sidecar/Dockerfile .

# ── Build stage ──────────────────────────────────────────────────────
FROM rust:1.90-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy workspace manifest
COPY Cargo.toml ./

# Copy real Cargo.toml for the 10 crates in the sidecar's dep chain
COPY crates/vlinder-podman-sidecar/Cargo.toml crates/vlinder-podman-sidecar/
COPY crates/vlinder-provider-server/Cargo.toml crates/vlinder-provider-server/
COPY crates/vlinder-core/Cargo.toml crates/vlinder-core/
COPY crates/vlinder-nats/Cargo.toml crates/vlinder-nats/
COPY crates/vlinder-sql-registry/Cargo.toml crates/vlinder-sql-registry/
COPY crates/vlinder-sql-state/Cargo.toml crates/vlinder-sql-state/
COPY crates/vlinder-ollama/Cargo.toml crates/vlinder-ollama/
COPY crates/vlinder-infer-openrouter/Cargo.toml crates/vlinder-infer-openrouter/
COPY crates/vlinder-sqlite-kv/Cargo.toml crates/vlinder-sqlite-kv/
COPY crates/vlinder-sqlite-vec/Cargo.toml crates/vlinder-sqlite-vec/

# Stub Cargo.toml for workspace members NOT in the dep chain.
# No [dependencies] avoids Cargo feature unification on tonic etc.
COPY crates/vlinder-podman-sidecar/stubs/vlinderd.toml crates/vlinderd/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder.toml crates/vlinder/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-lambda-adapter.toml crates/vlinder-lambda-adapter/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-harness.toml crates/vlinder-harness/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-catalog.toml crates/vlinder-catalog/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-podman-runtime.toml crates/vlinder-podman-runtime/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-lambda-runtime.toml crates/vlinder-lambda-runtime/Cargo.toml
COPY crates/vlinder-podman-sidecar/stubs/vlinder-git-dag.toml crates/vlinder-git-dag/Cargo.toml

# Stub out src directories so Cargo can resolve the workspace
RUN mkdir -p crates/vlinder-core/src && echo "" > crates/vlinder-core/src/lib.rs && \
    mkdir -p crates/vlinder-ollama/src && echo "" > crates/vlinder-ollama/src/lib.rs && \
    mkdir -p crates/vlinder-infer-openrouter/src && echo "" > crates/vlinder-infer-openrouter/src/lib.rs && \
    mkdir -p crates/vlinder-sqlite-vec/src && echo "" > crates/vlinder-sqlite-vec/src/lib.rs && \
    mkdir -p crates/vlinder-sqlite-kv/src && echo "" > crates/vlinder-sqlite-kv/src/lib.rs && \
    mkdir -p crates/vlinder-nats/src && echo "" > crates/vlinder-nats/src/lib.rs && \
    mkdir -p crates/vlinder-sql-state/src && echo "" > crates/vlinder-sql-state/src/lib.rs && \
    mkdir -p crates/vlinder-sql-registry/src && echo "" > crates/vlinder-sql-registry/src/lib.rs && \
    mkdir -p crates/vlinder-provider-server/src && echo "" > crates/vlinder-provider-server/src/lib.rs && \
    mkdir -p crates/vlinder-podman-sidecar/src && echo "fn main() {}" > crates/vlinder-podman-sidecar/src/main.rs && \
    mkdir -p crates/vlinderd/src && echo "fn main() {}" > crates/vlinderd/src/main.rs && \
    mkdir -p crates/vlinder/src && echo "fn main() {}" > crates/vlinder/src/main.rs && \
    mkdir -p crates/vlinder-lambda-adapter/src && echo "fn main() {}" > crates/vlinder-lambda-adapter/src/main.rs && \
    mkdir -p crates/vlinder-harness/src && echo "" > crates/vlinder-harness/src/lib.rs && \
    mkdir -p crates/vlinder-catalog/src && echo "" > crates/vlinder-catalog/src/lib.rs && \
    mkdir -p crates/vlinder-podman-runtime/src && echo "" > crates/vlinder-podman-runtime/src/lib.rs && \
    mkdir -p crates/vlinder-lambda-runtime/src && echo "" > crates/vlinder-lambda-runtime/src/lib.rs && \
    mkdir -p crates/vlinder-git-dag/src && echo "" > crates/vlinder-git-dag/src/lib.rs

# Proto build dependencies (only for dep-chain crates with gRPC)
COPY crates/vlinder-nats/build.rs crates/vlinder-nats/
COPY crates/vlinder-nats/proto crates/vlinder-nats/proto
COPY crates/vlinder-sql-state/build.rs crates/vlinder-sql-state/
COPY crates/vlinder-sql-state/proto crates/vlinder-sql-state/proto
COPY crates/vlinder-sql-registry/build.rs crates/vlinder-sql-registry/
COPY crates/vlinder-sql-registry/proto crates/vlinder-sql-registry/proto

# Pre-build dependencies (this layer is cached unless Cargo.toml changes)
RUN cargo build --release -p vlinder-podman-sidecar 2>/dev/null || true

# Copy actual source code (only the 10 dep-chain crates)
COPY crates/vlinder-core/src crates/vlinder-core/src
COPY crates/vlinder-ollama/src crates/vlinder-ollama/src
COPY crates/vlinder-infer-openrouter/src crates/vlinder-infer-openrouter/src
COPY crates/vlinder-sqlite-vec/src crates/vlinder-sqlite-vec/src
COPY crates/vlinder-sqlite-kv/src crates/vlinder-sqlite-kv/src
COPY crates/vlinder-nats/src crates/vlinder-nats/src
COPY crates/vlinder-sql-state/src crates/vlinder-sql-state/src
COPY crates/vlinder-sql-registry/src crates/vlinder-sql-registry/src
COPY crates/vlinder-provider-server/src crates/vlinder-provider-server/src
COPY crates/vlinder-podman-sidecar/src crates/vlinder-podman-sidecar/src

# Touch source files to invalidate the cache for actual compilation
RUN touch crates/vlinder-core/src/lib.rs \
          crates/vlinder-ollama/src/lib.rs \
          crates/vlinder-infer-openrouter/src/lib.rs \
          crates/vlinder-sqlite-vec/src/lib.rs \
          crates/vlinder-sqlite-kv/src/lib.rs \
          crates/vlinder-nats/src/lib.rs \
          crates/vlinder-sql-state/src/lib.rs \
          crates/vlinder-sql-registry/src/lib.rs \
          crates/vlinder-provider-server/src/lib.rs \
          crates/vlinder-podman-sidecar/src/main.rs

# Build the sidecar binary
RUN cargo build --release -p vlinder-podman-sidecar

# ── Runtime stage ────────────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/vlinder-podman-sidecar /usr/local/bin/vlinder-podman-sidecar

ENTRYPOINT ["vlinder-podman-sidecar"]
