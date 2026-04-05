# =============================================================================
# Build Recipes
#
# The typical dev loop is:
#
#   just clean && just build-everything
#
# This is designed to be FAST. Here's how:
#
#   - `clean` cleans runtime state (DBs, conversations, NATS streams,
#     agent containers) but NOT build artifacts. Cargo's incremental
#     compilation is correct — it tracks every input via fingerprints
#     and recompiles exactly what changed. Nuking target/ just to feel
#     safe costs minutes for zero correctness gain.
#
#   - `build-sidecar` hashes the source files that feed the sidecar's
#     Dockerfile. If nothing changed, it skips the Podman build entirely
#     (sub-second no-op). The sidecar image is also preserved across
#     `clean` — it's platform infrastructure, not test state.
#
#   - `build` runs `cargo build` which compiles all workspace crates
#     (default-members = members in Cargo.toml). Incremental builds
#     after small changes take seconds.
#
# If you suspect toolchain corruption or dep weirdness, use `just
# reset` which IS the nuclear option: cargo clean + nuke all
# localhost/* images including the sidecar.
# =============================================================================

# Build what's needed for local testing (incremental — fast when little changed)
build-everything: build build-sidecar

# Build all workspace crates (incremental — only recompiles what changed)
build:
    cargo build

# Run all workspace tests
test:
    cargo test --workspace

# Run only podman-runtime crate tests (fast, no daemon deps)
test-podman-runtime:
    cargo test -p vlinder-podman-runtime

# =============================================================================
# Sidecar Image Build
#
# The sidecar is an OCI container image built via Podman. Unlike host
# binaries, Podman doesn't have Cargo's fingerprinting — it will happily
# re-run a multi-stage Rust build even if nothing changed.
#
# To avoid this, we compute a SHA-256 hash of every file that feeds into
# the Dockerfile (source code, Cargo.toml files, proto files, stubs, and
# the Dockerfile itself). The hash is stored in target/.sidecar-hash.
# If the hash matches AND the image exists, we skip the build.
#
# The 11 crates in the sidecar's dependency chain:
#   vlinder-podman-sidecar, vlinder-provider-server, vlinder-core,
#   vlinder-dolt, vlinder-nats, vlinder-sql-registry, vlinder-sql-state,
#   vlinder-ollama, vlinder-infer-openrouter, vlinder-sqlite-kv,
#   vlinder-sqlite-vec
#
# If you add a crate to the sidecar's dep chain, you MUST add its
# source paths to the find command below, or the hash won't detect
# changes in the new crate.
# =============================================================================

# Build vlinder-podman-sidecar OCI image, skipping if source unchanged.
build-sidecar:
    #!/usr/bin/env bash
    set -euo pipefail

    # Every file that can change the sidecar image. If you add a crate
    # to the sidecar's dep chain, add its paths here.
    hash=$(find \
        Cargo.toml \
        crates/vlinder-podman-sidecar/Dockerfile \
        crates/vlinder-podman-sidecar/Cargo.toml \
        crates/vlinder-podman-sidecar/src \
        crates/vlinder-podman-sidecar/stubs \
        crates/vlinder-provider-server/Cargo.toml \
        crates/vlinder-provider-server/src \
        crates/vlinder-core/Cargo.toml \
        crates/vlinder-core/src \
        crates/vlinder-dolt/Cargo.toml \
        crates/vlinder-dolt/src \
        crates/vlinder-nats/Cargo.toml \
        crates/vlinder-nats/src \
        crates/vlinder-nats/build.rs \
        crates/vlinder-nats/proto \
        crates/vlinder-sql-registry/Cargo.toml \
        crates/vlinder-sql-registry/src \
        crates/vlinder-sql-registry/build.rs \
        crates/vlinder-sql-registry/proto \
        crates/vlinder-sql-state/Cargo.toml \
        crates/vlinder-sql-state/src \
        crates/vlinder-sql-state/build.rs \
        crates/vlinder-sql-state/proto \
        crates/vlinder-ollama/Cargo.toml \
        crates/vlinder-ollama/src \
        crates/vlinder-infer-openrouter/Cargo.toml \
        crates/vlinder-infer-openrouter/src \
        crates/vlinder-sqlite-kv/Cargo.toml \
        crates/vlinder-sqlite-kv/src \
        crates/vlinder-sqlite-vec/Cargo.toml \
        crates/vlinder-sqlite-vec/src \
        -type f | sort | xargs shasum -a 256 | shasum -a 256 | cut -d' ' -f1)

    if podman image exists localhost/vlinder-podman-sidecar:latest 2>/dev/null \
        && [ -f target/.sidecar-hash ] \
        && [ "$(cat target/.sidecar-hash)" = "$hash" ]; then
        echo "build-sidecar: image up to date (hash match), skipping"
        exit 0
    fi

    echo "build-sidecar: source changed, rebuilding image..."
    podman build -t localhost/vlinder-podman-sidecar:latest \
        -f crates/vlinder-podman-sidecar/Dockerfile .

    mkdir -p target
    echo "$hash" > target/.sidecar-hash
    echo "build-sidecar: done"

# =============================================================================
# Clean / Reset Recipes
#
# There are two levels:
#
#   just clean    — Clean runtime state for testing. This is what you
#                   want 99% of the time. It kills processes, purges
#                   NATS streams, wipes DBs and conversations, and
#                   removes agent containers. It does NOT touch build
#                   artifacts or the sidecar image, because:
#
#                     1. Cargo's incremental build is correct. It
#                        fingerprints every input and recompiles what
#                        changed. You cannot get stale binaries.
#
#                     2. The sidecar image is platform infrastructure.
#                        Nuking it forces a full Rust recompile inside
#                        the Podman build (~5 min), and it has nothing
#                        to do with test state.
#
#                   Agent container images (localhost/echo-container,
#                   etc.) ARE removed because they are test artifacts
#                   that you may want rebuilt from scratch.
#
#   just reset    — Nuclear option. Everything in `clean` PLUS cargo
#                   clean and removal of ALL localhost/* images
#                   including the sidecar. Use this only when you
#                   suspect toolchain corruption, dependency cache
#                   issues, or want to verify a true clean build.
#                   Expect a full rebuild afterwards (~5-10 min).
# =============================================================================

# Clean runtime state for testing. Does not touch build artifacts or sidecar image.
clean:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "=== Vlinder Clean ==="

    # 1. Stop daemon/worker processes
    #    Running processes hold old binaries in memory — killing them is
    #    the one thing you actually need to pick up new code.
    if pgrep -f "vlinder.*daemon" >/dev/null 2>&1 || pgrep -f "VLINDER_WORKER_ROLE" >/dev/null 2>&1; then
        echo "  Stopping vlinder processes..."
        pkill -f "VLINDER_WORKER_ROLE" 2>/dev/null || true
        pkill -f "vlinder.*daemon" 2>/dev/null || true
        sleep 1
        echo "  ✓ Processes stopped"
    else
        echo "  - No vlinder processes running"
    fi

    # 2. Podman: remove agent containers and their images.
    #    Preserves the sidecar image (platform infrastructure) and
    #    pulled base images (rust, debian, python, etc.).
    echo "  Cleaning Podman..."
    podman pod rm -a -f 2>/dev/null || true
    podman rm -a -f 2>/dev/null || true
    podman images --format '{{"{{"}} .Repository {{"}}"}}:{{"{{"}} .Tag {{"}}"}}' \
        | grep '^localhost/' \
        | grep -v '^localhost/vlinder-podman-sidecar:' \
        | while read -r img; do podman rmi -f "$img" 2>/dev/null || true; done \
        || true
    podman volume prune -f 2>/dev/null || true
    echo "  ✓ Podman clean (sidecar + base images preserved)"

    # 3. NATS JetStream — always wipe the on-disk store so stale messages
    #    cannot be replayed when NATS restarts. Also purge live streams
    #    if the server happens to be reachable.
    echo "  Cleaning NATS JetStream data..."
    rm -rf /tmp/nats/jetstream
    if command -v nats >/dev/null 2>&1 && nc -z localhost 4222 2>/dev/null; then
        for stream in $(nats stream ls --names 2>/dev/null || true); do
            nats stream rm -f "$stream" 2>/dev/null || true
        done
        echo "  ✓ NATS streams purged + JetStream data wiped"
    else
        echo "  ✓ JetStream data wiped (NATS not running — no live streams to purge)"
    fi

    # 4. ~/.vlinder data plane (preserve config.toml and nats.conf)
    echo "  Cleaning ~/.vlinder data..."
    rm -f ~/.vlinder/*.db ~/.vlinder/*.db-shm ~/.vlinder/*.db-wal
    rm -rf ~/.vlinder/conversations
    rm -rf ~/.vlinder/state
    rm -rf ~/.vlinder/logs
    rm -rf ~/.vlinder/nats-data
    echo "  ✓ Data plane clean"

    echo ""
    echo "=== Clean complete ==="
    echo "Preserved: ~/.vlinder/config.toml, ~/.vlinder/client.toml, ~/.vlinder/nats.conf, ~/.vlinder/*.creds"
    echo ""
    echo "Next steps:"
    echo "  1. Rebuild:  just build-everything"
    echo "  2. Daemon:   cargo run -p vlinderd -- daemon"

# Nuclear reset: runtime state + build artifacts + all images. Full rebuild needed after.
reset: clean
    #!/usr/bin/env bash
    set -euo pipefail

    echo "=== Nuclear cleanup (build artifacts + sidecar) ==="

    # Remove sidecar image and its hash marker
    podman rmi -f localhost/vlinder-podman-sidecar:latest 2>/dev/null || true
    rm -f target/.sidecar-hash
    echo "  ✓ Sidecar image removed"

    # Nuke Cargo's target directory
    cargo clean 2>/dev/null || true
    echo "  ✓ Build artifacts cleaned"

    echo ""
    echo "=== Full reset complete. Next: just build-everything ==="
    echo ""

# Clean data plane (DBs, WAL files, state pointers, conversations)
clean-data:
    rm -f ~/.vlinder/*.db ~/.vlinder/*.db-shm ~/.vlinder/*.db-wal
    rm -rf ~/.vlinder/conversations
    rm -rf ~/.vlinder/state

# Check license compliance (fails on GPL/copyleft)
license-check:
    cargo deny check licenses

# =============================================================================
# Local S3 (LocalStack + s3fs-fuse)
#
# S3 mounts (ADR 107) use s3fs-fuse to present S3 bucket prefixes as
# read-only FUSE filesystems inside agent containers. On macOS this
# requires setup inside the Podman Machine VM (CoreOS on Apple HV):
#
# Chain: s3-install → s3-setup → s3-start → s3-seed
#
# s3-install:  rpm-ostree install s3fs-fuse into the CoreOS VM.
#              CoreOS is immutable — rpm-ostree layers packages on top
#              and requires a reboot (machine stop/start) to apply.
#              The COPR repo for podman-release can cause GPG key 404s;
#              if that happens: podman machine ssh "sudo mv
#              /etc/yum.repos.d/podman-release-copr.repo /tmp/"
#
# s3-setup:   Symlink mount.fuse.s3fs → s3fs in /usr/sbin.
#              The `mount` command searches /sbin and /usr/sbin for
#              helpers named mount.<type>. CoreOS /usr/sbin is normally
#              read-only, but `sudo ln -sf` works through the composefs
#              overlay. /usr/local/sbin does NOT work (not in mount's
#              search path).
#
# s3-start:   Run LocalStack in a Podman container. Idempotent: skips
#              if already running, removes stale stopped container first.
#              IMPORTANT: LocalStack stops when the Podman machine
#              restarts (stop/start or reset). If s3fs mounts hang after
#              a machine restart, check `podman ps -a` — LocalStack is
#              probably "Exited". Run `just s3-start` to bring it back.
#
# s3-seed:    Populate the test bucket with sample data. Creates a
#              zero-byte directory marker at each prefix path — s3fs 1.97
#              crashes without this (see note in s3-seed below).
# =============================================================================

# Install s3fs-fuse in the Podman VM (requires machine restart)
# CoreOS uses rpm-ostree which layers packages immutably; reboot needed to apply.
s3-install:
    #!/usr/bin/env bash
    set -euo pipefail
    if podman machine ssh "command -v s3fs" >/dev/null 2>&1; then
        echo "  ✓ s3fs-fuse already installed"
    else
        echo "  Installing s3fs-fuse (rpm-ostree)..."
        podman machine ssh "sudo rpm-ostree install s3fs-fuse"
        echo "  Restarting Podman machine (CoreOS needs reboot for rpm-ostree)..."
        podman machine stop
        podman machine start
        echo "  ✓ s3fs-fuse installed"
    fi

# Symlink the s3fs mount helper so `mount -t fuse.s3fs` finds it.
# The mount command looks for /usr/sbin/mount.<type> — on CoreOS the
# s3fs binary lands in /usr/bin but mount won't find it there.
# `sudo ln -sf` works through CoreOS's composefs overlay even though
# /usr/sbin appears read-only (cp fails, but symlink works).
s3-setup: s3-install
    #!/usr/bin/env bash
    set -euo pipefail
    if podman machine ssh "test -f /usr/sbin/mount.fuse.s3fs" 2>/dev/null; then
        echo "  ✓ mount.fuse.s3fs already installed"
    else
        podman machine ssh "sudo ln -sf /usr/bin/s3fs /usr/sbin/mount.fuse.s3fs"
        echo "  ✓ mount.fuse.s3fs symlinked in /usr/sbin"
    fi

# Start LocalStack for local S3 development (idempotent).
# Handles three states: running (skip), stopped/stale (rm + create), absent (create).
# Uses --filter + --quiet instead of --format '{{ "{{" }}.Names{{ "}}" }}'
# because {{ is justfile interpolation syntax.
s3-start: s3-setup
    #!/usr/bin/env bash
    set -euo pipefail
    if podman ps --filter name=^localstack$ --quiet 2>/dev/null | grep -q .; then
        echo "  ✓ LocalStack already running"
    else
        podman rm -f localstack 2>/dev/null || true
        podman run -d --name localstack -p 4566:4566 localstack/localstack:latest
        echo "  LocalStack S3 running at http://localhost:4566"
    fi

# Stop and remove LocalStack
s3-stop:
    podman rm -f localstack 2>/dev/null || true

# Create a test bucket and upload sample data.
#
# IMPORTANT: creates a zero-byte directory marker at the prefix path.
# s3fs 1.97 has a bug in remote_mountpath_exists: when mounting a sub-path
# (e.g., bucket:/v0.1.0/), it does HEAD on the prefix key. If that returns
# 404 (no directory marker), s3fs crashes with:
#   basic_string::back() Assertion '!empty()' failed
# This kills the FUSE daemon, leaving a stale mount point that reports
# "Transport endpoint is not connected" for all access attempts.
# AWS S3 Console creates these markers when you "create a folder" —
# LocalStack and other S3-compatible stores do not create them automatically.
#
# The `|| true` on `s3 ls` prevents broken pipe (SIGPIPE/exit 120) when
# `head` closes the pipe before `aws s3 ls` finishes writing.
s3-seed:
    #!/usr/bin/env bash
    set -euo pipefail
    export AWS_ACCESS_KEY_ID=test
    export AWS_SECRET_ACCESS_KEY=test
    export AWS_DEFAULT_REGION=us-east-1
    aws --endpoint-url http://localhost:4566 s3 mb s3://vlinder-support 2>/dev/null || true
    # Directory marker — without this, s3fs 1.97 crashes on sub-path mounts
    aws --endpoint-url http://localhost:4566 s3api put-object --bucket vlinder-support --key "v0.1.0/" --content-length 0 >/dev/null
    echo "The mount works." | aws --endpoint-url http://localhost:4566 s3 cp - s3://vlinder-support/v0.1.0/probe.txt
    aws --endpoint-url http://localhost:4566 s3 sync docs/ s3://vlinder-support/v0.1.0/docs/
    aws --endpoint-url http://localhost:4566 s3 sync crates/vlinder-core/src/ s3://vlinder-support/v0.1.0/src/
    echo "Seeded s3://vlinder-support/v0.1.0/"
    aws --endpoint-url http://localhost:4566 s3 ls s3://vlinder-support/v0.1.0/ --recursive | head -20 || true

# Start a Doltgres container for SQL time-travel storage.
# Uses user=postgres password=password (Doltgres defaults when user section is in config).
# The config binds to 0.0.0.0 so the host can reach it on port 5433.
doltgres:
    #!/usr/bin/env bash
    set -euo pipefail
    if podman ps --filter name=^vlinder-doltgres$ --quiet 2>/dev/null | grep -q .; then
        echo "  ✓ Doltgres already running"
    else
        podman rm -f vlinder-doltgres 2>/dev/null || true
        cfg_dir=$(mktemp -d)
        cat > "$cfg_dir/config.yaml" << 'YAML'
    log_level: info
    behavior:
      read_only: false
      dolt_transaction_commit: false
    user:
      name: postgres
      password: password
    listener:
      host: 0.0.0.0
      port: 5432
      read_timeout_millis: 28800000
      write_timeout_millis: 28800000
    data_dir: /var/lib/doltgres/
    cfg_dir: .doltcfg
    privilege_file: .doltcfg/privileges.db
    branch_control_file: .doltcfg/branch_control.db
    YAML
        podman run -d --name vlinder-doltgres -p 5433:5432 \
            -v "$cfg_dir:/etc/doltgres/servercfg.d:ro" \
            docker.io/dolthub/doltgresql:latest
        echo "  Doltgres running on localhost:5433 (user=postgres password=password)"
    fi

# Stop and remove Doltgres container
doltgres-stop:
    podman rm -f vlinder-doltgres 2>/dev/null || true

# =============================================================================
# Integration Tests (ADR 082)
# =============================================================================

# Run all integration tests with prerequisite checking and isolation
run-integration-tests:
    #!/usr/bin/env bash
    set -euo pipefail

    echo "=== Checking prerequisites ==="

    # 1. Check required binaries
    missing=()
    command -v podman >/dev/null 2>&1 || missing+=("podman — install from https://podman.io")
    command -v nats-server >/dev/null 2>&1 || missing+=("nats-server — brew install nats-server")
    command -v ollama >/dev/null 2>&1 || missing+=("ollama — install from https://ollama.com")

    if [ ${#missing[@]} -gt 0 ]; then
        echo "Missing prerequisites:"
        for m in "${missing[@]}"; do
            echo "  ✗ $m"
        done
        exit 1
    fi
    echo "  ✓ podman, nats-server, ollama"

    # 2. Verify Ollama endpoint responds
    if ! curl -sf http://localhost:11434/api/tags >/dev/null 2>&1; then
        echo "  ✗ Ollama not responding on localhost:11434"
        echo "    Run: ollama serve"
        exit 1
    fi
    echo "  ✓ Ollama endpoint"

    # 3. Check required models (if any listed in tests/required-models.txt)
    if [ -f tests/required-models.txt ]; then
        while IFS= read -r model || [ -n "$model" ]; do
            # Skip comments and empty lines
            [[ "$model" =~ ^#.*$ || -z "$model" ]] && continue
            if ! ollama list 2>/dev/null | grep -q "^${model}"; then
                echo "  ✗ Missing model: $model"
                echo "    Run: ollama pull $model"
                exit 1
            fi
            echo "  ✓ Model: $model"
        done < tests/required-models.txt
    fi

    # 4. Check OpenRouter API key (optional — tests skip gracefully if absent)
    if [ -n "${VLINDER_OPENROUTER_API_KEY:-}" ]; then
        echo "  ✓ VLINDER_OPENROUTER_API_KEY set — OpenRouter tests will run"
    else
        echo "  ⚠ VLINDER_OPENROUTER_API_KEY not set — OpenRouter tests will be skipped"
        echo "    To enable: export VLINDER_OPENROUTER_API_KEY=<your-key>"
    fi

    # 5. Check container images
    if ! podman image exists localhost/echo-container:latest 2>/dev/null; then
        echo "  ✗ Missing image: localhost/echo-container:latest"
        echo "    Run: just build-fleet-agent todoapp echo-container"
        exit 1
    fi
    echo "  ✓ Container image: echo-container"

    # 6. Handle NATS
    echo ""
    echo "=== Setting up NATS ==="

    # Stop any existing NATS we started
    if [ -f /tmp/vlinder-nats/nats.pid ]; then
        old_pid=$(cat /tmp/vlinder-nats/nats.pid 2>/dev/null || true)
        if [ -n "$old_pid" ] && kill -0 "$old_pid" 2>/dev/null; then
            echo "  Stopping previous NATS (pid $old_pid)"
            kill "$old_pid" 2>/dev/null || true
            sleep 1
        fi
    fi

    # Clean JetStream state
    rm -rf /tmp/vlinder-nats
    mkdir -p /tmp/vlinder-nats

    # Start NATS fresh
    nats-server -js -sd /tmp/vlinder-nats -p 4222 --pid /tmp/vlinder-nats/nats.pid &
    NATS_PID=$!
    echo "  Started NATS (pid $NATS_PID)"

    # Cleanup trap
    cleanup() {
        echo ""
        echo "=== Stopping NATS (pid $NATS_PID) ==="
        kill "$NATS_PID" 2>/dev/null || true
    }
    trap cleanup EXIT

    # Wait for NATS to be ready
    for i in $(seq 1 30); do
        if nc -z localhost 4222 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    if ! nc -z localhost 4222 2>/dev/null; then
        echo "  ✗ NATS failed to start on localhost:4222"
        exit 1
    fi
    echo "  ✓ NATS ready on localhost:4222"

    # 7. Create date-stamped run directory
    RUN_DIR="/tmp/vlinder-integration/$(date +%Y-%m-%d-%H%M%S)"
    mkdir -p "$RUN_DIR"
    echo ""
    echo "=== Run directory: $RUN_DIR ==="
    echo ""

    # 8. Run integration tests
    echo "=== Running integration tests ==="
    VLINDER_INTEGRATION_RUN="$RUN_DIR" cargo test --features test-support --test '*' -- --ignored --test-threads=1

    echo ""
    echo "=== Done. Artifacts in: $RUN_DIR ==="

# Generate a diagnostic prompt from a failed integration test directory
debug-integration-test path:
    #!/usr/bin/env bash
    set -euo pipefail

    TEST_DIR="{{path}}"
    VLINDER="$TEST_DIR/.vlinder"

    if [ ! -d "$VLINDER" ]; then
        echo "No .vlinder directory found at: $VLINDER"
        echo "Usage: just debug-integration-test /tmp/vlinder-integration/<run>/<test_name>"
        exit 1
    fi

    echo "=== Integration Test Diagnostic ==="
    echo "Test directory: $TEST_DIR"
    echo ""

    # Conversations git log
    if [ -d "$VLINDER/conversations/.git" ]; then
        echo "--- Conversation History ---"
        git -C "$VLINDER/conversations" log --oneline --all --graph 2>/dev/null || echo "(no commits)"
        echo ""
        echo "--- Last Commit Details ---"
        git -C "$VLINDER/conversations" log -1 --format=full 2>/dev/null || echo "(no commits)"
        echo ""
        echo "--- Branches ---"
        git -C "$VLINDER/conversations" branch -a 2>/dev/null || echo "(no branches)"
        echo ""
    else
        echo "--- No conversation repo ---"
        echo ""
    fi

    # Logs
    if [ -d "$VLINDER/logs" ]; then
        echo "--- Logs ---"
        for f in "$VLINDER/logs"/*; do
            [ -f "$f" ] || continue
            echo "[$f]"
            tail -50 "$f"
            echo ""
        done
    fi

    # Config
    if [ -f "$VLINDER/config.toml" ]; then
        echo "--- Config ---"
        cat "$VLINDER/config.toml"
        echo ""
    fi

    # Database files
    echo "--- Database Files ---"
    find "$VLINDER" -name "*.db" -exec ls -lh {} \; 2>/dev/null || echo "(none)"
    echo ""

    echo "--- Directory Tree ---"
    find "$TEST_DIR" -maxdepth 4 -type f | head -50
    echo ""
    echo "=== End Diagnostic ==="
