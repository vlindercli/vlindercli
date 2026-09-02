# ADR 059: Installation, CI, and Release

**Status:** Accepted

## Context

Running `vlinder support` fails immediately:

```
Failed to deploy fleet agent 'support': registration failed: model not registered: default
```

The support fleet's agents declare a model requirement (`default = "ollama://localhost:11434/phi3:latest"`). The registry validates model requirements at deploy time (ADR 050). No model is registered because the user never ran `vlinder model add`. The support agent — the one thing that should always work — is the first thing that breaks.

This is a bootstrapping problem. The platform has a chicken-and-egg dependency: agents need registered models, models need explicit registration, and the command that helps users figure out what went wrong (`vlinder support`) is itself an agent that needs a registered model.

The current installation story is "build from source and figure it out." There is no installer, no bootstrap step, no first-run experience. Every prerequisite is manual: install NATS, install Podman, install Ollama, pull a model, register the model, write config, then finally run agents. Missing any step produces a cryptic error.

### Distributed mode is the deployment target

Vlinder runs in distributed mode for all real usage. The daemon spawns worker processes that communicate via NATS queues (ADR 043). In-memory mode exists only for development and testing — it uses substring matching instead of token-based routing, has no consumer model, and hides bugs that surface in production.

This means:
- NATS is a hard prerequisite, not optional infrastructure
- `config.toml` must explicitly configure `queue.backend = "nats"` and `state.backend = "grpc"`
- `vlinderd` is the standard way to run the platform
- The in-memory queue should never appear in a user-facing installation path

### Inference is Ollama-only

Local inference goes through Ollama (ADR 060). There is no in-process llama.cpp backend. This simplifies the build — no native C++ toolchain required — but makes Ollama a prerequisite for any agent that uses LLMs.

## Decision

### 1. CI pipeline

CI runs on every push and pull request. It validates that the code compiles, passes tests, and meets quality standards — without requiring external services.

#### Test tiers

| Tier | Runs in CI | Requires | Invocation |
|------|-----------|----------|------------|
| Unit + integration (no services) | Yes | Nothing | `cargo test` |
| Ollama integration | No | Running Ollama | `cargo test -- --ignored` |
| NATS integration | No | Running NATS | `cargo test -- --ignored` |
| Container integration | No | Podman + built images | `cargo test -- --ignored` |

Tests that need external services are `#[ignore]`-annotated. `cargo test` skips them by default, so CI runs the full test suite without special configuration.

#### CI jobs

| Job | Purpose |
|-----|---------|
| **Lint** | `cargo fmt --check` + `cargo clippy -- -D warnings` |
| **Test** | `cargo test` (unit + integration, no external services) |
| **License** | `cargo deny check licenses` (no GPL/copyleft) |

Build dependencies: `protobuf-compiler` only. No cmake, no C++ toolchain (ADR 060 removed llama-cpp-2).

#### What CI does NOT do

- Run ignored tests (those need Ollama, NATS, or Podman)
- Build release artifacts (that's the release workflow)
- Deploy anything

### 2. Release pipeline

Triggered by pushing a version tag (`v*`). Builds release binaries for all supported targets and publishes them as a GitHub release.

#### Targets

| Target | Runner | Notes |
|--------|--------|-------|
| `x86_64-unknown-linux-gnu` | `ubuntu-latest` | Standard Linux servers |
| `x86_64-apple-darwin` | `macos-13` | Intel Macs |
| `aarch64-apple-darwin` | `macos-latest` | Apple Silicon |

#### Artifact format

Each target produces a tarball: `vlinder-{target}.tar.gz` containing the `vlinder` CLI and `vlinderd` daemon binaries. These are attached to the GitHub release. The install script downloads the appropriate tarball based on `uname -s` and `uname -m`.

### 3. Install script

A single `install.sh` that works on both macOS and Linux. The install script is the primary distribution channel.

```bash
curl -fsSL https://vlindercli.dev/install.sh | sh
```

#### What it does

1. **Detect platform** — Darwin/Linux, x86_64/aarch64
2. **Download vlinder binaries** — the CLI and daemon from the latest GitHub release
3. **Create directory structure** — `~/.vlinder/{agents,conversations,logs,registry}`
4. **Write config** — `config.toml` with NATS queues, gRPC state, and the default distributed workers
5. **Check prerequisites** — NATS, Podman, Ollama. If required deps are missing, print install commands and exit
6. **Write NATS config** — `~/.vlinder/nats.conf` with JetStream enabled. Preserves existing config
7. **Start NATS service** — launchd/systemd. Detects existing NATS services and skips with a JetStream reminder
8. **Start `vlinderd` service** — launchd/systemd
9. **Pull default model** — `ollama pull phi3` with visible progress (skipped if Ollama not present)
10. **Register default model** — `vlinder model add phi3`
11. **Pull support fleet images** — `ghcr.io/vlindercli/vlinder-{support,code-analyst,log-analyst}:latest`
12. **Write support fleet manifests** — `fleet.toml` and `agent.toml` for each agent to `~/.vlinder/support-fleet/`

#### Two-phase install

If NATS or Podman are missing, the script installs the vlinder binary and config but does **not** start any services. It prints platform-specific install commands and asks the user to re-run. This avoids crash-looping the daemon when prerequisites are absent.

On re-run, the script detects that the binary and config already exist, skips those steps, and proceeds to service setup.

#### Principles

- **Explicit**: every step prints a status line with a checkmark, cross, or dash
- **Idempotent**: re-running skips what's already done (existing binaries, existing config, existing services)
- **Graceful degradation**: if Ollama isn't available, the script skips model setup. The system works for everything except LLM inference
- **Non-invasive**: the script does not install third-party software. It checks for prerequisites and prints platform-specific install commands
- **Service-first**: NATS and `vlinderd` both run as user services. No manual terminal management

#### Platform-specific service management

Two services are managed: `dev.vlinder.nats` (NATS with JetStream) and `dev.vlinder.daemon` (`vlinderd`).

| Concern | macOS | Linux |
|---------|-------|-------|
| Service manager | launchd (`~/Library/LaunchAgents/`) | systemd (`~/.config/systemd/user/`) |
| Auto-start | `RunAtLoad` in plist | `systemctl --user enable` |
| Vlinder logs | `~/Library/Logs/vlinder/daemon.log` | `journalctl --user -u vlinder` |
| NATS logs | `~/Library/Logs/vlinder/nats.log` | `journalctl --user -u vlinder-nats` |

If an existing NATS service is detected (e.g., brew-managed), the script skips NATS service creation and reminds the user to ensure JetStream is enabled.

#### What the install script does NOT do

- Install third-party software (NATS, Podman, Ollama)
- Configure network or firewall rules
- Build container images (pre-built images are pulled from ghcr.io)
- Modify shell profiles or PATH

### 4. The default model

The support fleet needs a model. The choice of which model is a configuration decision, not an agent decision. The agent manifests declare a logical alias (`default`), and the installer ensures that alias resolves to a real model.

The initial default is `phi3:latest` via Ollama. This is a pragmatic choice: small enough to run on most hardware, capable enough for triage and classification. Users can change it later via `vlinder model add`.

If Ollama is not available during install, the installer warns the user and skips model setup. The system is partially installed — everything except inference works. This is better than failing entirely.

### 5. Guard on `vlinder support`

`vlinder support` should never show a raw registration error. If the support fleet fails to deploy, the command should detect why and print actionable guidance:

```
Support fleet requires a registered model.
Run 'vlinder model add phi3' to register one manually.
```

This is a UI concern, not an architecture change. The registry validation is correct — the error message is the problem.

### 6. Prerequisites

Three external services form the minimum viable Vlinder installation:

| Prerequisite | Role | Required for |
|---|---|---|
| **NATS** | Message queue (distributed mode) | All agent execution |
| **Podman** | Container runtime | Running agents |
| **Ollama** | Inference and embedding backend (ADR 060) | Agents that use LLMs |

The installer checks for each and reports what's missing. NATS and Podman are hard requirements. Ollama is soft — the platform works without inference, but agents that need LLMs won't run.

## Scope

### Day One

- CI workflow: lint, test, license check (no external services)
- Release workflow: build binaries for 3 targets, publish GitHub release
- `install.sh`: cross-platform (macOS + Linux), prerequisite check, NATS + `vlinderd` service setup, model bootstrap
- Improved error message on `vlinder support` when prerequisites are missing

### Deferred

- Homebrew formula
- DMG installer for macOS
- Linux package managers (apt, dnf) as standalone packages
- Windows support
- `vlinder doctor` — a diagnostic command that checks system health post-install

### 7. Support fleet repo

The support fleet (support, code-analyst, log-analyst) lives in a separate repo (`vlindercli/support-agent`). Tagging a release builds three container images and pushes them to ghcr.io:

- `ghcr.io/vlindercli/vlinder-support:latest`
- `ghcr.io/vlindercli/vlinder-code-analyst:latest`
- `ghcr.io/vlindercli/vlinder-log-analyst:latest`

The install script pulls these images and writes manifests to `~/.vlinder/support-fleet/`. The agents remain in the monorepo for local development (`just build-support-fleet`).

### 8. Fleet path resolution

`vlinder support` resolves the fleet path in two stages:

1. **Production**: `~/.vlinder/support-fleet/` (written by the installer)
2. **Development**: `CARGO_MANIFEST_DIR/fleets/support` (source tree fallback)

This lets `vlinder support` work both from an installed binary and from `cargo run` during development.

## Consequences

- `vlinder support` works immediately after installation — the bootstrapping gap is closed
- CI catches regressions without requiring external services
- Release artifacts are produced automatically on tag push
- No intermediate init step. Install → daemon → use.
- The error path for missing prerequisites becomes actionable guidance instead of raw errors
- The default model choice is centralized in the installer, not scattered across agent manifests
- Single install script works on both macOS and Linux
- Support fleet images are built independently from the main release — they can be updated without a new vlinder binary release
