# Working Guidelines

This file is for Claude Code (Opus) used as a high-level collaborator — drafting ADRs, making design decisions, investigating failures, steering the project. Mechanical code execution is often delegated to a cheaper executor agent (Pi with Qwen3-Coder or similar), which reads `AGENTS.md`, not this file.

When you write specs or step-by-step instructions for the executor, assume `AGENTS.md` is already loaded into its context. Don't re-state the executor's general rules — do include scope fences and anything that goes beyond the general guidance.

### Writing executor specs
- Describe WHAT to do, not HOW step-by-step. Capable executors can trace code, follow compilers, and make mechanical decisions. Over-specification wastes spec tokens and anchors the executor to the architect's assumptions.
- Follow the runtime errors, don't speculate. When debugging async cascade issues, fix what the runtime tells you is broken. Don't preemptively change 13 methods because you think they'll all need it — change one, run, see what breaks next.
- Review the executor's work by reading the CODE first. Form your own opinion about correctness before reading the executor's session logs or reasoning. Logs bias the reviewer toward the executor's perspective.

## TL;DR

Vlinder is a Rust CLI platform for running AI agents locally with time-travel debugging. The core idea: every agent side effect becomes a node on a content-addressed Merkle DAG, so runs can fork, resume, and be proven after the fact. Runtime is a Rust workspace; persistence is SQLite plus per-agent storage backends; agents run in Podman containers behind a sidecar; the control plane is a message queue (NATS or AMQP). CQRS is strict — writes go through the queue, reads come from the store. Dogfooding is a way of life.

## Docs

Reference material loaded on demand:

- `docs/DOMAIN_MODEL.md` — domain glossary (Session, Submission, DAG, readiness check, harness, sidecar, conversation).
- `docs/ARCHITECTURE.md` — workspace crate layout.
- `docs/OBSERVABILITY.md` — logging and telemetry conventions.
- `docs/REQUEST_FLOW.md` — end-to-end request path through the system.
- `docs/MOTIVATION.md`, `docs/VISION.md` — project framing.
- `docs/TIMELINE.md`, `docs/TIMELINE_WALKTHROUGH.md` — time-travel semantics.
- `docs/BRING_YOUR_OWN_STORAGE.md` — pluggable storage backend story.
- `docs/adr/` — one file per architectural decision. Cite by number when passing work to the executor.

## Process

### Decision Flow
1. Discuss the next domain concept
2. Draft ADR as running notes (minimal, one decision — don't commit yet)
3. Write code to validate the decision
4. Revisit and compact the ADR based on what we learned
5. Commit ADR + code together

ADRs are records of validated decisions, not speculative ones. Each ADR captures one domain decision. Strip out anything deferrable — future decisions get their own ADRs when needed.

### Branching
- Cut a branch off main, stacked diff approach, one branch at a time.
- Naming: `<feature-name>/<step-xx>-<change>`
- Typically 1–3 commits per step.

### Change Strategy
Default to **strangler fig pattern**:
1. Dead code — new types/traits alongside old
2. Dual write — both paths populated
3. Cut over reads — promote new code
4. Delete old code

### Decision Making
- Whittle decisions down to the smallest possible increment.
- Domain insights first, implementation details later.
- **Think critically, don't yes-and**: evaluate proposals independently. If it's wrong, say so with reasoning. Never rubber-stamp.

## Code

### Principles
- Top-down ordering (per file): main type first, then its errors, then supporting types.
- Separate manifest (`foo_manifest.rs`) from value types (`foo.rs`).
- **Value types over strings**: domain properties get their own types (SessionId, AgentName, SubmissionId). Convert to/from String only at true boundaries (SQLite, protobuf, CLI input). Example: `SessionId` is a newtype in `vlinder-core`; session-touching code accepts `&SessionId`, never `&str` — the only places `String` appears are the storage serialization layer and CLI argument parsing.
- **CQRS**: writes go through the message queue, reads come from the store. No direct store writes from CLI or harness.
- Avoid smells: stringly typed values, overly long functions, too many parameters.

### Testing
- TDD: red → green → refactor.
- Tests express the domain model.
- Clippy pedantic is the bar.

## Build

- `cargo build` is sufficient — `target/debug` is on PATH. Do NOT `cargo install`.
- `just reset` for nuclear clean, `just build-everything` for full rebuild (cargo + sidecar image).
- Always use `justfile` for build, setup, and fixture operations — `just --list` to see recipes.
- If a recipe is missing, add it to the justfile rather than doing it ad-hoc.

## Troubleshooting

### Dogfooding
Dogfooding is a way of life. Observability gets better every time agents fail.

### When Agents Fail
Investigate in this order — exhaust each before escalating:
1. **Conversations** (`~/.vlinder/conversations/`) — the full state of the agent run.
2. **SQLite DB** (`~/.vlinder/dag.db`) — readiness checks, DAG nodes, sessions.
3. **Logs** (`~/.vlinder/logs/`) — system-level events.
4. **Add observability** — capture more state in conversations, add logs, ask the human to run again.

### E2E Failures
- Back up logs/db/conversations before investigating.
- Isolate systematically: baseline at last known good, test one variable at a time.
- Progressively harden e2e script and observability as we go.

### Verifying Agent Runs
When asked to check a run, prove you looked at the data — don't just say "looks good."
- Read `~/.vlinder/conversations/` for actual payloads.
- Read `~/.vlinder/logs/` for system events.
- Report concretely: actual data, actual items, actual errors.

### Remote Hosts
- **Always show commands before executing** — never run SSH or destructive commands without explicit approval.
- SSH: `ssh -i ~/.ssh/id_vlinder ec2-user@dev.test.vlindercli.dev`
- Test infra docs: `~/vlindercli/test-infra/README.md`

## Prototyping

Write throwaway code to learn, not to keep. Once you understand, delete and take a small confident step forward. The value is in what you learned, not the code.

## Git

- Never mention Claude in commit messages. No "Co-Authored-By" lines.
- **Never use `--no-verify`** — if the hook fails, fix the issue.
- **Rebase does not trigger pre-commit hooks.** After rebasing a stacked diff chain, manually run `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test` on each rebased branch. Dead code or new warnings introduced by conflict resolution won't be caught otherwise.

### Commit Format
```
<short summary in imperative mood>

<why this change matters - 1-2 sentences>
<what was changed - if not obvious from summary>
```

## When Changes Are Rejected

Explain your inner reasoning — what led to that specific action, step by step. Don't just apologize and retry; expose the thinking so the failure mode becomes visible.

# vlinder dev environment

When the dev stack is running, there's a tmux session called `vlinder` with these panes:

- `vlinder:main.0` — nats-server (-js)
- `vlinder:main.1` — `nats sub "vlinder.>"` (live message bus tap)
- `vlinder:main.2` — `vlinderd` with RUST_BACKTRACE=1
- `vlinder:main.3` — todoapp agent deploy (in `sample-agents-fleets/agents/todoapp`)
- `vlinder:main.4` — Claude Code (you, if you're running inside the session)

Use the `tmux-inspect` skill to read or drive these panes.