# Working Guidelines

Project-specific context goes in TODO.md. Git history of this file captures how the working style evolved.

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
- Top-down ordering: main type first, then errors, then supporting types.
- Separate manifest (`foo_manifest.rs`) from value types (`foo.rs`).
- **Value types over strings**: domain properties get their own types (SessionId, AgentName, SubmissionId). Convert to/from String only at true boundaries (SQLite, protobuf, CLI input).
- **CQRS**: writes go through the message queue, reads come from the store. No direct store writes from CLI or harness.
- Avoid smells: stringly typed values, overly long functions, too many parameters.

### Refactoring
- **Compiler-driven**: change the type or signature, then build. Fix each error one at a time. Don't search the codebase manually — the compiler finds all usages.
- Never use `replace_all` to batch-fix compiler errors — each call site has different context.
- Never pre-read files to "understand the scope" of breakage. Build, read the error, fix that line, repeat.

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

### Commit Format
```
<short summary in imperative mood>

<why this change matters - 1-2 sentences>
<what was changed - if not obvious from summary>
```

## When Changes Are Rejected

Explain your inner reasoning — what led to that specific action, step by step. Don't just apologize and retry; expose the thinking so the failure mode becomes visible.
