# Agent Execution Guide

This file is for agents doing mechanical code execution on this repo — typically Pi with Qwen3-Coder or a similar cheaper model driven from a pre-written specification. If you are a high-level collaborator drafting ADRs or making design decisions, read `CLAUDE.md` instead.

Your job is to execute a specification that someone else wrote. Not to rewrite the spec, not to expand scope, not to "improve" code that wasn't in scope. Execute, commit, stop. The spec may reference ADRs under `docs/adr/` — treat those as read-only reference material. Never draft, modify, or re-scope an ADR on your own initiative.

## TL;DR

Vlinder is a Rust CLI platform for running AI agents locally with time-travel debugging. The core idea: every agent side effect becomes a node on a content-addressed Merkle DAG, so runs can fork, resume, and be proven after the fact. Runtime is a Rust workspace; persistence is SQLite plus per-agent storage backends; agents run in Podman containers behind a sidecar; the control plane is a message queue (NATS or AMQP). CQRS is strict — writes go through the queue, reads come from the store. Dogfooding is a way of life.

## Pointers

Load these on demand when the task needs them. Do NOT load them proactively.

- `docs/DOMAIN_MODEL.md` — glossary for Session, Submission, DAG, readiness check, harness, sidecar, conversation, node, and other project jargon. Load when the spec uses a term you don't recognize.
- `docs/ARCHITECTURE.md` — workspace crate layout. Load when you need to find where a type or behavior lives.
- `docs/adr/<NNN>-<slug>.md` — one file per architectural decision. The spec will cite ADR numbers; load those directly. Do not browse the directory unprompted.

## Do Not Read

Avoid loading these paths into context by default. They eat tokens without helping you execute the task:

- `target/` — build artifacts. Never load.
- `~/.vlinder/conversations/`, `~/.vlinder/logs/`, `~/.vlinder/dag.db` — agent state for investigation. This is the architect's territory, not the executor's. If the spec points you at a specific file, load exactly that file, not the directory.
- Any vendored or generated directory in the repo.
- `docs/` files not listed in Pointers unless the spec directs you there.

When in doubt about whether to load a file, ask. Loading unnecessarily burns context that you need for the task.

## Reading the Spec

You will receive tasks as written specifications. Every spec should have these sections — if one is missing, ask for it before you start:

1. **Task** — one-line statement of what to do.
2. **Context** — why this matters, what came before, any ADR reference.
3. **In scope** — explicit list of files and changes you are authorized to make.
4. **Out of scope** — explicit list of "do NOT touch." This is as important as in scope.
5. **Build and test gates** — exact commands that must pass before you commit.
6. **Done criteria** — a checkbox list you can verify yourself.
7. **Stop and report** — conditions under which you should bail out rather than improvise.

Do not add changes beyond the in-scope list, even if they look related. Do not interpret the task more liberally than it is written. When the spec is ambiguous, ask — do not guess.

## Code

### Principles
- Top-down ordering (per file): main type first, then its errors, then supporting types.
- Separate manifest (`foo_manifest.rs`) from value types (`foo.rs`).
- **Value types over strings**: domain properties get their own types (`SessionId`, `AgentName`, `SubmissionId`). Convert to/from `String` only at true boundaries (SQLite, protobuf, CLI input). Example: `SessionId` is a newtype in `vlinder-core`; session-touching code accepts `&SessionId`, never `&str` — the only places `String` appears are the storage serialization layer and CLI argument parsing.
- **CQRS**: writes go through the message queue, reads come from the store. No direct store writes from CLI or harness.
- Avoid smells: stringly typed values, overly long functions, too many parameters.

### Refactoring
- **Compiler-driven**: change the type or signature, then build. Fix each error one at a time. Don't search the codebase manually — the compiler finds all usages.
- Never use `replace_all` to batch-fix compiler errors — each call site has different context.
- Never pre-read files to "understand the scope" of breakage. Build, read the error, fix that line, repeat.

### Editing Files
- The file on disk is authoritative; your in-context memory of it is not. Re-Read a file immediately before every Edit, even if you read it earlier in the session.
- Never generate an `old_string` from memory or from what the file "probably looks like." Copy it directly from fresh Read output, preserving whitespace exactly (tabs, trailing spaces, line endings — all of it).
- Anchor every `old_string` with at least 3 lines of context (target line plus surrounding lines). Single-line `old_string`s are where byte-exact matching fails — near-duplicates elsewhere in the file break uniqueness.
- A failed Edit match means your `old_string` is wrong. Re-Read the file and fix the input. Do NOT retry the identical Edit, and do NOT reach for `replace_all` as a workaround.
- If the file changed between your Read and your Edit (a formatter, a pre-commit hook, another process), re-Read before retrying. A stale cache is not a valid Edit target.

### Scope
- Do exactly what the spec says. Do not add features, refactor unrelated code, or "improve" things that weren't in scope.
- No opportunistic reformatting. If you did not touch a line, do not reformat it.
- Do not add comments, docstrings, or type annotations to code you did not change. Only add comments where the logic isn't self-evident.
- Do not create files unless the spec tells you to. Prefer editing an existing file over creating a new one.
- Do not add error handling, fallbacks, or validation for scenarios that can't happen. Trust internal code and framework guarantees.

### Testing
- TDD: red → green → refactor.
- Tests express the domain model.
- Clippy pedantic is the bar — `cargo clippy --workspace --all-targets -- -D warnings` must pass.

## Build

- `cargo build` is sufficient — `target/debug` is on PATH. Do NOT `cargo install`.
- `just build-everything` for a full rebuild (cargo + sidecar image). Only when the spec asks for it.
- Use `justfile` for build, setup, and fixture operations — `just --list` to see recipes.
- If a recipe is missing, add it to the justfile rather than doing it ad-hoc.

## Feedback Loop

The tight edit → check → fix cycle. Use the smallest command that would catch your class of change — smaller commands are faster and keep the loop tight.

**Iterating on body-only edits inside one crate**: `cargo check -p <crate>`. Faster than workspace-wide. Use this as your default during iteration.

**Type or signature change that crosses crate boundaries** (trait change, enum variant, fn signature): `cargo check --workspace --all-targets`. Scoping to one crate HIDES errors in downstream crates — the whole point of a compiler-driven refactor is to see the full cascade.

**Targeted tests during iteration**: `cargo test -p <crate> <test_filter>`.

**Structured diagnostics** (when the error list is long and you need to distill it): `cargo check --message-format=json` piped through `jq`.

**Commit gate — these must all pass before any commit**:
- `cargo build --workspace --all-targets`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets -- -D warnings`

Clippy pedantic is the bar. If it fires a new lint on code you touched, fix it properly — do not `#[allow]` your way past it.

**Full integration suite**: only when the spec asks for it, via `just run-integration-tests`. Do NOT run this in the edit loop — it is slow and has side effects.

## Escape Hatches

Tools for specific situations. Default to `cargo check` and `rg` for the common case; reach for these only when the common case is not enough.

- `rustc --explain E0XXX` — read the full explanation for an error code before guessing at a fix. Do this when a borrow-checker or trait-resolution error is not immediately obvious. Cheap, often decisive.
- `ast-grep -p '<pattern>' -l rust` — structural code search when you need a syntactic pattern rather than a literal string. Examples:
  - `ast-grep -p '$EXPR.unwrap()' -l rust` — find all `.unwrap()` calls.
  - `ast-grep -p 'impl $TRAIT for $TYPE { $$$ }' -l rust` — find all trait impls.
  - `ast-grep -p 'fn $NAME($$$) -> Result<$$$> { $$$ }' -l rust` — find all fallible functions.
- `cargo expand <module::path>` — see what derives and proc macros actually generate. Use when a derive error is confusing.
- `cargo tree -i <crate>` — figure out why a dependency is in the graph. Use when a version conflict or duplicate crate is blocking a build.

Routing rule:
- Type errors, borrow checker, name resolution → `cargo check`, then `rustc --explain`.
- Syntactic pattern → `ast-grep`.
- Literal string → `rg`.
- Macro output mystery → `cargo expand`.

## Branching

- Cut a branch off main, stacked diff approach, one branch at a time.
- Naming: `<feature-name>/<step-xx>-<change>`
- Typically 1–3 commits per step.

## Git

- Never mention Claude, Pi, Qwen, or any AI attribution in commit messages. No "Co-Authored-By" lines.
- **Never use `--no-verify`** — if the pre-commit hook fails, diagnose and fix the root cause. If you genuinely cannot, stop and report back rather than bypassing the hook.
- **Rebase does not trigger pre-commit hooks.** After rebasing a stacked diff chain, manually run `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test` on each rebased branch. Dead code or new warnings introduced by conflict resolution won't be caught otherwise.
- Do not commit unless the spec or the user explicitly asks you to.

### Commit Format
```
<short summary in imperative mood>

<why this change matters - 1-2 sentences>
<what was changed - if not obvious from summary>
```

## When Changes Are Rejected

Explain your inner reasoning — what led to that specific action, step by step. Don't just apologize and retry; expose the thinking so the failure mode becomes visible.

## Stop and Report

Stop immediately and report back — do not improvise a workaround — if any of the following happens:

- A build or test error you cannot resolve after one focused attempt.
- An Edit tool failure that repeats after a fresh Read of the target file.
- A test that was passing starts failing, and the fix is not obviously "add `.await` here" or similar trivial call-site adjustment.
- An instruction in the spec conflicts with something in this file.
- You discover that completing the task requires changes outside the spec's declared scope.
- Any action that feels destructive (rm, force push, dropping a table) or goes beyond what was asked.

The cost of stopping is a round-trip to the supervisor. The cost of improvising is a bad commit that someone has to untangle. Always pay the cheaper cost.
