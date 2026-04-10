# Claude Working Guidelines

Keep this file general - reusable across projects. Project-specific context goes in TODO.md.

Git history of this file captures how the working style evolved.

## Workflow

1. Discuss the next domain concept
2. Draft ADR (minimal, one decision) - do not commit yet
3. Write code to validate the decision
4. Update ADR based on what we learned
5. Commit ADR + code together

ADRs are records of validated decisions, not speculative ones.

## Decision Making

- Whittle decisions down to the smallest possible increment
- One ADR captures one domain decision
- Don't overreach - focus on what's needed now
- Domain insights first, implementation details later
- **Think critically, don't yes-and**: When the user proposes something, evaluate it independently. If it's wrong, say so with reasoning. Never rubber-stamp an idea just because the user said it.

## ADRs

- Each ADR should be minimal and focused
- Strip out anything that can be deferred
- If an ADR mentions multiple decisions, split them
- Future decisions get their own ADRs when we actually need to make them

## Implementation (TDD)

- Write tests first
- Tests should express the domain model
- Red: write failing test
- Green: minimal code to make it pass
- Refactor: clean up if needed
- Run tests to verify

## Code Principles

- Top-down ordering: main type first, then errors, then supporting types
- Separate manifest (TOML deserialization) from value types (resolved data)
- Each has its own file: `foo_manifest.rs` and `foo.rs`
- **Value types over strings**: domain properties get their own types (SessionId, AgentId, SubmissionId, etc). Convert to/from String only at true boundaries (SQLite, protobuf, CLI input). Never detype a value back to String inside domain code.
- **CQRS**: all writes happen by sending a message through the queue. All reads come from the store. No direct store writes from CLI or harness — if you're calling `store.write_something()` outside a queue listener, it's a violation.
- **Compiler-driven refactoring**: change the type or signature, then build. Fix each error the compiler shows one at a time. Don't search the codebase manually for usages — the compiler finds them all. Never use `replace_all` to batch-fix compiler errors — each call site has different context. Never pre-read files to "understand the scope" of breakage. Build, read the error, fix that line, repeat.

## Verifying agent runs

- After the human runs an agent, they will ask Claude to check the logs and conversations.
- This is a trust-building exercise: prove you actually looked at the data, don't just say "looks good."
- Technique: the human asks "what am I up to?" — Claude must answer from the conversation payloads, not guess.
    - Read `~/.vlinder/conversations/*/payload` to see actual user inputs and agent responses
    - Read `~/.vlinder/logs/` for system-level events
    - Report what you found concretely (actual data, actual todo items, actual errors)

## Troubleshooting running of agents

- We are developing a runtime that makes agents highly observable.
- When agents fail, we should dogfood platform capabilities.
- Here is how
    - ~/.vlinder directory has all the config, state and logs.
    - ~/.vlinder/conversations has the entire state of an agent run
    - The human who is testing the agent always tries to test it with a clean slate.
        - see `just reset` (the human has likely run that before testing the agent)
    - So when an agent fails, all the events leading up to the failure, and the states the system went through should be present in ~/.vlinder/conversations
    - Use that information first, before trying to analyse the code
    - If that doesn't work look at the logs in ~/.vlinder/logs
    - If that also doesn't work, then add code to make the observability richer
      - add code to capture more state in conversations
      - add logs
      - ask the human to run it again
    - the more we dogfood, better the product becomes
    - observability gets better every time agents fail. That is a good thing.

## Build & Setup

- Always use `justfile` for build, setup, and fixture operations
- Before creating files/directories manually, check if there's a just recipe for it
- If a recipe is missing, add it to the justfile rather than doing it ad-hoc
- Run `just --list` to see available recipes

## Prototyping

- Write throwaway code to learn, not to keep
- Prototypes reveal consequences you couldn't see upfront
- Once you understand, delete and take a small confident step forward
- The value is in what you learned, not the code itself

## Git

- Never mention Claude in commit messages
- No "Co-Authored-By" lines
- Commit messages should read as if written by the user
- **Never use `--no-verify`** — pre-commit hooks exist for a reason. If the hook fails, fix the issue.

### Commit Message Format

```
<short summary in imperative mood>

<why this change matters - 1-2 sentences>
<what was changed - if not obvious from summary>
```

- First line: imperative mood, ~50 chars ("Add X", "Fix Y", "Rename A to B")
- Body: explain *why*, not just *what*
- If aligning with Vision/ADR, mention it
- Keep it concise but meaningful

## When changes are rejected

When the user rejects a proposed change, explain your inner reasoning — what led you to that specific action, step by step. This helps the user understand how to guide you better. Don't just apologize and retry; expose the thinking so the failure mode becomes visible.
