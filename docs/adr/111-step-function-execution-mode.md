# ADR 111: Durable Execution Mode

**Status:** Superseded by ADR 123 (Agents as Tool Servers) and ADR 124 (Harness-Mediated Delegation)

## Context

ADR 075 gave us time travel via state machine agents: the platform drove
the loop, each step was a git commit, any step was independently
resumable. ADR 105 removed it because the AgentEvent/AgentAction enum
protocol was hostile — 10 variants, two-sided schema coupling, every new
service required changes in both the sidecar and every agent SDK.

ADR 090/105 replaced it with typed messages: agents call provider
hostnames directly using native wire protocols (OpenAI, Anthropic, etc.).
Great DX — use any HTTP library, any SDK. But intermediate state lives
on the call stack. When the function returns, it's gone. No time travel.

The event-driven model (ADR 075) was sound — what broke was pinholing
every service call into a dedicated enum variant. The model itself —
handler returns an action, platform executes it, calls back with the
result — is exactly what enables time travel.

## Decision

Two execution modes coexist. Agents start unmanaged and migrate to
durable at their own pace.

### Unmanaged mode (default, unchanged)

Agent receives `POST /invoke`, drives its own loop, calls providers
directly via `*.vlinder.local:3544`. Uses any library — OpenAI SDK,
raw HTTP. The platform records every service call in the DAG (it
already does), but cannot replay from intermediate points because the
agent's state was on the call stack.

### Durable mode (opt-in via SDK)

Same event-driven model as ADR 075 but with one generic primitive:
`ctx.call(url, payload, then=handler)`. Each handler ends with one of
three actions:

- `ctx.call(url, payload, then=handler)` — HTTP call to a provider,
  platform journals and executes it, calls the named handler with the
  result
- `ctx.delegate(agent, input, then=handler)` — delegate to another
  agent, platform orchestrates it, calls the named handler with the
  result
- `ctx.complete(payload)` — return the final result

```python
from vlinder import Agent

agent = Agent()

@agent.on_invoke
def handle_invoke(ctx):
    ctx.call(
        "http://sqlite-kv.vlinder.local:3544/put",
        json={"path": "/query.txt", "content": ctx.input},
        then=handle_saved,
    )

@agent.handler
def handle_saved(ctx):
    ctx.call(
        "http://openrouter.vlinder.local:3544/v1/chat/completions",
        json={"model": "anthropic/claude-sonnet-4",
              "messages": [{"role": "user", "content": ctx.input}]},
        then=handle_result,
    )

@agent.handler
def handle_result(ctx):
    response = ctx.result.json()["choices"][0]["message"]["content"]
    ctx.complete(response)
```

No pinholed methods. No enum variants. `ctx.call()` is a generic HTTP
action — the agent uses real provider wire protocols (OpenAI,
Anthropic, etc.) directly. The `then=` parameter names the callback
handler, so the platform knows exactly which function to invoke with
the result.

### Wire protocol

Both modes use the same endpoint: `POST /invoke`. All durable-mode
requests are JSON with a `handler` field naming the function to call:

```json
{"handler": "on_invoke", "input": "user text"}
{"handler": "handle_result", "result": {...}}
```

The sidecar wraps the raw invoke payload:
`{"handler": "on_invoke", "input": "<payload>"}`. Callbacks use the
checkpoint name from the service response:
`{"handler": "<checkpoint>", "result": {...}}`.

Durable agents return a JSON action with the `X-Vlinder-Mode: durable`
response header. Unmanaged agents receive raw bytes on `/invoke` and
return raw bytes (no header). The sidecar detects durable mode from
the header on the first response.

For repair, the sidecar sends the callback JSON directly to `/invoke`
with the checkpoint name from the DAG.

### Event-driven, not loop-driven

The sidecar already has an event loop that polls for invoke and
delegate messages. Durable execution does not add a separate
checkpoint loop. Instead, service responses are a third event type
in the same loop:

```
loop {
    if invoke arrives   → POST /invoke to agent (raw payload)
    if delegate arrives → handle delegation
    if response arrives → POST /invoke to agent (callback JSON)
    else → sleep
}
```

The agent returns a JSON action from `/invoke`. The sidecar
interprets the action:

- `{"action":"call", ...}` — send a service request to the queue
  (fire and forget). The response will arrive later as an event.
- `{"action":"complete", ...}` — build CompleteMessage, send it.

Service requests and responses are decoupled. The sidecar does not
block waiting for a response (`call_service()`). It sends the request
and moves on. When the service response arrives on the queue, the
sidecar picks it up and delivers it to the agent via `/invoke`,
naming the checkpoint handler from the response.

This is the same pattern as invoke and delegate — message arrives,
sidecar acts on it. No special orchestration. No nested loop.

Note: `call_service()` (blocking request-reply) is a convenience
wrapper on the `MessageQueue` trait — it calls `send_request()` then
polls `receive_response()` in a loop. The decoupled primitives
already exist on the trait. The sidecar's durable-mode code path
calls them separately: `send_request()` to fire the request, then
`receive_response()` later when processing queue events. No trait
changes needed. `call_service()` remains unchanged for unmanaged
mode (used by the provider server's `InvokeHandler`).

### No ctx.state — use KV

There is no `ctx.state` dict. Working memory between handlers goes
through the KV store, same as ADR 075 agents. The platform tracks
state via content-addressed hashes (the `State:` trailer on DAG
commits). This is what makes time travel work — the platform doesn't
carry the full state, it carries a hash. The agent retrieves
point-in-time state from KV using that hash.

An in-memory `ctx.state` dict would bypass the state tracking system:
the platform couldn't persist it, repair couldn't restore it, and
time travel would break. Every piece of data an agent needs across
checkpoint boundaries must go through a platform-managed store.

### Handlers are plain functions

Each handler is a named, top-level function. No nesting, no lambdas.
`then=` takes a function reference, not an anonymous callback — the
platform needs the handler name to journal it and to jump to it on
replay.

This makes handlers independently testable:

```python
def test_handle_result():
    ctx = Context(
        result=FakeResponse({"choices": [{"message": {"content": "world"}}]}),
    )
    handle_result(ctx)
    assert ctx._action["payload"] == "world"
```

No async runtime, no replay framework, no mocking infrastructure.
Just call the function, check the output.

### Branching and loops

Handlers can branch — `then=` is chosen at runtime:

```python
@agent.handler
def route(ctx):
    intent = ctx.result.json()["choices"][0]["message"]["content"]
    if "search" in intent:
        ctx.call(..., then=handle_search)
    else:
        ctx.call(..., then=handle_chat)
```

Handlers can loop — name a previous handler as `then=`:

```python
@agent.handler
def evaluate(ctx):
    score = ctx.result.json()["score"]
    if score < 7:
        ctx.call(..., then=generate)  # loop back
    else:
        ctx.complete(ctx.result.json()["draft"])
```

Fan-out is a fleet concern — `ctx.delegate()` to multiple agents,
not multiple `ctx.call()` within one agent.

### Progressive adoption

The migration from unmanaged to durable is mechanical — take each
direct HTTP call and wrap it in `ctx.call()` with a `then=` callback:

```python
# Before (unmanaged) — direct HTTP call in handler
response = requests.post(
    "http://openrouter.vlinder.local:3544/v1/chat/completions",
    json={"model": "anthropic/claude-sonnet-4",
          "messages": [{"role": "user", "content": prompt}]},
)

# After (durable) — same URL, same payload, handler chain
ctx.call(
    "http://openrouter.vlinder.local:3544/v1/chat/completions",
    json={"model": "anthropic/claude-sonnet-4",
          "messages": [{"role": "user", "content": prompt}]},
    then=handle_result,
)
```

Same wire protocol. Same provider hostnames. An agent can migrate one
call at a time. Unmigrated calls still work — they just aren't
checkpointed.

### Time travel and repair

At each checkpoint, the DAG commit contains the handler name (the
`Checkpoint:` trailer), the state hash (`State:` trailer), the
outbound request, and the response.

#### DAG parent

Every invoke message carries a `dag_parent` field — the commit hash
that the invoke's DAG commit should parent on. This is not optional;
the harness always populates it.

- **Normal invoke:** the CLI reads the current DAG tip for the
  timeline and passes it to the harness.
- **Repair invoke:** the CLI passes the checked-out commit hash.

The GitDagWorker reads `dag_parent` from the invoke and parents
the commit on it. If the parent isn't the current tip, git naturally
creates a fork. The worker also creates the repair branch.

This works regardless of repo topology. The local git repo is a
peer, not a read-only projection — the CLI browses and branches in
it, the GitDagWorker writes commits to it. Both `checkout` and
`branch` are idempotent operations, so when both peers perform them
(CLI for immediate UX, worker from the message stream), the results
converge. If the repos are separate (e.g. a clone), each peer
materializes the branch independently from the same message.

#### Repair targets

Two types of commits are valid repair targets:

**Request commits with a checkpoint field** — re-execute a failed
service call. The platform sends a repair invoke to the sidecar,
which re-queues the request to the service backend. The repaired
branch records the full audit trail: the repair invoke, the
re-queued request, the new response, and continuation to
completion. This is intentional — the DAG is an honest record of
what happened, not a sanitized version of what a clean run would
have looked like.

```
$ vlinder timeline checkout a9be203    # the failed embed request
At: request: todoapp → embed.ollama
Checkpoint: handle_result

$ vlinder timeline repair              # re-queues, calls /invoke
Re-queuing request to embed.ollama...
Response received (200 OK)
Calling handler 'handle_result'...
Agent completed.
```

**Complete commits** — rewind to a known good point and continue
with new input. The platform restores state from the commit and
opens a REPL for new invocations.

```
$ vlinder timeline checkout 0c647f9    # last successful complete
$ vlinder timeline repair              # restore state, open REPL
> add buy bread                        # new invocation from here
```

All other commit types (invoke, response) are rejected by repair —
they don't represent actionable resume points.

Request-with-checkpoint repair only works for durable agents.
Unmanaged agents have no checkpoint field, so only complete-based
repair (rewind and re-invoke) is available.

No replay from the top. The platform jumps directly to the handler
because each handler is independently addressable by name. This is
the key advantage over Temporal/Restate's deterministic replay model.

### Forking

Because checkpoints are git commits, the DAG supports forking: take
checkpoint N, try a different path (different model, different prompt,
different handler), compare results. Temporal and Restate journals are
append-only and linear — they support retry but not branching.

### Language portability

The pattern is named functions + function references as callbacks.
No generators, no async/await, no coroutines. Every language supports
this:

```typescript
// TypeScript
agent.onInvoke(function handleInvoke(ctx) {
    ctx.call("http://...", { json: {...} }, handleResult);
});
```

```go
// Go
agent.OnInvoke(handleInvoke)
func handleResult(ctx *vlinder.Context) {
    ctx.Call("http://...", payload, handleSaved)
}
```

## Consequences

- Unmanaged mode is untouched — existing agents keep working
- Durable mode earns time travel with one generic primitive (`ctx.call`)
- Event-driven — service responses are events in the sidecar's
  existing event loop, not a separate checkpoint loop
- Decoupled request/reply — sidecar sends service requests to the
  queue without blocking, picks up responses when they arrive
- No pinholed enum variants — `ctx.call()` + `ctx.delegate()` +
  `ctx.complete()` replace all 10 ADR 075 action types
- Agent code uses real provider wire protocols — not platform
  abstractions
- Each handler is a named, testable function — no framework mocking
  needed
- Progressive migration: one call at a time, not all-or-nothing
- No deterministic replay — platform jumps to handlers directly,
  unlike Temporal/Restate which re-run from the top
- Git-backed checkpoints enable forking — explore alternate paths
  from any checkpoint, not just retry
- Agent SDKs need `ctx.call()` + handler dispatch per language —
  simple implementation (named functions, no coroutines)
- Opaque orchestration libraries (LangChain, etc.) hide intermediate
  state behind their own abstractions — incompatible with platform
  observability. AI coding tools can decompose their logic into
  explicit handler chains instead.
