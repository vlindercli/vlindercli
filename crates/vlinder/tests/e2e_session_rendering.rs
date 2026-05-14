//! E2E tests for session rendering.
//!
//! These tests shell out to the `vlinder` CLI binary and require a
//! running daemon with NATS, a configured `OpenRouter` agent, and network
//! access. They are intended to be run as part of the integration test
//! suite (`just run-integration-tests`) or manually when infrastructure
//! is available.
//!
//! # Usage
//!
//!     cargo test --test e2e_session_rendering e2e_tool_call_visibility
//!     cargo test --test e2e_session_rendering e2e_pure_text_turn_renders_unchanged
//!
//! `e2e_multi_turn_ordering` is `#[ignore]` until commit 9 (`SubmissionId` fix):
//!
//!     cargo test --test e2e_session_rendering e2e_multi_turn_ordering -- --ignored

use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

/// Path to the `vlinder` CLI binary, set by cargo at build time.
const VLINDER_BIN: &str = env!("CARGO_BIN_EXE_vlinder");

/// Counter for unique external session IDs across tests.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Create a fresh session, run one agent turn, and return the session UUID
/// for use with `vlinder session get`.
///
/// Uses the `openrouter-test` agent (must be deployed). Creates a session
/// via `session ensure openrouter-test --id e2e-<n>`, then runs
/// the agent with `--session e2e-<n>`.
fn create_session_and_run(agent: &str, prompt: &str) -> String {
    let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
    let external_id = format!("e2e-{id}");

    // Step 1: ensure the session exists with a known external ID
    let ensure_out = Command::new(VLINDER_BIN)
        .args(["session", "ensure", agent, "--id", &external_id])
        .output()
        .unwrap_or_else(|e| panic!("failed to run vlinder session ensure: {e}"));

    assert!(
        ensure_out.status.success(),
        "vlinder session ensure failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&ensure_out.stdout),
        String::from_utf8_lossy(&ensure_out.stderr),
    );

    // Parse session UUID from ensure output line: "<name>  <uuid>  <timestamp>  <branches>"
    let ensure_stdout = String::from_utf8_lossy(&ensure_out.stdout);
    let session_uuid = ensure_stdout
        .lines()
        .next()
        .and_then(|line| {
            let parts: Vec<&str> = line.split_whitespace().collect();
            parts.get(1).map(ToString::to_string)
        })
        .unwrap_or_else(|| {
            panic!("could not parse session UUID from ensure output:\n{ensure_stdout}");
        });

    // Step 2: run the agent on this session
    let run_out = Command::new(VLINDER_BIN)
        .args([
            "agent",
            "run",
            agent,
            "-p",
            prompt,
            "--session",
            &external_id,
        ])
        .output()
        .unwrap_or_else(|e| panic!("failed to run vlinder agent run: {e}"));

    assert!(
        run_out.status.success(),
        "vlinder agent run failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&run_out.stdout),
        String::from_utf8_lossy(&run_out.stderr),
    );

    session_uuid
}

/// Run `vlinder session get <session>` and return the output.
fn get_session_output(session_uuid: &str) -> String {
    let output = Command::new(VLINDER_BIN)
        .args(["session", "get", session_uuid])
        .output()
        .unwrap_or_else(|e| panic!("failed to run vlinder session get: {e}"));

    assert!(
        output.status.success(),
        "vlinder session get failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    String::from_utf8_lossy(&output.stdout).to_string()
}

// ============================================================================
// 4.6a — E2E: tool-call visibility
// ============================================================================

/// Run a one-turn agent invocation that triggers a tool call, then
/// verify the session view shows Request/Response lines.
///
/// Requires a running daemon with NATS, a configured `OpenRouter` agent,
/// and network access. Ignored by default — run explicitly with `--ignored`.
#[ignore = "requires running daemon with NATS, OpenRouter agent, and network"]
#[test]
fn e2e_tool_call_visibility() {
    // Use a deterministic prompt that reliably triggers brave_web_search
    // or another configured MCP tool.
    let session = create_session_and_run("openrouter-test", "search for latest Mars rover news");
    let output = get_session_output(&session);

    // Verify the output contains lines with the `->` arrow pattern
    // (present in all non-Invoke/Complete rendered lines)
    assert!(
        output.contains("-> "),
        "expected a service/agent target (arrow) in output:\n{output}"
    );

    // Verify there's at least one line with "op:"
    assert!(
        output.contains("op:"),
        "expected operation field in output:\n{output}"
    );

    // Verify there are Invoke and Complete lines
    assert!(
        output.to_lowercase().contains("invoke"),
        "expected invoke type in output:\n{output}"
    );
    assert!(
        output.to_lowercase().contains("complete"),
        "expected complete type in output:\n{output}"
    );

    // Verify there's a request/svc_request line reflecting tool dispatch
    let has_request =
        output.to_lowercase().contains("request") || output.to_lowercase().contains("svc_request");
    assert!(
        has_request,
        "expected a request/svc_request line reflecting tool call:\n{output}"
    );

    // Verify there's a response/svc_response line reflecting tool result
    let has_response = output.to_lowercase().contains("response")
        || output.to_lowercase().contains("svc_response");
    assert!(
        has_response,
        "expected a response/svc_response line reflecting tool result:\n{output}"
    );
}

// ============================================================================
// 4.6b — E2E: multi-turn ordering (ignored until commit 9)
// ============================================================================

/// Run two consecutive turns (each with a tool call) and verify the
/// session view shows exactly 2 Turn blocks, each with correct causal
/// ordering.
///
/// This test depends on commit 9's SubmissionId-per-turn fix. Without
/// it, a single user turn with 2 tool-call cycles produces 3+ Turn
/// blocks.
#[ignore = "requires running daemon with NATS, OpenRouter agent, and network"]
#[test]
fn e2e_multi_turn_ordering() {
    let session = create_session_and_run("openrouter-test", "search for Mars rover news");

    // Second turn in the same session
    create_session_and_run("openrouter-test", "search for Jupiter moon news");

    let output = get_session_output(&session);

    // Count Turn blocks — each block starts with "Turn <id>"
    let turn_count = output
        .lines()
        .filter(|l| l.trim().starts_with("Turn"))
        .count();

    assert_eq!(
        turn_count, 2,
        "expected exactly 2 Turn blocks for 2 user turns, got {turn_count}:\n{output}"
    );

    // Within each Turn block, verify causal ordering:
    // Invoke before Request(s), Request before Response, all before Complete.
    let turn_blocks: Vec<&str> = output
        .lines()
        .collect::<Vec<_>>()
        .split(|l| l.trim().starts_with("Turn"))
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| chunk.join("\n"))
        .map(|s| Box::leak(s.into_boxed_str()) as &str)
        .collect();

    for block in &turn_blocks {
        let lines: Vec<&str> = block.lines().collect();
        let mut saw_invoke = false;
        let mut saw_request = false;
        let mut saw_complete = false;

        for line in &lines {
            let lc = line.to_lowercase();
            if lc.contains("invoke") {
                assert!(
                    !saw_request,
                    "Invoke after Request violates causal order:\n{block}"
                );
                assert!(
                    !saw_complete,
                    "Invoke after Complete violates causal order:\n{block}"
                );
                saw_invoke = true;
            } else if lc.contains("request") || lc.contains("svc_request") {
                assert!(
                    saw_invoke,
                    "Request before Invoke violates causal order:\n{block}"
                );
                assert!(
                    !saw_complete,
                    "Request after Complete violates causal order:\n{block}"
                );
                saw_request = true;
            } else if lc.contains("complete") {
                assert!(!saw_complete, "duplicate Complete in turn block:\n{block}");
                saw_complete = true;
            }
        }

        assert!(saw_invoke, "Turn block missing Invoke:\n{block}");
        assert!(saw_complete, "Turn block missing Complete:\n{block}");
    }
}

// ============================================================================
// 4.7 — Pure-text regression sanity (1 test)
// ============================================================================

/// Run a one-turn agent invocation with a prompt that does NOT trigger
/// any tool calls, then verify the session view shows only Invoke and
/// Complete lines — no Request, Response, `SvcRequest`, or `SvcResponse`.
///
/// Also verifies the Complete line uses the routing key's actual agent
/// name (e.g., `openrouter-test`), not the literal string `"agent"`.
/// This catches hardcoded-literal regressions in the renderer.
///
/// Requires a running daemon with NATS, a configured `OpenRouter` agent,
/// and network access. Ignored by default — run explicitly with `--ignored`.
#[ignore = "requires running daemon with NATS, OpenRouter agent, and network"]
#[test]
fn e2e_pure_text_turn_renders_unchanged() {
    let session = create_session_and_run("openrouter-test", "what is 2+2");
    let output = get_session_output(&session);

    // Count message-type lines
    let invoke_count = output
        .lines()
        .filter(|l| l.to_lowercase().contains("invoke"))
        .count();
    let complete_count = output
        .lines()
        .filter(|l| l.to_lowercase().contains("complete"))
        .count();
    let request_count = output
        .lines()
        .filter(|l| {
            let lc = l.to_lowercase();
            lc.contains("request") || lc.contains("svc_request")
        })
        .count();
    let response_count = output
        .lines()
        .filter(|l| {
            let lc = l.to_lowercase();
            lc.contains("response") || lc.contains("svc_response")
        })
        .count();

    assert!(
        invoke_count >= 1,
        "expected at least 1 Invoke line, got {invoke_count}:\n{output}"
    );
    assert!(
        complete_count >= 1,
        "expected at least 1 Complete line, got {complete_count}:\n{output}"
    );
    assert_eq!(
        request_count, 0,
        "expected 0 Request/SvcRequest lines (no tool calls), got {request_count}:\n{output}"
    );
    assert_eq!(
        response_count, 0,
        "expected 0 Response/SvcResponse lines (no tool calls), got {response_count}:\n{output}"
    );

    // Verify the Complete line uses the agent name from the routing key,
    // not the hardcoded literal "agent". The routing key has the agent
    // name on the left side of Complete lines.
    assert!(
        output.contains("openrouter-test"),
        "expected agent name 'openrouter-test' in session output:\n{output}"
    );
    assert!(
        !output.contains("\"agent\""),
        "output must not contain literal 'agent' string — hardcoded-literal regression:\n{output}"
    );
}
