//! Integration tests for the time-travel workflow (ADR 081, 082).
//!
//! These tests use `GitDagWorker` directly to create realistic conversation
//! repos with proper trailers, then exercise timeline operations on them.
//! Each test gets its own isolated `VLINDER_DIR` under the date-stamped run
//! directory.
//!
//! Requires: `just run-integration-tests` (sets `VLINDER_INTEGRATION_RUN`)

mod common;

use common::*;

use vlinder_core::domain::DagWorker;
use vlinder_git_dag::GitDagWorker;

// ============================================================================
// Test: checkout shows trailers and state
// ============================================================================

#[test]
#[ignore = "requires vlinder daemon and NATS"]
fn checkout_shows_trailers_and_state() {
    let vlinder_dir = test_vlinder_dir("checkout_shows_trailers_and_state");
    let conv_dir = conversations_path(&vlinder_dir);
    let mut worker = test_conversations_worker(&vlinder_dir);

    // Write invoke (no state) + complete (with state)
    let (key, invoke, t1) = make_invoke(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"question",
        None,
        1000,
    );
    worker.on_invoke(&key, &invoke, t1);

    let (ckey, complete, t2) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"answer",
        Some("state-abc123".to_string()),
        1001,
    );
    worker.on_complete(&ckey, &complete, t2);

    // HEAD is the complete commit — should have all three trailers
    let head = read_head_sha(&conv_dir).expect("HEAD should exist");
    assert_eq!(
        read_trailer(&conv_dir, &head, "Session").as_deref(),
        Some("d4761d76-dee4-4ebf-9df4-43b52efa4f78"),
    );
    assert_eq!(
        read_trailer(&conv_dir, &head, "Submission").as_deref(),
        Some("sub-1"),
    );
    assert_eq!(
        read_trailer(&conv_dir, &head, "State").as_deref(),
        Some("state-abc123"),
    );

    // First commit (invoke) — has Session and Submission, but no State
    let commits = git(&conv_dir, &["rev-list", "--reverse", "main"]).expect("should list commits");
    let first_commit = commits
        .lines()
        .next()
        .expect("should have at least one commit");

    assert_eq!(
        read_trailer(&conv_dir, first_commit, "Session").as_deref(),
        Some("d4761d76-dee4-4ebf-9df4-43b52efa4f78"),
    );
    assert_eq!(
        read_trailer(&conv_dir, first_commit, "Submission").as_deref(),
        Some("sub-1"),
    );
    assert!(
        read_trailer(&conv_dir, first_commit, "State").is_none(),
        "invoke without state should not have State trailer",
    );
}

// ============================================================================
// Test: promote moves main and labels old
// ============================================================================

#[test]
#[ignore = "requires vlinder daemon and NATS"]
fn promote_moves_main_and_labels_old() {
    let vlinder_dir = test_vlinder_dir("promote_moves_main_and_labels_old");
    let conv_dir = conversations_path(&vlinder_dir);
    let mut worker = test_conversations_worker(&vlinder_dir);

    // Write invoke + complete
    let (key, invoke, t1) = make_invoke(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"q",
        None,
        1000,
    );
    worker.on_invoke(&key, &invoke, t1);
    let (ckey, complete, t2) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"a",
        Some("state-1".to_string()),
        1001,
    );
    worker.on_complete(&ckey, &complete, t2);

    // Record original main SHA
    let original_main = git(&conv_dir, &["rev-parse", "main"]).expect("main should exist");

    // Get first commit (invoke)
    let commits = git(&conv_dir, &["rev-list", "--reverse", "main"]).unwrap();
    let invoke_sha = commits.lines().next().unwrap().to_string();

    // Create a fix branch at the invoke commit
    git(&conv_dir, &["checkout", "-b", "fix-branch", &invoke_sha]).unwrap();

    // Simulate promote workflow:
    // 1. Label old main as broken-*
    let broken_name = "broken-test";
    git(&conv_dir, &["branch", broken_name, "main"]).unwrap();

    // 2. Move main to current HEAD (invoke commit)
    git(&conv_dir, &["branch", "-f", "main", "HEAD"]).unwrap();

    // 3. Switch to main
    git(&conv_dir, &["checkout", "main"]).unwrap();

    // Verify: broken-test points to original main
    let broken_sha = git(&conv_dir, &["rev-parse", broken_name]).unwrap();
    assert_eq!(broken_sha, original_main);

    // Verify: main now points to invoke commit
    let new_main = git(&conv_dir, &["rev-parse", "main"]).unwrap();
    assert_eq!(new_main, invoke_sha);

    // Verify: broken-test has 2 commits (invoke + complete), main has 1 (invoke only)
    let broken_count = git(&conv_dir, &["rev-list", "--count", broken_name]).unwrap();
    assert_eq!(broken_count, "2");

    let main_count = git(&conv_dir, &["rev-list", "--count", "main"]).unwrap();
    assert_eq!(main_count, "1");
}

// ============================================================================
// Test: fork creates independent branch
// ============================================================================

#[test]
#[ignore = "requires vlinder daemon and NATS"]
fn fork_creates_independent_branch() {
    let vlinder_dir = test_vlinder_dir("fork_creates_independent_branch");
    let conv_dir = conversations_path(&vlinder_dir);
    let mut worker = test_conversations_worker(&vlinder_dir);

    // Write invoke + complete on main
    let (key, invoke, t1) = make_invoke(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"question",
        None,
        1000,
    );
    worker.on_invoke(&key, &invoke, t1);
    let (ckey, complete, t2) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"original-answer",
        Some("state-1".to_string()),
        1001,
    );
    worker.on_complete(&ckey, &complete, t2);

    // Main should have 2 commits
    let main_count = git(&conv_dir, &["rev-list", "--count", "main"]).unwrap();
    assert_eq!(main_count, "2");

    // Get invoke SHA
    let commits = git(&conv_dir, &["rev-list", "--reverse", "main"]).unwrap();
    let invoke_sha = commits.lines().next().unwrap().to_string();

    // Checkout invoke (detached HEAD)
    git(&conv_dir, &["checkout", &invoke_sha]).unwrap();

    // Create repair branch
    git(&conv_dir, &["checkout", "-b", "repair-test"]).unwrap();

    // Write a new complete on the repair branch (different answer)
    // Re-open the worker on the repair branch
    let mut repair_worker = GitDagWorker::open(&conv_dir, "test.local:9000", None).unwrap();
    let (alt_ckey, alt_complete, t3) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"repaired-answer",
        Some("state-2".to_string()),
        1002,
    );
    repair_worker.on_complete(&alt_ckey, &alt_complete, t3);

    // Verify: repair-test has 2 commits (invoke + alt_complete)
    let repair_count = git(&conv_dir, &["rev-list", "--count", "repair-test"]).unwrap();
    assert_eq!(repair_count, "2");

    // Verify: main still has 2 commits (invoke + original_complete)
    let main_count = git(&conv_dir, &["rev-list", "--count", "main"]).unwrap();
    assert_eq!(main_count, "2");

    // Verify: the tips are different (they diverged after invoke)
    let main_tip = git(&conv_dir, &["rev-parse", "main"]).unwrap();
    let repair_tip = git(&conv_dir, &["rev-parse", "repair-test"]).unwrap();
    assert_ne!(main_tip, repair_tip);

    // Verify: they share the same root (invoke commit)
    let merge_base = git(&conv_dir, &["merge-base", "main", "repair-test"]).unwrap();
    assert_eq!(merge_base, invoke_sha);

    // Verify: the repair branch has the repaired payload
    let repair_head = git(&conv_dir, &["rev-parse", "repair-test"]).unwrap();
    let repair_state = read_trailer(&conv_dir, &repair_head, "State");
    assert_eq!(repair_state.as_deref(), Some("state-2"));
}

// ============================================================================
// Test: checkout then promote full workflow
// ============================================================================

#[test]
#[ignore = "requires vlinder daemon and NATS"]
#[allow(clippy::too_many_lines)]
fn checkout_then_promote_full_workflow() {
    let vlinder_dir = test_vlinder_dir("checkout_then_promote_full_workflow");
    let conv_dir = conversations_path(&vlinder_dir);
    let mut worker = test_conversations_worker(&vlinder_dir);

    // Build a multi-turn conversation: invoke1 → complete1 → invoke2 → complete2
    let (key1, invoke1, t1) = make_invoke(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"turn-1-question",
        None,
        1000,
    );
    worker.on_invoke(&key1, &invoke1, t1);

    let (ckey1, complete1, t2) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"turn-1-answer",
        Some("state-after-turn-1".to_string()),
        1001,
    );
    worker.on_complete(&ckey1, &complete1, t2);

    let (key2, invoke2, t3) = make_invoke(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-2",
        b"turn-2-question",
        Some("state-after-turn-1".to_string()),
        1002,
    );
    worker.on_invoke(&key2, &invoke2, t3);

    let (ckey2, complete2, t4) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-2",
        b"turn-2-answer",
        Some("state-after-turn-2".to_string()),
        1003,
    );
    worker.on_complete(&ckey2, &complete2, t4);

    // Verify: main has 4 commits
    let main_count = git(&conv_dir, &["rev-list", "--count", "main"]).unwrap();
    assert_eq!(main_count, "4");

    // Record the original main SHA (at complete2)
    let original_main = git(&conv_dir, &["rev-parse", "main"]).unwrap();

    // Find complete1 — it's the 2nd commit (index 1)
    let commits = git(&conv_dir, &["rev-list", "--reverse", "main"]).unwrap();
    let commit_list: Vec<&str> = commits.lines().collect();
    assert_eq!(commit_list.len(), 4);
    let complete1_sha = commit_list[1].to_string();

    // Verify complete1 has the expected state trailer
    assert_eq!(
        read_trailer(&conv_dir, &complete1_sha, "State").as_deref(),
        Some("state-after-turn-1"),
    );

    // === Checkout complete1 (detached HEAD) ===
    git(&conv_dir, &["checkout", &complete1_sha]).unwrap();

    // === Create repair branch ===
    git(&conv_dir, &["checkout", "-b", "repair-branch"]).unwrap();

    // Write a new alternative complete (different answer for turn 1)
    let mut repair_worker = GitDagWorker::open(&conv_dir, "test.local:9000", None).unwrap();
    let (alt_ckey, alt_complete, t5) = make_complete(
        "d4761d76-dee4-4ebf-9df4-43b52efa4f78",
        "sub-1",
        b"turn-1-repaired-answer",
        Some("state-repaired".to_string()),
        1004,
    );
    repair_worker.on_complete(&alt_ckey, &alt_complete, t5);

    // Verify: repair-branch has 3 commits (invoke1 + complete1 + alt_complete)
    let repair_count = git(&conv_dir, &["rev-list", "--count", "repair-branch"]).unwrap();
    assert_eq!(repair_count, "3");

    // === Promote: label old main, move main to repair, switch ===
    let broken_name = "broken-original";
    git(&conv_dir, &["branch", broken_name, "main"]).unwrap();
    git(&conv_dir, &["branch", "-f", "main", "HEAD"]).unwrap();
    git(&conv_dir, &["checkout", "main"]).unwrap();

    // Verify: broken-original still points to the original 4-commit history
    let broken_sha = git(&conv_dir, &["rev-parse", broken_name]).unwrap();
    assert_eq!(broken_sha, original_main);
    let broken_count = git(&conv_dir, &["rev-list", "--count", broken_name]).unwrap();
    assert_eq!(broken_count, "4");

    // Verify: main now points to the repaired 3-commit history
    let new_main = git(&conv_dir, &["rev-parse", "main"]).unwrap();
    let new_main_count = git(&conv_dir, &["rev-list", "--count", "main"]).unwrap();
    assert_eq!(new_main_count, "3");

    // Verify: new main's HEAD has the repaired state
    let new_state = read_trailer(&conv_dir, &new_main, "State");
    assert_eq!(new_state.as_deref(), Some("state-repaired"));

    // Verify: the histories share invoke1 as common ancestor
    let all_main_commits = git(&conv_dir, &["rev-list", "--reverse", "main"]).unwrap();
    let all_broken_commits = git(&conv_dir, &["rev-list", "--reverse", broken_name]).unwrap();
    let main_list: Vec<&str> = all_main_commits.lines().collect();
    let broken_list: Vec<&str> = all_broken_commits.lines().collect();

    // First two commits (invoke1, complete1) should be shared
    assert_eq!(main_list[0], broken_list[0], "invoke1 should be shared");
    assert_eq!(main_list[1], broken_list[1], "complete1 should be shared");

    // Third commit diverges
    assert_ne!(
        main_list[2], broken_list[2],
        "should diverge after complete1"
    );
}
