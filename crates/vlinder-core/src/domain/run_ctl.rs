//! Harness execution controller — channel-based event emission and approval mode.
//!
//! `RunCtl` is a lightweight handle that the harness carries through the
//! execution loop. It wraps an optional event sender and an approval mode,
//! giving the caller (CLI, API server, tests) control over how execution
//! events are surfaced and whether tool calls require human approval.

use tokio::sync::mpsc;

use super::HarnessEvent;

/// Controls event emission and approval mode during harness execution.
///
/// - `quiet()` — no events emitted, autonomous dispatch (the default).
/// - `streaming(tx)` — events sent to `tx`, autonomous dispatch.
/// - `streaming_with_approval(tx)` — events sent to `tx`, each tool call
///   waits for an `ApprovalDecision` before dispatching.
#[allow(dead_code)]
pub struct RunCtl {
    events: Option<mpsc::Sender<HarnessEvent>>,
    approval: ApprovalMode,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ApprovalMode {
    Autonomous,
    RequireApproval,
}

#[allow(dead_code)]
impl RunCtl {
    /// No events, autonomous dispatch. Preserves today's exact behavior.
    pub fn quiet() -> Self {
        Self {
            events: None,
            approval: ApprovalMode::Autonomous,
        }
    }

    /// Events emitted, autonomous dispatch.
    pub fn streaming(events: mpsc::Sender<HarnessEvent>) -> Self {
        Self {
            events: Some(events),
            approval: ApprovalMode::Autonomous,
        }
    }

    /// Events emitted, tool calls require human approval.
    pub fn streaming_with_approval(events: mpsc::Sender<HarnessEvent>) -> Self {
        Self {
            events: Some(events),
            approval: ApprovalMode::RequireApproval,
        }
    }

    /// Emit an event if the sender is configured. Silently drops if the
    /// receiver has been dropped (caller hung up — do not fail the run).
    pub async fn emit(&self, event: HarnessEvent) {
        if let Some(tx) = &self.events {
            let _ = tx.send(event).await;
        }
    }

    /// Whether the harness should wait for human approval before dispatching
    /// each tool call.
    pub fn requires_approval(&self) -> bool {
        matches!(self.approval, ApprovalMode::RequireApproval)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::BranchId;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn quiet_discards_events() {
        let ctl = RunCtl::quiet();
        // Emit a dummy event — should not panic
        ctl.emit(HarnessEvent::RunStarted {
            agent_id: crate::domain::ResourceId::new("agent://test"),
            session_id: crate::domain::SessionId::new(),
            timeline: crate::domain::BranchId::from(1),
        })
        .await;
        // No panic = pass
    }

    #[tokio::test]
    async fn streaming_delivers_events() {
        let (tx, mut rx) = mpsc::channel(8);
        let ctl = RunCtl::streaming(tx);

        let session_id = crate::domain::SessionId::new();
        let timeline = crate::domain::BranchId::from(42);

        ctl.emit(HarnessEvent::RunStarted {
            agent_id: crate::domain::ResourceId::new("agent://test"),
            session_id: session_id.clone(),
            timeline,
        })
        .await;

        let evt = rx.recv().await.unwrap();
        match evt {
            HarnessEvent::RunStarted {
                agent_id,
                session_id: sid,
                timeline: tl,
            } => {
                assert_eq!(agent_id.as_str(), "agent://test");
                assert_eq!(sid, session_id);
                assert_eq!(tl, BranchId::from(42));
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn streaming_preserves_order() {
        let (tx, mut rx) = mpsc::channel(8);
        let ctl = RunCtl::streaming(tx);

        ctl.emit(HarnessEvent::TurnStarted { turn_index: 0 }).await;
        ctl.emit(HarnessEvent::TurnStarted { turn_index: 1 }).await;
        ctl.emit(HarnessEvent::TurnStarted { turn_index: 2 }).await;

        assert_eq!(
            rx.recv().await.map(|e| match e {
                HarnessEvent::TurnStarted { turn_index } => turn_index,
                _ => 999,
            }),
            Some(0)
        );
        assert_eq!(
            rx.recv().await.map(|e| match e {
                HarnessEvent::TurnStarted { turn_index } => turn_index,
                _ => 999,
            }),
            Some(1)
        );
        assert_eq!(
            rx.recv().await.map(|e| match e {
                HarnessEvent::TurnStarted { turn_index } => turn_index,
                _ => 999,
            }),
            Some(2)
        );
    }

    #[test]
    fn requires_approval_toggles() {
        let (tx, _) = mpsc::channel(8);
        let autonomous = RunCtl::streaming(tx.clone());
        let approval = RunCtl::streaming_with_approval(tx);
        let quiet = RunCtl::quiet();

        assert!(!autonomous.requires_approval());
        assert!(approval.requires_approval());
        assert!(!quiet.requires_approval());
    }
}
