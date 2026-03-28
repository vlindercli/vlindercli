//! DAG worker helpers — convert typed messages to `DagNode` for storage
//! (ADR 065, 067, 078, 080).
//!
//! `SQLite` recording is handled synchronously by the transactional outbox
//! (`RecordingQueue`, ADR 080). The `dag-git` NATS consumer reconstructs
//! messages via `vlinder-nats` and passes them here for `DagNode` conversion.

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use crate::domain::{
        BranchId, DagNode, DagNodeId, MessageType, SessionId, Snapshot, SubmissionId,
    };

    fn session() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }
    fn submission() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }

    // --- DagNode accessors on invoke ---

    #[test]
    fn dag_node_accessors_on_invoke() {
        let session = session();
        let sub = submission();

        let node = DagNode {
            id: DagNodeId::from("test-hash".to_string()),
            parent_id: DagNodeId::root(),
            created_at: chrono::Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Invoke,
            session: session.clone(),
            submission: sub.clone(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };

        assert_eq!(node.message_type(), MessageType::Invoke);
        assert_eq!(*node.session_id(), session);
        assert_eq!(*node.submission_id(), sub);
        assert_eq!(*node.branch_id(), BranchId::from(1));
        assert_eq!(node.payload(), b"");
        assert_eq!(node.protocol_version(), "v1");
    }

    #[test]
    fn dag_node_returns_empty_payload() {
        let node = DagNode {
            id: DagNodeId::from("empty".to_string()),
            parent_id: DagNodeId::root(),
            created_at: chrono::Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Invoke,
            session: session(),
            submission: submission(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };
        assert_eq!(node.payload(), b"");
    }
}
