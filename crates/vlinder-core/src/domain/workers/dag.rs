//! DAG worker helpers — convert `ObservableMessage` to `DagNode` for storage
//! (ADR 065, 067, 078, 080).
//!
//! `SQLite` recording is handled synchronously by the transactional outbox
//! (`RecordingQueue`, ADR 080). The `dag-git` NATS consumer reconstructs
//! messages via `vlinder-nats` and passes them here for `DagNode` conversion.

use chrono::Utc;

use crate::domain::message::SessionPlane;
use crate::domain::{hash_dag_node, DagNode, DagNodeId, Instance, Snapshot, StateHash};

/// Build a `DagNode` from an `ObservableMessage` and Merkle chain state.
///
/// Computes the content-addressed hash and wraps the message with DAG metadata.
/// The snapshot is initialized from the message's state if present, otherwise
/// empty. Callers that have the parent node should call `with_parent_snapshot`
/// to inherit unchanged store states.
pub fn build_dag_node(
    msg: &SessionPlane,
    parent_id: &DagNodeId,
    parent_state: &Snapshot,
) -> DagNode {
    let id = hash_dag_node(
        msg.payload(),
        parent_id,
        &msg.message_type(),
        &msg.diagnostics_json(),
        msg.session(),
    );

    // Inherit parent's snapshot, updating the KV store entry if this message
    // carries a state hash.
    let state = match msg.state() {
        Some(s) if !s.is_empty() => {
            parent_state.with_state(Instance::from("kv"), StateHash::from(s.to_string()))
        }
        _ => parent_state.clone(),
    };

    DagNode {
        id,
        parent_id: parent_id.clone(),
        created_at: Utc::now(),
        state,
        msg_type: msg.message_type(),
        session: msg.session().clone(),
        submission: msg.submission().clone(),
        branch: *msg.branch(),
        protocol_version: msg.protocol_version().to_string(),
        message: Some(msg.clone()),
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{BranchId, MessageType, SessionId, SubmissionId};

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
            message: None,
        };

        assert_eq!(node.message_type(), MessageType::Invoke);
        assert_eq!(*node.session_id(), session);
        assert_eq!(*node.submission_id(), sub);
        assert_eq!(*node.branch_id(), BranchId::from(1));
        // message: None for typed-table invoke rows — payload returns empty
        assert_eq!(node.payload(), b"");
        assert_eq!(node.protocol_version(), "v1");
    }

    #[test]
    fn dag_node_message_none_returns_empty_payload() {
        // A DagNode can have `message: None` for typed-table rows (invoke).
        // This is not an error — payload() simply returns empty.
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
            message: None,
        };
        assert_eq!(node.payload(), b"");
    }
}
