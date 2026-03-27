//! Conversions between domain `DagNode` and protobuf `DagNode`.

use chrono::{DateTime, Utc};

use super::proto;
use vlinder_core::domain::{
    Branch, BranchId, DagNode, DagNodeId, ObservableMessage, SessionId, SessionSummary,
};

// =============================================================================
// DagNode → proto::DagNode
// =============================================================================

fn dag_node_to_proto(node: &DagNode) -> proto::DagNode {
    let msg = node
        .message
        .as_ref()
        .expect("dag_node_to_proto: message must be present");
    let (from, to) = msg.sender_receiver();
    let message_blob = serde_json::to_string(msg).ok();
    let diagnostics = msg.diagnostics_json();
    let stderr = msg.stderr().to_vec();
    let state = msg.state().map(str::to_string);
    let checkpoint = msg.checkpoint().map(str::to_string);
    let operation = msg.operation().map(str::to_string);

    proto::DagNode {
        hash: node.id.to_string(),
        parent_hash: node.parent_id.to_string(),
        message_type: node.message_type().as_str().to_string(),
        sender: from,
        receiver: to,
        session_id: node.session_id().as_str().to_string(),
        submission_id: node.submission_id().to_string(),
        payload: node.payload().to_vec(),
        diagnostics,
        stderr,
        created_at: node.created_at.to_rfc3339(),
        state,
        protocol_version: node.protocol_version().to_string(),
        checkpoint,
        operation,
        message_blob,
        branch_id: node.branch_id().as_i64(),
    }
}

impl From<DagNode> for proto::DagNode {
    fn from(node: DagNode) -> Self {
        if node.message.is_none() {
            return proto::DagNode {
                hash: node.id.to_string(),
                parent_hash: node.parent_id.to_string(),
                message_type: node.message_type().as_str().to_string(),
                session_id: node.session_id().as_str().to_string(),
                submission_id: node.submission_id().to_string(),
                created_at: node.created_at.to_rfc3339(),
                protocol_version: node.protocol_version().to_string(),
                branch_id: node.branch_id().as_i64(),
                ..Default::default()
            };
        }
        dag_node_to_proto(&node)
    }
}

impl From<&DagNode> for proto::DagNode {
    fn from(node: &DagNode) -> Self {
        dag_node_to_proto(node)
    }
}

// =============================================================================
// proto::DagNode → DagNode
// =============================================================================

impl TryFrom<proto::DagNode> for DagNode {
    type Error = String;

    fn try_from(node: proto::DagNode) -> Result<Self, Self::Error> {
        let created_at: DateTime<Utc> = node
            .created_at
            .parse()
            .map_err(|e| format!("invalid created_at: {e}"))?;

        let msg_type = node
            .message_type
            .parse::<vlinder_core::domain::MessageType>()
            .unwrap_or(vlinder_core::domain::MessageType::Complete);

        let blob = node.message_blob.as_deref().unwrap_or("");

        let session = SessionId::try_from(node.session_id).unwrap_or_else(|_| {
            SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string()).unwrap()
        });
        let submission = vlinder_core::domain::SubmissionId::from(node.submission_id);
        let pv = node.protocol_version.clone();

        // Empty blob = typed table row (invoke via insert_invoke_node).
        if blob.is_empty() {
            return Ok(Self {
                id: DagNodeId::from(node.hash),
                parent_id: DagNodeId::from(node.parent_hash),
                created_at,
                state: vlinder_core::domain::Snapshot::empty(),
                msg_type,
                session,
                submission,
                branch: BranchId::from(node.branch_id),
                protocol_version: pv,
                message: None,
            });
        }

        // V2 blob = typed table row. Callers use get_invoke_node for content.
        if serde_json::from_str::<vlinder_core::domain::DataPlane>(blob).is_ok() {
            return Ok(Self {
                id: DagNodeId::from(node.hash),
                parent_id: DagNodeId::from(node.parent_hash),
                created_at,
                state: vlinder_core::domain::Snapshot::empty(),
                msg_type,
                session,
                submission,
                branch: BranchId::from(node.branch_id),
                protocol_version: pv,
                message: None,
            });
        }

        {
            let mut message: ObservableMessage = serde_json::from_str(blob)
                .map_err(|e| format!("invalid message_blob JSON: {e}"))?;
            if !node.payload.is_empty() {
                message.set_payload(node.payload);
            }
            Ok(Self {
                id: DagNodeId::from(node.hash),
                parent_id: DagNodeId::from(node.parent_hash),
                created_at,
                state: vlinder_core::domain::Snapshot::empty(),
                msg_type,
                session,
                submission,
                protocol_version: pv,
                branch: BranchId::from(node.branch_id),
                message: Some(message),
            })
        }
    }
}

// =============================================================================
// Branch → proto::Branch
// =============================================================================

impl From<Branch> for proto::Branch {
    fn from(b: Branch) -> Self {
        Self {
            id: b.id.as_i64(),
            name: b.name,
            session_id: b.session_id.as_str().to_string(),
            fork_point: b.fork_point.map(|fp| fp.to_string()),
            head: b.head.map(|h| h.to_string()),
            created_at: b.created_at.to_rfc3339(),
            broken_at: b.broken_at.map(|dt| dt.to_rfc3339()),
        }
    }
}

// =============================================================================
// proto::Branch → Branch
// =============================================================================

impl TryFrom<proto::Branch> for Branch {
    type Error = String;

    fn try_from(b: proto::Branch) -> Result<Self, Self::Error> {
        let created_at: DateTime<Utc> = b
            .created_at
            .parse()
            .map_err(|e| format!("invalid created_at: {e}"))?;
        let broken_at = b
            .broken_at
            .map(|s| s.parse::<DateTime<Utc>>())
            .transpose()
            .map_err(|e| format!("invalid broken_at: {e}"))?;

        Ok(Self {
            id: BranchId::from(b.id),
            name: b.name,
            session_id: SessionId::try_from(b.session_id)?,
            fork_point: b.fork_point.map(DagNodeId::from),
            head: b.head.map(DagNodeId::from),
            created_at,
            broken_at,
        })
    }
}

// =============================================================================
// SessionSummary → proto::SessionSummary
// =============================================================================

impl From<SessionSummary> for proto::SessionSummary {
    fn from(s: SessionSummary) -> Self {
        Self {
            session_id: s.session_id.as_str().to_string(),
            agent_name: s.agent_name,
            started_at: s.started_at.to_rfc3339(),
            message_count: s.message_count as u64,
            is_open: s.is_open,
        }
    }
}

// =============================================================================
// proto::SessionSummary → SessionSummary
// =============================================================================

impl TryFrom<proto::SessionSummary> for SessionSummary {
    type Error = String;

    fn try_from(s: proto::SessionSummary) -> Result<Self, Self::Error> {
        let started_at: DateTime<Utc> = s
            .started_at
            .parse()
            .map_err(|e| format!("invalid started_at: {e}"))?;

        Ok(Self {
            session_id: SessionId::try_from(s.session_id)?,
            agent_name: s.agent_name,
            started_at,
            message_count: usize::try_from(s.message_count).unwrap_or(0),
            is_open: s.is_open,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use vlinder_core::domain::workers::dag::build_dag_node;
    use vlinder_core::domain::{
        AgentName, BranchId, DagNodeId, ForkMessage, MessageId, SessionId, Snapshot, SubmissionId,
        PROTOCOL_VERSION,
    };

    fn sample_observable() -> ObservableMessage {
        ObservableMessage::Fork(ForkMessage {
            id: MessageId::new(),
            protocol_version: PROTOCOL_VERSION.to_string(),
            branch: BranchId::from(1),
            submission: SubmissionId::from("sub-001".to_string()),
            session: SessionId::new(),
            agent_name: AgentName::new("agent-echo"),
            branch_name: "test-branch".to_string(),
            fork_point: DagNodeId::from("parent456".to_string()),
        })
    }

    fn sample_dag_node() -> DagNode {
        let msg = sample_observable();
        build_dag_node(
            &msg,
            &DagNodeId::from("parent456".to_string()),
            &Snapshot::empty(),
        )
    }

    #[test]
    fn dag_node_round_trip() {
        let original = sample_dag_node();
        let proto_node: proto::DagNode = original.clone().into();
        let recovered: DagNode = proto_node.try_into().unwrap();

        assert_eq!(recovered.id, original.id);
        assert_eq!(recovered.parent_id, original.parent_id);
        assert_eq!(recovered.message_type(), original.message_type());
        assert_eq!(recovered.session_id(), original.session_id());
        assert_eq!(recovered.submission_id(), original.submission_id());
        assert_eq!(recovered.protocol_version(), original.protocol_version());
    }

    #[test]
    fn dag_node_without_state_round_trips() {
        let msg = sample_observable();
        let node = build_dag_node(&msg, &DagNodeId::root(), &Snapshot::empty());

        let proto_node: proto::DagNode = node.clone().into();
        let recovered: DagNode = proto_node.try_into().unwrap();

        assert_eq!(recovered.message.as_ref().unwrap().state(), None);
    }

    #[test]
    fn missing_message_blob_returns_empty_dag_node() {
        let proto_node = proto::DagNode {
            message_type: "invoke".to_string(),
            created_at: Utc::now().to_rfc3339(),
            message_blob: None,
            ..Default::default()
        };
        let node = DagNode::try_from(proto_node).unwrap();
        assert!(node.message.is_none());
        assert_eq!(
            node.message_type(),
            vlinder_core::domain::MessageType::Invoke
        );
    }

    #[test]
    fn invalid_created_at_fails() {
        let proto_node = proto::DagNode {
            message_type: "complete".to_string(),
            created_at: "not-a-date".to_string(),
            message_blob: Some("{}".to_string()),
            ..Default::default()
        };
        assert!(DagNode::try_from(proto_node).is_err());
    }
}
