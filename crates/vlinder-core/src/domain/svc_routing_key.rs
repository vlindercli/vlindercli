//! V2 service routing key — independent of V1 `DataRoutingKey`.
//!
//! Uses `ServiceBackendV2` and `ServiceOperation` instead of
//! V1's `ServiceBackend` and `Operation`. No changes to V1 types.

use super::{
    AgentName, BranchId, Sequence, ServiceBackendV2, ServiceOperation, SessionId, SubmissionId,
};

/// V2 service routing key — used by the harness-mediated dispatch path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SvcRoutingKey {
    pub session: SessionId,
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub kind: SvcMessageKind,
}

/// V2 message kind — parallel to `DataMessageKind` but for the
/// harness‑mediated service dispatch path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum SvcMessageKind {
    SvcRequest {
        agent: AgentName,
        service: ServiceBackendV2,
        operation: ServiceOperation,
        sequence: Sequence,
    },
    SvcResponse {
        agent: AgentName,
        service: ServiceBackendV2,
        operation: ServiceOperation,
        sequence: Sequence,
    },
}

impl SvcMessageKind {
    pub fn kind_str(&self) -> &'static str {
        match self {
            SvcMessageKind::SvcRequest { .. } => "svc_request",
            SvcMessageKind::SvcResponse { .. } => "svc_response",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn svc_message_kind_str() {
        let request = SvcMessageKind::SvcRequest {
            agent: AgentName::new("agent1"),
            service: ServiceBackendV2::Mcp("server-everything".to_string()),
            operation: ServiceOperation::new("echo"),
            sequence: Sequence::first(),
        };
        assert_eq!(request.kind_str(), "svc_request");

        let response = SvcMessageKind::SvcResponse {
            agent: AgentName::new("agent1"),
            service: ServiceBackendV2::Mcp("server-everything".to_string()),
            operation: ServiceOperation::new("echo"),
            sequence: Sequence::first(),
        };
        assert_eq!(response.kind_str(), "svc_response");
    }

    #[test]
    #[allow(clippy::wildcard_enum_match_arm)]
    fn svc_routing_key_construction() {
        let key = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcRequest {
                agent: AgentName::new("agent1"),
                service: ServiceBackendV2::Mcp("brave".to_string()),
                operation: ServiceOperation::new("echo"),
                sequence: Sequence::first(),
            },
        };
        // ensure fields are accessible
        assert_eq!(key.branch.as_i64(), 1);
        assert_eq!(key.kind.kind_str(), "svc_request");
        #[allow(clippy::match_wildcard_for_single_variants)]
        match &key.kind {
            SvcMessageKind::SvcRequest {
                agent,
                service,
                operation,
                sequence,
            } => {
                assert_eq!(agent.as_str(), "agent1");
                assert_eq!(service.backend_str(), "brave");
                assert_eq!(operation.as_str(), "echo");
                assert_eq!(sequence.as_u32(), 1);
            }
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn svc_routing_key_serde_round_trip() {
        let original = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(2),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcResponse {
                agent: AgentName::new("agent2"),
                service: ServiceBackendV2::Mcp("jira".to_string()),
                operation: ServiceOperation::new("get_issue"),
                sequence: Sequence::first().next(),
            },
        };
        let json = serde_json::to_string(&original).unwrap();
        let back: SvcRoutingKey = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn svc_routing_key_hashable() {
        use std::collections::HashSet;
        let key1 = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcRequest {
                agent: AgentName::new("agent1"),
                service: ServiceBackendV2::Mcp("server".to_string()),
                operation: ServiceOperation::new("op"),
                sequence: Sequence::first(),
            },
        };
        let key2 = SvcRoutingKey {
            session: key1.session.clone(),
            branch: key1.branch,
            submission: key1.submission.clone(),
            kind: SvcMessageKind::SvcRequest {
                agent: AgentName::new("agent1"),
                service: ServiceBackendV2::Mcp("server".to_string()),
                operation: ServiceOperation::new("op"),
                sequence: Sequence::first(),
            },
        };
        // same fields, should be equal and hash equal
        assert_eq!(key1, key2);
        let mut set = HashSet::new();
        set.insert(key1);
        assert!(set.contains(&key2));
    }
}
