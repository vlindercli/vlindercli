//! AMQP routing key construction, parsing, and binding patterns.
//!
//! Routing keys use the same dot-delimited format as NATS subjects.
//! Binding patterns use `*` (one word) and `#` (zero or more words)
//! for AMQP topic exchange filtering.

use std::str::FromStr;

use vlinder_core::domain::{
    AgentName, BranchId, DataMessageKind, DataRoutingKey, HarnessType, InfraMessageKind,
    InfraRoutingKey, Operation, RuntimeType, Sequence, ServiceBackend, ServiceType, SessionId,
    SessionMessageKind, SessionRoutingKey, SubmissionId,
};

// ============================================================================
// Invoke
//
// Format: vlinder.data.v1.{session}.{branch}.{submission}.invoke.{harness}.{runtime}.{agent}
// Index:  0       1    2   3         4        5            6      7         8         9
// ============================================================================

const DATA_PREFIX: &str = "vlinder.data.v1";
const INVOKE_KIND: &str = "invoke";
const INVOKE_SEGMENT_COUNT: usize = 10;

pub(crate) fn invoke_routing_key(key: &DataRoutingKey) -> String {
    let DataMessageKind::Invoke {
        harness,
        runtime,
        agent,
    } = &key.kind
    else {
        panic!("invoke_routing_key called with non-Invoke key");
    };
    format!(
        "{DATA_PREFIX}.{}.{}.{}.{INVOKE_KIND}.{harness}.{runtime}.{agent}",
        key.session, key.branch, key.submission
    )
}

pub(crate) fn invoke_parse(routing_key: &str) -> Option<DataRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != INVOKE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != INVOKE_KIND {
        return None;
    }

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Invoke {
            harness: HarnessType::from_str(s[7]).ok()?,
            runtime: RuntimeType::from_str(s[8]).ok()?,
            agent: AgentName::new(s[9]),
        },
    })
}

pub(crate) fn invoke_binding(agent: &AgentName) -> String {
    format!("{DATA_PREFIX}.*.*.*.{INVOKE_KIND}.*.*.{}", agent.as_str())
}

// ============================================================================
// Complete
//
// Format: vlinder.data.v1.{session}.{branch}.{submission}.complete.{agent}.{harness}
// Index:  0       1    2   3         4        5            6        7       8
// ============================================================================

const COMPLETE_KIND: &str = "complete";
const COMPLETE_SEGMENT_COUNT: usize = 9;

pub(crate) fn complete_routing_key(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    harness: HarnessType,
) -> String {
    format!("{DATA_PREFIX}.{session}.{branch}.{submission}.{COMPLETE_KIND}.{agent}.{harness}")
}

pub(crate) fn complete_parse(routing_key: &str) -> Option<DataRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != COMPLETE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != COMPLETE_KIND {
        return None;
    }

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Complete {
            agent: AgentName::new(s[7]),
            harness: HarnessType::from_str(s[8]).ok()?,
        },
    })
}

pub(crate) fn complete_binding(
    submission: &SubmissionId,
    agent: &AgentName,
    harness: HarnessType,
) -> String {
    format!(
        "{DATA_PREFIX}.*.*.{}.{COMPLETE_KIND}.{}.{}",
        submission.as_str(),
        agent.as_str(),
        harness.as_str()
    )
}

// ============================================================================
// Request
//
// Format: vlinder.data.v1.{session}.{branch}.{sub}.request.{agent}.{svc_type}.{backend}.{op}.{seq}
// Index:  0       1    2   3         4        5     6       7       8          9         10   11
// ============================================================================

const REQUEST_KIND: &str = "request";
const REQUEST_SEGMENT_COUNT: usize = 12;

pub(crate) fn request_routing_key(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{DATA_PREFIX}.{session}.{branch}.{submission}.{REQUEST_KIND}.{agent}.{}.{}.{}.{}",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

pub(crate) fn request_parse(routing_key: &str) -> Option<DataRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != REQUEST_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != REQUEST_KIND {
        return None;
    }

    let service = ServiceBackend::from_parts(ServiceType::from_str(s[8]).ok()?, s[9])?;
    let operation: Operation = s[10].parse().ok()?;
    let sequence = Sequence::from(s[11].parse::<u32>().ok()?);

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Request {
            agent: AgentName::new(s[7]),
            service,
            operation,
            sequence,
        },
    })
}

pub(crate) fn request_binding(service: ServiceBackend, operation: Operation) -> String {
    format!(
        "{DATA_PREFIX}.*.*.*.{REQUEST_KIND}.*.{}.{}.{}.#",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
    )
}

// ============================================================================
// Response
//
// Format: vlinder.data.v1.{session}.{branch}.{sub}.response.{agent}.{svc_type}.{backend}.{op}.{seq}
// Index:  0       1    2   3         4        5     6        7       8          9         10   11
// ============================================================================

const RESPONSE_KIND: &str = "response";
const RESPONSE_SEGMENT_COUNT: usize = 12;

pub(crate) fn response_routing_key(
    session: &SessionId,
    branch: BranchId,
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{DATA_PREFIX}.{session}.{branch}.{submission}.{RESPONSE_KIND}.{agent}.{}.{}.{}.{}",
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

pub(crate) fn response_parse(routing_key: &str) -> Option<DataRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != RESPONSE_SEGMENT_COUNT {
        return None;
    }
    if s[0] != "vlinder" || s[1] != "data" || s[2] != "v1" || s[6] != RESPONSE_KIND {
        return None;
    }

    let service = ServiceBackend::from_parts(ServiceType::from_str(s[8]).ok()?, s[9])?;
    let operation: Operation = s[10].parse().ok()?;
    let sequence = Sequence::from(s[11].parse::<u32>().ok()?);

    Some(DataRoutingKey {
        session: SessionId::try_from(s[3].to_string()).ok()?,
        branch: BranchId::from(s[4].parse::<i64>().unwrap_or(0)),
        submission: SubmissionId::from(s[5].to_string()),
        kind: DataMessageKind::Response {
            agent: AgentName::new(s[7]),
            service,
            operation,
            sequence,
        },
    })
}

pub(crate) fn response_binding(
    submission: &SubmissionId,
    agent: &AgentName,
    service: ServiceBackend,
    operation: Operation,
    sequence: Sequence,
) -> String {
    format!(
        "{DATA_PREFIX}.*.*.{}.{RESPONSE_KIND}.{}.{}.{}.{}.{}",
        submission.as_str(),
        agent.as_str(),
        service.service_type(),
        service.backend_str(),
        operation.as_str(),
        sequence.as_u32(),
    )
}

// ============================================================================
// Fork
//
// Format: vlinder.{session}.{branch}.{submission}.fork.{agent}
// ============================================================================

pub(crate) fn fork_routing_key(key: &SessionRoutingKey, agent_name: &AgentName) -> String {
    format!(
        "vlinder.{}.1.{}.fork.{}",
        key.session, key.submission, agent_name
    )
}

pub(crate) fn fork_parse(routing_key: &str) -> Option<SessionRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != 6 || s[0] != "vlinder" || s[4] != "fork" {
        return None;
    }
    Some(SessionRoutingKey {
        session: SessionId::try_from(s[1].to_string()).ok()?,
        submission: SubmissionId::from(s[3].to_string()),
        kind: SessionMessageKind::Fork {
            agent_name: AgentName::new(s[5]),
        },
    })
}

pub(crate) fn fork_binding() -> &'static str {
    "vlinder.*.*.*.fork.*"
}

// ============================================================================
// Promote
//
// Format: vlinder.{session}.{branch}.{submission}.promote.{agent}
// ============================================================================

pub(crate) fn promote_routing_key(
    key: &SessionRoutingKey,
    agent_name: &AgentName,
    branch_id: BranchId,
) -> String {
    format!(
        "vlinder.{}.{}.{}.promote.{}",
        key.session, branch_id, key.submission, agent_name
    )
}

pub(crate) fn promote_parse(routing_key: &str) -> Option<SessionRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != 6 || s[0] != "vlinder" || s[4] != "promote" {
        return None;
    }
    Some(SessionRoutingKey {
        session: SessionId::try_from(s[1].to_string()).ok()?,
        submission: SubmissionId::from(s[3].to_string()),
        kind: SessionMessageKind::Promote {
            agent_name: AgentName::new(s[5]),
        },
    })
}

pub(crate) fn promote_binding() -> &'static str {
    "vlinder.*.*.*.promote.*"
}

// ============================================================================
// Infra: Deploy Agent
//
// Format: vlinder.infra.v1.{submission}.deploy-agent
// ============================================================================

pub(crate) fn deploy_agent_routing_key(key: &InfraRoutingKey) -> String {
    format!("vlinder.infra.v1.{}.deploy-agent", key.submission)
}

pub(crate) fn deploy_agent_parse(routing_key: &str) -> Option<InfraRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != 5
        || s[0] != "vlinder"
        || s[1] != "infra"
        || s[2] != "v1"
        || s[4] != "deploy-agent"
    {
        return None;
    }
    Some(InfraRoutingKey {
        submission: SubmissionId::from(s[3].to_string()),
        kind: InfraMessageKind::DeployAgent,
    })
}

pub(crate) fn deploy_agent_binding() -> &'static str {
    "vlinder.infra.v1.*.deploy-agent"
}

// ============================================================================
// Infra: Delete Agent
//
// Format: vlinder.infra.v1.{submission}.delete-agent
// ============================================================================

pub(crate) fn delete_agent_routing_key(key: &InfraRoutingKey) -> String {
    format!("vlinder.infra.v1.{}.delete-agent", key.submission)
}

pub(crate) fn delete_agent_parse(routing_key: &str) -> Option<InfraRoutingKey> {
    let s: Vec<&str> = routing_key.split('.').collect();
    if s.len() != 5
        || s[0] != "vlinder"
        || s[1] != "infra"
        || s[2] != "v1"
        || s[4] != "delete-agent"
    {
        return None;
    }
    Some(InfraRoutingKey {
        submission: SubmissionId::from(s[3].to_string()),
        kind: InfraMessageKind::DeleteAgent,
    })
}

pub(crate) fn delete_agent_binding() -> &'static str {
    "vlinder.infra.v1.*.delete-agent"
}

// ============================================================================
// Consumer queue name derivation
// ============================================================================

pub(crate) fn binding_to_queue_name(binding: &str) -> String {
    binding
        .replace('.', "-")
        .replace('*', "W")
        .replace('#', "H")
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{EmbeddingBackendType, InferenceBackendType, ObjectStorageType};

    fn session() -> SessionId {
        SessionId::try_from("00000000-0000-4000-8000-000000000001".to_string()).unwrap()
    }
    fn branch() -> BranchId {
        BranchId::from(1)
    }
    fn submission() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }
    fn agent() -> AgentName {
        AgentName::new("echo")
    }

    // ── Invoke ──

    #[test]
    fn invoke_routing_key_format() {
        let key = DataRoutingKey {
            session: session(),
            branch: branch(),
            submission: submission(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: agent(),
            },
        };
        let rk = invoke_routing_key(&key);
        assert_eq!(
            rk,
            "vlinder.data.v1.00000000-0000-4000-8000-000000000001.1.sub-1.invoke.cli.container.echo"
        );
    }

    #[test]
    fn invoke_round_trip() {
        let key = DataRoutingKey {
            session: session(),
            branch: branch(),
            submission: submission(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: agent(),
            },
        };
        let rk = invoke_routing_key(&key);
        let parsed = invoke_parse(&rk).unwrap();
        assert_eq!(parsed.submission, key.submission);
        assert_eq!(parsed.kind, key.kind);
    }

    #[test]
    fn invoke_binding_uses_wildcards() {
        let b = invoke_binding(&agent());
        assert_eq!(b, "vlinder.data.v1.*.*.*.invoke.*.*.echo");
    }

    #[test]
    fn invoke_parse_rejects_wrong_kind() {
        assert!(invoke_parse("vlinder.data.v1.s.1.sub.complete.agent.cli").is_none());
    }

    #[test]
    fn invoke_parse_rejects_wrong_segment_count() {
        assert!(invoke_parse("vlinder.data.v1.too.few").is_none());
    }

    // ── Complete ──

    #[test]
    fn complete_routing_key_format() {
        let rk = complete_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            HarnessType::Grpc,
        );
        assert_eq!(
            rk,
            "vlinder.data.v1.00000000-0000-4000-8000-000000000001.1.sub-1.complete.echo.grpc"
        );
    }

    #[test]
    fn complete_round_trip() {
        let rk = complete_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            HarnessType::Cli,
        );
        let parsed = complete_parse(&rk).unwrap();
        assert_eq!(parsed.submission, submission());
        if let DataMessageKind::Complete { agent: a, harness } = &parsed.kind {
            assert_eq!(a.as_str(), "echo");
            assert_eq!(*harness, HarnessType::Cli);
        } else {
            panic!("expected Complete kind");
        }
    }

    #[test]
    fn complete_binding_filters_by_submission_and_agent() {
        let b = complete_binding(&submission(), &agent(), HarnessType::Cli);
        assert_eq!(b, "vlinder.data.v1.*.*.sub-1.complete.echo.cli");
    }

    // ── Request ──

    #[test]
    fn request_routing_key_format() {
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);
        let rk = request_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            service,
            Operation::Get,
            Sequence::from(1),
        );
        assert!(rk.contains(".request.echo.kv.sqlite.get.1"));
    }

    #[test]
    fn request_round_trip() {
        let service = ServiceBackend::Infer(InferenceBackendType::OpenRouter);
        let key_str = request_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            service,
            Operation::Run,
            Sequence::from(3),
        );
        let parsed = request_parse(&key_str).unwrap();
        if let DataMessageKind::Request {
            service: s,
            operation,
            sequence,
            ..
        } = &parsed.kind
        {
            assert_eq!(*s, service);
            assert_eq!(*operation, Operation::Run);
            assert_eq!(sequence.as_u32(), 3);
        } else {
            panic!("expected Request kind");
        }
    }

    #[test]
    fn request_binding_uses_hash_wildcard() {
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);
        let b = request_binding(service, Operation::Get);
        assert!(b.ends_with(".#"));
    }

    // ── Response ──

    #[test]
    fn response_routing_key_format() {
        let service = ServiceBackend::Embed(EmbeddingBackendType::Ollama);
        let rk = response_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            service,
            Operation::Run,
            Sequence::from(2),
        );
        assert!(rk.contains(".response.echo.embed.ollama.run.2"));
    }

    #[test]
    fn response_round_trip() {
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);
        let key_str = response_routing_key(
            &session(),
            branch(),
            &submission(),
            &agent(),
            service,
            Operation::Get,
            Sequence::from(5),
        );
        let parsed = response_parse(&key_str).unwrap();
        assert_eq!(parsed.submission, submission());
        if let DataMessageKind::Response { sequence, .. } = &parsed.kind {
            assert_eq!(sequence.as_u32(), 5);
        } else {
            panic!("expected Response kind");
        }
    }

    #[test]
    fn response_binding_includes_all_filter_params() {
        let service = ServiceBackend::Kv(ObjectStorageType::Sqlite);
        let b = response_binding(
            &submission(),
            &agent(),
            service,
            Operation::Get,
            Sequence::from(1),
        );
        assert!(b.contains("sub-1"));
        assert!(b.contains("echo"));
        assert!(b.contains("kv.sqlite.get.1"));
    }

    // ── Fork ──

    #[test]
    fn fork_round_trip() {
        let key = SessionRoutingKey {
            session: session(),
            submission: submission(),
            kind: SessionMessageKind::Fork {
                agent_name: agent(),
            },
        };
        let rk = fork_routing_key(&key, &agent());
        let parsed = fork_parse(&rk).unwrap();
        assert_eq!(parsed.session, key.session);
        assert_eq!(parsed.submission, key.submission);
    }

    #[test]
    fn fork_binding_pattern() {
        assert_eq!(fork_binding(), "vlinder.*.*.*.fork.*");
    }

    // ── Promote ──

    #[test]
    fn promote_round_trip() {
        let key = SessionRoutingKey {
            session: session(),
            submission: submission(),
            kind: SessionMessageKind::Promote {
                agent_name: agent(),
            },
        };
        let rk = promote_routing_key(&key, &agent(), branch());
        let parsed = promote_parse(&rk).unwrap();
        assert_eq!(parsed.session, key.session);
        assert_eq!(parsed.submission, key.submission);
    }

    #[test]
    fn promote_binding_pattern() {
        assert_eq!(promote_binding(), "vlinder.*.*.*.promote.*");
    }

    // ── Deploy Agent ──

    #[test]
    fn deploy_agent_round_trip() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeployAgent,
        };
        let rk = deploy_agent_routing_key(&key);
        let parsed = deploy_agent_parse(&rk).unwrap();
        assert_eq!(parsed.submission, key.submission);
        assert_eq!(parsed.kind, InfraMessageKind::DeployAgent);
    }

    #[test]
    fn deploy_agent_binding_pattern() {
        assert_eq!(deploy_agent_binding(), "vlinder.infra.v1.*.deploy-agent");
    }

    // ── Delete Agent ──

    #[test]
    fn delete_agent_round_trip() {
        let key = InfraRoutingKey {
            submission: submission(),
            kind: InfraMessageKind::DeleteAgent,
        };
        let rk = delete_agent_routing_key(&key);
        let parsed = delete_agent_parse(&rk).unwrap();
        assert_eq!(parsed.submission, key.submission);
        assert_eq!(parsed.kind, InfraMessageKind::DeleteAgent);
    }

    #[test]
    fn delete_agent_binding_pattern() {
        assert_eq!(delete_agent_binding(), "vlinder.infra.v1.*.delete-agent");
    }

    // ── Queue name derivation ──

    #[test]
    fn binding_to_queue_name_replaces_special_chars() {
        let name = binding_to_queue_name("vlinder.data.v1.*.*.*.invoke.*.*.echo");
        assert_eq!(name, "vlinder-data-v1-W-W-W-invoke-W-W-echo");
    }

    #[test]
    fn binding_to_queue_name_replaces_hash() {
        let name = binding_to_queue_name("vlinder.data.v1.*.*.*.request.*.kv.sqlite.get.#");
        assert!(name.ends_with("-H"));
    }
}
