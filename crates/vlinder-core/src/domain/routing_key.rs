//! Routing keys as domain types (ADR 096).
//!
//! A routing key captures the dimensions that uniquely identify where a
//! message should be delivered. Collision-freedom is structural — derived
//! from equality over all dimensions.

use std::str::FromStr;

use super::{
BranchId, HarnessType, ObjectStorageType, Operation, RuntimeType, Sequence, ServiceType,
SessionId, SqlStorageType, SubmissionId, TimelineId, VectorStorageType,
};

/// Agent identity within the routing bounded context.
///
/// Distinct from ResourceId (registration context). For routing, this is
/// the unique identifier that distinguishes one agent from another.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct AgentId(String);

impl AgentId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AgentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Inference backend implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum InferenceBackendType {
    Ollama,
    OpenRouter,
}

impl InferenceBackendType {
    pub fn as_str(&self) -> &'static str {
        match self {
            InferenceBackendType::Ollama => "ollama",
            InferenceBackendType::OpenRouter => "openrouter",
        }
    }
}

impl std::str::FromStr for InferenceBackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ollama" => Ok(Self::Ollama),
            "openrouter" => Ok(Self::OpenRouter),
            _ => Err(format!("unknown inference backend: {}", s)),
        }
    }
}

/// Embedding backend implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum EmbeddingBackendType {
    Ollama,
}

impl EmbeddingBackendType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EmbeddingBackendType::Ollama => "ollama",
        }
    }
}

impl std::str::FromStr for EmbeddingBackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ollama" => Ok(Self::Ollama),
            _ => Err(format!("unknown embedding backend: {}", s)),
        }
    }
}

/// Service-backend pair for routing.
///
/// Each service type scopes its own set of valid backends. Invalid
/// combinations (e.g., Kv + Ollama) are unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ServiceBackend {
    Kv(ObjectStorageType),
    Vec(VectorStorageType),
    Sql(SqlStorageType),
    Infer(InferenceBackendType),
    Embed(EmbeddingBackendType),
}

impl ServiceBackend {
    /// The service type for this backend (wire format compatibility).
    pub fn service_type(&self) -> ServiceType {
        match self {
            ServiceBackend::Kv(_) => ServiceType::Kv,
            ServiceBackend::Vec(_) => ServiceType::Vec,
            ServiceBackend::Sql(_) => ServiceType::Sql,
            ServiceBackend::Infer(_) => ServiceType::Infer,
            ServiceBackend::Embed(_) => ServiceType::Embed,
        }
    }

    /// The backend string for this backend (wire format compatibility).
    pub fn backend_str(&self) -> &'static str {
        match self {
            ServiceBackend::Kv(b) => b.as_str(),
            ServiceBackend::Vec(b) => b.as_str(),
            ServiceBackend::Sql(b) => b.as_str(),
            ServiceBackend::Infer(b) => b.as_str(),
            ServiceBackend::Embed(b) => b.as_str(),
        }
    }

    /// Reconstruct from separate service type and backend string.
    pub fn from_parts(service: ServiceType, backend: &str) -> Option<Self> {
        match service {
            ServiceType::Kv => match backend {
                "sqlite" => Some(Self::Kv(ObjectStorageType::Sqlite)),
                "memory" => Some(Self::Kv(ObjectStorageType::InMemory)),
                _ => None,
            },
            ServiceType::Vec => match backend {
                "sqlite-vec" => Some(Self::Vec(VectorStorageType::SqliteVec)),
                "memory" => Some(Self::Vec(VectorStorageType::InMemory)),
                _ => None,
            },
            ServiceType::Sql => match backend {
                "doltgres" => Some(Self::Sql(SqlStorageType::Doltgres)),
                _ => None,
            },
            ServiceType::Infer => InferenceBackendType::from_str(backend)
                .ok()
                .map(Self::Infer),
            ServiceType::Embed => EmbeddingBackendType::from_str(backend)
                .ok()
                .map(Self::Embed),
        }
    }
}

impl std::fmt::Display for ServiceBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.service_type(), self.backend_str())
    }
}

/// A value used exactly once to ensure uniqueness.
///
/// Prevents routing key collisions when the same caller delegates to the
/// same target multiple times within a submission.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Nonce(String);

impl Nonce {
    /// Generate a unique nonce.
    pub fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    /// Create a nonce from a known value (for testing or deserialization).
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for Nonce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Routing key for message delivery.
///
/// Each variant corresponds to one message direction in the protocol.
/// Equality and hashing derive from all fields — two routing keys are
/// equal if and only if they are the same variant and every dimension
/// matches.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RoutingKey {
    Invoke {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        harness: HarnessType,
        runtime: RuntimeType,
        agent: AgentId,
    },
    Complete {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        agent: AgentId,
        harness: HarnessType,
    },
    Request {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        agent: AgentId,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    },
    Response {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        service: ServiceBackend,
        agent: AgentId,
        operation: Operation,
        sequence: Sequence,
    },
    Delegate {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        caller: AgentId,
        target: AgentId,
    },
    DelegateReply {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        caller: AgentId,
        target: AgentId,
        nonce: Nonce,
    },
    /// Repair: Platform → Sidecar (replay a failed service call, ADR 113).
    Repair {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        harness: HarnessType,
        agent: AgentId,
    },
    /// Fork: CLI → Platform (create a branch).
    Fork {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        agent_name: String,
    },
    /// Promote: CLI → Platform (promote a branch to main).
    Promote {
        session: SessionId,
        branch: BranchId,
        submission: SubmissionId,
        agent_name: String,
    },
}

impl RoutingKey {
    /// Produce the reply routing key for this request routing key.
    ///
    /// Every request variant deterministically maps to its reply variant
    /// (ADR 096 §3). Delegate requires a nonce to distinguish multiple
    /// delegations to the same target within one submission.
    ///
    /// Reply variants (Complete, Response, DelegateReply) return None —
    /// hops are one level deep (reply asymmetry).
    pub fn reply_key(&self, nonce: Option<Nonce>) -> Option<RoutingKey> {
        match self {
            RoutingKey::Invoke {
                session,
                branch,
                submission,
                harness,
                agent,
                ..
            } => Some(RoutingKey::Complete {
                session: session.clone(),
                branch: *branch,
                submission: submission.clone(),
                agent: agent.clone(),
                harness: *harness,
            }),
            RoutingKey::Request {
                session,
                branch,
                submission,
                agent,
                service,
                operation,
                sequence,
            } => Some(RoutingKey::Response {
                session: session.clone(),
                branch: *branch,
                submission: submission.clone(),
                service: *service,
                agent: agent.clone(),
                operation: *operation,
                sequence: *sequence,
            }),
            RoutingKey::Delegate {
                session,
                branch,
                submission,
                caller,
                target,
            } => nonce.map(|n| RoutingKey::DelegateReply {
                session: session.clone(),
                branch: *branch,
                submission: submission.clone(),
                caller: caller.clone(),
                target: target.clone(),
                nonce: n,
            }),
            // Reply variants and repair don't produce further replies.
            RoutingKey::Complete { .. }
            | RoutingKey::Response { .. }
            | RoutingKey::DelegateReply { .. }
            | RoutingKey::Repair { .. }
            | RoutingKey::Fork { .. }
            | RoutingKey::Promote { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionId {
        SessionId::try_from("ses-1".to_string()).unwrap()
    }

    fn branch() -> BranchId {
        BranchId::from(1)
    }

    fn branch_alt() -> BranchId {
        BranchId::from(2)
    }

    fn submission() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }

    fn submission_alt() -> SubmissionId {
        SubmissionId::from("sub-2".to_string())
    }

    fn agent() -> AgentId {
        AgentId::new("echo")
    }

    fn agent_alt() -> AgentId {
        AgentId::new("pensieve")
    }

    // ========================================================================
    // Collision-freedom: Invoke (5 dimensions)
    // ========================================================================

    fn base_invoke() -> RoutingKey {
        RoutingKey::Invoke {
            session: session(),
            branch: branch(),
            submission: submission(),
            harness: HarnessType::Cli,
            runtime: RuntimeType::Container,
            agent: agent(),
        }
    }

    #[test]
    fn invoke_equal_when_all_dimensions_match() {
        assert_eq!(base_invoke(), base_invoke());
    }

    #[test]
    fn invoke_differs_by_branch() {
        let mut key = base_invoke();
        if let RoutingKey::Invoke { ref mut branch, .. } = key {
            *branch = branch_alt();
        }
        assert_ne!(base_invoke(), key);
    }

    #[test]
    fn invoke_differs_by_submission() {
        let mut key = base_invoke();
        if let RoutingKey::Invoke {
            ref mut submission, ..
        } = key
        {
            *submission = submission_alt();
        }
        assert_ne!(base_invoke(), key);
    }

    #[test]
    fn invoke_differs_by_harness() {
        let mut key = base_invoke();
        if let RoutingKey::Invoke {
            ref mut harness, ..
        } = key
        {
            *harness = HarnessType::Web;
        }
        assert_ne!(base_invoke(), key);
    }

    #[test]
    fn invoke_differs_by_agent() {
        let mut key = base_invoke();
        if let RoutingKey::Invoke { ref mut agent, .. } = key {
            *agent = agent_alt();
        }
        assert_ne!(base_invoke(), key);
    }

    // ========================================================================
    // Collision-freedom: Complete (4 dimensions)
    // ========================================================================

    fn base_complete() -> RoutingKey {
        RoutingKey::Complete {
            session: session(),
            branch: branch(),
            submission: submission(),
            agent: agent(),
            harness: HarnessType::Cli,
        }
    }

    #[test]
    fn complete_equal_when_all_dimensions_match() {
        assert_eq!(base_complete(), base_complete());
    }

    #[test]
    fn complete_differs_by_agent() {
        let mut key = base_complete();
        if let RoutingKey::Complete { ref mut agent, .. } = key {
            *agent = agent_alt();
        }
        assert_ne!(base_complete(), key);
    }

    #[test]
    fn complete_differs_by_harness() {
        let mut key = base_complete();
        if let RoutingKey::Complete {
            ref mut harness, ..
        } = key
        {
            *harness = HarnessType::Web;
        }
        assert_ne!(base_complete(), key);
    }

    // ========================================================================
    // Collision-freedom: Request (6 dimensions)
    // ========================================================================

    fn base_request() -> RoutingKey {
        RoutingKey::Request {
            session: session(),
            branch: branch(),
            submission: submission(),
            agent: agent(),
            service: ServiceBackend::Kv(ObjectStorageType::Sqlite),
            operation: Operation::Get,
            sequence: Sequence::first(),
        }
    }

    #[test]
    fn request_equal_when_all_dimensions_match() {
        assert_eq!(base_request(), base_request());
    }

    #[test]
    fn request_differs_by_service_type() {
        let mut key = base_request();
        if let RoutingKey::Request {
            ref mut service, ..
        } = key
        {
            *service = ServiceBackend::Vec(VectorStorageType::SqliteVec);
        }
        assert_ne!(base_request(), key);
    }

    #[test]
    fn request_differs_by_sql_service_type() {
        let mut key = base_request();
        if let RoutingKey::Request { ref mut service, .. } = key {
            *service = ServiceBackend::Sql(SqlStorageType::Doltgres);
        }
        assert_ne!(base_request(), key);
    }

    #[test]
    fn request_differs_by_backend_within_service() {
        let mut key = base_request();
        if let RoutingKey::Request {
            ref mut service, ..
        } = key
        {
            *service = ServiceBackend::Kv(ObjectStorageType::InMemory);
        }
        assert_ne!(base_request(), key);
    }

    #[test]
    fn request_differs_by_operation() {
        let mut key = base_request();
        if let RoutingKey::Request {
            ref mut operation, ..
        } = key
        {
            *operation = Operation::Put;
        }
        assert_ne!(base_request(), key);
    }

    #[test]
    fn request_differs_by_sequence() {
        let mut key = base_request();
        if let RoutingKey::Request {
            ref mut sequence, ..
        } = key
        {
            *sequence = Sequence::from(2);
        }
        assert_ne!(base_request(), key);
    }

    #[test]
    fn request_differs_by_agent() {
        let mut key = base_request();
        if let RoutingKey::Request { ref mut agent, .. } = key {
            *agent = agent_alt();
        }
        assert_ne!(base_request(), key);
    }

    // ========================================================================
    // Collision-freedom: Response (6 dimensions)
    // ========================================================================

    fn base_response() -> RoutingKey {
        RoutingKey::Response {
            session: session(),
            branch: branch(),
            submission: submission(),
            service: ServiceBackend::Kv(ObjectStorageType::Sqlite),
            agent: agent(),
            operation: Operation::Get,
            sequence: Sequence::first(),
        }
    }

    #[test]
    fn response_equal_when_all_dimensions_match() {
        assert_eq!(base_response(), base_response());
    }

    #[test]
    fn response_differs_by_agent() {
        let mut key = base_response();
        if let RoutingKey::Response { ref mut agent, .. } = key {
            *agent = agent_alt();
        }
        assert_ne!(base_response(), key);
    }

    #[test]
    fn response_differs_by_service() {
        let mut key = base_response();
        if let RoutingKey::Response {
            ref mut service, ..
        } = key
        {
            *service = ServiceBackend::Infer(InferenceBackendType::Ollama);
        }
        assert_ne!(base_response(), key);
    }

    // ========================================================================
    // Collision-freedom: Delegate (4 dimensions)
    // ========================================================================

    fn base_delegate() -> RoutingKey {
        RoutingKey::Delegate {
            session: session(),
            branch: branch(),
            submission: submission(),
            caller: agent(),
            target: agent_alt(),
        }
    }

    #[test]
    fn delegate_equal_when_all_dimensions_match() {
        assert_eq!(base_delegate(), base_delegate());
    }

    #[test]
    fn delegate_differs_by_caller() {
        let mut key = base_delegate();
        if let RoutingKey::Delegate { ref mut caller, .. } = key {
            *caller = AgentId::new("planner");
        }
        assert_ne!(base_delegate(), key);
    }

    #[test]
    fn delegate_differs_by_target() {
        let mut key = base_delegate();
        if let RoutingKey::Delegate { ref mut target, .. } = key {
            *target = AgentId::new("fact-checker");
        }
        assert_ne!(base_delegate(), key);
    }

    // ========================================================================
    // Collision-freedom: DelegateReply (5 dimensions)
    // ========================================================================

    fn base_delegate_reply() -> RoutingKey {
        RoutingKey::DelegateReply {
            session: session(),
            branch: branch(),
            submission: submission(),
            caller: agent(),
            target: agent_alt(),
            nonce: Nonce::new("abc123"),
        }
    }

    #[test]
    fn delegate_reply_equal_when_all_dimensions_match() {
        assert_eq!(base_delegate_reply(), base_delegate_reply());
    }

    #[test]
    fn delegate_reply_differs_by_nonce() {
        let mut key = base_delegate_reply();
        if let RoutingKey::DelegateReply { ref mut nonce, .. } = key {
            *nonce = Nonce::new("def456");
        }
        assert_ne!(base_delegate_reply(), key);
    }

    // ========================================================================
    // Cross-variant: different variants never equal
    // ========================================================================

    #[test]
    fn invoke_and_complete_never_equal() {
        assert_ne!(base_invoke(), base_complete());
    }

    #[test]
    fn request_and_response_never_equal() {
        assert_ne!(base_request(), base_response());
    }

    #[test]
    fn delegate_and_delegate_reply_never_equal() {
        assert_ne!(base_delegate(), base_delegate_reply());
    }

    // ========================================================================
    // Reply pairing (invariant 2): request → reply is deterministic
    // ========================================================================

    #[test]
    fn invoke_reply_is_complete() {
        let reply = base_invoke().reply_key(None).unwrap();
        assert_eq!(reply, base_complete());
    }

    #[test]
    fn invoke_reply_ignores_nonce() {
        let with = base_invoke()
            .reply_key(Some(Nonce::new("ignored")))
            .unwrap();
        let without = base_invoke().reply_key(None).unwrap();
        assert_eq!(with, without);
    }

    #[test]
    fn invoke_reply_drops_runtime() {
        // Complete doesn't carry RuntimeType — it goes back to harness, not runtime
        let reply = base_invoke().reply_key(None).unwrap();
        match reply {
            RoutingKey::Complete { .. } => {}
            other => panic!("expected Complete, got {:?}", other),
        }
    }

    #[test]
    fn request_reply_is_response() {
        let reply = base_request().reply_key(None).unwrap();
        assert_eq!(reply, base_response());
    }

    #[test]
    fn request_reply_preserves_all_dimensions() {
        let reply = base_request().reply_key(None).unwrap();
        match reply {
            RoutingKey::Response {
                session: s,
                branch: b,
                submission,
                service,
                agent,
                operation,
                sequence,
            } => {
                assert_eq!(s, self::session());
                assert_eq!(b, self::branch());
                assert_eq!(submission, self::submission());
                assert_eq!(service, ServiceBackend::Kv(ObjectStorageType::Sqlite));
                assert_eq!(agent, self::agent());
                assert_eq!(operation, Operation::Get);
                assert_eq!(sequence, Sequence::first());
            }
            other => panic!("expected Response, got {:?}", other),
        }
    }

    #[test]
    fn delegate_reply_is_delegate_reply_with_nonce() {
        let nonce = Nonce::new("abc123");
        let reply = base_delegate().reply_key(Some(nonce)).unwrap();
        assert_eq!(reply, base_delegate_reply());
    }

    #[test]
    fn delegate_without_nonce_returns_none() {
        assert!(base_delegate().reply_key(None).is_none());
    }

    #[test]
    fn reply_key_is_deterministic() {
        // Same inputs → same reply key, always
        let a = base_request().reply_key(None).unwrap();
        let b = base_request().reply_key(None).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn different_nonces_produce_different_delegate_replies() {
        let reply_a = base_delegate()
            .reply_key(Some(Nonce::new("nonce-1")))
            .unwrap();
        let reply_b = base_delegate()
            .reply_key(Some(Nonce::new("nonce-2")))
            .unwrap();
        assert_ne!(reply_a, reply_b);
    }

    // ========================================================================
    // Reply asymmetry (invariant 3): reply variants have no replies
    // ========================================================================

    #[test]
    fn complete_has_no_reply() {
        assert!(base_complete().reply_key(None).is_none());
    }

    #[test]
    fn response_has_no_reply() {
        assert!(base_response().reply_key(None).is_none());
    }

    #[test]
    fn delegate_reply_has_no_reply() {
        assert!(base_delegate_reply().reply_key(None).is_none());
    }

    #[test]
    fn delegate_reply_has_no_reply_even_with_nonce() {
        assert!(base_delegate_reply()
            .reply_key(Some(Nonce::new("wont-help")))
            .is_none());
    }
}
