//! Routing keys as domain types (ADR 096).
//!
//! A routing key captures the dimensions that uniquely identify where a
//! message should be delivered. Collision-freedom is structural — derived
//! from equality over all dimensions.

use std::str::FromStr;

use super::{
    BranchId, HarnessType, ObjectStorageType, Operation, RuntimeType, Sequence, ServiceType,
    SessionId, SubmissionId, VectorStorageType,
};

/// Agent identity within the routing bounded context.
///
/// Distinct from `ResourceId` (registration context). For routing, this is
/// the unique identifier that distinguishes one agent from another.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct AgentName(String);

impl AgentName {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for AgentName {
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
            _ => Err(format!("unknown inference backend: {s}")),
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
            _ => Err(format!("unknown embedding backend: {s}")),
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
    Infer(InferenceBackendType),
    Embed(EmbeddingBackendType),
}

impl ServiceBackend {
    /// The service type for this backend (wire format compatibility).
    pub fn service_type(&self) -> ServiceType {
        match self {
            ServiceBackend::Kv(_) => ServiceType::Kv,
            ServiceBackend::Vec(_) => ServiceType::Vec,
            ServiceBackend::Infer(_) => ServiceType::Infer,
            ServiceBackend::Embed(_) => ServiceType::Embed,
        }
    }

    /// The backend string for this backend (wire format compatibility).
    pub fn backend_str(&self) -> &'static str {
        match self {
            ServiceBackend::Kv(b) => b.as_str(),
            ServiceBackend::Vec(b) => b.as_str(),
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
            ServiceType::Infer => InferenceBackendType::from_str(backend)
                .ok()
                .map(Self::Infer),
            ServiceType::Embed => EmbeddingBackendType::from_str(backend)
                .ok()
                .map(Self::Embed),
        }
    }
}

impl std::str::FromStr for ServiceBackend {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (svc, backend) = s
            .split_once('.')
            .ok_or_else(|| format!("invalid ServiceBackend: {s}"))?;
        let service_type: ServiceType =
            svc.parse().map_err(|_| format!("unknown service: {svc}"))?;
        Self::from_parts(service_type, backend)
            .ok_or_else(|| format!("unknown backend '{backend}' for service '{svc}'"))
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
/// Composite routing key: common prefix + variant-specific suffix.
///
/// Each routing key uniquely identifies a message direction in the protocol.
/// The common prefix (session, branch, submission) maps to the NATS subject
/// prefix `vlinder.{session}.{branch}.{submission}`. The kind maps to the
/// variant-specific suffix.
///
/// Equality and hashing derive from all fields — two routing keys are
/// equal if and only if they have the same prefix and the same kind.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutingKey {
    pub session: SessionId,
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub kind: RoutingKind,
}

/// Variant-specific routing dimensions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RoutingKind {
    /// Fork: CLI → Platform (create a branch).
    Fork { agent_name: AgentName },
    /// Promote: CLI → Platform (promote a branch to main).
    Promote { agent_name: AgentName },
}

impl RoutingKey {}

// ============================================================================
// Data-plane routing (ADR 121)
// ============================================================================

/// Data plane routing key — agent execution messages.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct DataRoutingKey {
    pub session: SessionId,
    pub branch: BranchId,
    pub submission: SubmissionId,
    pub kind: DataMessageKind,
}

/// Data plane message kinds.
///
/// New variants are added here as each message type migrates from `RoutingKind`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum DataMessageKind {
    Invoke {
        harness: HarnessType,
        runtime: RuntimeType,
        agent: AgentName,
    },
    Complete {
        agent: AgentName,
        harness: HarnessType,
    },
    Request {
        agent: AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    },
    Response {
        agent: AgentName,
        service: ServiceBackend,
        operation: Operation,
        sequence: Sequence,
    },
}

/// Session plane routing key — compensating transactions (fork, promote).
///
/// Scoped to a session, not a branch. Session plane operations manage
/// branches within a session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SessionRoutingKey {
    pub session: SessionId,
    pub submission: SubmissionId,
    pub kind: SessionMessageKind,
}

/// Session plane message kinds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionMessageKind {
    Fork { agent_name: AgentName },
    Promote { agent_name: AgentName },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session() -> SessionId {
        SessionId::try_from("00000000-0000-4000-8000-000000000001".to_string()).unwrap()
    }

    fn session_alt() -> SessionId {
        SessionId::try_from("00000000-0000-4000-8000-000000000002".to_string()).unwrap()
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

    fn agent() -> AgentName {
        AgentName::new("echo")
    }

    fn agent_alt() -> AgentName {
        AgentName::new("pensieve")
    }

    // ========================================================================
    // Collision-freedom: Fork (4 dimensions)
    // ========================================================================

    fn base_fork() -> RoutingKey {
        RoutingKey {
            session: session(),
            branch: branch(),
            submission: submission(),
            kind: RoutingKind::Fork {
                agent_name: agent(),
            },
        }
    }

    #[test]
    fn fork_equal_when_all_dimensions_match() {
        assert_eq!(base_fork(), base_fork());
    }

    #[test]
    fn fork_differs_by_session() {
        let mut key = base_fork();
        key.session = session_alt();
        assert_ne!(base_fork(), key);
    }

    #[test]
    fn fork_differs_by_branch() {
        let mut key = base_fork();
        key.branch = branch_alt();
        assert_ne!(base_fork(), key);
    }

    #[test]
    fn fork_differs_by_submission() {
        let mut key = base_fork();
        key.submission = submission_alt();
        assert_ne!(base_fork(), key);
    }

    #[test]
    fn fork_differs_by_agent_name() {
        let mut key = base_fork();
        if let RoutingKind::Fork {
            ref mut agent_name, ..
        } = key.kind
        {
            *agent_name = agent_alt();
        }
        assert_ne!(base_fork(), key);
    }

    // ========================================================================
    // Collision-freedom: Promote (4 dimensions)
    // ========================================================================

    fn base_promote() -> RoutingKey {
        RoutingKey {
            session: session(),
            branch: branch(),
            submission: submission(),
            kind: RoutingKind::Promote {
                agent_name: agent(),
            },
        }
    }

    #[test]
    fn promote_equal_when_all_dimensions_match() {
        assert_eq!(base_promote(), base_promote());
    }

    #[test]
    fn promote_differs_by_session() {
        let mut key = base_promote();
        key.session = session_alt();
        assert_ne!(base_promote(), key);
    }

    #[test]
    fn promote_differs_by_branch() {
        let mut key = base_promote();
        key.branch = branch_alt();
        assert_ne!(base_promote(), key);
    }

    #[test]
    fn promote_differs_by_submission() {
        let mut key = base_promote();
        key.submission = submission_alt();
        assert_ne!(base_promote(), key);
    }

    #[test]
    fn promote_differs_by_agent_name() {
        let mut key = base_promote();
        if let RoutingKind::Promote {
            ref mut agent_name, ..
        } = key.kind
        {
            *agent_name = agent_alt();
        }
        assert_ne!(base_promote(), key);
    }

    // ========================================================================
    // Cross-variant: different variants never equal
    // ========================================================================

    #[test]
    fn fork_and_promote_never_equal() {
        assert_ne!(base_fork(), base_promote());
    }
}
