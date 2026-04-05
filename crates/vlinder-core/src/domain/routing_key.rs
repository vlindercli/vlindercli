//! Routing keys as domain types (ADR 096).
//!
//! A routing key captures the dimensions that uniquely identify where a
//! message should be delivered. Collision-freedom is structural — derived
//! from equality over all dimensions.

use std::str::FromStr;

use super::{
    BranchId, HarnessType, ObjectStorageType, Operation, RuntimeType, Sequence, ServiceType,
    SessionId, SqlStorageType, SubmissionId, VectorStorageType,
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
    Sql(SqlStorageType),
}

impl ServiceBackend {
    /// The service type for this backend (wire format compatibility).
    pub fn service_type(&self) -> ServiceType {
        match self {
            ServiceBackend::Kv(_) => ServiceType::Kv,
            ServiceBackend::Vec(_) => ServiceType::Vec,
            ServiceBackend::Infer(_) => ServiceType::Infer,
            ServiceBackend::Embed(_) => ServiceType::Embed,
            ServiceBackend::Sql(_) => ServiceType::Sql,
        }
    }

    /// The backend string for this backend (wire format compatibility).
    pub fn backend_str(&self) -> &'static str {
        match self {
            ServiceBackend::Kv(b) => b.as_str(),
            ServiceBackend::Vec(b) => b.as_str(),
            ServiceBackend::Infer(b) => b.as_str(),
            ServiceBackend::Embed(b) => b.as_str(),
            ServiceBackend::Sql(b) => b.as_str(),
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
            ServiceType::Sql => match backend {
                "doltgres" => Some(Self::Sql(SqlStorageType::Doltgres)),
                _ => None,
            },
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
    Start { agent_name: AgentName },
    Fork { agent_name: AgentName },
    Promote { agent_name: AgentName },
}

// ============================================================================
// Infra-plane routing (ADR 121)
// ============================================================================

/// Infra plane routing key — provisioning operations.
///
/// Cluster-scoped, not session-scoped. No branch, no session.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InfraRoutingKey {
    pub submission: SubmissionId,
    pub kind: InfraMessageKind,
}

/// Infra plane message kinds.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum InfraMessageKind {
    DeployAgent,
    DeleteAgent,
}
