//! Domain module - the complete Vlinder protocol specification.
//!
//! This module contains both:
//! - **Data structures**: Configuration types describing WHAT to use
//! - **Capability traits**: Abstract interfaces defining the protocol
//!
//! Infrastructure modules implement these traits; the domain module
//! is the authoritative source for the runtime's abstract protocol.

mod agent;
mod agent_manifest;
mod agent_state;
mod catalog;
mod container_id;
mod dag;
mod diagnostics;
mod fleet;
mod fleet_manifest;
pub mod harness;
mod identity;
mod image_digest;
mod image_ref;
mod message;
mod message_queue;
mod model;
mod model_manifest;
mod operation;
mod path;
mod pod_id;
mod provider;
mod registry;
pub mod registry_memory;
mod registry_repository;
mod resource_id;
mod routing_key;
mod runtime;
mod secret_store;
mod service_type;
pub mod session;
mod storage;
pub mod wire;
pub mod workers;

// ============================================================================
// Agent & Fleet
// ============================================================================

pub use agent::{Agent, LoadError as AgentLoadError, Prompts, Requirements};
pub use agent_state::{AgentState, AgentStatus};
pub use container_id::ContainerId;
pub use image_digest::ImageDigest;
pub use image_ref::ImageRef;
pub use operation::Operation;
pub use pod_id::PodId;
pub use service_type::ServiceType;

// ============================================================================
// Message Queue (protocol types + trait)
// ============================================================================

pub use diagnostics::{
    HealthSnapshot, HealthWindow, InvokeDiagnostics, RequestDiagnostics, RuntimeDiagnostics,
    RuntimeInfo, ServiceDiagnostics, ServiceMetrics,
};
pub use message::{
    BranchId, CompleteMessage, DagNodeId, DeleteAgentMessage, DeployAgentMessage, ForkMessage,
    HarnessType, Instance, InvokeMessage, MessageId, PromoteMessage, RequestMessage,
    ResponseMessage, Sequence, SequenceCounter, SessionId, SessionStartMessage, StateHash,
    SubmissionId, PROTOCOL_VERSION,
};
pub use message_queue::{agent_routing_key, Acknowledgement, MessageQueue, QueueError};
pub use routing_key::{
    AgentName, DataMessageKind, DataRoutingKey, EmbeddingBackendType, InferenceBackendType,
    InfraMessageKind, InfraRoutingKey, ServiceBackend, SessionMessageKind, SessionRoutingKey,
};

// ============================================================================
// DAG (content-addressed Merkle DAG)
// ============================================================================

pub use dag::{
    hash_dag_node, Branch, DagNode, DagStore, DagWorker, InMemoryDagStore, MessageType, Plane,
    SessionSummary, Snapshot,
};

// ============================================================================
// Paths
// ============================================================================

pub use agent_manifest::{AgentManifest, MountConfig, Protocol, RequirementsConfig, ServiceConfig};
pub use fleet::{Fleet, LoadError as FleetLoadError};
pub use fleet_manifest::{FleetManifest, ParseError as FleetManifestParseError};
pub use path::{AbsolutePath, AbsoluteUri};
pub use provider::{
    HttpMethod, PayloadError, Provider, ProviderHost, ProviderRoute, TypeValidator,
};

// ============================================================================
// Storage (config types only — implementations in provider crates)
// ============================================================================

pub use storage::{ObjectStorageType, SqlStorageType, VectorStorageType};

// ============================================================================
// Resource ID (registry key)
// ============================================================================

pub use resource_id::ResourceId;

// ============================================================================
// Model
// ============================================================================

pub use model::{LoadError as ModelLoadError, Model, ModelType};
pub use model_manifest::{ModelManifest, ModelTypeConfig};

// ============================================================================
// Model Catalog (trait)
// ============================================================================

pub use catalog::{CatalogError, CatalogService, CompositeCatalog, ModelCatalog, ModelInfo};

// ============================================================================
// Runtime (trait)
// ============================================================================

pub use runtime::{Runtime, RuntimeType};

// ============================================================================
// Harness (API surface for agent interaction)
// ============================================================================

pub use harness::{CoreHarness, ForkParams, Harness, PromoteParams};
pub use session::Session;

// ============================================================================
// Secret Store (ADR 083)
// ============================================================================

pub use identity::{ensure_agent_identity, AgentIdentity, IdentityError};
pub use secret_store::{InMemorySecretStore, SecretStore, SecretStoreError};

// ============================================================================
// Registry
// ============================================================================

pub use registry::{Job, JobId, JobStatus, RegistrationError, Registry};
pub use registry_memory::InMemoryRegistry;
pub use registry_repository::{RegistryRepository, RepositoryError, StoredAgent, StoredModel};
