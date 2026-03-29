#[cfg(feature = "server")]
pub mod persistent;
pub mod registry_service;

#[cfg(feature = "server")]
pub use persistent::PersistentRegistry;

#[cfg(feature = "server")]
use vlinder_core::domain::Provider;

/// Configuration for the registry crate.
///
/// Describes the cluster topology relevant to the registry: which inference
/// and embedding providers are available. Built by the supervisor from its
/// knowledge of spawned workers.
#[cfg(feature = "server")]
pub struct RegistryConfig {
    pub inference_engines: Vec<Provider>,
    pub embedding_engines: Vec<Provider>,
}
