//! gRPC Registry Service.
//!
//! Exposes the Registry trait over gRPC for distributed mode.
//! - `RegistryServer`: Wraps a Registry impl, serves gRPC requests
//! - `GrpcRegistryClient`: Implements Registry trait via gRPC calls

#[cfg(feature = "client")]
mod client;
mod convert;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::{ping_registry, GrpcRegistryClient};
#[cfg(feature = "server")]
pub use server::RegistryServer;

/// Generated protobuf types — tonic does not emit `#[automatically_derived]` yet,
/// so we suppress pedantic lints manually until it does.
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]
pub mod proto {
    tonic::include_proto!("vlinder.registry");
}
