//! gRPC Secret Store Service.
//!
//! Exposes the `SecretStore` trait over gRPC for distributed mode.
//! - `SecretServiceServer`: Wraps a `SecretStore` impl, serves gRPC requests
//! - `GrpcSecretClient`: Implements `SecretStore` trait via gRPC calls

#[cfg(feature = "secret-client")]
mod client;
#[cfg(feature = "secret-store")]
mod server;

#[cfg(feature = "secret-client")]
pub use client::{ping_secret_service, GrpcSecretClient};
#[cfg(feature = "secret-store")]
pub use server::SecretServer;

/// Generated protobuf types — tonic does not emit `#[automatically_derived]` yet,
/// so we suppress pedantic lints manually until it does.
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]
pub mod proto {
    tonic::include_proto!("vlinder.secret_store");
}
