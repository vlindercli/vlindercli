//! gRPC State Service (ADR 079).
//!
//! Exposes the `DagStore` trait over gRPC for distributed mode.
//! - `StateServiceServer`: Wraps a `DagStore` impl, serves gRPC requests
//! - `GrpcStateClient`: Implements `DagStore` trait via gRPC calls

#[cfg(feature = "client")]
mod client;
mod convert;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::{ping_state_service_async, GrpcStateClient};
#[cfg(feature = "server")]
pub use server::StateServiceServer;

#[cfg(test)]
mod tests;

/// Generated protobuf types — tonic does not emit `#[automatically_derived]` yet,
/// so we suppress pedantic lints manually until it does.
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]
pub mod proto {
    tonic::include_proto!("vlinder.state");
}
