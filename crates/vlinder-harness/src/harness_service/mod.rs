//! gRPC Harness Service (ADR 101).
//!
//! Exposes the Harness trait over gRPC for CLI decoupling.
//! - `HarnessServiceServer`: Wraps a `CoreHarness`, serves gRPC requests
//! - `GrpcHarnessClient`: Implements `Harness` trait via gRPC calls

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::{ping_harness, GrpcHarnessClient};
#[cfg(feature = "server")]
pub use server::HarnessServer;

/// Generated protobuf types — tonic does not emit `#[automatically_derived]` yet,
/// so we suppress pedantic lints manually until it does.
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]
pub mod proto {
    tonic::include_proto!("vlinder.harness");
}
