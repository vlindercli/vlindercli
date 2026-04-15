//! gRPC Catalog Service.
//!
//! Exposes the `CatalogService` trait over gRPC for distributed mode.
//! - `CatalogServiceServer`: Wraps a `CatalogService` impl, serves gRPC requests
//! - `GrpcCatalogClient`: Implements `CatalogService` trait via gRPC calls

#[cfg(feature = "client")]
mod client;
mod convert;
#[cfg(feature = "server")]
mod server;

#[cfg(feature = "client")]
pub use client::{ping_catalog_service, ping_catalog_service_async, GrpcCatalogClient};
#[cfg(feature = "server")]
pub use server::CatalogServiceServer;

/// Generated protobuf types — tonic does not emit `#[automatically_derived]` yet,
/// so we suppress pedantic lints manually until it does.
#[allow(
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]
pub mod proto {
    tonic::include_proto!("vlinder.catalog");
}
