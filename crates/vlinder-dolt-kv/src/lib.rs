//! Doltgres-backed SQL provider — declares hostname/routes and worker.

#[cfg(feature = "worker")]
mod worker;
mod types;

#[cfg(feature = "worker")]
pub use worker::SqlWorker;
pub use types::{SqlQueryRequest, SqlQueryResponse};

use vlinder_core::domain::{HttpMethod, Operation, ProviderHost, ProviderRoute, ServiceBackend};
use vlinder_core::domain::{SqlStorageType};

/// The virtual hostname the sidecar will serve for dolt-kv.
pub const HOSTNAME: &str = "dolt-kv.vlinder.local";

/// Build the provider host declaration for doltgres SQL storage.
pub fn provider_host() -> ProviderHost {
    let backend = ServiceBackend::Sql(SqlStorageType::Doltgres);
    ProviderHost::new(
        HOSTNAME,
        vec![ProviderRoute::new::<SqlQueryRequest, SqlQueryResponse>(
            HttpMethod::Post,
            "/execute",
            backend,
            Operation::Execute,
        )],
    )
}
