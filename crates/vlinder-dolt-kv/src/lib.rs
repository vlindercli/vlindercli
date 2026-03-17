//! Doltgres-backed SQL provider — declares hostname/routes and worker.

mod types;
#[cfg(feature = "worker")]
mod worker;

pub use types::{SqlQueryRequest, SqlQueryResponse};
#[cfg(feature = "worker")]
pub use worker::SqlWorker;

use vlinder_core::domain::SqlStorageType;
use vlinder_core::domain::{HttpMethod, Operation, ProviderHost, ProviderRoute, ServiceBackend};

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
