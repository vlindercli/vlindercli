//! Host table construction — builds virtual-host routing from agent requirements.

use vlinder_core::domain::{Agent, ProviderHost};

#[cfg(feature = "sqlite-kv")]
use vlinder_core::domain::ObjectStorageType;
#[cfg(feature = "sqlite-vec")]
use vlinder_core::domain::VectorStorageType;
#[cfg(any(feature = "openrouter", feature = "ollama"))]
use vlinder_core::domain::{Provider, ServiceType};

/// Build the virtual-host table from an agent's service requirements.
pub fn build_hosts(agent: &Agent) -> Vec<ProviderHost> {
    let mut hosts = Vec::new();
    hosts.push(ProviderHost::new("vlinder.local", vec![]));

    #[cfg(feature = "openrouter")]
    {
        let needs_openrouter = agent
            .requirements
            .services
            .get(&ServiceType::Infer)
            .is_some_and(|svc| svc.provider == Provider::OpenRouter);

        if needs_openrouter {
            hosts.push(vlinder_infer_openrouter::provider_host());
        }
    }

    #[cfg(feature = "ollama")]
    {
        let needs_ollama_infer = agent
            .requirements
            .services
            .get(&ServiceType::Infer)
            .is_some_and(|svc| svc.provider == Provider::Ollama);

        let needs_ollama_embed = agent
            .requirements
            .services
            .get(&ServiceType::Embed)
            .is_some_and(|svc| svc.provider == Provider::Ollama);

        if needs_ollama_infer || needs_ollama_embed {
            hosts.push(vlinder_ollama::provider_host(
                needs_ollama_infer,
                needs_ollama_embed,
            ));
        }
    }

    #[cfg(feature = "sqlite-vec")]
    {
        let needs_sqlite_vec = agent
            .vector_storage
            .as_ref()
            .and_then(|uri| VectorStorageType::from_scheme(uri.scheme()))
            .is_some_and(|t| t == VectorStorageType::SqliteVec);

        if needs_sqlite_vec {
            hosts.push(vlinder_sqlite_vec::provider_host());
        }
    }

    #[cfg(feature = "sqlite-kv")]
    {
        let needs_sqlite_kv = agent
            .object_storage
            .as_ref()
            .and_then(|uri| ObjectStorageType::from_scheme(uri.scheme()))
            .is_some_and(|t| t == ObjectStorageType::Sqlite);

        if needs_sqlite_kv {
            hosts.push(vlinder_sqlite_kv::provider_host());
        }
    }

    let host_names: Vec<&str> = hosts
        .iter()
        .map(|h: &ProviderHost| h.hostname.as_str())
        .collect();
    tracing::info!(
        event = "provider_server.hosts_ready",
        agent = %agent.name,
        hosts = ?host_names,
        "Provider hosts built"
    );
    hosts
}
