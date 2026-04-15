//! Async worker entry points.
//!
//! Worker roles are migrated here one by one, replacing the old
//! `while { rt.block_on(tick()); sleep(10ms); }` polling pattern with a
//! single `block_on` wrapping a `select!` event loop.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;

/// Resolves when the `AtomicBool` shutdown flag is set.
/// Polls every 10ms — replaced by `watch::Receiver` in a later step.
async fn shutdown_signal(shutdown: &Arc<AtomicBool>) {
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(feature = "openrouter")]
pub async fn run_inference_openrouter_worker(config: &Config, shutdown: Arc<AtomicBool>) {
    use vlinder_infer_openrouter::OpenRouterWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let worker = OpenRouterWorker::new(
        queue,
        config.openrouter.endpoint.clone(),
        config.openrouter.api_key.clone(),
    );

    tracing::info!(endpoint = %config.openrouter.endpoint, "OpenRouter inference worker ready");

    loop {
        tokio::select! {
            _ = worker.tick() => {}
            () = shutdown_signal(&shutdown) => break,
        }
    }
}

#[cfg(feature = "ollama")]
pub async fn run_inference_ollama_worker(config: &Config, shutdown: Arc<AtomicBool>) {
    use vlinder_ollama::OllamaWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let worker = OllamaWorker::new(queue, config.ollama.endpoint.clone());

    tracing::info!(endpoint = %config.ollama.endpoint, "Ollama inference worker ready");

    loop {
        tokio::select! {
            _ = worker.tick() => {}
            () = shutdown_signal(&shutdown) => break,
        }
    }
}

#[cfg(feature = "sqlite-kv")]
pub async fn run_storage_object_sqlite_worker(config: &Config, shutdown: Arc<AtomicBool>) {
    use vlinder_core::domain::{ObjectStorageType, ServiceBackend};
    use vlinder_sqlite_kv::KvWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");
    let worker = KvWorker::new(
        queue,
        registry,
        ServiceBackend::Kv(ObjectStorageType::Sqlite),
    );

    let registry_addr = &config.distributed.registry_addr;
    tracing::info!(registry = %registry_addr, "SQLite object storage worker ready");

    loop {
        tokio::select! {
            _ = worker.tick() => {}
            () = shutdown_signal(&shutdown) => break,
        }
    }
}

#[cfg(feature = "sqlite-vec")]
pub async fn run_storage_vector_sqlite_worker(config: &Config, shutdown: Arc<AtomicBool>) {
    use vlinder_core::domain::{ServiceBackend, VectorStorageType};
    use vlinder_sqlite_vec::SqliteVecWorker;

    let queue = crate::queue_factory::recording_from_config_async(config)
        .await
        .expect("Failed to create queue");
    let registry = crate::registry_factory::from_config_async(config)
        .await
        .expect("Failed to connect to registry");
    let worker = SqliteVecWorker::new(
        queue,
        registry,
        ServiceBackend::Vec(VectorStorageType::SqliteVec),
    );

    let registry_addr = &config.distributed.registry_addr;
    tracing::info!(registry = %registry_addr, "SQLite-vec vector storage worker ready");

    loop {
        tokio::select! {
            _ = worker.tick() => {}
            () = shutdown_signal(&shutdown) => break,
        }
    }
}
