//! vlinder-lambda-adapter — Lambda extension that gives agents provider services.
//!
//! Replaces `aws-lambda-web-adapter` inside Lambda container images. Speaks the
//! Lambda Runtime API on one side and runs a full `ProviderServer` on the other,
//! giving Lambda agents access to inference, KV, vector storage, and delegation.
//!
//! Lifecycle:
//! 1. Read config from env
//! 2. Connect to NATS + registry + state
//! 3. Wait for agent to be ready on localhost
//! 4. Enter Lambda Runtime API loop:
//!    a. GET /runtime/invocation/next (blocks until Lambda dispatches)
//!    b. Deserialize `LambdaInvokePayload` from body
//!    c. Start `ProviderServer`, POST payload to agent
//!    d. Build complete message with diagnostics and state
//!    e. Send complete to NATS
//!    f. POST response back to Lambda Runtime API

mod adapter;
mod config;
mod lambda_invoke_receiver;

use std::sync::Arc;
use std::time::{Duration, Instant};

use vlinder_provider_server::dispatch as shared;
use vlinder_provider_server::factory;

use adapter::build_lambda_diagnostics;
use config::AdapterConfig;

use std::path::PathBuf;
use vlinder_s3_storage::{checkout, commit, create_client, S3Config};

#[tokio::main]
async fn main() {
    // Install rustls crypto provider before any TLS connection (AMQPS).
    let _ = rustls::crypto::ring::default_provider().install_default();

    let filter = std::env::var("RUST_LOG")
        .unwrap_or_else(|_| "warn,vlinder_lambda_adapter=info".to_string());
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = match AdapterConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "Failed to parse adapter config from env");
            std::process::exit(1);
        }
    };

    tracing::info!(
        event = "adapter.config",
        agent = %config.agent,
        runtime_api = %config.runtime_api,
        registry_url = %config.registry_url,
        state_url = %config.state_url,
        agent_port = config.agent_port,
        "Lambda adapter configuration loaded"
    );

    register_extension(&config.runtime_api);

    let store = match factory::connect_state_async(&config.state_url).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to state service");
            std::process::exit(1);
        }
    };

    let registry = match factory::connect_registry_async(&config.registry_url).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to registry");
            std::process::exit(1);
        }
    };

    let http = ureq::Agent::new();
    if let Err(e) = wait_for_agent(&http, config.agent_port).await {
        tracing::error!(error = %e, "Agent did not become ready");
        std::process::exit(1);
    }

    let invoke_receiver = lambda_invoke_receiver::LambdaInvokeReceiver::new(&config.runtime_api);

    tracing::info!(event = "adapter.started", agent = %config.agent, "Entering dispatch loop");
    dispatch_loop(
        &config.agent,
        config.agent_port,
        &invoke_receiver,
        &config.queue,
        config.secret_url.as_deref(),
        &store,
        &registry,
    )
    .await;
}

/// Create a fresh per-invocation queue connection.
///
/// Connects to the configured backend (AMQP or NATS), wraps it with
/// DAG recording, and returns the queue. For NATS, resolves credentials
/// from the secret store when `secret_url` is provided.
async fn create_invocation_queue(
    queue_backend: &config::QueueBackendConfig,
    secret_url: Option<&str>,
    store: &Arc<dyn vlinder_core::domain::DagStore>,
) -> Result<Arc<dyn vlinder_core::domain::MessageQueue + Send + Sync>, String> {
    match queue_backend {
        config::QueueBackendConfig::Amqp { url } => {
            let amqp_config = vlinder_amqp::AmqpConfig { url: url.clone() };
            let q = factory::connect_async(&factory::QueueConfig::Amqp(amqp_config))
                .await
                .map_err(|e| format!("failed to connect to AMQP: {e}"))?;
            Ok(factory::with_recording(q, store.clone()))
        }
        config::QueueBackendConfig::Nats { url } => {
            let nats_config = factory::resolve_nats_config(secret_url, url).await;
            let q = factory::connect_async(&factory::QueueConfig::Nats(nats_config))
                .await
                .map_err(|e| format!("failed to connect to NATS: {e}"))?;
            Ok(factory::with_recording(q, store.clone()))
        }
    }
}

/// Attempt to report a dispatch error back via a fresh queue connection.
async fn send_error_result(
    queue_backend: &config::QueueBackendConfig,
    secret_url: Option<&str>,
    store: &Arc<dyn vlinder_core::domain::DagStore>,
    key: &vlinder_core::domain::DataRoutingKey,
    agent: &vlinder_core::domain::AgentName,
    error: String,
) {
    let error_queue = create_invocation_queue(queue_backend, secret_url, store).await;
    match error_queue {
        Ok(eq) => {
            shared::send_complete(
                eq.as_ref(),
                key,
                agent,
                format!("[error] {error}").into_bytes(),
                None,
                vlinder_core::domain::RuntimeDiagnostics::placeholder(0),
            )
            .await;
        }
        Err(qe) => {
            tracing::error!(error = %qe, "Failed to create error-path queue");
        }
    }
}

/// Receive invokes from the Lambda Runtime API, dispatch to agent, send complete.
#[allow(clippy::too_many_lines)]
async fn dispatch_loop(
    function_name: &str,
    agent_port: u16,
    invoke_receiver: &lambda_invoke_receiver::LambdaInvokeReceiver,
    queue_backend: &config::QueueBackendConfig,
    secret_url: Option<&str>,
    store: &Arc<dyn vlinder_core::domain::DagStore>,
    registry: &Arc<dyn vlinder_core::domain::Registry>,
) {
    let agent_id = vlinder_core::domain::AgentName::new(function_name);
    loop {
        match invoke_receiver.receive_invoke(&agent_id) {
            Ok((key, invoke, ack)) => {
                let vlinder_core::domain::DataMessageKind::Invoke { ref agent, .. } = key.kind
                else {
                    continue;
                };

                let invoke_result = async {
                    let start = std::time::Instant::now();
                    let queue = create_invocation_queue(queue_backend, secret_url, store).await?;
                    tracing::info!(
                        event = "amqp.connect_per_invoke",
                        duration_ms =
                            u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                        "Per-invocation queue connection established"
                    );

                    // Look up agent info to get object_storage configuration.
                    let agent_info = registry
                        .get_agent_by_name(agent.as_str())
                        .await
                        .ok_or_else(|| {
                            format!("registry lookup for agent '{agent}' failed: agent not found")
                        })?;

                    // Determine if this agent uses S3 object storage.
                    let s3_config = agent_info
                        .object_storage
                        .as_ref()
                        .and_then(|uri| {
                            if uri.scheme() == Some("s3") {
                                Some(
                                    S3Config::from_resource_id(uri)
                                        .map_err(|e| format!("invalid S3 URI: {e}")),
                                )
                            } else {
                                None
                            }
                        })
                        .transpose()?;

                    if let Some(ref config) = &s3_config {
                        tracing::debug!(
                            bucket = %config.bucket,
                            prefix = %config.prefix,
                            "S3 storage configured"
                        );
                    }

                    let session_id = key.session.to_string();
                    let storage_root = PathBuf::from("/tmp/vlinder");

                    // Perform checkout if S3 storage is configured.
                    if let Some(config) = &s3_config {
                        let client = create_client()
                            .await
                            .map_err(|e| format!("failed to create S3 client: {e:#}"))?;
                        tracing::info!(
                            session_id = %session_id,
                            parent_state = invoke.state.as_deref().unwrap_or(""),
                            storage_root = %storage_root.display(),
                            bucket = %config.bucket,
                            prefix = %config.prefix,
                            "[s3.diag] adapter: starting S3 checkout"
                        );
                        checkout(
                            &client,
                            config,
                            &session_id,
                            invoke.state.as_deref(),
                            &storage_root,
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                error_alt = %format!("{e:#}"),
                                error_debug = ?e,
                                session_id = %session_id,
                                parent_state = invoke.state.as_deref().unwrap_or(""),
                                "[s3.diag] adapter: S3 checkout failed (full chain in error_alt)"
                            );
                            format!("checkout failed: {e:#}")
                        })?;
                    }
                    // Dispatch the invocation as usual.
                    let dispatch_result =
                        shared::dispatch_invoke(&queue, registry, agent_port, &key, &invoke).await;

                    match dispatch_result {
                        Ok(result) => {
                            // If S3 storage is configured, commit the workspace and use the new version ID as state.
                            let final_state = if let Some(config) = &s3_config {
                                let client = create_client().await.map_err(|e| {
                                    format!("failed to create S3 client for commit: {e:#}")
                                })?;
                                tracing::info!(
                                    session_id = %session_id,
                                    storage_root = %storage_root.display(),
                                    bucket = %config.bucket,
                                    prefix = %config.prefix,
                                    "[s3.diag] adapter: starting S3 commit"
                                );
                                match commit(&client, config, &session_id, &storage_root).await {
                                    Ok(new_version_id) => {
                                        // Clean up temp directory after successful commit.
                                        let _ = tokio::fs::remove_dir_all(&storage_root).await;
                                        Some(new_version_id)
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            error_alt = %format!("{e:#}"),
                                            error_debug = ?e,
                                            "[s3.diag] adapter: S3 commit failed (full chain in error_alt)"
                                        );
                                        return Err(format!("commit failed: {e:#}"));
                                    }
                                }
                            } else {
                                result.state
                            };

                            let region = std::env::var("AWS_REGION")
                                .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                                .unwrap_or_else(|_| "unknown".to_string());
                            let diagnostics = build_lambda_diagnostics(
                                function_name,
                                &region,
                                result.duration_ms,
                            );
                            shared::send_complete(
                                queue.as_ref(),
                                &key,
                                agent,
                                result.output,
                                final_state,
                                diagnostics,
                            )
                            .await;
                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                }
                .await;

                match invoke_result {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "Dispatch failed");
                        send_error_result(queue_backend, secret_url, store, &key, agent, e.clone())
                            .await;
                    }
                }
                if let Err(e) = ack().await {
                    tracing::error!(error = %e, "Failed to ack invocation");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, "receive_invoke failed");
                std::process::exit(1);
            }
        }
    }
}

/// Register with the Lambda Extensions API and block on event/next in a
/// background thread. This tells Lambda the extension is alive so init
/// doesn't time out. We request no events (`[]`) — the thread just parks.
fn register_extension(runtime_api: &str) {
    let runtime_api = runtime_api.to_string();
    std::thread::spawn(move || {
        let http = ureq::Agent::new();

        let register_url = format!("http://{runtime_api}/2020-01-01/extension/register");
        let resp = http
            .post(&register_url)
            .set("Lambda-Extension-Name", "vlinder-lambda-adapter")
            .send_string(r#"{ "events": [] }"#);

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(error = %e, "Extension registration failed");
                std::process::exit(1);
            }
        };

        let extension_id = resp
            .header("Lambda-Extension-Identifier")
            .unwrap_or("")
            .to_string();

        tracing::info!(
            event = "extension.registered",
            extension_id = %extension_id,
            "Registered as Lambda extension"
        );

        // Block forever waiting for events (we requested none, so this
        // just keeps the extension process alive).
        let next_url = format!("http://{runtime_api}/2020-01-01/extension/event/next");
        let _ = http
            .get(&next_url)
            .set("Lambda-Extension-Identifier", &extension_id)
            .call();
    });
}

/// Block until the agent's health endpoint responds (up to 60s).
async fn wait_for_agent(http: &ureq::Agent, port: u16) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + Duration::from_mins(1);

    tracing::info!(
        event = "adapter.waiting",
        port = port,
        "Waiting for agent to become ready"
    );

    loop {
        if Instant::now() > deadline {
            return Err(format!(
                "agent did not become ready within 60s (port {port})"
            ));
        }
        if http.get(&url).call().is_ok() {
            tracing::info!(event = "adapter.agent_ready", "Agent is ready");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
