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
mod lambda_runtime_queue;

use std::sync::Arc;
use std::time::{Duration, Instant};

use vlinder_core::domain::{MessageQueue, QueueError};

use vlinder_provider_server::dispatch as shared;
use vlinder_provider_server::factory;

use adapter::build_lambda_diagnostics;
use config::AdapterConfig;

fn main() {
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
        nats_url = %config.nats_url,
        registry_url = %config.registry_url,
        state_url = %config.state_url,
        agent_port = config.agent_port,
        "Lambda adapter configuration loaded"
    );

    register_extension(&config.runtime_api);

    let nats_config = factory::resolve_nats_config(config.secret_url.as_deref(), &config.nats_url);
    let queue = match factory::connect(&factory::QueueConfig::Nats(nats_config)) {
        Ok(q) => q,
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to queue");
            std::process::exit(1);
        }
    };
    let store = match factory::connect_state(&config.state_url) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to state service");
            std::process::exit(1);
        }
    };
    let queue = factory::with_recording(queue, store);

    let registry = match factory::connect_registry(&config.registry_url) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "Failed to connect to registry");
            std::process::exit(1);
        }
    };

    let http = ureq::Agent::new();
    if let Err(e) = wait_for_agent(&http, config.agent_port) {
        tracing::error!(error = %e, "Agent did not become ready");
        std::process::exit(1);
    }

    let lambda_queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(
        lambda_runtime_queue::LambdaRuntimeQueue::new(queue, &config.runtime_api),
    );

    tracing::info!(event = "adapter.started", agent = %config.agent, "Entering dispatch loop");
    dispatch_loop(&config.agent, config.agent_port, &lambda_queue, &registry);
}

/// Receive invokes from the Lambda Runtime API, dispatch to agent, send complete.
fn dispatch_loop(
    function_name: &str,
    agent_port: u16,
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    registry: &Arc<dyn vlinder_core::domain::Registry>,
) {
    let agent_id = vlinder_core::domain::AgentName::new(function_name);
    loop {
        match queue.receive_invoke(&agent_id) {
            Ok((key, invoke, ack)) => {
                let vlinder_core::domain::DataMessageKind::Invoke { ref agent, .. } = key.kind
                else {
                    continue;
                };

                match shared::dispatch_invoke(queue, registry, agent_port, &key, &invoke) {
                    Ok(result) => {
                        let region = std::env::var("AWS_REGION")
                            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                            .unwrap_or_else(|_| "unknown".to_string());
                        let diagnostics =
                            build_lambda_diagnostics(function_name, &region, result.duration_ms);
                        shared::send_complete(
                            queue.as_ref(),
                            &key,
                            agent,
                            result.output,
                            result.state,
                            diagnostics,
                        );
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Dispatch failed");
                        shared::send_complete(
                            queue.as_ref(),
                            &key,
                            agent,
                            format!("[error] {e}").into_bytes(),
                            None,
                            vlinder_core::domain::RuntimeDiagnostics::placeholder(0),
                        );
                    }
                }
                if let Err(e) = ack() {
                    tracing::error!(error = %e, "Failed to ack invocation");
                }
            }
            Err(QueueError::Timeout) => {}
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
fn wait_for_agent(http: &ureq::Agent, port: u16) -> Result<(), String> {
    let url = format!("http://127.0.0.1:{port}/health");
    let deadline = Instant::now() + Duration::from_secs(60);

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
        std::thread::sleep(Duration::from_millis(100));
    }
}
