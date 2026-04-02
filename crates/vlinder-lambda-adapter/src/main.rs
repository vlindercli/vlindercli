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

use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use vlinder_core::domain::{MessageQueue, Registry};

use vlinder_provider_server::factory;

use adapter::{build_error_body, build_lambda_diagnostics, deserialize_invoke};
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

    // Register as a Lambda extension immediately — must happen before
    // Lambda's init phase timeout (10s). The registration thread blocks
    // on event/next forever, keeping the extension alive.
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

    tracing::info!(event = "adapter.started", agent = %config.agent, "Entering Runtime API loop");

    if let Err(e) = runtime_api_loop(&config, &http, &queue, &registry) {
        tracing::error!(error = %e, "Runtime API loop exited with error");
        std::process::exit(1);
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

/// Main loop: poll Lambda Runtime API, dispatch to agent, respond.
fn runtime_api_loop(
    config: &AdapterConfig,
    http: &ureq::Agent,
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    registry: &Arc<dyn Registry>,
) -> Result<(), String> {
    let next_url = format!(
        "http://{}/2018-06-01/runtime/invocation/next",
        config.runtime_api,
    );

    loop {
        // Block until Lambda dispatches an invocation.
        let response = http
            .get(&next_url)
            .call()
            .map_err(|e| format!("GET invocation/next failed: {e}"))?;

        let request_id = response
            .header("Lambda-Runtime-Aws-Request-Id")
            .unwrap_or("unknown")
            .to_string();

        let mut body = Vec::new();
        response
            .into_reader()
            .read_to_end(&mut body)
            .map_err(|e| format!("failed to read invocation body: {e}"))?;

        tracing::info!(
            event = "adapter.invocation",
            request_id = %request_id,
            body_bytes = body.len(),
            "Received Lambda invocation"
        );

        match handle_invocation(config, queue, registry, &request_id, &body) {
            Ok(output) => {
                let response_url = format!(
                    "http://{}/2018-06-01/runtime/invocation/{}/response",
                    config.runtime_api, request_id,
                );
                http.post(&response_url)
                    .send_bytes(&output)
                    .map_err(|e| format!("POST invocation response failed: {e}"))?;
            }
            Err(e) => {
                tracing::error!(
                    event = "adapter.invocation_error",
                    request_id = %request_id,
                    error = %e,
                    "Invocation failed"
                );
                let error_url = format!(
                    "http://{}/2018-06-01/runtime/invocation/{}/error",
                    config.runtime_api, request_id,
                );
                let _ = http
                    .post(&error_url)
                    .send_bytes(build_error_body(&e).as_bytes());
            }
        }
    }
}

/// Handle a single Lambda invocation using the shared dispatch.
fn handle_invocation(
    config: &AdapterConfig,
    queue: &Arc<dyn MessageQueue + Send + Sync>,
    registry: &Arc<dyn Registry>,
    request_id: &str,
    body: &[u8],
) -> Result<Vec<u8>, String> {
    use vlinder_provider_server::dispatch as shared;

    let payload = deserialize_invoke(body)?;
    let key = payload.key;
    let invoke_msg = payload.msg;

    let vlinder_core::domain::DataMessageKind::Invoke { ref agent, .. } = key.kind else {
        return Err("expected Invoke".into());
    };

    let result = shared::dispatch_invoke(queue, registry, config.agent_port, &key, &invoke_msg)?;

    let region = std::env::var("AWS_REGION")
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "unknown".to_string());

    let diagnostics = build_lambda_diagnostics(&config.agent, &region, result.duration_ms);
    shared::send_complete(
        queue.as_ref(),
        &key,
        agent,
        result.output.clone(),
        result.state,
        diagnostics,
    );

    tracing::info!(
        event = "adapter.invocation_complete",
        request_id = %request_id,
        duration_ms = result.duration_ms,
        output_bytes = result.output.len(),
        "Invocation complete"
    );

    Ok(result.output)
}
