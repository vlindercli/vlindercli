//! Integration tests for `ContainerRuntime` (long-running model).
//!
//! Requires: podman installed + `just build-fleet-agent todoapp echo-container` + NATS running.
//! The sidecar creates its own queue from config, so these tests require
//! shared queue infrastructure (NATS) — in-memory queues won't work.
//!
//! Compile with: cargo test --features test-support

#![cfg(feature = "test-support")]

use vlinder_core::domain::{
    Agent, AgentName, BranchId, DataMessageKind, DataRoutingKey, HarnessType, InvokeDiagnostics,
    InvokeMessage, MessageId, Runtime, RuntimeType, SessionId, SubmissionId,
};
use vlinder_podman_runtime::{ContainerRuntime, PodmanRuntimeConfig};
use vlinderd::config::Config;

#[test]
#[ignore] // Run via: just run-integration-tests
fn container_runtime_executes_echo_agent() {
    let config = Config::for_test();
    let registry =
        vlinderd::registry_factory::from_config(&config).expect("Failed to create registry");
    let podman_config = PodmanRuntimeConfig {
        image_policy: config.runtime.image_policy.clone(),
        podman_socket: config.runtime.podman_socket.clone(),
        sidecar_image: config.runtime.sidecar_image.clone(),
        queue: vlinder_core::domain::QueueBackend::Nats {
            url: config.queue.nats_url.clone(),
        },
        registry_addr: config.distributed.registry_addr.clone(),
        state_addr: config.distributed.state_addr.clone(),
        secret_addr: config.distributed.secret_addr.clone(),
    };
    let mut runtime = ContainerRuntime::new(&podman_config, registry.clone()).unwrap();

    // Use the injected registry and a queue from config for test setup.
    // The sidecar creates its own queue — requires shared infra (NATS) to work.
    let queue = vlinderd::queue_factory::recording_from_config(&config).unwrap();

    registry.register_runtime(RuntimeType::Container);

    let agent = Agent::from_toml(
        r#"
        name = "echo-container"
        description = "Echo container agent"
        runtime = "container"
        executable = "localhost/echo-container:latest"
        [requirements]

    "#,
    )
    .unwrap();
    registry.register_agent(agent).unwrap();
    let agent_id = AgentName::new("echo-container");

    // Send invoke
    let submission = SubmissionId::new();
    let session = SessionId::new();
    let key = DataRoutingKey {
        session: session.clone(),
        branch: BranchId::from(1),
        submission: submission.clone(),
        kind: DataMessageKind::Invoke {
            harness: HarnessType::Cli,
            runtime: RuntimeType::Container,
            agent: agent_id,
        },
    };
    let invoke = InvokeMessageV2 {
        id: MessageId::new(),
        state: None,
        diagnostics: InvokeDiagnostics {
            harness_version: String::new(),
        },
        dag_parent: String::new().into(),
        payload: b"hello from container".to_vec(),
    };
    queue.send_invoke(&key, &invoke).unwrap();

    // Tick until work completes
    let start = std::time::Instant::now();
    loop {
        runtime.tick();
        if start.elapsed() > std::time::Duration::from_secs(60) {
            panic!("container did not complete within 60 seconds");
        }
        // Check for completion
        if let Ok((complete, ack)) = queue.receive_complete(&submission, HarnessType::Cli) {
            assert_eq!(
                String::from_utf8(complete.payload).unwrap(),
                "hello from container"
            );
            ack().unwrap();
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Explicit shutdown (stops containers)
    runtime.shutdown();
}
