//! Sidecar — the agent's reality controller.
//!
//! Runs as a standalone binary inside a Podman pod, alongside the agent
//! container. Owns queue and registry connections, mediates all
//! communication between the agent container and the platform.

use std::time::Duration;

use vlinder_core::domain::{AgentName, ContainerId, HealthWindow, ImageDigest, ImageRef};

use vlinder_provider_server::factory;

use crate::config::SidecarConfig;
use crate::dispatch::{self, DispatchContext};
use crate::health;

/// The sidecar process — mediates between the platform queue and the agent container.
pub struct Sidecar {
    dispatch: DispatchContext,
    /// Agent name (queue subscription key).
    agent_name: String,
    /// Sliding window of agent health observations.
    health: HealthWindow,
}

impl Sidecar {
    /// Create a new sidecar from env-var configuration.
    ///
    /// Connects to NATS (with DAG recording) and the Registry Service,
    /// then fetches the Agent from the registry to determine storage backends.
    pub fn new(config: &SidecarConfig) -> Result<Self, Box<dyn std::error::Error>> {
        use crate::config::QueueBackendConfig;

        let queue_config = match &config.queue {
            QueueBackendConfig::Nats { url, secret_url } => {
                let nats = factory::resolve_nats_config(secret_url.as_deref(), url);
                factory::QueueConfig::Nats(nats)
            }
            #[cfg(feature = "sqs")]
            QueueBackendConfig::Sqs {
                region,
                queue_prefix,
            } => factory::QueueConfig::Sqs(vlinder_sqs::SqsConfig {
                region: region.clone(),
                queue_prefix: queue_prefix.clone(),
            }),
        };
        let queue = factory::connect(&queue_config)?;
        let store = factory::connect_state(&config.state_url)?;
        let queue = factory::with_recording(queue, store);
        let registry = factory::connect_registry(&config.registry_url)?;
        let image_ref = config
            .image_ref
            .as_ref()
            .and_then(|r| ImageRef::parse(r).ok());
        let image_digest = config
            .image_digest
            .as_ref()
            .and_then(|d| ImageDigest::parse(d).ok());
        let container_id = config
            .container_id
            .as_ref()
            .map_or_else(ContainerId::unknown, ContainerId::new);

        Ok(Self {
            dispatch: DispatchContext {
                queue,
                registry,
                container_port: config.container_port,
                container_id,
                image_ref,
                image_digest,
            },
            agent_name: config.agent.clone(),
            health: HealthWindow::new(60_000), // 60 second window
        })
    }

    /// Main loop: wait for agent, then poll invoke/delegate/response queues.
    pub fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        health::wait_for_ready(
            &mut self.health,
            self.dispatch.container_port,
            &self.agent_name,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        tracing::info!(event = "sidecar.started", agent = %self.agent_name, "Sidecar loop started");

        loop {
            let agent_id = AgentName::new(&self.agent_name);

            if let Ok((key, invoke, ack)) = self.dispatch.queue.receive_invoke(&agent_id) {
                let _ = ack();
                tracing::info!(
                    event = "dispatch.started",
                    submission = %key.submission,
                    session = %key.session,
                    "Dispatching invoke to container"
                );
                dispatch::handle_invoke(&self.dispatch, &mut self.health, &key, &invoke);
            } else {
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}
