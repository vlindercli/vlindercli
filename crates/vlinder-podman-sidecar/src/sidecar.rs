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
    pub async fn new(config: &SidecarConfig) -> Result<Self, Box<dyn std::error::Error>> {
        use crate::config::QueueBackendConfig;

        let queue = match &config.queue {
            QueueBackendConfig::Nats { url } => {
                let nats_config =
                    factory::resolve_nats_config(config.secret_url.as_deref(), url).await;
                factory::connect_async(&factory::QueueConfig::Nats(nats_config)).await?
            }
            QueueBackendConfig::Amqp { url } => {
                let amqp_config = vlinder_amqp::AmqpConfig { url: url.clone() };
                factory::connect_async(&factory::QueueConfig::Amqp(amqp_config)).await?
            }
        };
        let store = factory::connect_state_async(&config.state_url).await?;
        let queue = factory::with_recording(queue, store.clone());
        let registry = factory::connect_registry_async(&config.registry_url).await?;
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
                store,
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
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        health::wait_for_ready(
            &mut self.health,
            self.dispatch.container_port,
            &self.agent_name,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let agent_id = AgentName::new(&self.agent_name);

        // Receive the first invoke so we can latch session_id/branch/submission
        // for the session-scoped ProviderServer. These fields are stable across
        // every invoke in a run (verified empirically; submission_id is run-scoped,
        // not invoke-scoped). Retry on receive timeout — that's normal polling
        // behavior, not a fatal error.
        let (first_key, first_invoke, first_ack) = loop {
            match self.dispatch.queue.receive_invoke(&agent_id).await {
                Ok(tuple) => break tuple,
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        };
        let _ = first_ack().await;

        let session_provider = vlinder_provider_server::dispatch::start_session_provider(
            &self.dispatch.queue,
            &self.dispatch.registry,
            &agent_id,
            first_key.branch,
            first_key.submission.clone(),
            first_key.session.clone(),
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        tracing::info!(event = "sidecar.started", agent = %self.agent_name, "Sidecar loop started");

        // Dispatch the first invoke we already pulled.
        self.dispatch_one(&session_provider, &first_key, &first_invoke)
            .await;

        loop {
            if let Ok((key, invoke, ack)) = self.dispatch.queue.receive_invoke(&agent_id).await {
                let _ = ack().await;
                self.dispatch_one(&session_provider, &key, &invoke).await;
            } else {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    async fn dispatch_one(
        &mut self,
        session_provider: &vlinder_provider_server::dispatch::SessionProvider,
        key: &vlinder_core::domain::DataRoutingKey,
        invoke: &vlinder_core::domain::InvokeMessage,
    ) {
        tracing::debug!(
            event = "chain_head_trace",
            site = "sidecar.receive_invoke",
            session = %key.session,
            submission = %key.submission,
            agent = %self.agent_name,
            msg_dag_id = %invoke.dag_id,
            msg_dag_parent = %invoke.dag_parent,
            "sidecar received invoke from queue"
        );
        tracing::info!(
            event = "dispatch.started",
            submission = %key.submission,
            session = %key.session,
            "Dispatching invoke to container"
        );
        dispatch::handle_invoke(
            &self.dispatch,
            &mut self.health,
            session_provider,
            key,
            invoke,
        )
        .await;
    }
}
