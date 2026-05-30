//! Sidecar — the agent's reality controller.
//!
//! Runs as a standalone binary inside a Podman pod, alongside the agent
//! container. Owns queue and registry connections, mediates all
//! communication between the agent container and the platform.

use std::sync::Arc;

use vlinder_core::domain::{AgentName, ContainerId, HealthWindow, ImageDigest, ImageRef};

use vlinder_provider_server::factory;

use crate::config::SidecarConfig;
use crate::dispatch::{self, DispatchContext};
use crate::dispatch_endpoint::{self, DispatchEndpointState};
use crate::health;

/// The sidecar process — mediates between the platform queue and the agent container.
pub struct Sidecar {
    dispatch: DispatchContext,
    /// Agent name (queue subscription key).
    agent_name: String,
    /// Sliding window of agent health observations.
    health: HealthWindow,
    /// Port the `/v1/dispatch` HTTP endpoint binds to (Phase 5.1).
    dispatch_port: u16,
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
            dispatch_port: config.dispatch_port,
        })
    }

    /// Main: wait for agent, then run the `/v1/dispatch` HTTP server until
    /// shutdown.
    ///
    /// Phase 5.4 removes queue polling from the sidecar — the runtime now
    /// owns Invoke consumption and POSTs to the sidecar's `/v1/dispatch`.
    /// The sidecar still holds queue + registry + store handles (passed to
    /// the dispatch endpoint state so the lazy `SessionProvider` can use
    /// them for the agent's provider callbacks).
    pub async fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        health::wait_for_ready(
            &mut self.health,
            self.dispatch.container_port,
            &self.agent_name,
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        let endpoint_state = Arc::new(DispatchEndpointState {
            queue: self.dispatch.queue.clone(),
            registry: self.dispatch.registry.clone(),
            store: self.dispatch.store.clone(),
            container_port: self.dispatch.container_port,
            agent_name: AgentName::new(&self.agent_name),
            session_provider: tokio::sync::Mutex::new(None),
        });

        tracing::info!(
            event = "sidecar.started",
            agent = %self.agent_name,
            dispatch_port = self.dispatch_port,
            "Sidecar serving /v1/dispatch"
        );

        dispatch_endpoint::serve(endpoint_state, self.dispatch_port)
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e.into() })
    }

    #[allow(dead_code)]
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
