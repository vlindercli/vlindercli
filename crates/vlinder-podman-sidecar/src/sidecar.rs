//! Sidecar — the agent's reality controller.
//!
//! Runs as a standalone binary inside a Podman pod, alongside the agent
//! container. Owns queue and registry connections, mediates all
//! communication between the agent container and the platform.

use std::time::Duration;

use vlinder_core::domain::{
    AgentName, ContainerId, DataMessageKind, HealthWindow, ImageDigest, ImageRef,
};

use vlinder_provider_server::factory;

use crate::config::SidecarConfig;
use crate::dispatch::{self, DispatchContext, DurableSession, InvokeOutcome};
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
        let queue = factory::connect_queue(
            &config.nats_url,
            &config.state_url,
            config.secret_url.as_deref(),
        )?;
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
    #[allow(clippy::too_many_lines)]
    pub fn run(mut self) -> Result<(), Box<dyn std::error::Error>> {
        health::wait_for_ready(
            &mut self.health,
            self.dispatch.container_port,
            &self.agent_name,
        )
        .map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;

        tracing::info!(event = "sidecar.started", agent = %self.agent_name, "Sidecar loop started");

        let mut durable_session: Option<DurableSession> = None;

        loop {
            let agent_id = AgentName::new(&self.agent_name);

            // Poll for service responses first (durable mode).
            if let Some(session) = durable_session.take() {
                match self.dispatch.queue.receive_response(
                    &session.submission,
                    session.pending_service,
                    session.pending_operation,
                    session.pending_sequence,
                ) {
                    Ok((_key, response, ack)) => {
                        let _ = ack();
                        match dispatch::handle_service_response(&self.dispatch, session, &response)
                        {
                            Ok(InvokeOutcome::Done) => {}
                            Ok(InvokeOutcome::Pending(next)) => {
                                durable_session = Some(*next);
                            }
                            Err(e) => {
                                tracing::error!(
                                    event = "durable.handler_error",
                                    error = %e,
                                    agent = %self.agent_name,
                                    "Durable handler failed"
                                );
                                break;
                            }
                        }
                    }
                    Err(vlinder_core::domain::QueueError::Timeout) => {
                        // No response yet — put session back and continue polling.
                        durable_session = Some(session);
                    }
                    Err(e) => {
                        tracing::warn!(
                            event = "durable.response_error",
                            error = %e,
                            "Failed to receive service response"
                        );
                        break;
                    }
                }
            } else if let Ok((key, invoke, ack)) = self.dispatch.queue.receive_invoke(&agent_id) {
                let _ = ack();
                let DataMessageKind::Invoke {
                    harness,
                    runtime: _,
                    agent,
                } = &key.kind
                else {
                    continue;
                };
                tracing::info!(
                    event = "dispatch.started",
                    sha = %key.submission,
                    session = %key.session,
                    agent = %agent,
                    "Dispatching invoke to container"
                );
                match dispatch::handle_invoke(
                    &self.dispatch,
                    &mut self.health,
                    key.branch,
                    key.submission.clone(),
                    key.session.clone(),
                    agent.clone(),
                    *harness,
                    invoke.payload,
                    invoke.state,
                ) {
                    Ok(InvokeOutcome::Done) => {}
                    Ok(InvokeOutcome::Pending(session)) => {
                        durable_session = Some(*session);
                    }
                    Err(e) => {
                        tracing::error!(
                            event = "dispatch.error",
                            error = %e,
                            agent = %self.agent_name,
                            "Dispatch failed"
                        );
                        break;
                    }
                }
            } else {
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        tracing::info!(event = "sidecar.stopped", agent = %self.agent_name, "Sidecar loop exited");
        Ok(())
    }
}
