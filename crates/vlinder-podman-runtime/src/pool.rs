//! `ContainerRuntime` — manages the lifecycle of Podman pods for container agents.
//!
//! Two pod lifecycles coexist in v1:
//! 1. Per-agent pods, created at deploy by `start()`. Vestigial under the
//!    new dispatch model; their sidecar is never targeted by /v1/dispatch.
//!    Removal is a follow-up cleanup outside this branch's scope.
//! 2. Per-`(session, branch)` actor pods, created lazily by
//!    `activate_actor()` on the first invoke for each `(session, branch)`.
//!    Each carries the agent + sidecar containers, with the sidecar's
//!    `/v1/dispatch` host port runtime-chosen via `PortMapping`.
//!
//! `ensure_containers` is the reconciliation loop: per-agent pod create on
//! transition to Deploying, tear-down on Deleting. `shutdown` drains both
//! lifecycles plus all per-agent invoke tasks.

use std::collections::HashMap;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::time::{Duration, Instant};

use vlinder_core::domain::{
    Agent, AgentName, AgentStatus, BranchId, ImageRef, MessageQueue, PodId, ReadinessCheck,
    Registry, RegistryRepository, ResourceId, Runtime, RuntimeType, SessionId, WorkspaceStore,
};

use async_trait::async_trait;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::PodmanRuntimeConfig;
use crate::podman_client::{PodmanClient, RunTarget};

/// Image resolution policy for container agents (ADR 073).
///
/// Controls which OCI reference is passed to `podman run`:
/// - `Mutable`: Uses the tag from `agent.executable` — rebuilt images picked up automatically.
/// - `Pinned`: Uses the content-addressed digest from `agent.image_digest` — deterministic.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ImagePolicy {
    Mutable,
    Pinned,
}

impl ImagePolicy {
    pub fn from_config(s: &str) -> Self {
        match s {
            "pinned" => Self::Pinned,
            _ => Self::Mutable,
        }
    }
}

/// A running pod: agent + sidecar containers sharing a network namespace.
struct Pod {
    pod_id: PodId,
}

/// Orchestrates Podman pods for container agents.
///
/// Maps agent names to running pods. Each pod contains an agent container
/// and a sidecar container. The sidecar handles dispatch; the runtime handles
/// compute lifecycle.
pub struct ContainerRuntime {
    id: ResourceId,
    registry: Arc<dyn Registry>,
    repo: Arc<dyn RegistryRepository>,
    pods: HashMap<String, Pod>,
    config: PodmanRuntimeConfig,
    image_policy: ImagePolicy,
    podman: Arc<dyn PodmanClient>,
    /// Queue handle consumed by the per-agent invoke task.
    queue: Arc<dyn MessageQueue + Send + Sync>,
    /// Optional versioned workspace store (ADR 133). `None` when the daemon
    /// has no `[workspace_store]` configured — agents then run without a
    /// versioned workspace bind-mount.
    workspace_store: Option<Arc<dyn WorkspaceStore>>,
    /// Per-(session, branch) actor TTL. v1 default = INFINITY (0 secs). No
    /// eviction logic runs in v1.
    #[allow(dead_code)]
    actor_ttl: Duration,
    /// Per-(session, branch) actor registry. Lazily populated by
    /// `activate_actor` on the first invoke for each `(session, branch)`.
    actors: Arc<AsyncMutex<HashMap<(SessionId, BranchId), ActorPod>>>,
    /// Per-agent background invoke tasks (mirrors Lambda's pattern).
    invoke_tasks: HashMap<String, (JoinHandle<()>, CancellationToken)>,
    /// Parent cancellation token; per-agent tasks get a child token.
    shutdown: CancellationToken,
    /// Allocator for runtime-chosen sidecar dispatch ports. Starts at 32000.
    port_allocator: Arc<AtomicU16>,
}

/// In-memory record of a per-(session, branch) pod (the "actor's realization").
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ActorPod {
    pod_id: PodId,
    /// Host port the sidecar's `/v1/dispatch` endpoint is published on.
    sidecar_dispatch_port: u16,
    /// For future TTL eviction; unused in v1.
    created_at: Instant,
}

/// Sidecar dispatch port inside the pod (matches the sidecar's default; the
/// runtime publishes this to a runtime-chosen host port via `PortMapping`).
const SIDECAR_DISPATCH_CONTAINER_PORT: u16 = 3546;

impl ContainerRuntime {
    /// Tear down all `(session, branch)` actor pods for a given agent —
    /// called from the Deleting branch of `ensure_containers`. v1: drains
    /// every actor in the registry without filtering by agent (since v1
    /// only deploys a single agent per `ContainerRuntime` in practice).
    /// A future per-agent filter will land when multi-agent deployments are
    /// exercised.
    async fn drain_actors_for(&self, _agent: &str) {
        let actors_map: Vec<((SessionId, BranchId), ActorPod)> = {
            let mut guard = self.actors.lock().await;
            guard.drain().collect()
        };
        for ((_session, _branch), pod) in actors_map {
            self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
        }
    }
}

impl ContainerRuntime {
    pub fn new(
        config: &PodmanRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        podman: Arc<dyn PodmanClient>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
        workspace_store: Option<Arc<dyn WorkspaceStore>>,
        actor_ttl: Duration,
    ) -> Self {
        let registry_id = ResourceId::new(&config.registry_addr);
        let id = ResourceId::new(format!(
            "{}/runtimes/{}",
            registry_id.as_str(),
            RuntimeType::Container.as_str()
        ));
        let image_policy = ImagePolicy::from_config(&config.image_policy);
        tracing::info!(event = "runtime.image_policy", policy = ?image_policy, "Container image policy");
        tracing::info!(
            event = "runtime.workspace_store",
            configured = workspace_store.is_some(),
            actor_ttl_secs = actor_ttl.as_secs(),
            "Container runtime substrates wired"
        );
        Self {
            id,
            registry,
            repo,
            pods: HashMap::new(),
            config: config.clone(),
            image_policy,
            podman,
            queue,
            workspace_store,
            actor_ttl,
            actors: Arc::new(AsyncMutex::new(HashMap::new())),
            invoke_tasks: HashMap::new(),
            shutdown: CancellationToken::new(),
            port_allocator: Arc::new(AtomicU16::new(32000)),
        }
    }

    /// Spawn a per-agent background task that long-polls invokes and dispatches
    /// them to per-(session, branch) actor pods. Mirrors the pattern in
    /// `vlinder-lambda-runtime::LambdaRuntime::spawn_invoke_task`.
    ///
    /// Called from `ensure_containers` after the per-agent pod is up; the
    /// task owns queue consumption from then until `undeploy` or `shutdown`
    /// cancels its child token.
    #[allow(clippy::too_many_lines)]
    pub(crate) fn spawn_invoke_task(&mut self, agent_name: &AgentName) {
        use vlinder_core::domain::{
            CompleteMessage, DataMessageKind, DataRoutingKey, MessageId, QueueError,
            RuntimeDiagnostics,
        };

        let token = self.shutdown.child_token();
        let child = token.clone();
        let queue = Arc::clone(&self.queue);
        let workspace_store = self.workspace_store.clone();
        let podman = Arc::clone(&self.podman);
        let actors = Arc::clone(&self.actors);
        let port_allocator = Arc::clone(&self.port_allocator);
        let agent_id = agent_name.clone();
        let registry = Arc::clone(&self.registry);
        let sidecar_image = self.config.sidecar_image.clone();
        let pod_runtime_config = self.config.clone();

        let handle = tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .timeout(Duration::from_mins(5))
                .build()
                .expect("reqwest client must build");

            loop {
                let received = tokio::select! {
                    () = child.cancelled() => break,
                    result = queue.receive_invoke(&agent_id) => result,
                };

                let (key, invoke, ack) = match received {
                    Ok(tuple) => tuple,
                    Err(QueueError::Timeout) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            agent = agent_id.as_str(),
                            error = %e,
                            "container invoke receive error"
                        );
                        continue;
                    }
                };
                let _ = ack().await;

                let DataMessageKind::Invoke { harness, agent, .. } = &key.kind else {
                    continue;
                };

                let session = key.session.clone();
                let branch = key.branch;

                // 1. HashMap lookup; if miss, activate the actor pod.
                let dispatch_port = {
                    let mut guard = actors.lock().await;
                    if let Some(existing) = guard.get(&(session.clone(), branch)) {
                        existing.sidecar_dispatch_port
                    } else {
                        match activate_actor(
                            &podman,
                            workspace_store.as_ref(),
                            &registry,
                            &port_allocator,
                            &pod_runtime_config,
                            &sidecar_image,
                            &agent_id,
                            &session,
                            branch,
                            invoke.state.as_deref(),
                        )
                        .await
                        {
                            Ok(pod) => {
                                let port = pod.sidecar_dispatch_port;
                                guard.insert((session.clone(), branch), pod);
                                port
                            }
                            Err(e) => {
                                tracing::error!(
                                    event = "actor.activate_failed",
                                    agent = agent_id.as_str(),
                                    error = %e,
                                    "Failed to activate actor pod"
                                );
                                send_error_complete(&*queue, &key, agent, *harness, e).await;
                                continue;
                            }
                        }
                    }
                };

                // 2. POST to sidecar's /v1/dispatch.
                let url = format!("http://localhost:{dispatch_port}/v1/dispatch");
                let body = serde_json::json!({ "key": key, "msg": invoke });
                let dispatch_result = http.post(&url).json(&body).send().await;

                let resp = match dispatch_result {
                    Ok(r) if r.status().is_success() => r,
                    Ok(r) => {
                        let status = r.status();
                        let body = r.text().await.unwrap_or_default();
                        let err = format!("dispatch returned {status}: {body}");
                        tracing::error!(event = "dispatch.failed", agent = agent_id.as_str(), error = %err);
                        send_error_complete(&*queue, &key, agent, *harness, err).await;
                        continue;
                    }
                    Err(e) => {
                        let err = format!("dispatch transport failed: {e}");
                        tracing::error!(event = "dispatch.failed", agent = agent_id.as_str(), error = %err);
                        send_error_complete(&*queue, &key, agent, *harness, err).await;
                        continue;
                    }
                };

                let dispatch_resp = match resp.json::<DispatchWire>().await {
                    Ok(d) => d,
                    Err(e) => {
                        let err = format!("dispatch response decode failed: {e}");
                        tracing::error!(event = "dispatch.failed", agent = agent_id.as_str(), error = %err);
                        send_error_complete(&*queue, &key, agent, *harness, err).await;
                        continue;
                    }
                };

                // 3. workspace_store.snapshot (if configured).
                let state_pointer = match &workspace_store {
                    Some(ws) => match ws
                        .snapshot(&agent_id, &session, &branch, &dispatch_resp.chain_head)
                        .await
                    {
                        Ok(sp) => Some(sp),
                        Err(e) => {
                            tracing::warn!(
                                event = "workspace.snapshot_failed",
                                agent = agent_id.as_str(),
                                error = %e,
                                "Falling back to agent-reported state"
                            );
                            dispatch_resp.state.clone()
                        }
                    },
                    None => dispatch_resp.state.clone(),
                };

                // 4. send_complete.
                let complete_key = DataRoutingKey {
                    session,
                    branch,
                    submission: key.submission.clone(),
                    kind: DataMessageKind::Complete {
                        agent: agent.clone(),
                        harness: *harness,
                    },
                };
                let complete = CompleteMessage {
                    id: MessageId::new(),
                    dag_id: dispatch_resp.chain_head.clone(),
                    dag_parent: dispatch_resp.chain_head.clone(),
                    state: state_pointer,
                    diagnostics: RuntimeDiagnostics::placeholder(dispatch_resp.duration_ms),
                    content: dispatch_resp.content,
                    tool_calls: dispatch_resp.tool_calls,
                    payload: dispatch_resp.output,
                };
                if let Err(e) = queue.send_complete(complete_key, complete).await {
                    tracing::error!(
                        event = "complete.send_failed",
                        agent = agent_id.as_str(),
                        error = %e,
                    );
                }
            }
        });

        self.invoke_tasks
            .insert(agent_name.as_str().to_string(), (handle, token));
    }

    /// Access the registry (test-only, for integration test setup).
    #[cfg(test)]
    pub fn registry(&self) -> &Arc<dyn Registry> {
        &self.registry
    }

    /// Start a pod with agent + sidecar containers.
    ///
    /// 1. Compute the workspace bind-mount, if any (ADR 133)
    /// 2. Create a Podman pod named `vlinder-{name}`
    /// 3. Add the agent container (user image, with workspace bind-mount)
    /// 4. Add the sidecar container (vlinder-podman-sidecar image, env vars for config)
    /// 5. Start the pod (all containers start together)
    async fn start(&mut self, name: &str, agent: &Agent) -> Result<(), String> {
        if let Some(pod) = self.pods.get(name) {
            // Clone pod_id to release the borrow on self.pods before awaiting.
            let pod_id = pod.pod_id.clone();
            if self.podman.is_pod_live(&pod_id).await {
                return Ok(());
            }
            // Pod in hashmap but not running — remove stale entry
            tracing::warn!(agent = name, "Pod not running, recreating");
            let _ = self.pods.remove(name).unwrap();
        }

        let image_ref = ImageRef::parse(&agent.executable)
            .unwrap_or_else(|_| ImageRef::parse("unknown/unknown").unwrap());

        // Select what to pass to `podman run` based on policy (ADR 073)
        let run_target = match self.image_policy {
            ImagePolicy::Mutable => RunTarget::Ref(&image_ref),
            ImagePolicy::Pinned => agent
                .image_digest
                .as_ref()
                .map(RunTarget::Digest)
                .unwrap_or(RunTarget::Ref(&image_ref)),
        };

        // 1. Workspace bind-mount (ADR 133). Per-(session, branch) bind-mount
        //    construction is wired in Phase 5.3 (spawn_invoke_task) once the
        //    runtime owns Arc<dyn WorkspaceStore>. For now no bind-mount is
        //    added here; the manifest's [requirements.mount] declaration is
        //    parsed but not yet acted on.
        let volume_pairs: Vec<(String, String)> = Vec::new();
        let _ = agent.requirements.mount.as_ref();

        // 2. Create pod (with host aliases for provider hostnames)
        // All *.vlinder.local hostnames are added unconditionally — the sidecar
        // only binds the ones the agent needs. Extra entries are harmless.
        // See #34 for replacing this with a sidecar DNS resolver.
        //
        // `metadata.vlinder.local` is the platform metadata endpoint (serves /v1/tools
        // and future endpoints such as /v1/history and /v1/memory).
        let host_aliases = vec![
            "vlinder.local:127.0.0.1".to_string(),
            "metadata.vlinder.local:127.0.0.1".to_string(),
            "runtime.vlinder.local:127.0.0.1".to_string(),
            "ollama.vlinder.local:127.0.0.1".to_string(),
            "openrouter.vlinder.local:127.0.0.1".to_string(),
            "sqlite-vec.vlinder.local:127.0.0.1".to_string(),
            "sqlite-kv.vlinder.local:127.0.0.1".to_string(),
        ];

        let pod_name = format!("vlinder-{name}");
        // Phase 5.2: trait carries port mappings, but pool.rs passes an empty
        // slice for now. Phase 5.3 introduces the actor registry and starts
        // populating this with the runtime-chosen sidecar dispatch port.
        let pod_id = self
            .podman
            .pod_create(&pod_name, &host_aliases, &[])
            .await
            .map_err(|e| e.to_string())?;

        // From here on, if anything fails we must remove the orphaned pod
        // and clean up any volumes we created.
        // Otherwise the next tick will try pod_create again and get "already exists".
        if let Err(e) = self
            .start_in_pod(name, &pod_id, run_target, &image_ref, &volume_pairs)
            .await
        {
            tracing::warn!(
                event = "pod.cleanup",
                agent = %name,
                pod = %pod_id,
                "Removing orphaned pod after start failure"
            );
            self.podman.pod_stop_and_remove(&pod_id, 0).await;
            return Err(e);
        }

        tracing::info!(
            event = "pod.started",
            agent = %name,
            pod = %pod_id,
            image_ref = %image_ref,
            "Pod started (agent + sidecar)"
        );

        self.pods.insert(name.to_string(), Pod { pod_id });
        Ok(())
    }

    /// Populate and start a pod that has already been created.
    ///
    /// Adds the agent container (with mount volumes), the sidecar container,
    /// and starts the pod.
    /// Called by `start()` — if this fails, `start()` cleans up the orphaned pod.
    async fn start_in_pod(
        &self,
        name: &str,
        pod_id: &PodId,
        run_target: RunTarget<'_>,
        image_ref: &ImageRef,
        volumes: &[(String, String)],
    ) -> Result<(), String> {
        // Build volume refs for the agent container
        let volume_refs: Vec<(&str, &str)> = volumes
            .iter()
            .map(|(vol, path)| (vol.as_str(), path.as_str()))
            .collect();

        // 3. Add agent container (with mount volumes, no env vars)
        self.podman
            .container_in_pod(run_target, pod_id, &[], &volume_refs)
            .await
            .map_err(|e| e.to_string())?;

        // 4. Build sidecar env vars
        let sidecar_image_ref = ImageRef::parse(&self.config.sidecar_image).unwrap_or_else(|_| {
            ImageRef::parse("localhost/vlinder-podman-sidecar:latest").unwrap()
        });
        let sidecar_target = RunTarget::Ref(&sidecar_image_ref);

        let registry_url = format!(
            "http://host.containers.internal:{}",
            extract_port(&self.config.registry_addr, 9090)
        );
        let state_url = format!(
            "http://host.containers.internal:{}",
            extract_port(&self.config.state_addr, 9092)
        );
        let secret_url = format!(
            "http://host.containers.internal:{}",
            extract_port(&self.config.secret_addr, 9093)
        );

        let image_digest_str = self
            .podman
            .image_digest(image_ref)
            .await
            .map(String::from)
            .unwrap_or_default();

        let mut env_vars: Vec<(&str, String)> = vec![
            ("VLINDER_AGENT", name.to_string()),
            ("VLINDER_QUEUE_BACKEND", self.config.queue_backend.clone()),
            ("VLINDER_REGISTRY_URL", registry_url),
            ("VLINDER_STATE_URL", state_url),
            ("VLINDER_SECRET_URL", secret_url),
            ("VLINDER_CONTAINER_PORT", "8080".to_string()),
            ("VLINDER_IMAGE_REF", image_ref.as_str().to_string()),
            ("VLINDER_IMAGE_DIGEST", image_digest_str),
        ];

        if self.config.queue_backend == "amqp" {
            let amqp_url = rewrite_host_for_container(&self.config.amqp_url);
            env_vars.push(("VLINDER_AMQP_URL", amqp_url));
        } else {
            let nats_url = format!(
                "nats://host.containers.internal:{}",
                extract_port(&self.config.nats_url, 4222)
            );
            env_vars.push(("VLINDER_NATS_URL", nats_url));
        }
        let env_refs: Vec<(&str, &str)> = env_vars.iter().map(|(k, v)| (*k, v.as_str())).collect();

        // 5. Add sidecar container (no volumes — sidecar doesn't need file mounts)
        self.podman
            .container_in_pod(sidecar_target, pod_id, &env_refs, &[])
            .await
            .map_err(|e| e.to_string())?;

        // 6. Start the pod (all containers start together)
        self.podman
            .pod_start(pod_id)
            .await
            .map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Reconcile pods with the registry: remove orphans, start missing.
    async fn ensure_containers(&mut self) {
        let agents = self
            .registry
            .get_agents_by_runtime(RuntimeType::Container)
            .await;

        // Collect agent names from registry
        let agent_names: std::collections::HashSet<&str> =
            agents.iter().map(|a| a.name.as_str()).collect();

        // Stop pods for agents no longer in registry (orphan cleanup)
        let orphaned: Vec<String> = self
            .pods
            .keys()
            .filter(|name| !agent_names.contains(name.as_str()))
            .cloned()
            .collect();

        for name in orphaned {
            if let Some(pod) = self.pods.remove(&name) {
                tracing::info!(event = "pod.orphaned", agent = %name, "Stopping orphaned pod");
                self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
            }
        }

        for agent in &agents {
            let status = self.repo.get_derived_status(&agent.name).ok().flatten();

            match status.as_ref() {
                // Deploying: start pod, transition to Live or Failed
                Some(AgentStatus::Deploying) => match self.start(&agent.name, agent).await {
                    Ok(()) => {
                        let agent_name = AgentName::new(&agent.name);
                        let check = ReadinessCheck::pending(
                            agent_name.clone(),
                            RuntimeType::Container.as_str(),
                        )
                        .ready();
                        let _ = self.repo.append_readiness_check(&check);
                        // Phase 5.4: spawn the per-agent invoke task that
                        // owns queue consumption. Idempotent: don't double-
                        // spawn if a task is already running for this agent.
                        if !self.invoke_tasks.contains_key(agent.name.as_str()) {
                            self.spawn_invoke_task(&agent_name);
                        }
                        tracing::info!(
                            event = "pod.deployed",
                            agent = %agent.name,
                            "Agent provisioned: Live"
                        );
                    }
                    Err(e) => {
                        let agent_name = AgentName::new(&agent.name);
                        let check =
                            ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str())
                                .failed(e.clone());
                        let _ = self.repo.append_readiness_check(&check);
                        tracing::error!(
                            event = "pod.start_failed",
                            agent = %agent.name,
                            error = %e,
                            "Failed to start pod"
                        );
                    }
                },

                // Deleting: tear down pod + cancel invoke task + drain actors
                Some(AgentStatus::Deleting) => {
                    let agent_name = AgentName::new(&agent.name);
                    // 1. Cancel the per-agent invoke task so it stops
                    //    activating new actor pods.
                    if let Some((handle, token)) = self.invoke_tasks.remove(agent.name.as_str()) {
                        token.cancel();
                        let _ = handle.await;
                    }
                    // 2. Drain per-(session, branch) actor pods for this agent.
                    self.drain_actors_for(&agent.name).await;
                    // 3. Tear down the per-agent pod created by start().
                    if let Some(pod) = self.pods.remove(&agent.name) {
                        tracing::info!(event = "pod.teardown", agent = %agent.name, "Tearing down pod");
                        self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
                    }
                    let check =
                        ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str())
                            .deleted();
                    let _ = self.repo.append_readiness_check(&check);
                    tracing::info!(event = "agent.deleted", agent = %agent.name, "Agent torn down: Deleted");
                }

                // Live, Registered, or no state: nothing to do
                _ => {}
            }
        }
    }
}

#[async_trait]
impl Runtime for ContainerRuntime {
    fn id(&self) -> &ResourceId {
        &self.id
    }

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Container
    }

    async fn tick(&mut self) -> bool {
        let before = self.pods.len();
        self.ensure_containers().await;
        self.pods.len() != before
    }

    async fn shutdown(&mut self) {
        // 1. Cancel all per-agent invoke tasks so they stop activating new
        //    actor pods mid-shutdown.
        self.shutdown.cancel();
        let tasks: Vec<(String, (JoinHandle<()>, CancellationToken))> =
            self.invoke_tasks.drain().collect();
        for (_, (handle, _)) in tasks {
            let _ = handle.await;
        }
        // 2. Drain per-(session, branch) actor pods.
        let actors_map: Vec<((SessionId, BranchId), ActorPod)> = {
            let mut guard = self.actors.lock().await;
            guard.drain().collect()
        };
        for ((_session, _branch), pod) in actors_map {
            self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
        }
        // 3. Tear down the per-agent pods created by start().
        let pods: Vec<(String, Pod)> = self.pods.drain().collect();
        for (name, pod) in pods {
            tracing::info!(event = "pod.stopped", agent = %name, pod = %pod.pod_id, "Stopping pod");
            self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
        }
    }
}

/// Extract port number from a URL string, with a default fallback.
/// Rewrite localhost/127.0.0.1 in a URL to host.containers.internal
/// so containers inside a pod can reach host services.
fn rewrite_host_for_container(url: &str) -> String {
    url.replace("localhost", "host.containers.internal")
        .replace("127.0.0.1", "host.containers.internal")
}

fn extract_port(url: &str, default: u16) -> u16 {
    url.rsplit(':')
        .next()
        .and_then(|s| s.trim_end_matches('/').parse().ok())
        .unwrap_or(default)
}

/// Wire shape mirroring `vlinder_podman_sidecar::dispatch_endpoint::DispatchResponse`.
/// Duplicated here (rather than imported) to avoid a runtime → sidecar
/// dependency, in the same spirit as the zfs-storage ↔ zfs-server wire types.
#[derive(serde::Deserialize)]
struct DispatchWire {
    output: Vec<u8>,
    content: Option<String>,
    tool_calls: Option<Vec<vlinder_core::domain::ToolCall>>,
    state: Option<String>,
    duration_ms: u64,
    chain_head: vlinder_core::domain::DagNodeId,
}

/// Activate a fresh per-(session, branch) actor pod: allocate a sidecar
/// dispatch port, materialise the workspace clone (if configured), and
/// create + start a Podman pod identical in shape to the per-agent pod
/// created by `ContainerRuntime::start`/`start_in_pod`, but with the
/// workspace bind-mounted on the agent container and the sidecar's
/// `/v1/dispatch` port published.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn activate_actor(
    podman: &Arc<dyn PodmanClient>,
    workspace_store: Option<&Arc<dyn WorkspaceStore>>,
    registry: &Arc<dyn Registry>,
    port_allocator: &Arc<AtomicU16>,
    pod_runtime_config: &PodmanRuntimeConfig,
    sidecar_image: &str,
    agent_name: &AgentName,
    session: &SessionId,
    branch: BranchId,
    parent_state: Option<&str>,
) -> Result<ActorPod, String> {
    use std::sync::atomic::Ordering;

    use crate::podman_client::PortMapping;

    // 1. Look up the agent in the registry for its image + mount declaration.
    let agent = registry
        .get_agent_by_name(agent_name.as_str())
        .await
        .ok_or_else(|| format!("agent '{}' not in registry", agent_name.as_str()))?;
    let image_ref = ImageRef::parse(&agent.executable)
        .unwrap_or_else(|_| ImageRef::parse("unknown/unknown").unwrap());

    // 2. Workspace bind-mount (only if both manifest declares a mount and the
    //    daemon has a workspace_store configured).
    let mount_pair = match (agent.requirements.mount.as_ref(), workspace_store) {
        (Some(mount_decl), Some(ws)) => {
            let host_path = ws
                .ensure_workspace(agent_name, session, &branch, parent_state)
                .await
                .map_err(|e| format!("ensure_workspace: {e}"))?;
            Some((
                host_path.to_string_lossy().into_owned(),
                mount_decl.path.clone(),
            ))
        }
        _ => None,
    };

    // 3. Allocate the runtime-chosen sidecar dispatch port and create the pod
    //    with the published port mapping.
    let host_port = port_allocator.fetch_add(1, Ordering::Relaxed);
    let pod_name = format!(
        "vlinder-{}-{}-{}",
        agent_name.as_str(),
        short(session.to_string().as_str()),
        branch.as_i64()
    );
    let host_aliases = vec![
        "vlinder.local:127.0.0.1".to_string(),
        "metadata.vlinder.local:127.0.0.1".to_string(),
        "runtime.vlinder.local:127.0.0.1".to_string(),
        "ollama.vlinder.local:127.0.0.1".to_string(),
        "openrouter.vlinder.local:127.0.0.1".to_string(),
        "sqlite-vec.vlinder.local:127.0.0.1".to_string(),
        "sqlite-kv.vlinder.local:127.0.0.1".to_string(),
    ];
    let port_mappings = [PortMapping {
        host_port,
        container_port: SIDECAR_DISPATCH_CONTAINER_PORT,
    }];
    let pod_id = podman
        .pod_create(&pod_name, &host_aliases, &port_mappings)
        .await
        .map_err(|e| e.to_string())?;

    // 4. Add the agent container (with optional workspace bind-mount).
    let volume_owned: Vec<(String, String)> = mount_pair
        .iter()
        .map(|(h, c)| (h.clone(), c.clone()))
        .collect();
    let volume_refs: Vec<(&str, &str)> = volume_owned
        .iter()
        .map(|(h, c)| (h.as_str(), c.as_str()))
        .collect();
    let run_target = RunTarget::Ref(&image_ref);
    if let Err(e) = podman
        .container_in_pod(run_target, &pod_id, &[], &volume_refs)
        .await
    {
        podman.pod_stop_and_remove(&pod_id, 0).await;
        return Err(format!("agent container_in_pod: {e}"));
    }

    // 5. Build sidecar env vars (same shape as the existing per-agent
    //    `start_in_pod` path; reused intentionally).
    let sidecar_image_ref = ImageRef::parse(sidecar_image)
        .unwrap_or_else(|_| ImageRef::parse("localhost/vlinder-podman-sidecar:latest").unwrap());
    let sidecar_target = RunTarget::Ref(&sidecar_image_ref);

    let registry_url = format!(
        "http://host.containers.internal:{}",
        extract_port(&pod_runtime_config.registry_addr, 9090)
    );
    let state_url = format!(
        "http://host.containers.internal:{}",
        extract_port(&pod_runtime_config.state_addr, 9092)
    );
    let secret_url = format!(
        "http://host.containers.internal:{}",
        extract_port(&pod_runtime_config.secret_addr, 9093)
    );
    let image_digest_str = podman
        .image_digest(&image_ref)
        .await
        .map(String::from)
        .unwrap_or_default();

    let dispatch_port_str = SIDECAR_DISPATCH_CONTAINER_PORT.to_string();
    let mut env_vars: Vec<(&str, String)> = vec![
        ("VLINDER_AGENT", agent_name.as_str().to_string()),
        (
            "VLINDER_QUEUE_BACKEND",
            pod_runtime_config.queue_backend.clone(),
        ),
        ("VLINDER_REGISTRY_URL", registry_url),
        ("VLINDER_STATE_URL", state_url),
        ("VLINDER_SECRET_URL", secret_url),
        ("VLINDER_CONTAINER_PORT", "8080".to_string()),
        ("VLINDER_SIDECAR_DISPATCH_PORT", dispatch_port_str),
        ("VLINDER_IMAGE_REF", image_ref.as_str().to_string()),
        ("VLINDER_IMAGE_DIGEST", image_digest_str),
    ];
    if pod_runtime_config.queue_backend == "amqp" {
        let amqp_url = rewrite_host_for_container(&pod_runtime_config.amqp_url);
        env_vars.push(("VLINDER_AMQP_URL", amqp_url));
    } else {
        let nats_url = format!(
            "nats://host.containers.internal:{}",
            extract_port(&pod_runtime_config.nats_url, 4222)
        );
        env_vars.push(("VLINDER_NATS_URL", nats_url));
    }
    let env_refs: Vec<(&str, &str)> = env_vars.iter().map(|(k, v)| (*k, v.as_str())).collect();
    if let Err(e) = podman
        .container_in_pod(sidecar_target, &pod_id, &env_refs, &[])
        .await
    {
        podman.pod_stop_and_remove(&pod_id, 0).await;
        return Err(format!("sidecar container_in_pod: {e}"));
    }

    // 6. Start the pod.
    if let Err(e) = podman.pod_start(&pod_id).await {
        podman.pod_stop_and_remove(&pod_id, 0).await;
        return Err(format!("pod_start: {e}"));
    }

    tracing::info!(
        event = "actor.activated",
        agent = agent_name.as_str(),
        session = %session,
        branch = branch.as_i64(),
        pod = %pod_id,
        sidecar_dispatch_port = host_port,
        "Per-(session, branch) actor pod activated"
    );

    Ok(ActorPod {
        pod_id,
        sidecar_dispatch_port: host_port,
        created_at: Instant::now(),
    })
}

/// First 8 characters of a string (for pod name shortening). Pads with `x` if
/// shorter; safe for `(SessionId, BranchId)` UUIDs and arbitrary names.
fn short(s: &str) -> String {
    let mut out: String = s.chars().take(8).collect();
    while out.len() < 8 {
        out.push('x');
    }
    out
}

/// Construct a Complete carrying an error payload and send it on the queue.
/// Used by `spawn_invoke_task` whenever a stage in the dispatch pipeline
/// (activation, transport, decode, response) fails for an individual invoke.
async fn send_error_complete(
    queue: &(dyn vlinder_core::domain::MessageQueue + Send + Sync),
    invoke_key: &vlinder_core::domain::DataRoutingKey,
    agent: &AgentName,
    harness: vlinder_core::domain::HarnessType,
    err: impl std::fmt::Display,
) {
    use vlinder_core::domain::{
        CompleteMessage, DagNodeId, DataMessageKind, DataRoutingKey, MessageId, RuntimeDiagnostics,
    };

    let complete_key = DataRoutingKey {
        session: invoke_key.session.clone(),
        branch: invoke_key.branch,
        submission: invoke_key.submission.clone(),
        kind: DataMessageKind::Complete {
            agent: agent.clone(),
            harness,
        },
    };
    let complete = CompleteMessage {
        id: MessageId::new(),
        dag_id: DagNodeId::root(),
        dag_parent: DagNodeId::root(),
        state: None,
        diagnostics: RuntimeDiagnostics::placeholder(0),
        content: None,
        tool_calls: None,
        payload: format!("[error] {err}").into_bytes(),
    };
    let _ = queue.send_complete(complete_key, complete).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::podman_client::{PodmanError, RunTarget};
    use async_trait::async_trait;
    use vlinder_core::domain::{ContainerId, ImageDigest};

    /// Build a `PodmanRuntimeConfig` for tests (matches `vlinderd`'s `Config::for_test` defaults).
    fn test_config() -> PodmanRuntimeConfig {
        PodmanRuntimeConfig {
            image_policy: "mutable".to_string(),
            podman_socket: "disabled".to_string(),
            sidecar_image: "localhost/vlinder-podman-sidecar:latest".to_string(),
            queue_backend: "nats".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            amqp_url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
            registry_addr: "http://127.0.0.1:9090".to_string(),
            state_addr: "http://127.0.0.1:9092".to_string(),
            secret_addr: "http://127.0.0.1:9093".to_string(),
        }
    }

    /// Build an `InMemoryRegistry` for tests.
    fn test_registry() -> Arc<dyn Registry> {
        use vlinder_core::domain::{InMemoryRegistry, InMemorySecretStore};
        let secret_store = Arc::new(InMemorySecretStore::new());
        Arc::new(InMemoryRegistry::new(secret_store))
    }

    fn test_repo() -> Arc<dyn RegistryRepository> {
        Arc::new(vlinder_core::domain::InMemoryDagStore::new())
    }

    fn test_queue() -> Arc<dyn MessageQueue + Send + Sync> {
        Arc::new(vlinder_core::queue::InMemoryQueue::new())
    }

    struct MockPodmanClient;

    #[async_trait]
    impl PodmanClient for MockPodmanClient {
        async fn engine_version(&self) -> Option<semver::Version> {
            Some(semver::Version::new(5, 0, 0))
        }
        async fn image_digest(&self, _: &ImageRef) -> Option<ImageDigest> {
            None
        }
        async fn pod_create(
            &self,
            _: &str,
            _: &[String],
            _: &[crate::podman_client::PortMapping],
        ) -> Result<PodId, PodmanError> {
            Ok(PodId::new("mock-pod"))
        }
        async fn container_in_pod(
            &self,
            _: RunTarget<'_>,
            _: &PodId,
            _: &[(&str, &str)],
            _: &[(&str, &str)],
        ) -> Result<ContainerId, PodmanError> {
            Ok(ContainerId::new("mock-container"))
        }
        async fn volume_create(
            &self,
            _: &str,
            _: &str,
            _: &[(&str, &str)],
        ) -> Result<(), PodmanError> {
            Ok(())
        }
        async fn volume_rm(&self, _: &str) {}
        async fn is_pod_live(&self, _: &PodId) -> bool {
            true
        }
        async fn pod_start(&self, _: &PodId) -> Result<(), PodmanError> {
            Ok(())
        }
        async fn pod_stop_and_remove(&self, _: &PodId, _: u32) {}
    }

    fn test_runtime() -> ContainerRuntime {
        ContainerRuntime::new(
            &test_config(),
            test_registry(),
            test_repo(),
            Arc::new(MockPodmanClient),
            test_queue(),
            None,
            Duration::from_secs(0),
        )
    }

    #[test]
    fn image_policy_from_config_pinned() {
        assert_eq!(ImagePolicy::from_config("pinned"), ImagePolicy::Pinned);
    }

    #[test]
    fn image_policy_from_config_mutable() {
        assert_eq!(ImagePolicy::from_config("mutable"), ImagePolicy::Mutable);
    }

    #[test]
    fn image_policy_from_config_default_is_mutable() {
        assert_eq!(ImagePolicy::from_config(""), ImagePolicy::Mutable);
        assert_eq!(ImagePolicy::from_config("unknown"), ImagePolicy::Mutable);
    }

    #[test]
    fn extract_port_nats_url() {
        assert_eq!(extract_port("nats://localhost:4222", 4222), 4222);
    }

    #[test]
    fn extract_port_http_url() {
        assert_eq!(extract_port("http://127.0.0.1:9090", 9090), 9090);
    }

    #[test]
    fn extract_port_custom_port() {
        assert_eq!(extract_port("nats://myhost:5555", 4222), 5555);
    }

    #[test]
    fn extract_port_no_port_returns_default() {
        assert_eq!(extract_port("nats://localhost", 4222), 4222);
    }

    #[test]
    fn extract_port_trailing_slash() {
        assert_eq!(extract_port("http://localhost:9090/", 9090), 9090);
    }

    #[test]
    fn runtime_id_format() {
        let runtime = test_runtime();

        assert_eq!(
            runtime.id().as_str(),
            "http://127.0.0.1:9090/runtimes/container"
        );
        assert_eq!(runtime.runtime_type(), RuntimeType::Container);
    }

    #[tokio::test]
    async fn tick_returns_false_when_no_agents() {
        let mut runtime = test_runtime();

        assert!(!runtime.tick().await);
    }

    // ── S3 mount volume naming (ADR 107) ──

    #[test]
    fn parse_s3_with_prefix() {
        let s3 = "vlinder-support/v0.1.0/";
        let (bucket, prefix) = s3.split_once('/').unwrap();
        assert_eq!(bucket, "vlinder-support");
        assert_eq!(format!("/{prefix}"), "/v0.1.0/");
    }

    #[test]
    fn parse_s3_bucket_only() {
        let s3 = "my-bucket";
        let result = s3.split_once('/');
        assert!(result.is_none());
        // Falls back to bucket = "my-bucket", prefix = "/"
    }

    #[test]
    fn mount_volume_name_format() {
        let name = format!("vlinder-mount-{}-{}", "support", "knowledge");
        assert_eq!(name, "vlinder-mount-support-knowledge");
    }

    use vlinder_core::domain::{AgentManifest, AgentName, AgentStatus, RequirementsConfig};

    async fn register_test_agent(runtime: &mut ContainerRuntime, name: &str) {
        let manifest = AgentManifest {
            name: name.to_string(),
            description: "test agent".to_string(),
            source: None,
            runtime: "container".to_string(),
            executable: format!("localhost/{name}:latest"),
            requirements: RequirementsConfig {
                models: HashMap::new(),
                services: HashMap::new(),
                mount: None,
                mcp: Vec::new(),
            },
            object_storage: None,
            vector_storage: None,
        };
        runtime.registry().register_runtime(RuntimeType::Container);
        runtime
            .registry()
            .register_manifest(manifest)
            .await
            .unwrap();
        // Create pending readiness check (mirrors PersistentRegistry.register_agent)
        let check = ReadinessCheck::pending(AgentName::new(name), RuntimeType::Container.as_str());
        runtime.repo.append_readiness_check(&check).unwrap();
    }

    fn set_agent_status(runtime: &ContainerRuntime, name: &str, status: &AgentStatus) {
        let agent_name = AgentName::new(name);
        let check = match status {
            AgentStatus::Deploying => {
                ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str())
            }
            AgentStatus::Deleting => {
                ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str()).deleting()
            }
            AgentStatus::Failed => {
                ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str())
                    .failed("test".to_string())
            }
            _ => ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str()),
        };
        runtime.repo.append_readiness_check(&check).unwrap();
    }

    fn get_agent_status(runtime: &ContainerRuntime, name: &str) -> AgentStatus {
        runtime.repo.get_derived_status(name).unwrap().unwrap()
    }

    #[tokio::test]
    async fn deploy_transitions_to_live() {
        let mut runtime = test_runtime();
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);
    }

    #[tokio::test]
    async fn redeploy_transitions_existing_agent_to_live() {
        let mut runtime = test_runtime();
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);

        // Re-deploy: set back to Deploying
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);
    }

    #[tokio::test]
    async fn delete_transitions_to_deleted() {
        let mut runtime = test_runtime();
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        // Deploy first
        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);

        // Delete — mark as deleting via readiness check
        let check =
            ReadinessCheck::pending(AgentName::new("my-agent"), RuntimeType::Container.as_str())
                .deleting();
        runtime.repo.append_readiness_check(&check).unwrap();
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deleting);
        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Deleted);
    }

    #[tokio::test]
    async fn failed_start_transitions_to_failed() {
        use crate::podman_client::PodmanError;

        struct FailingPodmanClient;

        #[async_trait]
        impl PodmanClient for FailingPodmanClient {
            async fn engine_version(&self) -> Option<semver::Version> {
                Some(semver::Version::new(5, 0, 0))
            }
            async fn image_digest(&self, _: &ImageRef) -> Option<ImageDigest> {
                None
            }
            async fn pod_create(
                &self,
                _: &str,
                _: &[String],
                _: &[crate::podman_client::PortMapping],
            ) -> Result<PodId, PodmanError> {
                Err(PodmanError::Run("simulated failure".into()))
            }
            async fn container_in_pod(
                &self,
                _: RunTarget<'_>,
                _: &PodId,
                _: &[(&str, &str)],
                _: &[(&str, &str)],
            ) -> Result<ContainerId, PodmanError> {
                Ok(ContainerId::new("x"))
            }
            async fn volume_create(
                &self,
                _: &str,
                _: &str,
                _: &[(&str, &str)],
            ) -> Result<(), PodmanError> {
                Ok(())
            }
            async fn volume_rm(&self, _: &str) {}
            async fn is_pod_live(&self, _: &PodId) -> bool {
                true
            }
            async fn pod_start(&self, _: &PodId) -> Result<(), PodmanError> {
                Ok(())
            }
            async fn pod_stop_and_remove(&self, _: &PodId, _: u32) {}
        }

        let mut runtime = ContainerRuntime::new(
            &test_config(),
            test_registry(),
            test_repo(),
            Arc::new(FailingPodmanClient),
            test_queue(),
            None,
            Duration::from_secs(0),
        );
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Failed);
    }

    #[tokio::test]
    async fn orphan_pod_is_removed() {
        let mut runtime = test_runtime();
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        // Deploy
        runtime.tick().await;
        assert!(runtime.pods.contains_key("my-agent"));

        // Remove from registry (simulate external deletion)
        runtime.registry().delete_agent("my-agent").await.unwrap();

        // Tick should clean up the orphaned pod
        runtime.tick().await;
        assert!(!runtime.pods.contains_key("my-agent"));
    }

    #[tokio::test]
    async fn crashed_pod_is_recreated() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct CrashablePodmanClient {
            pod_alive: Arc<AtomicBool>,
        }

        #[async_trait]
        impl PodmanClient for CrashablePodmanClient {
            async fn engine_version(&self) -> Option<semver::Version> {
                Some(semver::Version::new(5, 0, 0))
            }
            async fn image_digest(&self, _: &ImageRef) -> Option<ImageDigest> {
                None
            }
            async fn pod_create(
                &self,
                _: &str,
                _: &[String],
                _: &[crate::podman_client::PortMapping],
            ) -> Result<PodId, PodmanError> {
                Ok(PodId::new("mock-pod"))
            }
            async fn container_in_pod(
                &self,
                _: RunTarget<'_>,
                _: &PodId,
                _: &[(&str, &str)],
                _: &[(&str, &str)],
            ) -> Result<ContainerId, PodmanError> {
                Ok(ContainerId::new("mock-container"))
            }
            async fn volume_create(
                &self,
                _: &str,
                _: &str,
                _: &[(&str, &str)],
            ) -> Result<(), PodmanError> {
                Ok(())
            }
            async fn volume_rm(&self, _: &str) {}
            async fn is_pod_live(&self, _: &PodId) -> bool {
                self.pod_alive.load(Ordering::Relaxed)
            }
            async fn pod_start(&self, _: &PodId) -> Result<(), PodmanError> {
                Ok(())
            }
            async fn pod_stop_and_remove(&self, _: &PodId, _: u32) {}
        }

        let pod_alive = Arc::new(AtomicBool::new(true));
        let pod_alive_handle = Arc::clone(&pod_alive);
        let mut runtime = ContainerRuntime::new(
            &test_config(),
            test_registry(),
            test_repo(),
            Arc::new(CrashablePodmanClient { pod_alive }),
            test_queue(),
            None,
            Duration::from_secs(0),
        );
        register_test_agent(&mut runtime, "my-agent").await;
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);

        // Deploy — pod is alive
        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);
        assert!(runtime.pods.contains_key("my-agent"));

        // Simulate crash: pod is no longer running
        pod_alive_handle.store(false, Ordering::Relaxed);

        // Re-deploy — should detect the dead pod and recreate
        set_agent_status(&runtime, "my-agent", &AgentStatus::Deploying);
        runtime.tick().await;
        assert_eq!(get_agent_status(&runtime, "my-agent"), AgentStatus::Live);
        assert!(runtime.pods.contains_key("my-agent"));
    }
}
