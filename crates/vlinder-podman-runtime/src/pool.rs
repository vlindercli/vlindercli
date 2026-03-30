//! `ContainerRuntime` — manages the lifecycle of Podman pods for container agents.
//!
//! Each agent runs as a pod containing two containers:
//! 1. The agent container (user-provided OCI image)
//! 2. The sidecar container (vlinder-podman-sidecar, mediates queue ↔ agent)
//!
//! The runtime creates pods, starts them, and tears them down on shutdown.
//! Dead pod detection is deferred — `ensure_containers` restarts missing pods
//! on the next tick.

use std::collections::HashMap;
use std::sync::Arc;

use vlinder_core::domain::{
    Agent, AgentName, AgentState, AgentStatus, ImageRef, PodId, Registry, RegistryRepository,
    ResourceId, Runtime, RuntimeType,
};

use crate::config::PodmanRuntimeConfig;
use crate::podman_api::PodmanApiClient;
use crate::podman_cli::PodmanCliClient;
use crate::podman_client::{
    remove_s3_credentials, resolve_socket, write_s3_credentials, PodmanClient, RunTarget,
};

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
    /// Volume names created for S3 mounts (ADR 107). Cleaned up on shutdown.
    mount_volumes: Vec<String>,
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
    podman: Box<dyn PodmanClient>,
}

impl ContainerRuntime {
    /// Create a new runtime, connecting to the registry and detecting Podman.
    ///
    /// Selects socket API or CLI based on the `podman_socket` config value (ADR 077).
    /// The registry is passed in — the caller is responsible for creating it.
    pub fn new(
        config: &PodmanRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let registry_id = ResourceId::new(&config.registry_addr);
        let id = ResourceId::new(format!(
            "{}/runtimes/{}",
            registry_id.as_str(),
            RuntimeType::Container.as_str()
        ));

        let image_policy = ImagePolicy::from_config(&config.image_policy);
        let podman: Box<dyn PodmanClient> = if let Some(path) =
            resolve_socket(&config.podman_socket)
        {
            tracing::info!(event = "podman.socket", path = %path.display(), "Using Podman socket API");
            Box::new(PodmanApiClient::new(&path))
        } else {
            tracing::info!(event = "podman.cli", "Using Podman CLI");
            Box::new(PodmanCliClient)
        };

        let engine_version = podman.engine_version();
        if let Some(ref v) = engine_version {
            tracing::info!(event = "podman.detected", version = %v, "Podman engine detected");
        } else {
            tracing::warn!(
                event = "podman.not_found",
                "Podman not detected — container runtime degraded"
            );
        }
        tracing::info!(event = "runtime.image_policy", policy = ?image_policy, "Container image policy");
        Ok(Self {
            id,
            registry,
            repo,
            pods: HashMap::new(),
            config: config.clone(),
            image_policy,
            podman,
        })
    }

    /// Access the registry (test-only, for integration test setup).
    #[cfg(test)]
    pub fn registry(&self) -> &Arc<dyn Registry> {
        &self.registry
    }

    /// Start a pod with agent + sidecar containers.
    ///
    /// 1. Provision S3 mount volumes (ADR 107)
    /// 2. Create a Podman pod named `vlinder-{name}`
    /// 3. Add the agent container (user image, with mount volumes)
    /// 4. Add the sidecar container (vlinder-podman-sidecar image, env vars for config)
    /// 5. Start the pod (all containers start together)
    fn start(&mut self, name: &str, agent: &Agent) -> Result<(), String> {
        if self.pods.contains_key(name) {
            return Ok(());
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

        // 1. Provision S3 mount volumes (ADR 107)
        let mount_volumes = self.provision_mount_volumes(name, agent)?;
        let volume_pairs: Vec<(String, String)> = mount_volumes
            .iter()
            .zip(agent.requirements.mounts.values())
            .map(|(vol_name, mount)| (vol_name.clone(), mount.path.clone()))
            .collect();

        // 2. Create pod (with host aliases for provider hostnames)
        // All *.vlinder.local hostnames are added unconditionally — the sidecar
        // only binds the ones the agent needs. Extra entries are harmless.
        // See #34 for replacing this with a sidecar DNS resolver.
        let host_aliases = vec![
            "vlinder.local:127.0.0.1".to_string(),
            "runtime.vlinder.local:127.0.0.1".to_string(),
            "ollama.vlinder.local:127.0.0.1".to_string(),
            "openrouter.vlinder.local:127.0.0.1".to_string(),
            "sqlite-vec.vlinder.local:127.0.0.1".to_string(),
            "sqlite-kv.vlinder.local:127.0.0.1".to_string(),
        ];

        let pod_name = format!("vlinder-{name}");
        let pod_id = self
            .podman
            .pod_create(&pod_name, &host_aliases)
            .map_err(|e| e.to_string())?;

        // From here on, if anything fails we must remove the orphaned pod
        // and clean up any volumes we created.
        // Otherwise the next tick will try pod_create again and get "already exists".
        if let Err(e) = self.start_in_pod(name, &pod_id, run_target, &image_ref, &volume_pairs) {
            tracing::warn!(
                event = "pod.cleanup",
                agent = %name,
                pod = %pod_id,
                "Removing orphaned pod after start failure"
            );
            self.podman.pod_stop_and_remove(&pod_id, 0);
            self.cleanup_mount_volumes(&mount_volumes);
            return Err(e);
        }

        tracing::info!(
            event = "pod.started",
            agent = %name,
            pod = %pod_id,
            image_ref = %image_ref,
            "Pod started (agent + sidecar)"
        );

        self.pods.insert(
            name.to_string(),
            Pod {
                pod_id,
                mount_volumes,
            },
        );
        Ok(())
    }

    /// Populate and start a pod that has already been created.
    ///
    /// Adds the agent container (with mount volumes), the sidecar container,
    /// and starts the pod.
    /// Called by `start()` — if this fails, `start()` cleans up the orphaned pod.
    fn start_in_pod(
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
            .map_err(|e| e.to_string())?;

        // 4. Build sidecar env vars
        let sidecar_image_ref = ImageRef::parse(&self.config.sidecar_image).unwrap_or_else(|_| {
            ImageRef::parse("localhost/vlinder-podman-sidecar:latest").unwrap()
        });
        let sidecar_target = RunTarget::Ref(&sidecar_image_ref);

        let nats_url = format!(
            "nats://host.containers.internal:{}",
            extract_port(&self.config.nats_url, 4222)
        );
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
            .map(String::from)
            .unwrap_or_default();

        let env_vars: Vec<(&str, String)> = vec![
            ("VLINDER_AGENT", name.to_string()),
            ("VLINDER_NATS_URL", nats_url),
            ("VLINDER_REGISTRY_URL", registry_url),
            ("VLINDER_STATE_URL", state_url),
            ("VLINDER_SECRET_URL", secret_url),
            ("VLINDER_CONTAINER_PORT", "8080".to_string()),
            ("VLINDER_IMAGE_REF", image_ref.as_str().to_string()),
            ("VLINDER_IMAGE_DIGEST", image_digest_str),
        ];
        let env_refs: Vec<(&str, &str)> = env_vars.iter().map(|(k, v)| (*k, v.as_str())).collect();

        // 5. Add sidecar container (no volumes — sidecar doesn't need file mounts)
        self.podman
            .container_in_pod(sidecar_target, pod_id, &env_refs, &[])
            .map_err(|e| e.to_string())?;

        // 6. Start the pod (all containers start together)
        self.podman.pod_start(pod_id).map_err(|e| e.to_string())?;

        Ok(())
    }

    /// Provision S3-backed Podman volumes for an agent's declared mounts (ADR 107).
    ///
    /// Creates one Podman volume per mount using s3fs-fuse as the FUSE driver.
    /// Returns the list of volume names so the caller can attach them to the
    /// agent container and track them for cleanup.
    ///
    /// # How it works
    ///
    /// Podman volumes with `type=fuse.s3fs` are lazily mounted: `volume create`
    /// only stores metadata; the actual s3fs FUSE mount happens when a container
    /// starts and references the volume. At that point Podman invokes the
    /// `mount.fuse.s3fs` helper (a symlink to `/usr/bin/s3fs` installed by
    /// `just s3-setup`), which launches the s3fs daemon. The daemon connects
    /// to the S3 endpoint, registers a FUSE mount point, and daemonizes.
    /// Podman then bind-mounts that FUSE mount into the container.
    ///
    /// # Architecture (macOS with Podman Machine)
    ///
    /// Three network contexts matter:
    ///
    /// 1. **Mac host**: where the daemon (vlinderd) and `podman` CLI run.
    ///    `LocalStack` binds to `localhost:4566` here.
    /// 2. **Podman VM** (`CoreOS` on Apple HV): where s3fs and the container
    ///    engine actually run. `localhost:4566` reaches `LocalStack` via
    ///    Podman's port forwarding. `host.containers.internal` resolves to
    ///    `192.168.127.254` but port forwarding only binds to the Mac side,
    ///    so `host.containers.internal:4566` does NOT work from the VM.
    /// 3. **Container namespace**: shares the pod's network namespace.
    ///    The agent process runs here, but s3fs does NOT — it runs at the
    ///    VM level as a mount helper.
    ///
    /// Because s3fs runs at the VM level, we rewrite `host.containers.internal`
    /// to `localhost` in the endpoint URL.
    ///
    /// # s3fs mount options (hard-won lessons)
    ///
    /// Each option exists because of a specific failure mode we hit:
    ///
    /// - `ro`: read-only mount (agents should not write to S3 mounts)
    /// - `connect_timeout=10`: **prevents Podman deadlock**. Without this,
    ///   if the S3 endpoint is unreachable (e.g. `LocalStack` not running),
    ///   s3fs blocks the `mount()` syscall indefinitely. Since Podman holds
    ///   internal locks during container start, this deadlocks the entire
    ///   Podman daemon — `podman ps`, `podman machine ssh`, everything hangs.
    ///   The only recovery is `podman machine stop` (which also often hangs,
    ///   requiring force-killing `vfkit`/`gvproxy` processes).
    /// - `compat_dir`: **required for sub-path mounts**. S3 has no real
    ///   directories — only key prefixes. When mounting `bucket:/v0.1.0/`,
    ///   s3fs's `CheckBucket` does a HEAD on the prefix path. Without
    ///   `compat_dir`, `CheckBucket` enters an infinite retry loop on 404,
    ///   consuming CPU and blocking all FUSE requests (same deadlock as above).
    ///   With `compat_dir`, it uses LIST instead of HEAD to verify the path.
    ///   NOTE: even with `compat_dir`, a zero-byte directory marker object
    ///   must exist at the prefix key (e.g. `v0.1.0/`) or s3fs 1.97 crashes
    ///   with `basic_string::back() Assertion '!empty()' failed` in
    ///   `remote_mountpath_exists`. See `just s3-seed` for marker creation.
    /// - `allow_other`: lets non-root processes read the FUSE mount. Without
    ///   this, only the user who ran s3fs can access the files. Podman's
    ///   volume driver runs as root in the VM, so the mount is owned by root;
    ///   `allow_other` lets the container's processes read it.
    /// - `use_path_request_style`: required for non-AWS S3 backends
    ///   (`LocalStack`, `MinIO`). AWS uses virtual-hosted-style URLs
    ///   (`bucket.s3.amazonaws.com`), but local backends need path-style
    ///   (`localhost:4566/bucket`).
    /// - `passwd_file`: s3fs reads credentials from a colon-separated file
    ///   (`ACCESS_KEY:SECRET_KEY`). The file must exist in the Podman VM
    ///   filesystem (not the Mac), so we write it via `podman machine ssh`.
    ///   See `write_s3_credentials` in `podman.rs`.
    fn provision_mount_volumes(
        &self,
        agent_name: &str,
        agent: &Agent,
    ) -> Result<Vec<String>, String> {
        let mut volume_names = Vec::new();

        for (mount_name, mount) in &agent.requirements.mounts {
            let vol_name = format!("vlinder-mount-{agent_name}-{mount_name}");

            // Parse "bucket/prefix" → ("bucket", "/prefix")
            // s3fs device format: `bucket:/path` mounts only objects under that prefix.
            let (bucket, prefix) = match mount.s3.split_once('/') {
                Some((b, p)) => (b, format!("/{p}")),
                None => (mount.s3.as_str(), "/".to_string()),
            };

            let device = format!("{bucket}:{prefix}");
            let raw_endpoint = mount
                .endpoint
                .as_deref()
                .unwrap_or("https://s3.amazonaws.com");

            // Rewrite host.containers.internal → localhost for the VM context.
            // See architecture comment above for why this is necessary.
            let endpoint = raw_endpoint.replace("host.containers.internal", "localhost");

            // See doc comment above for why each option is here.
            let mut mount_flags = vec![format!(
                "ro,url={endpoint},connect_timeout=10,compat_dir,allow_other"
            )];

            if mount.endpoint.is_some() {
                mount_flags.push("use_path_request_style".to_string());
            }

            // Three independent concerns for credential handling:
            // 1. Secret resolution: currently hardcoded, will read from SecretStore (ADR 083)
            // 2. Credential delivery: write passwd file to the VM filesystem
            // 3. Mount option: tell s3fs where to find the passwd file
            if mount.secret.is_some() {
                // TODO: resolve from SecretStore (ADR 083) — this is the only
                // line that needs to change. The delivery pipeline (write to VM,
                // pass as mount option, clean up on teardown) is fully wired.
                let credentials = "test:test";
                let passwd_path = write_s3_credentials(&vol_name, credentials)?;
                mount_flags.push(format!("passwd_file={passwd_path}"));
            }

            let mount_opts = mount_flags.join(",");

            let options: Vec<(&str, &str)> = vec![
                ("type", "fuse.s3fs"),
                ("device", &device),
                ("o", &mount_opts),
            ];

            self.podman
                .volume_create(&vol_name, "local", &options)
                .map_err(|e| format!("failed to create volume {vol_name}: {e}"))?;

            tracing::info!(
                event = "volume.created",
                agent = %agent_name,
                mount = %mount_name,
                volume = %vol_name,
                s3 = %mount.s3,
                path = %mount.path,
                "S3 mount volume created"
            );

            volume_names.push(vol_name);
        }

        Ok(volume_names)
    }

    /// Remove mount volumes and their credential files (fire-and-forget).
    ///
    /// Must clean up both the Podman volume (which unmounts s3fs) and the
    /// passwd file written to the VM. If volume removal hangs (stale FUSE
    /// mount), Podman will force-remove it — the `connect_timeout` on the
    /// mount options prevents indefinite hangs.
    fn cleanup_mount_volumes(&self, volumes: &[String]) {
        for vol_name in volumes {
            tracing::info!(event = "volume.removed", volume = %vol_name, "Removing mount volume");
            self.podman.volume_rm(vol_name);
            remove_s3_credentials(vol_name);
        }
    }

    /// Reconcile pods with the registry: remove orphans, start missing.
    fn ensure_containers(&mut self) {
        let agents = self.registry.get_agents_by_runtime(RuntimeType::Container);

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
                self.podman.pod_stop_and_remove(&pod.pod_id, 5);
                self.cleanup_mount_volumes(&pod.mount_volumes);
            }
        }

        for agent in &agents {
            let state = self.repo.get_agent_state(&agent.name).ok().flatten();
            let status = state.as_ref().map(|s| &s.status);

            match status {
                // Deploying: start pod, transition to Live or Failed
                Some(AgentStatus::Deploying) if !self.pods.contains_key(&agent.name) => {
                    match self.start(&agent.name, agent) {
                        Ok(()) => {
                            let live = AgentState::registered(AgentName::new(&agent.name))
                                .transition(AgentStatus::Live, None);
                            if let Err(e) = self.repo.append_agent_state(&live) {
                                tracing::warn!(error = %e, "Failed to set Live state");
                            }
                            tracing::info!(
                                event = "pod.deployed",
                                agent = %agent.name,
                                "Agent provisioned: Live"
                            );
                        }
                        Err(e) => {
                            let failed = AgentState::registered(AgentName::new(&agent.name))
                                .transition(AgentStatus::Failed, Some(e.clone()));
                            if let Err(e2) = self.repo.append_agent_state(&failed) {
                                tracing::warn!(error = %e2, "Failed to set Failed state");
                            }
                            tracing::error!(
                                event = "pod.start_failed",
                                agent = %agent.name,
                                error = %e,
                                "Failed to start pod"
                            );
                        }
                    }
                }

                // Deleting: tear down pod, delete from registry, transition to Deleted
                Some(AgentStatus::Deleting) => {
                    if let Some(pod) = self.pods.remove(&agent.name) {
                        tracing::info!(event = "pod.teardown", agent = %agent.name, "Tearing down pod");
                        self.podman.pod_stop_and_remove(&pod.pod_id, 5);
                        self.cleanup_mount_volumes(&pod.mount_volumes);
                    }
                    // Soft delete: agent row stays in registry, state says Deleted
                    let deleted = AgentState::registered(AgentName::new(&agent.name))
                        .transition(AgentStatus::Deleted, None);
                    if let Err(e) = self.repo.append_agent_state(&deleted) {
                        tracing::warn!(error = %e, "Failed to set Deleted state");
                    }
                    tracing::info!(event = "agent.deleted", agent = %agent.name, "Agent torn down: Deleted");
                }

                // Live, Registered, or no state: nothing to do
                _ => {}
            }
        }
    }
}

impl Drop for ContainerRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Runtime for ContainerRuntime {
    fn id(&self) -> &ResourceId {
        &self.id
    }

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Container
    }

    fn tick(&mut self) -> bool {
        let before = self.pods.len();
        self.ensure_containers();
        self.pods.len() != before
    }

    fn shutdown(&mut self) {
        for (name, pod) in self.pods.drain() {
            tracing::info!(event = "pod.stopped", agent = %name, pod = %pod.pod_id, "Stopping pod");
            self.podman.pod_stop_and_remove(&pod.pod_id, 5);
            for vol_name in &pod.mount_volumes {
                tracing::info!(event = "volume.removed", volume = %vol_name, "Removing mount volume");
                self.podman.volume_rm(vol_name);
                remove_s3_credentials(vol_name);
            }
        }
    }
}

/// Extract port number from a URL string, with a default fallback.
fn extract_port(url: &str, default: u16) -> u16 {
    url.rsplit(':')
        .next()
        .and_then(|s| s.trim_end_matches('/').parse().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a `PodmanRuntimeConfig` for tests (matches `vlinderd`'s `Config::for_test` defaults).
    fn test_config() -> PodmanRuntimeConfig {
        PodmanRuntimeConfig {
            image_policy: "mutable".to_string(),
            podman_socket: "disabled".to_string(),
            sidecar_image: "localhost/vlinder-podman-sidecar:latest".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
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
        let config = test_config();
        let registry = test_registry();
        let runtime = ContainerRuntime::new(&config, registry, test_repo()).unwrap();

        assert_eq!(
            runtime.id().as_str(),
            "http://127.0.0.1:9090/runtimes/container"
        );
        assert_eq!(runtime.runtime_type(), RuntimeType::Container);
    }

    #[test]
    fn tick_returns_false_when_no_agents() {
        let config = test_config();
        let registry = test_registry();
        let mut runtime = ContainerRuntime::new(&config, registry, test_repo()).unwrap();

        assert!(!runtime.tick());
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
}
