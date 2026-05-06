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
    Agent, AgentName, AgentStatus, ImageRef, PodId, ReadinessCheck, Registry, RegistryRepository,
    ResourceId, Runtime, RuntimeType,
};

use async_trait::async_trait;

use crate::config::PodmanRuntimeConfig;
use crate::podman_client::{remove_s3_credentials, write_s3_credentials, PodmanClient, RunTarget};

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
    pub fn new(
        config: &PodmanRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        podman: Box<dyn PodmanClient>,
    ) -> Self {
        let registry_id = ResourceId::new(&config.registry_addr);
        let id = ResourceId::new(format!(
            "{}/runtimes/{}",
            registry_id.as_str(),
            RuntimeType::Container.as_str()
        ));
        let image_policy = ImagePolicy::from_config(&config.image_policy);
        tracing::info!(event = "runtime.image_policy", policy = ?image_policy, "Container image policy");
        Self {
            id,
            registry,
            repo,
            pods: HashMap::new(),
            config: config.clone(),
            image_policy,
            podman,
        }
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
    async fn start(&mut self, name: &str, agent: &Agent) -> Result<(), String> {
        if let Some(pod) = self.pods.get(name) {
            // Clone pod_id to release the borrow on self.pods before awaiting.
            let pod_id = pod.pod_id.clone();
            if self.podman.is_pod_live(&pod_id).await {
                return Ok(());
            }
            // Pod in hashmap but not running — remove stale entry
            tracing::warn!(agent = name, "Pod not running, recreating");
            let pod = self.pods.remove(name).unwrap();
            self.cleanup_mount_volumes(&pod.mount_volumes).await;
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
        let mount_volumes = self.provision_mount_volumes(name, agent).await?;
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
            self.cleanup_mount_volumes(&mount_volumes).await;
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
    async fn provision_mount_volumes(
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
                let passwd_path = write_s3_credentials(&vol_name, credentials).await?;
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
                .await
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
    async fn cleanup_mount_volumes(&self, volumes: &[String]) {
        for vol_name in volumes {
            tracing::info!(event = "volume.removed", volume = %vol_name, "Removing mount volume");
            self.podman.volume_rm(vol_name).await;
            remove_s3_credentials(vol_name).await;
        }
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
                self.cleanup_mount_volumes(&pod.mount_volumes).await;
            }
        }

        for agent in &agents {
            let status = self.repo.get_derived_status(&agent.name).ok().flatten();

            match status.as_ref() {
                // Deploying: start pod, transition to Live or Failed
                Some(AgentStatus::Deploying) => match self.start(&agent.name, agent).await {
                    Ok(()) => {
                        let agent_name = AgentName::new(&agent.name);
                        let check =
                            ReadinessCheck::pending(agent_name, RuntimeType::Container.as_str())
                                .ready();
                        let _ = self.repo.append_readiness_check(&check);
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

                // Deleting: tear down pod
                Some(AgentStatus::Deleting) => {
                    let agent_name = AgentName::new(&agent.name);
                    if let Some(pod) = self.pods.remove(&agent.name) {
                        tracing::info!(event = "pod.teardown", agent = %agent.name, "Tearing down pod");
                        self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
                        self.cleanup_mount_volumes(&pod.mount_volumes).await;
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
        // Collect owned pods first so we don't hold a &mut self.pods borrow
        // across .await points while also calling self.podman.*.
        let pods: Vec<(String, Pod)> = self.pods.drain().collect();
        for (name, pod) in pods {
            tracing::info!(event = "pod.stopped", agent = %name, pod = %pod.pod_id, "Stopping pod");
            self.podman.pod_stop_and_remove(&pod.pod_id, 5).await;
            for vol_name in &pod.mount_volumes {
                tracing::info!(event = "volume.removed", volume = %vol_name, "Removing mount volume");
                self.podman.volume_rm(vol_name).await;
                remove_s3_credentials(vol_name).await;
            }
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

    struct MockPodmanClient;

    #[async_trait]
    impl PodmanClient for MockPodmanClient {
        async fn engine_version(&self) -> Option<semver::Version> {
            Some(semver::Version::new(5, 0, 0))
        }
        async fn image_digest(&self, _: &ImageRef) -> Option<ImageDigest> {
            None
        }
        async fn pod_create(&self, _: &str, _: &[String]) -> Result<PodId, PodmanError> {
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
            Box::new(MockPodmanClient),
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
                mounts: HashMap::new(),
                mcp: Vec::new(),
            },
            prompts: None,
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
            async fn pod_create(&self, _: &str, _: &[String]) -> Result<PodId, PodmanError> {
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
            Box::new(FailingPodmanClient),
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
            async fn pod_create(&self, _: &str, _: &[String]) -> Result<PodId, PodmanError> {
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
            Box::new(CrashablePodmanClient { pod_alive }),
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
