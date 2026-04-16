//! Podman client abstraction — trait + shared utilities.
//!
//! The `PodmanClient` trait is the contract for all Podman interactions.
//! Two implementations exist:
//! - `PodmanApiClient` (primary) — REST API over Unix socket
//! - `PodmanCliClient` (fallback) — shells out to the `podman` binary

use std::fmt;

use async_trait::async_trait;
use vlinder_core::domain::{ContainerId, ImageDigest, ImageRef, PodId};

// ── Error type ──────────────────────────────────────────────────────

/// Podman operation failure.
#[derive(Debug)]
pub enum PodmanError {
    /// Container or pod create/start failed.
    Run(String),
}

impl fmt::Display for PodmanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PodmanError::Run(msg) => write!(f, "podman operation failed: {msg}"),
        }
    }
}

// ── Run target ──────────────────────────────────────────────────────

/// What to pass to `podman create` as the image argument.
///
/// Mutable policy uses the image ref (tag-based, picks up rebuilds).
/// Pinned policy uses the content-addressed digest (deterministic bytes).
pub enum RunTarget<'a> {
    /// Tag-based image reference (e.g., `localhost/echo:latest`).
    Ref(&'a ImageRef),
    /// Content-addressed digest (e.g., `sha256:abc123...`).
    Digest(&'a ImageDigest),
}

impl RunTarget<'_> {
    /// The string to pass to Podman (CLI flag or API field).
    pub(crate) fn as_str(&self) -> &str {
        match self {
            RunTarget::Ref(r) => r.as_str(),
            RunTarget::Digest(d) => d.as_str(),
        }
    }
}

// ── Trait ────────────────────────────────────────────────────────────

/// Client abstraction over the Podman container engine.
///
/// Pod-oriented: create pods, add containers to them, start/stop pods.
/// The trait is object-safe so `ContainerRuntime` can hold a `Box<dyn PodmanClient>`.
#[async_trait]
pub trait PodmanClient: Send + Sync {
    /// Engine version (e.g. 4.9.3).  None if Podman is unavailable.
    async fn engine_version(&self) -> Option<semver::Version>;

    /// Return the content-addressed digest for an image.
    async fn image_digest(&self, image_ref: &ImageRef) -> Option<ImageDigest>;

    /// Create a pod with the given name. Returns the pod ID.
    ///
    /// `host_aliases` are `hostname:ip` pairs injected into `/etc/hosts`
    /// (e.g., `["openrouter.vlinder.local:127.0.0.1"]`).
    async fn pod_create(&self, name: &str, host_aliases: &[String]) -> Result<PodId, PodmanError>;

    /// Create a container inside a pod. No port mapping — containers in
    /// a pod share a network namespace (like k8s).
    ///
    /// `volumes` are `(volume_name, container_path)` pairs for read-only
    /// volume mounts (ADR 107).
    async fn container_in_pod(
        &self,
        image: RunTarget<'_>,
        pod_id: &PodId,
        env_vars: &[(&str, &str)],
        volumes: &[(&str, &str)],
    ) -> Result<ContainerId, PodmanError>;

    /// Create a named Podman volume with the given driver and options.
    ///
    /// Used for S3-backed FUSE mounts (ADR 107). Options are key-value
    /// pairs passed to the volume driver (e.g., `type=fuse.s3fs`).
    async fn volume_create(
        &self,
        name: &str,
        driver: &str,
        options: &[(&str, &str)],
    ) -> Result<(), PodmanError>;

    /// Remove a named volume (fire-and-forget, like pod cleanup).
    async fn volume_rm(&self, name: &str);

    /// Check if a pod is actually running.
    async fn is_pod_live(&self, pod_id: &PodId) -> bool;

    /// Start all containers in a pod.
    async fn pod_start(&self, pod_id: &PodId) -> Result<(), PodmanError>;

    /// Stop and remove a pod (all containers within it).
    async fn pod_stop_and_remove(&self, pod_id: &PodId, timeout_secs: u32);
}

// ── S3 credential helpers (ADR 107) ─────────────────────────────────
//
// s3fs reads credentials from a "passwd" file containing `ACCESS_KEY:SECRET_KEY`.
// The file must exist where s3fs runs — which is NOT the Mac host, but the
// Podman Machine VM (CoreOS on Apple HV). On Linux without a VM, the file
// lives on the host filesystem.
//
// The credential pipeline has three parts:
// 1. Secret resolution: pool.rs resolves the secret name to credentials
//    (currently hardcoded "test:test", will use SecretStore per ADR 083)
// 2. Credential delivery: these helpers write the passwd file to the right
//    place (VM via `podman machine ssh`, or local filesystem on Linux)
// 3. Mount option: pool.rs passes `passwd_file=<path>` to s3fs
//
// Each agent mount gets its own passwd file (keyed by volume name) so
// multiple agents with different S3 credentials don't collide.
// Files are cleaned up when the agent's pod is torn down.

/// Write an s3fs credentials file to the Podman execution environment.
///
/// On macOS: writes via `podman machine ssh` into the `CoreOS` VM's `/tmp`.
/// On Linux: writes directly to the host's `/tmp`.
///
/// Returns the path where the file was written (in the execution
/// environment, not the Mac host). The caller passes this as
/// `passwd_file=<path>` in the s3fs mount options.
///
/// The file is chmod 600 — s3fs refuses to read passwd files with
/// group/other permissions.
pub(crate) async fn write_s3_credentials(name: &str, credentials: &str) -> Result<String, String> {
    let path = format!("/tmp/vlinder-s3-{name}.passwd");

    // Try podman machine ssh (macOS with Podman Machine VM)
    let write_cmd = format!("cat > {path} && chmod 600 {path}");
    let ssh_ok = if let Ok(mut child) = tokio::process::Command::new("podman")
        .args(["machine", "ssh", "--", "sh", "-c", &write_cmd])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            let _ = stdin.write_all(credentials.as_bytes()).await;
        }
        child.wait().await.map(|s| s.success()).unwrap_or(false)
    } else {
        false
    };

    if ssh_ok {
        tracing::debug!(path = %path, "Wrote s3fs credentials via podman machine ssh");
        return Ok(path);
    }

    // Fallback: direct write (Linux, no VM)
    std::fs::write(&path, credentials)
        .map_err(|e| format!("failed to write s3 credentials to {path}: {e}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .map_err(|e| format!("failed to chmod {path}: {e}"))?;
    }
    tracing::debug!(path = %path, "Wrote s3fs credentials to local filesystem");
    Ok(path)
}

/// Remove an s3fs credentials file (fire-and-forget).
pub(crate) async fn remove_s3_credentials(name: &str) {
    let path = format!("/tmp/vlinder-s3-{name}.passwd");

    // Try podman machine ssh (macOS)
    let _ = tokio::process::Command::new("podman")
        .args(["machine", "ssh", "--", "rm", "-f", &path])
        .output()
        .await;

    // Also try local (Linux) — no-op if file doesn't exist
    let _ = std::fs::remove_file(&path);
}

// ── Shared utilities ────────────────────────────────────────────────

/// Resolve a Podman socket path from the config value (ADR 077).
///
/// - `"disabled"` → None (force CLI mode)
/// - `"auto"` → probe standard paths in order, return first that exists
/// - anything else → treat as explicit path
pub fn resolve_socket(configured: &str) -> Option<std::path::PathBuf> {
    match configured {
        "disabled" => None,
        "auto" => probe_socket_paths(),
        path => {
            let p = std::path::PathBuf::from(path);
            if p.exists() {
                Some(p)
            } else {
                None
            }
        }
    }
}

/// Probe standard Podman socket locations.
fn probe_socket_paths() -> Option<std::path::PathBuf> {
    // 1. $XDG_RUNTIME_DIR/podman/podman.sock (standard rootless)
    if let Ok(xdg) = std::env::var("XDG_RUNTIME_DIR") {
        let p = std::path::PathBuf::from(xdg).join("podman/podman.sock");
        if p.exists() {
            return Some(p);
        }
    }

    // 2. macOS Podman Machine socket
    if let Some(home) = dirs::home_dir() {
        let p = home.join(".local/share/containers/podman/machine/podman.sock");
        if p.exists() {
            return Some(p);
        }
    }

    // 3. /run/podman/podman.sock (rootful)
    let p = std::path::PathBuf::from("/run/podman/podman.sock");
    if p.exists() {
        return Some(p);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_socket_disabled() {
        assert_eq!(resolve_socket("disabled"), None);
    }

    #[test]
    fn resolve_socket_explicit_path_exists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let sock = tmp.path().join("podman.sock");
        std::fs::write(&sock, "").unwrap();
        assert_eq!(resolve_socket(sock.to_str().unwrap()), Some(sock));
    }

    #[test]
    fn resolve_socket_explicit_path_missing() {
        assert_eq!(resolve_socket("/nonexistent/path/podman.sock"), None);
    }

    #[test]
    fn resolve_socket_auto_no_sockets() {
        let result = resolve_socket("auto");
        let _ = result;
    }

    #[test]
    fn run_target_ref() {
        let r = ImageRef::parse("localhost/echo:latest").unwrap();
        let target = RunTarget::Ref(&r);
        assert_eq!(target.as_str(), "localhost/echo:latest");
    }

    #[test]
    fn run_target_digest() {
        let d = ImageDigest::parse("sha256:abc123").unwrap();
        let target = RunTarget::Digest(&d);
        assert_eq!(target.as_str(), "sha256:abc123");
    }

    #[test]
    fn podman_error_display() {
        assert_eq!(
            PodmanError::Run("boom".to_string()).to_string(),
            "podman operation failed: boom"
        );
    }
}
