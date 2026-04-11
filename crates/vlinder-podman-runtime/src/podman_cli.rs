//! `PodmanCliClient` — Podman trait implementation via the `podman` CLI.
//!
//! Fallback implementation that shells out to the `podman` binary.
//! Used when no Podman socket is available (ADR 077).

use std::process::Command;

use vlinder_core::domain::{ContainerId, ImageDigest, ImageRef, PodId};

use crate::podman_client::{PodmanClient, PodmanError, RunTarget};

/// Fallback implementation that shells out to the `podman` CLI.
pub struct PodmanCliClient;

impl PodmanClient for PodmanCliClient {
    fn engine_version(&self) -> Option<semver::Version> {
        Command::new("podman")
            .args(["version", "--format", "{{.Client.Version}}"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| {
                let raw = String::from_utf8_lossy(&o.stdout).trim().to_string();
                parse_version(&raw)
            })
    }

    fn image_digest(&self, image_ref: &ImageRef) -> Option<ImageDigest> {
        Command::new("podman")
            .args([
                "image",
                "inspect",
                image_ref.as_str(),
                "--format",
                "{{.Digest}}",
            ])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
            .and_then(|s| ImageDigest::parse(s).ok())
    }

    // ── Pod operations ────────────────────────────────────────────────

    fn pod_create(&self, name: &str, host_aliases: &[String]) -> Result<PodId, PodmanError> {
        let mut args = vec!["pod", "create", "--name", name];
        let add_host_args: Vec<String> = host_aliases
            .iter()
            .flat_map(|alias| vec!["--add-host".to_string(), alias.clone()])
            .collect();
        let add_host_refs: Vec<&str> = add_host_args
            .iter()
            .map(std::string::String::as_str)
            .collect();
        args.extend(add_host_refs);

        let output = Command::new("podman")
            .args(&args)
            .output()
            .map_err(|e| PodmanError::Run(format!("failed to spawn podman: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PodmanError::Run(format!(
                "podman pod create failed: {stderr}"
            )));
        }

        let raw = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(PodId::new(raw))
    }

    fn container_in_pod(
        &self,
        image: RunTarget<'_>,
        pod_id: &PodId,
        env_vars: &[(&str, &str)],
        volumes: &[(&str, &str)],
    ) -> Result<ContainerId, PodmanError> {
        let mut args = vec![
            "create".to_string(),
            "--pod".to_string(),
            pod_id.as_str().to_string(),
        ];

        for (k, v) in env_vars {
            args.push("--env".to_string());
            args.push(format!("{k}={v}"));
        }

        for (vol_name, container_path) in volumes {
            args.push("--volume".to_string());
            args.push(format!("{vol_name}:{container_path}:ro"));
        }

        args.push(image.as_str().to_string());

        let output = Command::new("podman")
            .args(&args)
            .output()
            .map_err(|e| PodmanError::Run(format!("failed to spawn podman: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PodmanError::Run(format!(
                "podman create in pod failed: {stderr}"
            )));
        }

        let raw = String::from_utf8_lossy(&output.stdout).trim().to_string();
        Ok(ContainerId::new(raw))
    }

    fn volume_create(
        &self,
        name: &str,
        driver: &str,
        options: &[(&str, &str)],
    ) -> Result<(), PodmanError> {
        let mut args = vec![
            "volume".to_string(),
            "create".to_string(),
            "--driver".to_string(),
            driver.to_string(),
        ];

        for (k, v) in options {
            args.push("-o".to_string());
            args.push(format!("{k}={v}"));
        }

        args.push(name.to_string());

        let output = Command::new("podman")
            .args(&args)
            .output()
            .map_err(|e| PodmanError::Run(format!("failed to spawn podman: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PodmanError::Run(format!(
                "podman volume create failed: {stderr}"
            )));
        }

        Ok(())
    }

    fn volume_rm(&self, name: &str) {
        let _ = Command::new("podman")
            .args(["volume", "rm", "-f", name])
            .output();
    }

    fn is_pod_live(&self, pod_id: &PodId) -> bool {
        Command::new("podman")
            .args(["pod", "inspect", pod_id.as_str()])
            .output()
            .is_ok_and(|o| o.status.success())
    }

    fn pod_start(&self, pod_id: &PodId) -> Result<(), PodmanError> {
        let output = Command::new("podman")
            .args(["pod", "start", pod_id.as_str()])
            .output()
            .map_err(|e| PodmanError::Run(format!("failed to spawn podman: {e}")))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(PodmanError::Run(format!(
                "podman pod start failed: {stderr}"
            )));
        }

        Ok(())
    }

    fn pod_stop_and_remove(&self, pod_id: &PodId, timeout_secs: u32) {
        let timeout = timeout_secs.to_string();
        let id = pod_id.as_str();
        let _ = Command::new("podman")
            .args(["pod", "stop", "-t", &timeout, id])
            .output();
        let _ = Command::new("podman")
            .args(["pod", "rm", "-f", id])
            .output();
    }
}

// ── Pure parsing helpers (unit-testable without Podman) ──────────────

/// Parse a semver version string. Returns None on invalid input.
fn parse_version(raw: &str) -> Option<semver::Version> {
    semver::Version::parse(raw).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_version ──

    #[test]
    fn parse_version_valid() {
        assert_eq!(parse_version("4.9.3"), Some(semver::Version::new(4, 9, 3)));
    }

    #[test]
    fn parse_version_with_pre() {
        assert!(parse_version("5.0.0-rc1").is_some());
    }

    #[test]
    fn parse_version_garbage() {
        assert_eq!(parse_version("not-a-version"), None);
    }

    #[test]
    fn parse_version_empty() {
        assert_eq!(parse_version(""), None);
    }
}
