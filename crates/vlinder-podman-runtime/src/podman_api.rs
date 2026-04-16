//! `PodmanApiClient` — Podman trait implementation using the libpod REST API.
//!
//! Primary implementation. Talks to Podman's REST API over a Unix socket.
//! Uses hyper 1.x with a Unix socket connector for HTTP-over-socket.

use std::collections::HashMap;
use std::path::Path;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request};
use serde::{Deserialize, Serialize};

use vlinder_core::domain::{ContainerId, ImageDigest, ImageRef, PodId};

use crate::podman_client::{PodmanClient, PodmanError, RunTarget};
use crate::unix_transport::{unix_client, UnixConnector};

const API_BASE: &str = "http://localhost/v5.0.0/libpod";

/// Podman REST API client — primary implementation.
///
/// Talks to the libpod REST API over a Unix socket. The socket path is
/// resolved by `resolve_socket()` in `podman.rs` (ADR 077).
pub struct PodmanApiClient {
    client: hyper_util::client::legacy::Client<UnixConnector, Full<Bytes>>,
}

impl PodmanApiClient {
    pub fn new(socket_path: &Path) -> Self {
        Self {
            client: unix_client(socket_path),
        }
    }
}

// ── Podman trait implementation ──────────────────────────────────────

impl PodmanClient for PodmanApiClient {
    fn engine_version(&self) -> Option<semver::Version> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri("http://localhost/version")
                    .body(Full::new(Bytes::new()))
                    .ok()?;
                let resp = client.request(req).await.ok()?;
                if resp.status().as_u16() != 200 {
                    return None;
                }
                let bytes = resp.into_body().collect().await.ok()?.to_bytes();
                let body: VersionResponse = serde_json::from_slice(&bytes).ok()?;
                semver::Version::parse(&body.version).ok()
            })
        })
    }

    fn image_digest(&self, image_ref: &ImageRef) -> Option<ImageDigest> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let encoded = url_encode(image_ref.as_str());
        let url = format!("{API_BASE}/images/{encoded}/json");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri(url)
                    .body(Full::new(Bytes::new()))
                    .ok()?;
                let resp = client.request(req).await.ok()?;
                if resp.status().as_u16() != 200 {
                    return None;
                }
                let bytes = resp.into_body().collect().await.ok()?.to_bytes();
                let body: ImageInspect = serde_json::from_slice(&bytes).ok()?;
                ImageDigest::parse(body.digest).ok()
            })
        })
    }

    // ── Pod operations ────────────────────────────────────────────────

    fn pod_create(&self, name: &str, host_aliases: &[String]) -> Result<PodId, PodmanError> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let name = name.to_string();
        let host_aliases = host_aliases.to_vec();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let url = format!("{API_BASE}/pods/create");
                let hostadd = if host_aliases.is_empty() {
                    None
                } else {
                    Some(host_aliases)
                };
                let spec = PodCreateSpec { name, hostadd };
                let json_bytes = serde_json::to_vec(&spec)
                    .map_err(|e| PodmanError::Run(format!("pod create failed: {e}")))?;

                let req = Request::builder()
                    .method(Method::POST)
                    .uri(url)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(json_bytes)))
                    .map_err(|e| PodmanError::Run(e.to_string()))?;

                let resp = client
                    .request(req)
                    .await
                    .map_err(|e| PodmanError::Run(format!("pod create failed: {e}")))?;

                let status = resp.status().as_u16();
                if status != 200 && status != 201 {
                    let bytes = resp
                        .into_body()
                        .collect()
                        .await
                        .map(http_body_util::Collected::to_bytes)
                        .unwrap_or_default();
                    let body = String::from_utf8_lossy(&bytes).into_owned();
                    return Err(PodmanError::Run(format!(
                        "pod create HTTP {status}: {body}"
                    )));
                }

                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| PodmanError::Run(e.to_string()))?
                    .to_bytes();
                let created: PodCreateResponse = serde_json::from_slice(&bytes).map_err(|e| {
                    PodmanError::Run(format!("failed to parse pod create response: {e}"))
                })?;
                Ok(PodId::new(created.id))
            })
        })
    }

    fn container_in_pod(
        &self,
        image: RunTarget<'_>,
        pod_id: &PodId,
        env_vars: &[(&str, &str)],
        volumes: &[(&str, &str)],
    ) -> Result<ContainerId, PodmanError> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let env: HashMap<String, String> = env_vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mounts: Vec<MountSpec> = volumes
            .iter()
            .map(|(vol_name, container_path)| MountSpec {
                name: vol_name.to_string(),
                mount_type: "volume".to_string(),
                source: vol_name.to_string(),
                destination: container_path.to_string(),
                options: vec!["ro".to_string()],
            })
            .collect();
        let spec = PodContainerCreateSpec {
            image: image.as_str().to_string(),
            pod: pod_id.as_str().to_string(),
            env: if env.is_empty() { None } else { Some(env) },
            mounts: if mounts.is_empty() {
                None
            } else {
                Some(mounts)
            },
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let url = format!("{API_BASE}/containers/create");
                let json_bytes = serde_json::to_vec(&spec).map_err(|e| {
                    PodmanError::Run(format!("container create in pod failed: {e}"))
                })?;

                let req = Request::builder()
                    .method(Method::POST)
                    .uri(url)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(json_bytes)))
                    .map_err(|e| PodmanError::Run(e.to_string()))?;

                let resp = client.request(req).await.map_err(|e| {
                    PodmanError::Run(format!("container create in pod failed: {e}"))
                })?;

                let status = resp.status().as_u16();
                if status != 201 {
                    let bytes = resp
                        .into_body()
                        .collect()
                        .await
                        .map(http_body_util::Collected::to_bytes)
                        .unwrap_or_default();
                    let body = String::from_utf8_lossy(&bytes).into_owned();
                    return Err(PodmanError::Run(format!(
                        "container create in pod HTTP {status}: {body}"
                    )));
                }

                let bytes = resp
                    .into_body()
                    .collect()
                    .await
                    .map_err(|e| PodmanError::Run(e.to_string()))?
                    .to_bytes();
                let created: ContainerCreateResponse =
                    serde_json::from_slice(&bytes).map_err(|e| {
                        PodmanError::Run(format!("failed to parse container create response: {e}"))
                    })?;
                Ok(ContainerId::new(created.id))
            })
        })
    }

    fn volume_create(
        &self,
        name: &str,
        driver: &str,
        options: &[(&str, &str)],
    ) -> Result<(), PodmanError> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let driver_opts: HashMap<String, String> = options
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let spec = VolumeCreateSpec {
            name: name.to_string(),
            driver: driver.to_string(),
            options: if driver_opts.is_empty() {
                None
            } else {
                Some(driver_opts)
            },
        };

        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let url = format!("{API_BASE}/volumes/create");
                let json_bytes = serde_json::to_vec(&spec)
                    .map_err(|e| PodmanError::Run(format!("volume create failed: {e}")))?;

                let req = Request::builder()
                    .method(Method::POST)
                    .uri(url)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(json_bytes)))
                    .map_err(|e| PodmanError::Run(e.to_string()))?;

                let resp = client
                    .request(req)
                    .await
                    .map_err(|e| PodmanError::Run(format!("volume create failed: {e}")))?;

                let status = resp.status().as_u16();
                if status != 201 {
                    let bytes = resp
                        .into_body()
                        .collect()
                        .await
                        .map(http_body_util::Collected::to_bytes)
                        .unwrap_or_default();
                    let body = String::from_utf8_lossy(&bytes).into_owned();
                    return Err(PodmanError::Run(format!(
                        "volume create HTTP {status}: {body}"
                    )));
                }

                Ok(())
            })
        })
    }

    fn volume_rm(&self, name: &str) {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let url = format!("{API_BASE}/volumes/{name}?force=true");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let req = Request::builder()
                    .method(Method::DELETE)
                    .uri(url)
                    .body(Full::new(Bytes::new()))
                    .expect("valid delete request");
                client.request(req).await.ok();
            });
        });
    }

    fn is_pod_live(&self, pod_id: &PodId) -> bool {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let url = format!("{API_BASE}/pods/{}/json", pod_id.as_str());
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri(url)
                    .body(Full::new(Bytes::new()))
                    .ok()?;
                let resp = client.request(req).await.ok()?;
                let bytes = resp.into_body().collect().await.ok()?.to_bytes();
                let v: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
                v.get("State")?.as_str().map(|s| s == "Running")
            })
        })
        .unwrap_or(false)
    }

    fn pod_start(&self, pod_id: &PodId) -> Result<(), PodmanError> {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let url = format!("{API_BASE}/pods/{}/start", pod_id.as_str());
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                let req = Request::builder()
                    .method(Method::POST)
                    .uri(url)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::new()))
                    .map_err(|e| PodmanError::Run(e.to_string()))?;

                let resp = client
                    .request(req)
                    .await
                    .map_err(|e| PodmanError::Run(format!("pod start failed: {e}")))?;

                let status = resp.status().as_u16();
                if status != 200 && status != 204 && status != 304 {
                    return Err(PodmanError::Run(format!("pod start HTTP {status}")));
                }
                Ok(())
            })
        })
    }

    fn pod_stop_and_remove(&self, pod_id: &PodId, timeout_secs: u32) {
        // block_in_place bridge — removed when PodmanClient trait goes async (step 06)
        let client = self.client.clone();
        let id = pod_id.as_str().to_string();
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                // Stop — fire and forget
                let stop_url = format!("{API_BASE}/pods/{id}/stop?t={timeout_secs}");
                let req = Request::builder()
                    .method(Method::POST)
                    .uri(stop_url)
                    .body(Full::new(Bytes::new()))
                    .expect("valid stop request");
                client.request(req).await.ok();

                // Remove with force — fire and forget
                let rm_url = format!("{API_BASE}/pods/{id}?force=true");
                let req = Request::builder()
                    .method(Method::DELETE)
                    .uri(rm_url)
                    .body(Full::new(Bytes::new()))
                    .expect("valid delete request");
                client.request(req).await.ok();
            });
        });
    }
}

// ── Request/response serde types ─────────────────────────────────────

#[derive(Deserialize)]
struct VersionResponse {
    #[serde(alias = "Version")]
    version: String,
}

#[derive(Deserialize)]
struct ImageInspect {
    #[serde(alias = "Digest")]
    digest: String,
}

#[derive(Deserialize)]
struct ContainerCreateResponse {
    #[serde(alias = "Id")]
    id: String,
}

#[derive(Serialize)]
struct PodCreateSpec {
    name: String,
    /// Host aliases injected into `/etc/hosts` (`hostname:ip` pairs).
    #[serde(skip_serializing_if = "Option::is_none")]
    hostadd: Option<Vec<String>>,
}

#[derive(Deserialize)]
struct PodCreateResponse {
    #[serde(alias = "Id")]
    id: String,
}

#[derive(Serialize)]
struct PodContainerCreateSpec {
    image: String,
    pod: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    env: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mounts: Option<Vec<MountSpec>>,
}

/// OCI mount spec for attaching volumes to containers (ADR 107).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct MountSpec {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Type")]
    mount_type: String,
    #[serde(rename = "Source")]
    source: String,
    #[serde(rename = "Destination")]
    destination: String,
    #[serde(rename = "Options")]
    options: Vec<String>,
}

/// Volume creation spec for the Podman REST API (ADR 107).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct VolumeCreateSpec {
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "Driver")]
    driver: String,
    #[serde(rename = "Options", skip_serializing_if = "Option::is_none")]
    options: Option<HashMap<String, String>>,
}

// ── Helpers ──────────────────────────────────────────────────────────

/// URL-encode an image reference (e.g. `docker.io/library/nginx` → `docker.io%2Flibrary%2Fnginx`).
fn url_encode(s: &str) -> String {
    s.replace('/', "%2F").replace(':', "%3A")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── URL encoding ──

    #[test]
    fn url_encode_image_ref() {
        assert_eq!(
            url_encode("docker.io/library/nginx"),
            "docker.io%2Flibrary%2Fnginx"
        );
    }

    #[test]
    fn url_encode_with_tag() {
        assert_eq!(
            url_encode("localhost/myapp:latest"),
            "localhost%2Fmyapp%3Alatest"
        );
    }

    #[test]
    fn url_encode_simple_name() {
        assert_eq!(url_encode("nginx"), "nginx");
    }

    // ── Version response parsing ──

    #[test]
    fn parse_version_response() {
        let json = r#"{"Version":"5.3.1","ApiVersion":"5.3.1"}"#;
        let v: VersionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(v.version, "5.3.1");
    }

    // ── Image inspect response parsing ──

    #[test]
    fn parse_image_inspect_digest() {
        let json = r#"{"Digest":"sha256:abc123def456"}"#;
        let img: ImageInspect = serde_json::from_str(json).unwrap();
        assert_eq!(img.digest, "sha256:abc123def456");
    }

    // ── Container create response parsing ──

    #[test]
    fn parse_container_create_response() {
        let json = r#"{"Id":"abc123","Warnings":[]}"#;
        let resp: ContainerCreateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "abc123");
    }

    // ── Pod serde types ──

    #[test]
    fn pod_create_spec_serialization() {
        let spec = PodCreateSpec {
            name: "vlinder-echo".to_string(),
            hostadd: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("vlinder-echo"));
        assert!(!json.contains("hostadd"));
    }

    #[test]
    fn pod_create_spec_with_host_aliases() {
        let spec = PodCreateSpec {
            name: "vlinder-provider-test".to_string(),
            hostadd: Some(vec!["openrouter.vlinder.local:127.0.0.1".to_string()]),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("openrouter.vlinder.local:127.0.0.1"));
    }

    #[test]
    fn parse_pod_create_response() {
        let json = r#"{"Id":"pod123abc"}"#;
        let resp: PodCreateResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.id, "pod123abc");
    }

    #[test]
    fn pod_container_create_spec_with_env() {
        let mut env = HashMap::new();
        env.insert("KEY".to_string(), "VALUE".to_string());
        let spec = PodContainerCreateSpec {
            image: "localhost/echo:latest".to_string(),
            pod: "pod123".to_string(),
            env: Some(env),
            mounts: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("KEY"));
        assert!(json.contains("VALUE"));
        assert!(json.contains("pod123"));
    }

    #[test]
    fn pod_container_create_spec_without_env() {
        let spec = PodContainerCreateSpec {
            image: "localhost/echo:latest".to_string(),
            pod: "pod123".to_string(),
            env: None,
            mounts: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(!json.contains("env"));
        assert!(!json.contains("mounts"));
    }

    // ── Volume / mount serde types (ADR 107) ──

    #[test]
    fn mount_spec_serialization() {
        let spec = MountSpec {
            name: "vlinder-mount-support-knowledge".to_string(),
            mount_type: "volume".to_string(),
            source: "vlinder-mount-support-knowledge".to_string(),
            destination: "/knowledge".to_string(),
            options: vec!["ro".to_string()],
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains(r#""Type":"volume"#));
        assert!(json.contains(r#""Destination":"/knowledge"#));
        assert!(json.contains(r#""Options":["ro"]"#));
    }

    #[test]
    fn mount_spec_round_trip() {
        let spec = MountSpec {
            name: "vol".to_string(),
            mount_type: "volume".to_string(),
            source: "vol".to_string(),
            destination: "/data".to_string(),
            options: vec!["ro".to_string()],
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: MountSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, parsed);
    }

    #[test]
    fn volume_create_spec_serialization() {
        let mut opts = HashMap::new();
        opts.insert("type".to_string(), "fuse.s3fs".to_string());
        opts.insert("device".to_string(), "vlinder-support:/v0.1.0/".to_string());
        opts.insert("o".to_string(), "ro,url=http://localhost:4566".to_string());

        let spec = VolumeCreateSpec {
            name: "vlinder-mount-support-knowledge".to_string(),
            driver: "local".to_string(),
            options: Some(opts),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains(r#""Name":"vlinder-mount-support-knowledge"#));
        assert!(json.contains(r#""Driver":"local"#));
        assert!(json.contains("fuse.s3fs"));
    }

    #[test]
    fn volume_create_spec_round_trip() {
        let spec = VolumeCreateSpec {
            name: "test-vol".to_string(),
            driver: "local".to_string(),
            options: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        let parsed: VolumeCreateSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(spec, parsed);
    }

    #[test]
    fn pod_container_create_spec_with_mounts() {
        let spec = PodContainerCreateSpec {
            image: "localhost/support:latest".to_string(),
            pod: "pod456".to_string(),
            env: None,
            mounts: Some(vec![MountSpec {
                name: "vlinder-mount-support-knowledge".to_string(),
                mount_type: "volume".to_string(),
                source: "vlinder-mount-support-knowledge".to_string(),
                destination: "/knowledge".to_string(),
                options: vec!["ro".to_string()],
            }]),
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert!(json.contains("mounts"));
        assert!(json.contains("/knowledge"));
    }
}
