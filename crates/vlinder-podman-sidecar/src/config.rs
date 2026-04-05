//! Sidecar configuration — env-var driven.
//!
//! The sidecar runs inside a Podman pod. All configuration comes from
//! environment variables set by the pool when creating the container.
//! No config file, no filesystem probing — pure 12-factor.

/// Queue backend for the sidecar.
pub enum QueueBackendConfig {
    Nats { url: String },
    Amqp { url: String },
}

/// Sidecar configuration parsed from environment variables.
pub struct SidecarConfig {
    /// Agent name — used as the queue subscription key.
    pub agent: String,
    /// Queue backend configuration.
    pub queue: QueueBackendConfig,
    /// Registry gRPC address (e.g. `http://host.containers.internal:9090`).
    pub registry_url: String,
    /// State service gRPC address (e.g. `http://host.containers.internal:9092`).
    pub state_url: String,
    /// Secret store gRPC address (optional).
    pub secret_url: Option<String>,
    /// Agent container port (default 8080, localhost inside the pod).
    pub container_port: u16,
    /// OCI image reference (diagnostics only).
    pub image_ref: Option<String>,
    /// Content-addressed image digest (diagnostics only).
    pub image_digest: Option<String>,
    /// Container ID (diagnostics only).
    pub container_id: Option<String>,
}

impl SidecarConfig {
    /// Parse configuration from environment variables.
    ///
    /// Required: `VLINDER_AGENT`, `VLINDER_NATS_URL`, `VLINDER_REGISTRY_URL`, `VLINDER_STATE_URL`.
    /// Optional: `VLINDER_CONTAINER_PORT` (default 8080), `VLINDER_IMAGE_REF`,
    /// `VLINDER_IMAGE_DIGEST`, `VLINDER_CONTAINER_ID`.
    pub fn from_env() -> Result<Self, String> {
        let agent = required_env("VLINDER_AGENT")?;
        let registry_url = required_env("VLINDER_REGISTRY_URL")?;
        let state_url = required_env("VLINDER_STATE_URL")?;
        let secret_url = std::env::var("VLINDER_SECRET_URL").ok();

        let backend = std::env::var("VLINDER_QUEUE_BACKEND").unwrap_or_else(|_| "nats".to_string());
        let queue = match backend.as_str() {
            "amqp" => QueueBackendConfig::Amqp {
                url: required_env("VLINDER_AMQP_URL")?,
            },
            _ => QueueBackendConfig::Nats {
                url: required_env("VLINDER_NATS_URL")?,
            },
        };

        let container_port = std::env::var("VLINDER_CONTAINER_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8080);

        let image_ref = std::env::var("VLINDER_IMAGE_REF").ok();
        let image_digest = std::env::var("VLINDER_IMAGE_DIGEST").ok();
        let container_id = std::env::var("VLINDER_CONTAINER_ID").ok();

        Ok(Self {
            agent,
            queue,
            registry_url,
            state_url,
            secret_url,
            container_port,
            image_ref,
            image_digest,
            container_id,
        })
    }
}

fn required_env(key: &str) -> Result<String, String> {
    std::env::var(key).map_err(|_| format!("required env var {key} not set"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn required_env_missing() {
        let result = required_env("VLINDER_NONEXISTENT_TEST_VAR_12345");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("VLINDER_NONEXISTENT_TEST_VAR_12345"));
    }
}
