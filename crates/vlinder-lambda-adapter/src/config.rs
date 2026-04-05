//! Lambda adapter configuration — env-var driven.
//!
//! The adapter runs inside a Lambda container as an extension in
//! `/opt/extensions/`. All configuration comes from environment variables
//! set by the daemon when creating the Lambda function.

/// Queue backend for the adapter.
pub enum QueueBackendConfig {
    Nats { url: String },
    Amqp { url: String },
}

/// Adapter configuration parsed from environment variables.
pub struct AdapterConfig {
    /// Lambda Runtime API endpoint (set by Lambda service, e.g. `127.0.0.1:9001`).
    pub runtime_api: String,
    /// Agent name — used for queue subscription and registry lookup.
    pub agent: String,
    /// Queue backend configuration.
    pub queue: QueueBackendConfig,
    /// Registry gRPC address.
    pub registry_url: String,
    /// State service gRPC address.
    pub state_url: String,
    /// Secret store gRPC address (optional).
    pub secret_url: Option<String>,
    /// Agent HTTP port on localhost (default 8080).
    pub agent_port: u16,
}

impl AdapterConfig {
    /// Parse configuration from environment variables.
    ///
    /// `AWS_LAMBDA_RUNTIME_API` is set by the Lambda service itself.
    /// `VLINDER_*` vars are set by the daemon during function creation.
    pub fn from_env() -> Result<Self, String> {
        let runtime_api = required_env("AWS_LAMBDA_RUNTIME_API")?;
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

        let agent_port = std::env::var("VLINDER_AGENT_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(8080);

        Ok(Self {
            runtime_api,
            agent,
            queue,
            registry_url,
            state_url,
            secret_url,
            agent_port,
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
        let result = required_env("VLINDER_NONEXISTENT_TEST_VAR_99999");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("VLINDER_NONEXISTENT_TEST_VAR_99999"));
    }
}
