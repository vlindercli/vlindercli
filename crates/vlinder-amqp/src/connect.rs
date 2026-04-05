//! AMQP connection configuration and setup.

/// AMQP connection configuration.
#[derive(Debug, Clone)]
pub struct AmqpConfig {
    /// AMQP connection URL (e.g., `amqp://guest:guest@localhost:5672/%2f`).
    pub url: String,
}

impl Default for AmqpConfig {
    fn default() -> Self {
        Self {
            url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
        }
    }
}

/// Exchange name used for all vlinder messages.
#[allow(dead_code)]
pub(crate) const EXCHANGE_NAME: &str = "vlinder.topic";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_url() {
        let config = AmqpConfig::default();
        assert!(config.url.starts_with("amqp://"));
        assert!(config.url.contains("5672"));
    }
}
