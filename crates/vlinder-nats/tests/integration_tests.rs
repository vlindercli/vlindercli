//! Integration tests that require a running NATS server.

use vlinder_nats::{NatsConfig, NatsQueue};

#[tokio::test]
#[ignore = "requires a running NATS server — run via: just run-integration-tests"]
async fn connect_to_localhost() {
    let config = NatsConfig {
        url: "nats://localhost:4222".to_string(),
        creds_file: None,
        creds_content: None,
    };
    let queue = NatsQueue::connect_async(&config).await;
    assert!(queue.is_ok());
}
