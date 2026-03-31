//! Configuration for the Podman container runtime.

/// Configuration for the Podman container runtime.
///
/// Extracted from vlinderd's full Config to decouple the runtime
/// crate from daemon configuration.
#[derive(Clone, Debug)]
pub struct PodmanRuntimeConfig {
    /// "mutable" or "pinned" (ADR 073)
    pub image_policy: String,
    /// "auto", "disabled", or explicit socket path (ADR 077)
    pub podman_socket: String,
    /// OCI image ref for the sidecar container
    pub sidecar_image: String,
    /// Queue backend: "nats" or "sqs"
    pub queue_backend: String,
    /// NATS URL for sidecar env vars (when `queue_backend` = "nats")
    pub nats_url: String,
    /// SQS region (when `queue_backend` = "sqs")
    pub sqs_region: String,
    /// SQS queue name prefix (when `queue_backend` = "sqs")
    pub sqs_queue_prefix: String,
    /// Registry gRPC address for sidecar env vars
    pub registry_addr: String,
    /// State service gRPC address for sidecar env vars
    pub state_addr: String,
    /// Secret store gRPC address for sidecar env vars
    pub secret_addr: String,
}
