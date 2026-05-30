//! Podman container runtime — manages OCI container agents via Podman pods.
//!
//! Extracted from vlinderd to allow alternative runtimes (e.g., Lambda)
//! to implement the same `Runtime` trait.

mod config;
mod podman_api;
mod podman_cli;
mod podman_client;
mod pool;
mod unix_transport;

pub use config::PodmanRuntimeConfig;
pub use podman_api::PodmanApiClient;
pub use podman_cli::PodmanCliClient;
pub use podman_client::{resolve_socket, PodmanClient, PortMapping};
pub use pool::ContainerRuntime;
