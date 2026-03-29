#[cfg(feature = "queue")]
mod connect;
#[cfg(feature = "queue")]
mod queue;
pub mod secret_service;
#[cfg(feature = "secret-store")]
mod secret_store;

/// Expand `~/...` to the user's home directory.
#[cfg(feature = "queue")]
pub(crate) fn expand_tilde(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).to_string_lossy().to_string();
        }
    }
    path.to_string()
}

#[cfg(feature = "queue")]
pub use connect::NatsConfig;
#[cfg(feature = "queue")]
pub use queue::NatsQueue;
#[cfg(feature = "queue")]
pub use queue::{
    complete_parse_subject, fork_parse_subject, invoke_parse_subject, promote_parse_subject,
    request_parse_subject, response_parse_subject,
};
#[cfg(feature = "secret-store")]
pub use secret_store::NatsSecretStore;
