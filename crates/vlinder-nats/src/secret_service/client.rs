//! gRPC client implementing the `SecretStore` trait.

use async_trait::async_trait;
use tonic::transport::Channel;

use super::proto::{self, secret_store_service_client::SecretStoreServiceClient};
use vlinder_core::domain::{SecretStore, SecretStoreError};

/// `SecretStore` implementation that makes gRPC calls to a remote Secret Service.
pub struct GrpcSecretClient {
    client: SecretStoreServiceClient<Channel>,
    /// Kept alive so the tonic channel's background connection tasks continue running.
    /// `None` when constructed via `connect_async` — the caller's runtime keeps tasks alive.
    _runtime: Option<tokio::runtime::Runtime>,
}

impl GrpcSecretClient {
    /// Connect to a secret service server.
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let client = runtime
            .block_on(async { SecretStoreServiceClient::connect(addr.to_string()).await })?;

        Ok(Self {
            client,
            _runtime: Some(runtime),
        })
    }

    /// Async variant of `connect` — callable from within an existing Tokio runtime.
    pub async fn connect_async(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = SecretStoreServiceClient::connect(addr.to_string()).await?;
        Ok(Self {
            client,
            _runtime: None,
        })
    }
}

/// Ping a secret service at the given address, returning its protocol version.
///
/// Creates a temporary connection and sends a Ping. Returns the server's
/// version on success, None on any connection or transport error.
pub fn ping_secret_service(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(runtime) = tokio::runtime::Runtime::new() else {
        return None;
    };

    runtime.block_on(async {
        let Ok(mut client) = SecretStoreServiceClient::connect(addr.to_string()).await else {
            return None;
        };
        client.ping(proto::PingRequest {}).await.ok().map(|r| {
            let v = r.into_inner();
            (v.major, v.minor, v.patch)
        })
    })
}

pub async fn ping_secret_service_async(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(mut client) = SecretStoreServiceClient::connect(addr.to_string()).await else {
        return None;
    };
    client.ping(proto::PingRequest {}).await.ok().map(|r| {
        let v = r.into_inner();
        (v.major, v.minor, v.patch)
    })
}

#[async_trait]
impl SecretStore for GrpcSecretClient {
    async fn put(&self, name: &str, value: &[u8]) -> Result<(), SecretStoreError> {
        let request = proto::PutRequest {
            name: name.to_string(),
            value: value.to_vec(),
        };

        let mut client = self.client.clone();
        let response = client
            .put(request)
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(SecretStoreError::StoreFailed(
                resp.error.unwrap_or_else(|| "unknown error".to_string()),
            ))
        }
    }

    async fn get(&self, name: &str) -> Result<Vec<u8>, SecretStoreError> {
        let request = proto::GetRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .get(request)
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(err) = resp.error {
            return Err(SecretStoreError::StoreFailed(err));
        }
        if resp.found {
            Ok(resp.value)
        } else {
            Err(SecretStoreError::NotFound(name.to_string()))
        }
    }

    async fn exists(&self, name: &str) -> Result<bool, SecretStoreError> {
        let request = proto::ExistsRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .exists(request)
            .await
            .map_err(|e| SecretStoreError::StoreFailed(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(err) = resp.error {
            return Err(SecretStoreError::StoreFailed(err));
        }
        Ok(resp.exists)
    }

    async fn delete(&self, name: &str) -> Result<(), SecretStoreError> {
        let request = proto::DeleteRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .delete(request)
            .await
            .map_err(|e| SecretStoreError::DeleteFailed(e.to_string()))?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(SecretStoreError::DeleteFailed(
                resp.error.unwrap_or_else(|| "unknown error".to_string()),
            ))
        }
    }
}
