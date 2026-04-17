//! gRPC client implementing the `CatalogService` trait.

use async_trait::async_trait;
use tonic::transport::Channel;

use super::proto::{self, catalog_service_client::CatalogServiceClient};
use vlinder_core::domain::{CatalogError, CatalogService, Model, ModelInfo};

/// `CatalogService` implementation that makes gRPC calls to a remote Catalog Service.
///
/// A single client talks to the entire service — catalog names are passed
/// per-call, not baked into the constructor.
pub struct GrpcCatalogClient {
    client: CatalogServiceClient<Channel>,
}

impl GrpcCatalogClient {
    /// Connect to a catalog service server.
    pub async fn connect_async(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let client = CatalogServiceClient::connect(addr.to_string()).await?;
        Ok(Self { client })
    }
}

pub async fn ping_catalog_service_async(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(mut client) = CatalogServiceClient::connect(addr.to_string()).await else {
        return None;
    };
    client.ping(proto::PingRequest {}).await.ok().map(|r| {
        let v = r.into_inner();
        (v.major, v.minor, v.patch)
    })
}

#[async_trait]
impl CatalogService for GrpcCatalogClient {
    async fn catalogs(&self) -> Vec<String> {
        let mut client = self.client.clone();
        match client.list_catalogs(proto::ListCatalogsRequest {}).await {
            Ok(resp) => resp.into_inner().catalogs,
            Err(_) => Vec::new(),
        }
    }

    async fn resolve(&self, catalog: &str, name: &str) -> Result<Model, CatalogError> {
        let request = proto::ResolveRequest {
            catalog: catalog.to_string(),
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .resolve(request)
            .await
            .map_err(|e| CatalogError::Network(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(err) = resp.error {
            return Err(CatalogError::Network(err));
        }
        match resp.model {
            Some(m) => Model::try_from(m).map_err(CatalogError::Parse),
            None => Err(CatalogError::NotFound(name.to_string())),
        }
    }

    async fn list(&self, catalog: &str) -> Result<Vec<ModelInfo>, CatalogError> {
        let request = proto::ListRequest {
            catalog: catalog.to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .list(request)
            .await
            .map_err(|e| CatalogError::Network(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(err) = resp.error {
            return Err(CatalogError::Network(err));
        }
        Ok(resp.models.into_iter().map(Into::into).collect())
    }

    async fn available(&self, catalog: &str, name: &str) -> bool {
        let request = proto::AvailableRequest {
            catalog: catalog.to_string(),
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        match client.available(request).await {
            Ok(resp) => resp.into_inner().available,
            Err(_) => false,
        }
    }
}
