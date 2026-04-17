//! gRPC server wrapping the `SecretStore` trait.

use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::proto::{
    secret_store_service_server::SecretStoreService, DeleteRequest, DeleteResponse, ExistsRequest,
    ExistsResponse, GetRequest, GetResponse, PingRequest, PutRequest, PutResponse, SemVer,
};
use vlinder_core::domain::SecretStore;

/// gRPC server that wraps a `SecretStore` implementation.
pub struct SecretServer {
    store: Arc<dyn SecretStore>,
}

impl SecretServer {
    pub fn new(store: Arc<dyn SecretStore>) -> Self {
        Self { store }
    }

    /// Create a tonic service from this server.
    pub fn into_service(
        self,
    ) -> super::proto::secret_store_service_server::SecretStoreServiceServer<Self> {
        super::proto::secret_store_service_server::SecretStoreServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl SecretStoreService for SecretServer {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<SemVer>, Status> {
        Ok(Response::new(SemVer {
            major: 0,
            minor: 0,
            patch: 1,
        }))
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        match self.store.put(&req.name, &req.value).await {
            Ok(()) => Ok(Response::new(PutResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(PutResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        match self.store.get(&req.name).await {
            Ok(value) => Ok(Response::new(GetResponse {
                value,
                found: true,
                error: None,
            })),
            Err(vlinder_core::domain::SecretStoreError::NotFound(_)) => {
                Ok(Response::new(GetResponse {
                    value: Vec::new(),
                    found: false,
                    error: None,
                }))
            }
            Err(e) => Ok(Response::new(GetResponse {
                value: Vec::new(),
                found: false,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn exists(
        &self,
        request: Request<ExistsRequest>,
    ) -> Result<Response<ExistsResponse>, Status> {
        let req = request.into_inner();
        match self.store.exists(&req.name).await {
            Ok(exists) => Ok(Response::new(ExistsResponse {
                exists,
                error: None,
            })),
            Err(e) => Ok(Response::new(ExistsResponse {
                exists: false,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        match self.store.delete(&req.name).await {
            Ok(()) => Ok(Response::new(DeleteResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(DeleteResponse {
                success: false,
                error: Some(e.to_string()),
            })),
        }
    }
}
