//! gRPC server wrapping a `CatalogService` implementation.

use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::proto::{
    catalog_service_server::CatalogService as GrpcCatalogService, AvailableRequest,
    AvailableResponse, ListCatalogsRequest, ListCatalogsResponse, ListRequest, ListResponse,
    PingRequest, ResolveRequest, ResolveResponse, SemVer,
};
use vlinder_core::domain::CatalogService;

/// gRPC server that delegates catalog requests to a `CatalogService`.
///
/// The `CatalogService` trait handles dispatch to the right backend
/// by catalog name — this server is just a gRPC adapter.
pub struct CatalogServiceServer {
    service: Arc<dyn CatalogService>,
}

impl CatalogServiceServer {
    pub fn new(service: Arc<dyn CatalogService>) -> Self {
        Self { service }
    }

    /// Create a tonic service from this server.
    pub fn into_service(self) -> super::proto::catalog_service_server::CatalogServiceServer<Self> {
        super::proto::catalog_service_server::CatalogServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl GrpcCatalogService for CatalogServiceServer {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<SemVer>, Status> {
        Ok(Response::new(SemVer {
            major: 0,
            minor: 0,
            patch: 1,
        }))
    }

    async fn list_catalogs(
        &self,
        _request: Request<ListCatalogsRequest>,
    ) -> Result<Response<ListCatalogsResponse>, Status> {
        let names = self.service.catalogs().await;
        Ok(Response::new(ListCatalogsResponse { catalogs: names }))
    }

    async fn resolve(
        &self,
        request: Request<ResolveRequest>,
    ) -> Result<Response<ResolveResponse>, Status> {
        let req = request.into_inner();
        let result = self.service.resolve(&req.catalog, &req.name).await;

        match result {
            Ok(model) => Ok(Response::new(ResolveResponse {
                model: Some(model.into()),
                error: None,
            })),
            Err(e) => Ok(Response::new(ResolveResponse {
                model: None,
                error: Some(e.to_string()),
            })),
        }
    }

    async fn list(&self, request: Request<ListRequest>) -> Result<Response<ListResponse>, Status> {
        let req = request.into_inner();
        let result = self.service.list(&req.catalog).await;

        match result {
            Ok(models) => Ok(Response::new(ListResponse {
                models: models.into_iter().map(Into::into).collect(),
                error: None,
            })),
            Err(e) => Ok(Response::new(ListResponse {
                models: Vec::new(),
                error: Some(e.to_string()),
            })),
        }
    }

    async fn available(
        &self,
        request: Request<AvailableRequest>,
    ) -> Result<Response<AvailableResponse>, Status> {
        let req = request.into_inner();
        let available = self.service.available(&req.catalog, &req.name).await;

        Ok(Response::new(AvailableResponse {
            available,
            error: None,
        }))
    }
}
