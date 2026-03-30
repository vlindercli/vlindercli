//! gRPC server wrapping the Harness trait.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use super::proto::{
    self, harness_server::Harness as HarnessService, ForkTimelineRequest, ForkTimelineResponse,
    PingRequest, PromoteTimelineRequest, PromoteTimelineResponse, RunAgentRequest,
    RunAgentResponse, SemVer, StartSessionRequest, StartSessionResponse,
};
use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, ForkParams, Harness, PromoteParams, ResourceId, SessionId,
};

/// gRPC server that wraps a Harness implementation.
///
/// Uses `Arc<…>` so the harness can be shared into
/// `spawn_blocking` for long-running calls like `run_agent`.
pub struct HarnessServer {
    harness: Arc<Box<dyn Harness + Send + Sync>>,
}

impl HarnessServer {
    pub fn new(harness: Box<dyn Harness + Send + Sync>) -> Self {
        Self {
            harness: Arc::new(harness),
        }
    }

    /// Create a tonic service from this server.
    pub fn into_service(self) -> proto::harness_server::HarnessServer<Self> {
        proto::harness_server::HarnessServer::new(self)
    }
}

#[tonic::async_trait]
impl HarnessService for HarnessServer {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<SemVer>, Status> {
        Ok(Response::new(SemVer {
            major: 0,
            minor: 0,
            patch: 1,
        }))
    }

    async fn start_session(
        &self,
        request: Request<StartSessionRequest>,
    ) -> Result<Response<StartSessionResponse>, Status> {
        let req = request.into_inner();
        let harness = Arc::clone(&self.harness);
        let (session_id, branch_id) =
            tokio::task::spawn_blocking(move || harness.start_session(&req.agent_name))
                .await
                .map_err(|e| Status::internal(format!("spawn_blocking failed: {e}")))?;
        Ok(Response::new(StartSessionResponse {
            session_id: session_id.as_str().to_string(),
            default_branch_id: branch_id.as_i64(),
        }))
    }

    async fn run_agent(
        &self,
        request: Request<RunAgentRequest>,
    ) -> Result<Response<RunAgentResponse>, Status> {
        let req = request.into_inner();
        let harness = Arc::clone(&self.harness);

        let result = tokio::task::spawn_blocking(move || {
            let id = ResourceId::new(&req.agent_id);
            let session_id = SessionId::try_from(req.session_id)
                .map_err(|e| format!("invalid session_id: {e}"))?;
            let timeline = BranchId::from(req.branch_id.parse::<i64>().unwrap_or(0));
            let dag_parent = DagNodeId::from(req.dag_parent);
            harness.run_agent(
                &id,
                &req.input,
                session_id,
                timeline,
                req.sealed,
                req.initial_state,
                dag_parent,
            )
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking failed: {e}")))?;

        match result {
            Ok(output) => Ok(Response::new(RunAgentResponse {
                output,
                error: None,
            })),
            Err(e) => Ok(Response::new(RunAgentResponse {
                output: String::new(),
                error: Some(e),
            })),
        }
    }

    async fn fork_timeline(
        &self,
        request: Request<ForkTimelineRequest>,
    ) -> Result<Response<ForkTimelineResponse>, Status> {
        let req = request.into_inner();
        let harness = Arc::clone(&self.harness);

        let result = tokio::task::spawn_blocking(move || {
            let params = ForkParams {
                agent_name: AgentName::new(req.agent_name),
                branch_name: req.branch_name,
                fork_point: DagNodeId::from(req.fork_point),
            };
            let session_id = SessionId::try_from(req.session_id)
                .map_err(|e| format!("invalid session_id: {e}"))?;
            let timeline = BranchId::from(req.branch_id.parse::<i64>().unwrap_or(0));
            harness.fork_timeline(params, session_id, timeline)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking failed: {e}")))?;

        match result {
            Ok(()) => Ok(Response::new(ForkTimelineResponse { error: None })),
            Err(e) => Ok(Response::new(ForkTimelineResponse { error: Some(e) })),
        }
    }

    async fn promote_timeline(
        &self,
        request: Request<PromoteTimelineRequest>,
    ) -> Result<Response<PromoteTimelineResponse>, Status> {
        let req = request.into_inner();
        let harness = Arc::clone(&self.harness);

        let result = tokio::task::spawn_blocking(move || {
            let params = PromoteParams {
                agent_name: AgentName::new(req.agent_name),
            };
            let session_id = SessionId::try_from(req.session_id)
                .map_err(|e| format!("invalid session_id: {e}"))?;
            let timeline = BranchId::from(req.branch_id.parse::<i64>().unwrap_or(0));
            harness.promote_timeline(params, session_id, timeline)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking failed: {e}")))?;

        match result {
            Ok(()) => Ok(Response::new(PromoteTimelineResponse { error: None })),
            Err(e) => Ok(Response::new(PromoteTimelineResponse { error: Some(e) })),
        }
    }
}
