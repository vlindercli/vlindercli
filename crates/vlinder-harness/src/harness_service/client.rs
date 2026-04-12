//! gRPC client implementing the Harness trait.

use async_trait::async_trait;
use tonic::transport::Channel;

use super::proto::{self, harness_client::HarnessClient};
use vlinder_core::domain::{
    BranchId, DagNodeId, ForkParams, Harness, HarnessType, PromoteParams, ResourceId, SessionId,
};

/// Ping a harness service at the given address, returning its protocol version.
///
/// Creates a temporary connection and sends a Ping. Returns the server's
/// version on success, None on any connection or transport error.
pub fn ping_harness(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(runtime) = tokio::runtime::Runtime::new() else {
        return None;
    };

    runtime.block_on(async {
        let Ok(mut client) = HarnessClient::connect(addr.to_string()).await else {
            return None;
        };
        client.ping(proto::PingRequest {}).await.ok().map(|r| {
            let v = r.into_inner();
            (v.major, v.minor, v.patch)
        })
    })
}

/// Harness implementation that makes gRPC calls to a remote server.
pub struct GrpcHarnessClient {
    client: HarnessClient<Channel>,
    runtime: tokio::runtime::Runtime,
}

impl GrpcHarnessClient {
    /// Connect to a harness server.
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let client = runtime.block_on(async { HarnessClient::connect(addr.to_string()).await })?;

        Ok(Self { client, runtime })
    }
}

#[async_trait]
impl Harness for GrpcHarnessClient {
    fn harness_type(&self) -> HarnessType {
        HarnessType::Grpc
    }

    fn start_session(&self, agent_name: &str) -> (SessionId, BranchId) {
        let request = proto::StartSessionRequest {
            agent_name: agent_name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.start_session(request).await });

        let resp = response
            .expect("failed to start session via gRPC")
            .into_inner();
        let session_id =
            SessionId::try_from(resp.session_id).expect("server returned invalid session_id");
        let branch_id = BranchId::from(resp.default_branch_id);
        (session_id, branch_id)
    }

    async fn run_agent(
        &self,
        agent_id: &ResourceId,
        input: &str,
        session_id: SessionId,
        timeline: BranchId,
        sealed: bool,
        initial_state: Option<String>,
        dag_parent: DagNodeId,
    ) -> Result<String, String> {
        let request = proto::RunAgentRequest {
            agent_id: agent_id.as_str().to_string(),
            input: input.to_string(),
            branch_id: timeline.to_string(),
            sealed,
            initial_state,
            dag_parent: dag_parent.to_string(),
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = client
            .run_agent(request)
            .await
            .map_err(|e| format!("gRPC error: {e}"))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            Err(error)
        } else {
            Ok(resp.output)
        }
    }

    fn fork_timeline(
        &self,
        params: ForkParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String> {
        let request = proto::ForkTimelineRequest {
            agent_name: params.agent_name.to_string(),
            branch_name: params.branch_name,
            fork_point: params.fork_point.to_string(),
            branch_id: timeline.to_string(),
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.fork_timeline(request).await })
            .map_err(|e| format!("gRPC error: {e}"))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn promote_timeline(
        &self,
        params: PromoteParams,
        session_id: SessionId,
        timeline: BranchId,
    ) -> Result<(), String> {
        let request = proto::PromoteTimelineRequest {
            agent_name: params.agent_name.to_string(),
            branch_id: timeline.to_string(),
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.promote_timeline(request).await })
            .map_err(|e| format!("gRPC error: {e}"))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            Err(error)
        } else {
            Ok(())
        }
    }
}
