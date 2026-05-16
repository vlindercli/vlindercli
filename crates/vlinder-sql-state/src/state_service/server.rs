//! gRPC server wrapping the `DagStore` trait.

use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::{
    convert,
    proto::{
        self, state_service_server::StateService, CompleteNodeProto, CreateBranchRequest,
        CreateBranchResponse, CreateSessionRequest, CreateSessionResponse, GetBranchByIdRequest,
        GetBranchByNameRequest, GetBranchResponse, GetBranchesForSessionRequest,
        GetBranchesForSessionResponse, GetChildrenRequest, GetChildrenResponse,
        GetCompleteMessageRequest, GetCompleteMessageResponse, GetCompleteNodeRequest,
        GetCompleteNodeResponse, GetInvokeMessageRequest, GetInvokeMessageResponse,
        GetInvokeNodeRequest, GetInvokeNodeResponse, GetNodeByPrefixRequest, GetNodeRequest,
        GetNodeResponse, GetNodesBySubmissionRequest, GetNodesBySubmissionResponse,
        GetRequestNodeRequest, GetRequestNodeResponse, GetRequestV2Request, GetRequestV2Response,
        GetResponseNodeRequest, GetResponseNodeResponse, GetResponseV2Request,
        GetResponseV2Response, GetSessionByNameRequest, GetSessionNodesRequest,
        GetSessionNodesResponse, GetSessionRequest, GetSessionResponse, GetSvcRequestNodeRequest,
        GetSvcRequestNodeResponse, GetSvcResponseNodeRequest, GetSvcResponseNodeResponse,
        InsertCompleteNodeRequest, InsertCompleteNodeResponse, InsertDeleteAgentNodeRequest,
        InsertDeleteAgentNodeResponse, InsertDeployAgentNodeRequest, InsertDeployAgentNodeResponse,
        InsertForkNodeRequest, InsertForkNodeResponse, InsertInvokeNodeRequest,
        InsertInvokeNodeResponse, InsertPromoteNodeRequest, InsertPromoteNodeResponse,
        InsertRequestNodeRequest, InsertRequestNodeResponse, InsertResponseNodeRequest,
        InsertResponseNodeResponse, InsertSvcRequestNodeRequest, InsertSvcRequestNodeResponse,
        InsertSvcResponseNodeRequest, InsertSvcResponseNodeResponse, InvokeNodeProto,
        LatestNodeOnBranchRequest, LatestNodeOnBranchResponse, LatestNodesOnBranchRequest,
        LatestNodesOnBranchResponse, ListSessionsRequest, ListSessionsResponse, PingRequest,
        RenameBranchRequest, RenameBranchResponse, RequestNodeProto, ResponseNodeProto,
        SealBranchRequest, SealBranchResponse, SemVer, SvcRequestNodeProto, SvcResponseNodeProto,
        UpdateSessionDefaultBranchRequest, UpdateSessionDefaultBranchResponse,
    },
};
use vlinder_core::domain::{DagNodeId, DagStore, MessageType, SessionId};

/// gRPC server that wraps a `DagStore` implementation.
pub struct StateServiceServer {
    store: Arc<dyn DagStore>,
}

impl StateServiceServer {
    pub fn new(store: Arc<dyn DagStore>) -> Self {
        Self { store }
    }

    /// Create a tonic service from this server.
    pub fn into_service(self) -> proto::state_service_server::StateServiceServer<Self> {
        proto::state_service_server::StateServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl StateService for StateServiceServer {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<SemVer>, Status> {
        Ok(Response::new(SemVer {
            major: 0,
            minor: 0,
            patch: 1,
        }))
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();

        let node = self
            .store
            .get_node(&DagNodeId::from(req.hash))
            .await
            .map_err(Status::internal)?
            .map(std::convert::Into::into);

        Ok(Response::new(GetNodeResponse { node }))
    }

    async fn get_session_nodes(
        &self,
        request: Request<GetSessionNodesRequest>,
    ) -> Result<Response<GetSessionNodesResponse>, Status> {
        let req = request.into_inner();

        let nodes = self
            .store
            .get_session_nodes(
                &SessionId::try_from(req.session_id).map_err(Status::invalid_argument)?,
            )
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();

        Ok(Response::new(GetSessionNodesResponse { nodes }))
    }

    async fn get_children(
        &self,
        request: Request<GetChildrenRequest>,
    ) -> Result<Response<GetChildrenResponse>, Status> {
        let req = request.into_inner();

        let nodes = self
            .store
            .get_children(&DagNodeId::from(req.parent_hash))
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();

        Ok(Response::new(GetChildrenResponse { nodes }))
    }

    // -------------------------------------------------------------------------
    // Branch RPCs
    // -------------------------------------------------------------------------

    async fn create_branch(
        &self,
        request: Request<CreateBranchRequest>,
    ) -> Result<Response<CreateBranchResponse>, Status> {
        let req = request.into_inner();
        let fork_point = req.fork_point.map(DagNodeId::from);
        let id = self
            .store
            .create_branch(
                &req.name,
                &SessionId::try_from(req.session_id).map_err(Status::invalid_argument)?,
                fork_point.as_ref(),
            )
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(CreateBranchResponse { id: id.as_i64() }))
    }

    async fn get_branch_by_name(
        &self,
        request: Request<GetBranchByNameRequest>,
    ) -> Result<Response<GetBranchResponse>, Status> {
        let req = request.into_inner();
        let branch = self
            .store
            .get_branch_by_name(&req.name)
            .await
            .map_err(Status::internal)?
            .map(std::convert::Into::into);
        Ok(Response::new(GetBranchResponse { branch }))
    }

    async fn get_branch(
        &self,
        request: Request<GetBranchByIdRequest>,
    ) -> Result<Response<GetBranchResponse>, Status> {
        let req = request.into_inner();
        let branch = self
            .store
            .get_branch(vlinder_core::domain::BranchId::from(req.id))
            .await
            .map_err(Status::internal)?
            .map(std::convert::Into::into);
        Ok(Response::new(GetBranchResponse { branch }))
    }

    // -------------------------------------------------------------------------
    // Session query RPCs
    // -------------------------------------------------------------------------

    async fn list_sessions(
        &self,
        _request: Request<ListSessionsRequest>,
    ) -> Result<Response<ListSessionsResponse>, Status> {
        let sessions = self
            .store
            .list_sessions()
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();
        Ok(Response::new(ListSessionsResponse { sessions }))
    }

    async fn get_nodes_by_submission(
        &self,
        request: Request<GetNodesBySubmissionRequest>,
    ) -> Result<Response<GetNodesBySubmissionResponse>, Status> {
        let req = request.into_inner();
        let nodes = self
            .store
            .get_nodes_by_submission(&req.submission_id)
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();
        Ok(Response::new(GetNodesBySubmissionResponse { nodes }))
    }

    async fn get_node_by_prefix(
        &self,
        request: Request<GetNodeByPrefixRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();
        let node = self
            .store
            .get_node_by_prefix(&req.prefix)
            .await
            .map_err(Status::internal)?
            .map(std::convert::Into::into);
        Ok(Response::new(GetNodeResponse { node }))
    }

    async fn get_branches_for_session(
        &self,
        request: Request<GetBranchesForSessionRequest>,
    ) -> Result<Response<GetBranchesForSessionResponse>, Status> {
        let req = request.into_inner();
        let branches = self
            .store
            .get_branches_for_session(
                &SessionId::try_from(req.session_id).map_err(Status::invalid_argument)?,
            )
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect();
        Ok(Response::new(GetBranchesForSessionResponse { branches }))
    }

    // -------------------------------------------------------------------------
    // Session CRUD RPCs
    // -------------------------------------------------------------------------

    async fn create_session(
        &self,
        request: Request<CreateSessionRequest>,
    ) -> Result<Response<CreateSessionResponse>, Status> {
        let req = request.into_inner();
        let session_proto = req
            .session
            .ok_or_else(|| Status::invalid_argument("missing session"))?;
        let session_id = SessionId::try_from(session_proto.id).map_err(Status::invalid_argument)?;
        let ext_id = vlinder_core::domain::ExternalSessionId::new(&session_proto.external_id)
            .map_err(|e| Status::invalid_argument(format!("invalid external_id: {e}")))?;
        let session = vlinder_core::domain::Session::new(
            session_id,
            ext_id,
            &session_proto.agent_name,
            vlinder_core::domain::BranchId::from(session_proto.default_branch),
        );
        match self
            .store
            .create_session(&vlinder_core::domain::Session {
                name: session_proto.name,
                ..session
            })
            .await
        {
            Ok(()) => Ok(Response::new(CreateSessionResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(CreateSessionResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn get_session(
        &self,
        request: Request<GetSessionRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let req = request.into_inner();
        let session_id = SessionId::try_from(req.session_id).map_err(Status::invalid_argument)?;
        let session = self
            .store
            .get_session(&session_id)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(GetSessionResponse {
            session: session.map(session_to_proto),
        }))
    }

    async fn get_session_by_name(
        &self,
        request: Request<GetSessionByNameRequest>,
    ) -> Result<Response<GetSessionResponse>, Status> {
        let req = request.into_inner();
        let session = self
            .store
            .get_session_by_name(&req.name)
            .await
            .map_err(Status::internal)?;
        Ok(Response::new(GetSessionResponse {
            session: session.map(session_to_proto),
        }))
    }

    async fn get_invoke_node(
        &self,
        request: Request<GetInvokeNodeRequest>,
    ) -> Result<Response<GetInvokeNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_invoke_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| {
            let (harness, runtime, agent) = match &key.kind {
                vlinder_core::domain::DataMessageKind::Invoke {
                    harness,
                    runtime,
                    agent,
                } => (
                    harness.as_str().to_string(),
                    runtime.as_str().to_string(),
                    agent.to_string(),
                ),
                _ => unreachable!("get_invoke_node returned non-Invoke key"),
            };
            let payload = serde_json::to_vec(&msg).unwrap_or_default();
            InvokeNodeProto {
                session_id: key.session.to_string(),
                branch: key.branch.as_i64(),
                submission_id: key.submission.to_string(),
                harness,
                runtime,
                agent,
                message_id: msg.id.to_string(),
                state: msg.state,
                diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
                payload,
                dag_parent: msg.dag_parent.to_string(),
                dag_hash: msg.dag_id.to_string(),
            }
        });

        Ok(Response::new(GetInvokeNodeResponse { node }))
    }

    async fn get_complete_node(
        &self,
        request: Request<GetCompleteNodeRequest>,
    ) -> Result<Response<GetCompleteNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_complete_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| CompleteNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            agent: match &key.kind {
                vlinder_core::domain::DataMessageKind::Complete { agent, .. } => agent.to_string(),
                _ => String::new(),
            },
            harness: match &key.kind {
                vlinder_core::domain::DataMessageKind::Complete { harness, .. } => {
                    harness.as_str().to_string()
                }
                _ => String::new(),
            },
            message_id: msg.id.to_string(),
            state: msg.state.clone(),
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: serde_json::to_vec(&msg).unwrap_or_default(),
            dag_hash: msg.dag_id.to_string(),
        });

        Ok(Response::new(GetCompleteNodeResponse { node }))
    }

    async fn get_request_node(
        &self,
        request: Request<GetRequestNodeRequest>,
    ) -> Result<Response<GetRequestNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_request_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| RequestNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            agent: match &key.kind {
                vlinder_core::domain::DataMessageKind::Request { agent, .. } => agent.to_string(),
                _ => String::new(),
            },
            service: match &key.kind {
                vlinder_core::domain::DataMessageKind::Request { service, .. } => {
                    service.to_string()
                }
                _ => String::new(),
            },
            operation: match &key.kind {
                vlinder_core::domain::DataMessageKind::Request { operation, .. } => {
                    operation.to_string()
                }
                _ => String::new(),
            },
            sequence: match &key.kind {
                vlinder_core::domain::DataMessageKind::Request { sequence, .. } => {
                    sequence.as_u32()
                }
                _ => 0,
            },
            message_id: msg.id.to_string(),
            state: msg.state,
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload,
            dag_hash: msg.dag_id.to_string(),
            checkpoint: msg.checkpoint,
        });

        Ok(Response::new(GetRequestNodeResponse { node }))
    }

    async fn get_response_node(
        &self,
        request: Request<GetResponseNodeRequest>,
    ) -> Result<Response<GetResponseNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_response_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| ResponseNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            agent: match &key.kind {
                vlinder_core::domain::DataMessageKind::Response { agent, .. } => agent.to_string(),
                _ => String::new(),
            },
            service: match &key.kind {
                vlinder_core::domain::DataMessageKind::Response { service, .. } => {
                    service.to_string()
                }
                _ => String::new(),
            },
            operation: match &key.kind {
                vlinder_core::domain::DataMessageKind::Response { operation, .. } => {
                    operation.to_string()
                }
                _ => String::new(),
            },
            sequence: match &key.kind {
                vlinder_core::domain::DataMessageKind::Response { sequence, .. } => {
                    sequence.as_u32()
                }
                _ => 0,
            },
            message_id: msg.id.to_string(),
            correlation_id: msg.correlation_id.to_string(),
            state: msg.state,
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload,
            status_code: u32::from(msg.status_code),
            dag_hash: msg.dag_id.to_string(),
            checkpoint: msg.checkpoint,
        });

        Ok(Response::new(GetResponseNodeResponse { node }))
    }

    async fn get_svc_request_node(
        &self,
        request: Request<GetSvcRequestNodeRequest>,
    ) -> Result<Response<GetSvcRequestNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_svc_request_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| SvcRequestNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            agent: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcRequest { agent, .. } => agent.to_string(),
                vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => String::new(),
            },
            service_type: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcRequest { service, .. } => {
                    service.service_type_str().to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => String::new(),
            },
            service_backend: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcRequest { service, .. } => {
                    service.backend_str().to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => String::new(),
            },
            operation: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcRequest { operation, .. } => {
                    operation.to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => String::new(),
            },
            sequence: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcRequest { sequence, .. } => {
                    sequence.as_u32()
                }
                vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => 0,
            },
            message_id: msg.id.to_string(),
            tool_call_id: msg.tool_call_id.to_string(),
            state: msg.state,
            arguments: msg.payload,
            diagnostics: Some(serde_json::to_string(&msg.diagnostics).unwrap_or_default()),
        });

        Ok(Response::new(GetSvcRequestNodeResponse { node }))
    }

    async fn get_svc_response_node(
        &self,
        request: Request<GetSvcResponseNodeRequest>,
    ) -> Result<Response<GetSvcResponseNodeResponse>, Status> {
        let req = request.into_inner();
        let dag_hash = DagNodeId::from(req.dag_hash);
        let result = self
            .store
            .get_svc_response_node(&dag_hash)
            .await
            .map_err(Status::internal)?;

        let node = result.map(|(key, msg)| SvcResponseNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            agent: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcResponse { agent, .. } => {
                    agent.to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => String::new(),
            },
            service_type: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcResponse { service, .. } => {
                    service.service_type_str().to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => String::new(),
            },
            service_backend: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcResponse { service, .. } => {
                    service.backend_str().to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => String::new(),
            },
            operation: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcResponse { operation, .. } => {
                    operation.to_string()
                }
                vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => String::new(),
            },
            sequence: match &key.kind {
                vlinder_core::domain::SvcMessageKind::SvcResponse { sequence, .. } => {
                    sequence.as_u32()
                }
                vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => 0,
            },
            message_id: msg.id.to_string(),
            correlation_id: msg.correlation_id.to_string(),
            state: msg.state,
            payload: msg.payload,
            diagnostics: Some(serde_json::to_string(&msg.diagnostics).unwrap_or_default()),
        });

        Ok(Response::new(GetSvcResponseNodeResponse { node }))
    }

    async fn get_invoke_message(
        &self,
        request: Request<GetInvokeMessageRequest>,
    ) -> Result<Response<GetInvokeMessageResponse>, Status> {
        let req = request.into_inner();
        let dag_id = DagNodeId::from(req.dag_node_id);
        let result = self
            .store
            .get_invoke_message(&dag_id)
            .await
            .map_err(Status::internal)?;

        let message = result.map(|msg| convert::invoke_message_to_proto(&msg));
        Ok(Response::new(GetInvokeMessageResponse { message }))
    }

    async fn get_complete_message(
        &self,
        request: Request<GetCompleteMessageRequest>,
    ) -> Result<Response<GetCompleteMessageResponse>, Status> {
        let req = request.into_inner();
        let dag_id = DagNodeId::from(req.dag_node_id);
        let result = self
            .store
            .get_complete_message(&dag_id)
            .await
            .map_err(Status::internal)?;

        let message = result.map(|msg| convert::complete_message_to_proto(&msg));
        Ok(Response::new(GetCompleteMessageResponse { message }))
    }

    async fn get_request_v2(
        &self,
        request: Request<GetRequestV2Request>,
    ) -> Result<Response<GetRequestV2Response>, Status> {
        let req = request.into_inner();
        let dag_id = DagNodeId::from(req.dag_node_id);
        let result = self
            .store
            .get_request_v2(&dag_id)
            .await
            .map_err(Status::internal)?;

        let message = result.map(|msg| convert::request_v2_to_proto(&msg));
        Ok(Response::new(GetRequestV2Response { message }))
    }

    async fn get_response_v2(
        &self,
        request: Request<GetResponseV2Request>,
    ) -> Result<Response<GetResponseV2Response>, Status> {
        let req = request.into_inner();
        let dag_id = DagNodeId::from(req.dag_node_id);
        let result = self
            .store
            .get_response_v2(&dag_id)
            .await
            .map_err(Status::internal)?;

        let message = result.map(|msg| convert::response_v2_to_proto(&msg));
        Ok(Response::new(GetResponseV2Response { message }))
    }

    async fn insert_invoke_node(
        &self,
        request: Request<InsertInvokeNodeRequest>,
    ) -> Result<Response<InsertInvokeNodeResponse>, Status> {
        let req = request.into_inner();
        let n = req
            .node
            .ok_or_else(|| Status::invalid_argument("missing node"))?;

        let harness: vlinder_core::domain::HarnessType =
            n.harness.parse().map_err(Status::invalid_argument)?;
        let runtime: vlinder_core::domain::RuntimeType =
            n.runtime.parse().map_err(Status::invalid_argument)?;

        let dag_id = vlinder_core::domain::DagNodeId::from(n.dag_hash.clone());
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> = req
            .created_at
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?;
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;

        let key = vlinder_core::domain::DataRoutingKey {
            session: vlinder_core::domain::SessionId::try_from(n.session_id)
                .map_err(Status::invalid_argument)?,
            branch: vlinder_core::domain::BranchId::from(n.branch),
            submission: vlinder_core::domain::SubmissionId::from(n.submission_id),
            kind: vlinder_core::domain::DataMessageKind::Invoke {
                harness,
                runtime,
                agent: vlinder_core::domain::AgentName::new(n.agent),
            },
        };

        let diagnostics: vlinder_core::domain::InvokeDiagnostics =
            serde_json::from_slice(&n.diagnostics).unwrap_or_else(|_| {
                vlinder_core::domain::InvokeDiagnostics {
                    harness_version: String::new(),
                }
            });

        let msg: vlinder_core::domain::InvokeMessage = serde_json::from_slice(&n.payload)
            .unwrap_or_else(|_| vlinder_core::domain::InvokeMessage {
                id: vlinder_core::domain::MessageId::from(n.message_id.clone()),
                dag_id: vlinder_core::domain::DagNodeId::from(n.dag_hash.clone()),
                state: n.state.clone(),
                diagnostics,
                dag_parent: vlinder_core::domain::DagNodeId::from(n.dag_parent),
                current_input: vec![],
            });

        match self
            .store
            .insert_invoke_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
            .await
        {
            Ok(()) => Ok(Response::new(InsertInvokeNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertInvokeNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_complete_node(
        &self,
        request: Request<InsertCompleteNodeRequest>,
    ) -> Result<Response<InsertCompleteNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> = req
            .created_at
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?;
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;
        let session = vlinder_core::domain::SessionId::try_from(req.session_id)
            .map_err(Status::invalid_argument)?;
        let submission = vlinder_core::domain::SubmissionId::from(req.submission_id);
        let branch = vlinder_core::domain::BranchId::from(req.branch_id);
        let agent = vlinder_core::domain::AgentName::new(req.agent);
        let harness: vlinder_core::domain::HarnessType =
            req.harness.parse().map_err(Status::invalid_argument)?;

        let msg: vlinder_core::domain::CompleteMessage = serde_json::from_slice(&req.payload)
            .unwrap_or_else(|_| {
                let diagnostics = serde_json::from_slice(&req.diagnostics)
                    .unwrap_or_else(|_| vlinder_core::domain::RuntimeDiagnostics::placeholder(0));
                vlinder_core::domain::CompleteMessage {
                    id: vlinder_core::domain::MessageId::from(req.message_id.clone()),
                    dag_id: dag_id.clone(),
                    dag_parent: vlinder_core::domain::DagNodeId::root(),
                    state: req.state.clone(),
                    diagnostics,
                    content: None,
                    tool_calls: None,
                    payload: req.payload.clone(),
                }
            });

        match self
            .store
            .insert_complete_node(
                &dag_id,
                &parent_id,
                created_at,
                &snapshot,
                &session,
                &submission,
                branch,
                &agent,
                harness,
                &msg,
            )
            .await
        {
            Ok(()) => Ok(Response::new(InsertCompleteNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertCompleteNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_request_node(
        &self,
        request: Request<InsertRequestNodeRequest>,
    ) -> Result<Response<InsertRequestNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> = req
            .created_at
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?;
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;
        let session = vlinder_core::domain::SessionId::try_from(req.session_id)
            .map_err(Status::invalid_argument)?;
        let submission = vlinder_core::domain::SubmissionId::from(req.submission_id);
        let branch = vlinder_core::domain::BranchId::from(req.branch_id);
        let agent = vlinder_core::domain::AgentName::new(req.agent);
        let service: vlinder_core::domain::ServiceBackend =
            req.service.parse().map_err(Status::invalid_argument)?;
        let operation: vlinder_core::domain::Operation =
            req.operation.parse().map_err(Status::invalid_argument)?;
        let sequence = vlinder_core::domain::Sequence::from(req.sequence);

        let diagnostics: vlinder_core::domain::RequestDiagnostics =
            serde_json::from_slice(&req.diagnostics).unwrap_or_else(|_| {
                vlinder_core::domain::RequestDiagnostics {
                    sequence: 0,
                    endpoint: String::new(),
                    request_bytes: 0,
                    received_at_ms: 0,
                }
            });

        let msg = vlinder_core::domain::RequestMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            dag_id: dag_id.clone(),
            dag_parent: vlinder_core::domain::DagNodeId::root(),
            state: req.state,
            diagnostics,
            payload: req.payload,
            checkpoint: req.checkpoint,
        };

        match self
            .store
            .insert_request_node(
                &dag_id,
                &parent_id,
                created_at,
                &snapshot,
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
        {
            Ok(()) => Ok(Response::new(InsertRequestNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertRequestNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_response_node(
        &self,
        request: Request<InsertResponseNodeRequest>,
    ) -> Result<Response<InsertResponseNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> = req
            .created_at
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?;
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;
        let session = vlinder_core::domain::SessionId::try_from(req.session_id)
            .map_err(Status::invalid_argument)?;
        let submission = vlinder_core::domain::SubmissionId::from(req.submission_id);
        let branch = vlinder_core::domain::BranchId::from(req.branch_id);
        let agent = vlinder_core::domain::AgentName::new(req.agent);
        let service: vlinder_core::domain::ServiceBackend =
            req.service.parse().map_err(Status::invalid_argument)?;
        let operation: vlinder_core::domain::Operation =
            req.operation.parse().map_err(Status::invalid_argument)?;
        let sequence = vlinder_core::domain::Sequence::from(req.sequence);

        let diagnostics: vlinder_core::domain::ServiceDiagnostics =
            serde_json::from_slice(&req.diagnostics).unwrap_or_else(|_| {
                vlinder_core::domain::ServiceDiagnostics::storage(
                    vlinder_core::domain::ServiceType::Kv,
                    "unknown",
                    vlinder_core::domain::Operation::Get,
                    0,
                    0,
                )
            });

        let msg = vlinder_core::domain::ResponseMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            dag_id: dag_id.clone(),
            dag_parent: vlinder_core::domain::DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::from(req.correlation_id),
            state: req.state,
            diagnostics,
            payload: req.payload,
            status_code: u16::try_from(req.status_code).unwrap_or(200),
            checkpoint: req.checkpoint,
        };

        match self
            .store
            .insert_response_node(
                &dag_id,
                &parent_id,
                created_at,
                &snapshot,
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
        {
            Ok(()) => Ok(Response::new(InsertResponseNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertResponseNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_fork_node(
        &self,
        request: Request<InsertForkNodeRequest>,
    ) -> Result<Response<InsertForkNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;

        let key = vlinder_core::domain::SessionRoutingKey {
            session: vlinder_core::domain::SessionId::try_from(req.session_id)
                .map_err(Status::invalid_argument)?,
            submission: vlinder_core::domain::SubmissionId::from(req.submission_id),
            kind: vlinder_core::domain::SessionMessageKind::Fork {
                agent_name: vlinder_core::domain::AgentName::new(req.agent_name),
            },
        };

        let msg = vlinder_core::domain::ForkMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            branch_name: req.branch_name,
            fork_point: vlinder_core::domain::DagNodeId::from(req.fork_point),
        };

        match self
            .store
            .insert_fork_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
            .await
        {
            Ok(()) => Ok(Response::new(InsertForkNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertForkNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_promote_node(
        &self,
        request: Request<InsertPromoteNodeRequest>,
    ) -> Result<Response<InsertPromoteNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = vlinder_core::domain::DagNodeId::from(req.dag_hash);
        let parent_id = vlinder_core::domain::DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;

        let key = vlinder_core::domain::SessionRoutingKey {
            session: vlinder_core::domain::SessionId::try_from(req.session_id)
                .map_err(Status::invalid_argument)?,
            submission: vlinder_core::domain::SubmissionId::from(req.submission_id),
            kind: vlinder_core::domain::SessionMessageKind::Promote {
                agent_name: vlinder_core::domain::AgentName::new(req.agent_name),
            },
        };

        let msg = vlinder_core::domain::PromoteMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            branch_id: vlinder_core::domain::BranchId::from(req.branch_id),
        };

        match self
            .store
            .insert_promote_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
            .await
        {
            Ok(()) => Ok(Response::new(InsertPromoteNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertPromoteNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn latest_node_on_branch(
        &self,
        request: Request<LatestNodeOnBranchRequest>,
    ) -> Result<Response<LatestNodeOnBranchResponse>, Status> {
        let req = request.into_inner();
        let message_type = req
            .message_type
            .map(|s| s.parse::<MessageType>())
            .transpose()
            .map_err(Status::invalid_argument)?;
        let node = self
            .store
            .latest_node_on_branch(
                vlinder_core::domain::BranchId::from(req.branch_id),
                message_type,
            )
            .await
            .map_err(Status::internal)?
            .map(std::convert::Into::into);
        Ok(Response::new(LatestNodeOnBranchResponse { node }))
    }

    async fn latest_nodes_on_branch(
        &self,
        request: Request<LatestNodesOnBranchRequest>,
    ) -> Result<Response<LatestNodesOnBranchResponse>, Status> {
        let req = request.into_inner();
        let nodes = self
            .store
            .latest_nodes_on_branch(vlinder_core::domain::BranchId::from(req.branch_id), req.n)
            .await
            .map_err(Status::internal)?
            .into_iter()
            .map(std::convert::Into::into)
            .collect::<Vec<_>>();
        Ok(Response::new(LatestNodesOnBranchResponse { nodes }))
    }

    // -------------------------------------------------------------------------
    // Branch mutation RPCs
    // -------------------------------------------------------------------------

    async fn rename_branch(
        &self,
        request: Request<RenameBranchRequest>,
    ) -> Result<Response<RenameBranchResponse>, Status> {
        let req = request.into_inner();
        match self
            .store
            .rename_branch(vlinder_core::domain::BranchId::from(req.id), &req.new_name)
            .await
        {
            Ok(()) => Ok(Response::new(RenameBranchResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(RenameBranchResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn seal_branch(
        &self,
        request: Request<SealBranchRequest>,
    ) -> Result<Response<SealBranchResponse>, Status> {
        let req = request.into_inner();
        let broken_at: chrono::DateTime<chrono::Utc> = req
            .broken_at
            .parse()
            .map_err(|e| Status::invalid_argument(format!("invalid broken_at: {e}")))?;
        match self
            .store
            .seal_branch(vlinder_core::domain::BranchId::from(req.id), broken_at)
            .await
        {
            Ok(()) => Ok(Response::new(SealBranchResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(SealBranchResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    // -------------------------------------------------------------------------
    // Session mutation RPCs
    // -------------------------------------------------------------------------

    async fn update_session_default_branch(
        &self,
        request: Request<UpdateSessionDefaultBranchRequest>,
    ) -> Result<Response<UpdateSessionDefaultBranchResponse>, Status> {
        let req = request.into_inner();
        let session_id = SessionId::try_from(req.session_id).map_err(Status::invalid_argument)?;
        match self
            .store
            .update_session_default_branch(
                &session_id,
                vlinder_core::domain::BranchId::from(req.branch_id),
            )
            .await
        {
            Ok(()) => Ok(Response::new(UpdateSessionDefaultBranchResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(UpdateSessionDefaultBranchResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_deploy_agent_node(
        &self,
        request: Request<InsertDeployAgentNodeRequest>,
    ) -> Result<Response<InsertDeployAgentNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = DagNodeId::from(req.dag_hash);
        let parent_id = DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;

        let manifest: vlinder_core::domain::AgentManifest =
            serde_json::from_str(&req.manifest_json)
                .map_err(|e| Status::invalid_argument(format!("invalid manifest: {e}")))?;

        let key = vlinder_core::domain::InfraRoutingKey {
            submission: vlinder_core::domain::SubmissionId::from(req.submission_id),
            kind: vlinder_core::domain::InfraMessageKind::DeployAgent,
        };

        let msg = vlinder_core::domain::DeployAgentMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            manifest,
        };

        match self
            .store
            .insert_deploy_agent_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
            .await
        {
            Ok(()) => Ok(Response::new(InsertDeployAgentNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertDeployAgentNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_delete_agent_node(
        &self,
        request: Request<InsertDeleteAgentNodeRequest>,
    ) -> Result<Response<InsertDeleteAgentNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = DagNodeId::from(req.dag_hash);
        let parent_id = DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;

        let key = vlinder_core::domain::InfraRoutingKey {
            submission: vlinder_core::domain::SubmissionId::from(req.submission_id),
            kind: vlinder_core::domain::InfraMessageKind::DeleteAgent,
        };

        let msg = vlinder_core::domain::DeleteAgentMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            agent: vlinder_core::domain::AgentName::new(req.agent_name),
        };

        match self
            .store
            .insert_delete_agent_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
            .await
        {
            Ok(()) => Ok(Response::new(InsertDeleteAgentNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertDeleteAgentNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_svc_request_node(
        &self,
        request: Request<InsertSvcRequestNodeRequest>,
    ) -> Result<Response<InsertSvcRequestNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = DagNodeId::from(req.dag_hash);
        let parent_id = DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;
        let session = vlinder_core::domain::SessionId::try_from(req.session_id)
            .map_err(Status::invalid_argument)?;
        let submission = vlinder_core::domain::SubmissionId::from(req.submission_id);
        let branch = vlinder_core::domain::BranchId::from(req.branch_id);
        let agent = vlinder_core::domain::AgentName::new(req.agent);
        let service = vlinder_core::domain::ServiceBackendV2::from_parts(
            &req.service_type,
            &req.service_backend,
        )
        .ok_or_else(|| Status::invalid_argument("invalid service_type or service_backend"))?;
        let operation = vlinder_core::domain::ServiceOperation::new(req.operation);
        let sequence = vlinder_core::domain::Sequence::from(req.sequence);

        let state_str = req.state.as_deref().map(String::from);
        let diagnostics: vlinder_core::domain::SvcRequestDiagnostics = req
            .diagnostics
            .as_deref()
            .and_then(|d| serde_json::from_str(d).ok())
            .unwrap_or_default();

        let msg = vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            dag_id: dag_id.clone(),
            dag_parent: vlinder_core::domain::DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::from(req.tool_call_id),
            state: state_str,
            diagnostics,
            payload: req.arguments,
        };

        match self
            .store
            .insert_svc_request_node(
                &dag_id,
                &parent_id,
                created_at,
                &snapshot,
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
        {
            Ok(()) => Ok(Response::new(InsertSvcRequestNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertSvcRequestNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn insert_svc_response_node(
        &self,
        request: Request<InsertSvcResponseNodeRequest>,
    ) -> Result<Response<InsertSvcResponseNodeResponse>, Status> {
        let req = request.into_inner();

        let dag_id = DagNodeId::from(req.dag_hash);
        let parent_id = DagNodeId::from(req.parent_hash);
        let created_at: chrono::DateTime<chrono::Utc> =
            chrono::DateTime::parse_from_rfc3339(&req.created_at)
                .map_err(|e| Status::invalid_argument(format!("invalid created_at: {e}")))?
                .with_timezone(&chrono::Utc);
        let snapshot: vlinder_core::domain::Snapshot = serde_json::from_str(&req.snapshot)
            .map_err(|e| Status::invalid_argument(format!("invalid snapshot: {e}")))?;
        let session = vlinder_core::domain::SessionId::try_from(req.session_id)
            .map_err(Status::invalid_argument)?;
        let submission = vlinder_core::domain::SubmissionId::from(req.submission_id);
        let branch = vlinder_core::domain::BranchId::from(req.branch_id);
        let agent = vlinder_core::domain::AgentName::new(req.agent);
        let service = vlinder_core::domain::ServiceBackendV2::from_parts(
            &req.service_type,
            &req.service_backend,
        )
        .ok_or_else(|| Status::invalid_argument("invalid service_type or service_backend"))?;
        let operation = vlinder_core::domain::ServiceOperation::new(req.operation);
        let sequence = vlinder_core::domain::Sequence::from(req.sequence);

        let state_str = req.state.as_deref().map(String::from);
        let diagnostics: vlinder_core::domain::SvcResponseDiagnostics = req
            .diagnostics
            .as_deref()
            .and_then(|d| serde_json::from_str(d).ok())
            .unwrap_or_default();

        let msg = vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            dag_id: dag_id.clone(),
            dag_parent: vlinder_core::domain::DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::from(req.correlation_id),
            state: state_str,
            diagnostics,
            payload: req.payload,
        };

        match self
            .store
            .insert_svc_response_node(
                &dag_id,
                &parent_id,
                created_at,
                &snapshot,
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
        {
            Ok(()) => Ok(Response::new(InsertSvcResponseNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertSvcResponseNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }
}

fn session_to_proto(s: vlinder_core::domain::Session) -> proto::SessionProto {
    proto::SessionProto {
        id: s.id.as_str().to_string(),
        external_id: s.external_id.as_str().to_string(),
        name: s.name,
        agent_name: s.agent,
        default_branch: s.default_branch.as_i64(),
        created_at: s.created_at.to_rfc3339(),
    }
}
