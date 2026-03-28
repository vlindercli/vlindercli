//! gRPC server wrapping the `DagStore` trait.

use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::proto::{
    self, state_service_server::StateService, CompleteNodeProto, CreateBranchRequest,
    CreateBranchResponse, CreateSessionRequest, CreateSessionResponse, GetBranchByIdRequest,
    GetBranchByNameRequest, GetBranchResponse, GetBranchesForSessionRequest,
    GetBranchesForSessionResponse, GetChildrenRequest, GetChildrenResponse, GetCompleteNodeRequest,
    GetCompleteNodeResponse, GetInvokeNodeRequest, GetInvokeNodeResponse, GetNodeByPrefixRequest,
    GetNodeRequest, GetNodeResponse, GetNodesBySubmissionRequest, GetNodesBySubmissionResponse,
    GetRequestNodeRequest, GetRequestNodeResponse, GetResponseNodeRequest, GetResponseNodeResponse,
    GetSessionByNameRequest, GetSessionNodesRequest, GetSessionNodesResponse, GetSessionRequest,
    GetSessionResponse, InsertCompleteNodeRequest, InsertCompleteNodeResponse,
    InsertForkNodeRequest, InsertForkNodeResponse, InsertInvokeNodeRequest,
    InsertInvokeNodeResponse, InsertNodeRequest, InsertNodeResponse, InsertPromoteNodeRequest,
    InsertPromoteNodeResponse, InsertRequestNodeRequest, InsertRequestNodeResponse,
    InsertResponseNodeRequest, InsertResponseNodeResponse, InvokeNodeProto,
    LatestNodeOnBranchRequest, LatestNodeOnBranchResponse, ListSessionsRequest,
    ListSessionsResponse, PingRequest, RenameBranchRequest, RenameBranchResponse, RequestNodeProto,
    ResponseNodeProto, SealBranchRequest, SealBranchResponse, SemVer,
    UpdateSessionDefaultBranchRequest, UpdateSessionDefaultBranchResponse,
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

    async fn insert_node(
        &self,
        request: Request<InsertNodeRequest>,
    ) -> Result<Response<InsertNodeResponse>, Status> {
        let req = request.into_inner();
        let proto_node = req
            .node
            .ok_or_else(|| Status::invalid_argument("missing node"))?;

        let node = proto_node
            .try_into()
            .map_err(|e: String| Status::invalid_argument(e))?;

        match self.store.insert_node(&node) {
            Ok(()) => Ok(Response::new(InsertNodeResponse {
                success: true,
                error: None,
            })),
            Err(e) => Ok(Response::new(InsertNodeResponse {
                success: false,
                error: Some(e),
            })),
        }
    }

    async fn get_node(
        &self,
        request: Request<GetNodeRequest>,
    ) -> Result<Response<GetNodeResponse>, Status> {
        let req = request.into_inner();

        let node = self
            .store
            .get_node(&DagNodeId::from(req.hash))
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
        let session = vlinder_core::domain::Session::new(
            session_id,
            &session_proto.agent_name,
            vlinder_core::domain::BranchId::from(session_proto.default_branch),
        );
        match self.store.create_session(&vlinder_core::domain::Session {
            name: session_proto.name,
            ..session
        }) {
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
                payload: msg.payload,
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
            .map_err(Status::internal)?;

        let node = result.map(|msg| CompleteNodeProto {
            message_id: msg.id.to_string(),
            state: msg.state,
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload,
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
            .map_err(Status::internal)?;

        let node = result.map(|msg| RequestNodeProto {
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
            .map_err(Status::internal)?;

        let node = result.map(|msg| ResponseNodeProto {
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

        let msg = vlinder_core::domain::InvokeMessage {
            id: vlinder_core::domain::MessageId::from(n.message_id),
            dag_id: vlinder_core::domain::DagNodeId::from(n.dag_hash),
            state: n.state,
            diagnostics,
            dag_parent: vlinder_core::domain::DagNodeId::from(n.dag_parent),
            payload: n.payload,
        };

        match self
            .store
            .insert_invoke_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
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

        let diagnostics: vlinder_core::domain::RuntimeDiagnostics =
            serde_json::from_slice(&req.diagnostics)
                .unwrap_or_else(|_| vlinder_core::domain::RuntimeDiagnostics::placeholder(0));

        let msg = vlinder_core::domain::CompleteMessage {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            dag_id: dag_id.clone(),
            state: req.state,
            diagnostics,
            payload: req.payload,
        };

        match self.store.insert_complete_node(
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
        ) {
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
            state: req.state,
            diagnostics,
            payload: req.payload,
            checkpoint: req.checkpoint,
        };

        match self.store.insert_request_node(
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
        ) {
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
            correlation_id: vlinder_core::domain::MessageId::from(req.correlation_id),
            state: req.state,
            diagnostics,
            payload: req.payload,
            status_code: u16::try_from(req.status_code).unwrap_or(200),
            checkpoint: req.checkpoint,
        };

        match self.store.insert_response_node(
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
        ) {
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

        let msg = vlinder_core::domain::ForkMessageV2 {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            branch_name: req.branch_name,
            fork_point: vlinder_core::domain::DagNodeId::from(req.fork_point),
        };

        match self
            .store
            .insert_fork_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
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

        let msg = vlinder_core::domain::PromoteMessageV2 {
            id: vlinder_core::domain::MessageId::from(req.message_id),
            branch_id: vlinder_core::domain::BranchId::from(req.branch_id),
        };

        match self
            .store
            .insert_promote_node(&dag_id, &parent_id, created_at, &snapshot, &key, &msg)
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
            .map_err(Status::internal)?
            .map(std::convert::Into::into);
        Ok(Response::new(LatestNodeOnBranchResponse { node }))
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
        match self.store.update_session_default_branch(
            &session_id,
            vlinder_core::domain::BranchId::from(req.branch_id),
        ) {
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
}

fn session_to_proto(s: vlinder_core::domain::Session) -> proto::SessionProto {
    proto::SessionProto {
        id: s.id.as_str().to_string(),
        name: s.name,
        agent_name: s.agent,
        default_branch: s.default_branch.as_i64(),
        created_at: s.created_at.to_rfc3339(),
    }
}
