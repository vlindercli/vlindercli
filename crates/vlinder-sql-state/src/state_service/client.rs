//! gRPC client implementing the `DagStore` trait.

use tonic::transport::Channel;

use super::proto::{self, state_service_client::StateServiceClient};
use vlinder_core::domain::{Branch, BranchId, DagNode, DagNodeId, DagStore};

/// `DagStore` implementation that makes gRPC calls to a remote State Service.
pub struct GrpcStateClient {
    client: StateServiceClient<Channel>,
    runtime: tokio::runtime::Runtime,
}

impl GrpcStateClient {
    /// Connect to a state service server.
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let client =
            runtime.block_on(async { StateServiceClient::connect(addr.to_string()).await })?;

        Ok(Self { client, runtime })
    }
}

/// Ping a state service at the given address, returning its protocol version.
///
/// Creates a temporary connection and sends a Ping. Returns the server's
/// version on success, None on any connection or transport error.
pub fn ping_state_service(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(runtime) = tokio::runtime::Runtime::new() else {
        return None;
    };

    runtime.block_on(async {
        let Ok(mut client) = StateServiceClient::connect(addr.to_string()).await else {
            return None;
        };
        client.ping(proto::PingRequest {}).await.ok().map(|r| {
            let v = r.into_inner();
            (v.major, v.minor, v.patch)
        })
    })
}

impl DagStore for GrpcStateClient {
    fn insert_node(&self, node: &DagNode) -> Result<(), String> {
        let proto_node: proto::DagNode = node.into();
        let request = proto::InsertNodeRequest {
            node: Some(proto_node),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn get_node(&self, hash: &DagNodeId) -> Result<Option<DagNode>, String> {
        let request = proto::GetNodeRequest {
            hash: hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_node(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(proto_node) => {
                let node = proto_node.try_into()?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    fn get_session_nodes(
        &self,
        session_id: &vlinder_core::domain::SessionId,
    ) -> Result<Vec<DagNode>, String> {
        let request = proto::GetSessionNodesRequest {
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_session_nodes(request).await })
            .map_err(|e| e.to_string())?;

        response
            .into_inner()
            .nodes
            .into_iter()
            .map(std::convert::TryInto::try_into)
            .collect()
    }

    fn get_children(&self, parent_hash: &DagNodeId) -> Result<Vec<DagNode>, String> {
        let request = proto::GetChildrenRequest {
            parent_hash: parent_hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_children(request).await })
            .map_err(|e| e.to_string())?;

        response
            .into_inner()
            .nodes
            .into_iter()
            .map(std::convert::TryInto::try_into)
            .collect()
    }

    // -------------------------------------------------------------------------
    // Branch methods
    // -------------------------------------------------------------------------

    fn create_branch(
        &self,
        name: &str,
        session_id: &vlinder_core::domain::SessionId,
        fork_point: Option<&DagNodeId>,
    ) -> Result<BranchId, String> {
        let request = proto::CreateBranchRequest {
            name: name.to_string(),
            session_id: session_id.as_str().to_string(),
            fork_point: fork_point.map(std::string::ToString::to_string),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.create_branch(request).await })
            .map_err(|e| e.to_string())?;
        Ok(BranchId::from(response.into_inner().id))
    }

    fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String> {
        let request = proto::GetBranchByNameRequest {
            name: name.to_string(),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_branch_by_name(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().branch {
            Some(b) => Ok(Some(b.try_into()?)),
            None => Ok(None),
        }
    }

    fn get_branch(&self, id: BranchId) -> Result<Option<Branch>, String> {
        let request = proto::GetBranchByIdRequest { id: id.as_i64() };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_branch(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().branch {
            Some(b) => Ok(Some(b.try_into()?)),
            None => Ok(None),
        }
    }

    fn list_sessions(&self) -> Result<Vec<vlinder_core::domain::SessionSummary>, String> {
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.list_sessions(proto::ListSessionsRequest {}).await })
            .map_err(|e| e.to_string())?;

        response
            .into_inner()
            .sessions
            .into_iter()
            .map(std::convert::TryInto::try_into)
            .collect()
    }

    fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String> {
        let request = proto::GetNodesBySubmissionRequest {
            submission_id: submission_id.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_nodes_by_submission(request).await })
            .map_err(|e| e.to_string())?;

        response
            .into_inner()
            .nodes
            .into_iter()
            .map(std::convert::TryInto::try_into)
            .collect()
    }

    fn insert_invoke_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::DataRoutingKey,
        msg: &vlinder_core::domain::InvokeMessage,
    ) -> Result<(), String> {
        let vlinder_core::domain::DataMessageKind::Invoke {
            harness,
            runtime,
            agent,
        } = &key.kind
        else {
            return Err("insert_invoke_node: expected Invoke key".into());
        };

        let node = proto::InvokeNodeProto {
            session_id: key.session.to_string(),
            branch: key.branch.as_i64(),
            submission_id: key.submission.to_string(),
            harness: harness.as_str().to_string(),
            runtime: runtime.as_str().to_string(),
            agent: agent.to_string(),
            message_id: msg.id.to_string(),
            state: msg.state.clone(),
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload.clone(),
            dag_parent: msg.dag_parent.to_string(),
            dag_hash: dag_id.to_string(),
        };

        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let request = proto::InsertInvokeNodeRequest {
            node: Some(node),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_invoke_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn insert_complete_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &vlinder_core::domain::SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: vlinder_core::domain::BranchId,
        agent: &vlinder_core::domain::AgentName,
        harness: vlinder_core::domain::HarnessType,
        msg: &vlinder_core::domain::CompleteMessage,
    ) -> Result<(), String> {
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let request = proto::InsertCompleteNodeRequest {
            dag_hash: dag_id.to_string(),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
            session_id: session.as_str().to_string(),
            submission_id: submission.as_str().to_string(),
            branch_id: branch.as_i64(),
            agent: agent.to_string(),
            harness: harness.as_str().to_string(),
            message_id: msg.id.to_string(),
            state: msg.state.clone(),
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload.clone(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_complete_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn get_complete_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::CompleteMessage>, String> {
        let request = proto::GetCompleteNodeRequest {
            dag_hash: dag_hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_complete_node(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => {
                let diagnostics: vlinder_core::domain::RuntimeDiagnostics =
                    serde_json::from_slice(&n.diagnostics).unwrap_or_else(|_| {
                        vlinder_core::domain::RuntimeDiagnostics::placeholder(0)
                    });
                Ok(Some(vlinder_core::domain::CompleteMessage {
                    id: vlinder_core::domain::MessageId::from(n.message_id),
                    dag_id: vlinder_core::domain::DagNodeId::from(n.dag_hash),
                    state: n.state,
                    diagnostics,
                    payload: n.payload,
                }))
            }
            None => Ok(None),
        }
    }

    fn get_request_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::RequestMessage>, String> {
        let request = proto::GetRequestNodeRequest {
            dag_hash: dag_hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_request_node(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => {
                let diagnostics: vlinder_core::domain::RequestDiagnostics =
                    serde_json::from_slice(&n.diagnostics).unwrap_or_else(|_| {
                        vlinder_core::domain::RequestDiagnostics {
                            sequence: 0,
                            endpoint: String::new(),
                            request_bytes: 0,
                            received_at_ms: 0,
                        }
                    });
                Ok(Some(vlinder_core::domain::RequestMessage {
                    id: vlinder_core::domain::MessageId::from(n.message_id),
                    dag_id: vlinder_core::domain::DagNodeId::from(n.dag_hash),
                    state: n.state,
                    diagnostics,
                    payload: n.payload,
                    checkpoint: n.checkpoint,
                }))
            }
            None => Ok(None),
        }
    }

    fn get_response_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::ResponseMessage>, String> {
        let request = proto::GetResponseNodeRequest {
            dag_hash: dag_hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_response_node(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => {
                let diagnostics: vlinder_core::domain::ServiceDiagnostics =
                    serde_json::from_slice(&n.diagnostics).unwrap_or_else(|_| {
                        vlinder_core::domain::ServiceDiagnostics::storage(
                            vlinder_core::domain::ServiceType::Kv,
                            "unknown",
                            vlinder_core::domain::Operation::Get,
                            0,
                            0,
                        )
                    });
                Ok(Some(vlinder_core::domain::ResponseMessage {
                    id: vlinder_core::domain::MessageId::from(n.message_id),
                    dag_id: vlinder_core::domain::DagNodeId::from(n.dag_hash),
                    correlation_id: vlinder_core::domain::MessageId::from(n.correlation_id),
                    state: n.state,
                    diagnostics,
                    payload: n.payload,
                    status_code: u16::try_from(n.status_code).unwrap_or(200),
                    checkpoint: n.checkpoint,
                }))
            }
            None => Ok(None),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_request_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &vlinder_core::domain::SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: vlinder_core::domain::BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: vlinder_core::domain::ServiceBackend,
        operation: vlinder_core::domain::Operation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::RequestMessage,
    ) -> Result<(), String> {
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let request = proto::InsertRequestNodeRequest {
            dag_hash: dag_id.to_string(),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
            session_id: session.as_str().to_string(),
            submission_id: submission.as_str().to_string(),
            branch_id: branch.as_i64(),
            agent: agent.to_string(),
            service: service.to_string(),
            operation: operation.as_str().to_string(),
            sequence: sequence.as_u32(),
            message_id: msg.id.to_string(),
            state: msg.state.clone(),
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload.clone(),
            checkpoint: msg.checkpoint.clone(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_request_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_response_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &vlinder_core::domain::SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: vlinder_core::domain::BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: vlinder_core::domain::ServiceBackend,
        operation: vlinder_core::domain::Operation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::ResponseMessage,
    ) -> Result<(), String> {
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let request = proto::InsertResponseNodeRequest {
            dag_hash: dag_id.to_string(),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
            session_id: session.as_str().to_string(),
            submission_id: submission.as_str().to_string(),
            branch_id: branch.as_i64(),
            agent: agent.to_string(),
            service: service.to_string(),
            operation: operation.as_str().to_string(),
            sequence: sequence.as_u32(),
            message_id: msg.id.to_string(),
            correlation_id: msg.correlation_id.to_string(),
            state: msg.state.clone(),
            diagnostics: serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            payload: msg.payload.clone(),
            status_code: u32::from(msg.status_code),
            checkpoint: msg.checkpoint.clone(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_response_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn insert_fork_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::SessionRoutingKey,
        msg: &vlinder_core::domain::ForkMessageV2,
    ) -> Result<(), String> {
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let agent_name = match &key.kind {
            vlinder_core::domain::SessionMessageKind::Fork { agent_name } => agent_name.to_string(),
            _ => return Err("insert_fork_node: expected Fork key".into()),
        };

        let request = proto::InsertForkNodeRequest {
            dag_hash: dag_id.to_string(),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
            session_id: key.session.as_str().to_string(),
            submission_id: key.submission.as_str().to_string(),
            agent_name,
            message_id: msg.id.to_string(),
            branch_name: msg.branch_name.clone(),
            fork_point: msg.fork_point.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_fork_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn insert_promote_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::SessionRoutingKey,
        msg: &vlinder_core::domain::PromoteMessageV2,
    ) -> Result<(), String> {
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot: {e}"))?;

        let agent_name = match &key.kind {
            vlinder_core::domain::SessionMessageKind::Promote { agent_name } => {
                agent_name.to_string()
            }
            _ => return Err("insert_promote_node: expected Promote key".into()),
        };

        let request = proto::InsertPromoteNodeRequest {
            dag_hash: dag_id.to_string(),
            parent_hash: parent_id.to_string(),
            created_at: created_at.to_rfc3339(),
            snapshot: snapshot_json,
            session_id: key.session.as_str().to_string(),
            submission_id: key.submission.as_str().to_string(),
            agent_name,
            message_id: msg.id.to_string(),
            branch_id: msg.branch_id.as_i64(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.insert_promote_node(request).await })
            .map_err(|e| e.to_string())?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn get_invoke_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::DataRoutingKey,
            vlinder_core::domain::InvokeMessage,
        )>,
        String,
    > {
        let request = proto::GetInvokeNodeRequest {
            dag_hash: dag_hash.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_invoke_node(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => {
                let harness: vlinder_core::domain::HarnessType =
                    n.harness.parse().map_err(|e: String| e)?;
                let runtime: vlinder_core::domain::RuntimeType =
                    n.runtime.parse().map_err(|e: String| e)?;
                let key = vlinder_core::domain::DataRoutingKey {
                    session: vlinder_core::domain::SessionId::try_from(n.session_id)?,
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
                Ok(Some((key, msg)))
            }
            None => Ok(None),
        }
    }

    fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
        let request = proto::GetNodeByPrefixRequest {
            prefix: prefix.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_node_by_prefix(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => Ok(Some(n.try_into()?)),
            None => Ok(None),
        }
    }

    fn get_branches_for_session(
        &self,
        session_id: &vlinder_core::domain::SessionId,
    ) -> Result<Vec<Branch>, String> {
        let request = proto::GetBranchesForSessionRequest {
            session_id: session_id.as_str().to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_branches_for_session(request).await })
            .map_err(|e| e.to_string())?;

        response
            .into_inner()
            .branches
            .into_iter()
            .map(std::convert::TryInto::try_into)
            .collect()
    }

    fn latest_node_on_branch(
        &self,
        branch_id: BranchId,
        message_type: Option<vlinder_core::domain::MessageType>,
    ) -> Result<Option<vlinder_core::domain::DagNode>, String> {
        let request = proto::LatestNodeOnBranchRequest {
            branch_id: branch_id.as_i64(),
            message_type: message_type.map(|mt| mt.as_str().to_string()),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.latest_node_on_branch(request).await })
            .map_err(|e| e.to_string())?;

        match response.into_inner().node {
            Some(n) => Ok(Some(n.try_into()?)),
            None => Ok(None),
        }
    }

    fn rename_branch(
        &self,
        id: vlinder_core::domain::BranchId,
        new_name: &str,
    ) -> Result<(), String> {
        let request = proto::RenameBranchRequest {
            id: id.as_i64(),
            new_name: new_name.to_string(),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.rename_branch(request).await })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn seal_branch(
        &self,
        id: vlinder_core::domain::BranchId,
        broken_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), String> {
        let request = proto::SealBranchRequest {
            id: id.as_i64(),
            broken_at: broken_at.to_rfc3339(),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.seal_branch(request).await })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn update_session_default_branch(
        &self,
        session_id: &vlinder_core::domain::SessionId,
        branch_id: vlinder_core::domain::BranchId,
    ) -> Result<(), String> {
        let request = proto::UpdateSessionDefaultBranchRequest {
            session_id: session_id.as_str().to_string(),
            branch_id: branch_id.as_i64(),
        };
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.update_session_default_branch(request).await })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn create_session(&self, session: &vlinder_core::domain::Session) -> Result<(), String> {
        let mut client = self.client.clone();
        let req = proto::CreateSessionRequest {
            session: Some(proto::SessionProto {
                id: session.id.as_str().to_string(),
                name: session.name.clone(),
                agent_name: session.agent.clone(),
                default_branch: session.default_branch.as_i64(),
                created_at: session.created_at.to_rfc3339(),
            }),
        };
        let resp = self
            .runtime
            .block_on(async { client.create_session(req).await })
            .map_err(|e| e.to_string())?
            .into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(resp.error.unwrap_or_else(|| "unknown error".to_string()))
        }
    }

    fn get_session(
        &self,
        session_id: &vlinder_core::domain::SessionId,
    ) -> Result<Option<vlinder_core::domain::Session>, String> {
        let mut client = self.client.clone();
        let req = proto::GetSessionRequest {
            session_id: session_id.as_str().to_string(),
        };
        let resp = self
            .runtime
            .block_on(async { client.get_session(req).await })
            .map_err(|e| e.to_string())?
            .into_inner();
        resp.session.map(proto_to_session).transpose()
    }

    fn get_session_by_name(
        &self,
        name: &str,
    ) -> Result<Option<vlinder_core::domain::Session>, String> {
        let mut client = self.client.clone();
        let req = proto::GetSessionByNameRequest {
            name: name.to_string(),
        };
        let resp = self
            .runtime
            .block_on(async { client.get_session_by_name(req).await })
            .map_err(|e| e.to_string())?
            .into_inner();
        resp.session.map(proto_to_session).transpose()
    }
}

fn proto_to_session(s: proto::SessionProto) -> Result<vlinder_core::domain::Session, String> {
    let id = vlinder_core::domain::SessionId::try_from(s.id)?;
    let created_at: chrono::DateTime<chrono::Utc> = s
        .created_at
        .parse()
        .map_err(|e| format!("invalid created_at: {e}"))?;
    Ok(vlinder_core::domain::Session {
        id,
        name: s.name,
        agent: s.agent_name,
        default_branch: BranchId::from(s.default_branch),
        created_at,
    })
}
