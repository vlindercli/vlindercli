//! gRPC client implementing the Registry trait.

use tonic::transport::Channel;

use super::proto::{self, registry_client::RegistryClient};
use vlinder_core::domain::{
    Agent, Fleet, Job, JobId, JobStatus, Model, ObjectStorageType, Provider, RegistrationError,
    Registry, ResourceId, RuntimeType, SubmissionId, VectorStorageType,
};

/// Registry implementation that makes gRPC calls to a remote server.
pub struct GrpcRegistryClient {
    client: RegistryClient<Channel>,
    runtime: tokio::runtime::Runtime,
    id: ResourceId,
}

impl GrpcRegistryClient {
    /// Connect to a registry server.
    pub fn connect(addr: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = tokio::runtime::Runtime::new()?;
        let client = runtime.block_on(async { RegistryClient::connect(addr.to_string()).await })?;

        Ok(Self {
            client,
            runtime,
            id: ResourceId::new(addr),
        })
    }

    /// Ping the registry server, returning its protocol version.
    pub fn ping(&self) -> Option<(u32, u32, u32)> {
        let mut client = self.client.clone();
        self.runtime.block_on(async {
            client.ping(proto::PingRequest {}).await.ok().map(|r| {
                let v = r.into_inner();
                (v.major, v.minor, v.patch)
            })
        })
    }

    /// Submit an agent deploy via the infra plane (CQRS write path).
    ///
    /// Returns a submission ID for polling status.
    pub fn deploy_agent(
        &self,
        manifest: &vlinder_core::domain::AgentManifest,
    ) -> Result<SubmissionId, String> {
        let manifest_json =
            serde_json::to_string(&manifest).map_err(|e| format!("serialize manifest: {e}"))?;
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async {
                client
                    .deploy_agent(proto::DeployAgentRequest { manifest_json })
                    .await
            })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        Ok(SubmissionId::from(resp.submission_id))
    }

    /// Submit an agent delete via the infra plane (CQRS write path).
    pub fn submit_delete_agent(&self, name: &str) -> Result<SubmissionId, String> {
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async {
                client
                    .submit_delete_agent(proto::SubmitDeleteAgentRequest {
                        name: name.to_string(),
                    })
                    .await
            })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        Ok(SubmissionId::from(resp.submission_id))
    }

    /// Query agent deployment state.
    pub fn get_agent_state(
        &self,
        name: &str,
    ) -> Result<Option<vlinder_core::domain::AgentState>, String> {
        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async {
                client
                    .get_agent_state(proto::GetAgentStateRequest {
                        name: name.to_string(),
                    })
                    .await
            })
            .map_err(|e| e.to_string())?;
        let resp = response.into_inner();
        match resp.status {
            Some(status) => {
                let agent_status: vlinder_core::domain::AgentStatus =
                    status.parse().map_err(|e: String| e)?;
                let updated_at = resp
                    .updated_at
                    .as_deref()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map_or_else(chrono::Utc::now, |dt| dt.with_timezone(&chrono::Utc));
                Ok(Some(vlinder_core::domain::AgentState {
                    agent: vlinder_core::domain::AgentName::new(name),
                    status: agent_status,
                    updated_at,
                    error: resp.error,
                }))
            }
            None => Ok(None),
        }
    }
}

/// Ping a registry server at the given address, returning its protocol version.
///
/// Creates a temporary connection and sends a Ping. Returns the server's
/// version on success, None on any connection or transport error.
pub fn ping_registry(addr: &str) -> Option<(u32, u32, u32)> {
    let Ok(runtime) = tokio::runtime::Runtime::new() else {
        return None;
    };

    runtime.block_on(async {
        let Ok(mut client) = RegistryClient::connect(addr.to_string()).await else {
            return None;
        };
        client.ping(proto::PingRequest {}).await.ok().map(|r| {
            let v = r.into_inner();
            (v.major, v.minor, v.patch)
        })
    })
}

impl Registry for GrpcRegistryClient {
    fn id(&self) -> ResourceId {
        self.id.clone()
    }

    // --- Agent operations ---

    fn register_agent(&self, agent: Agent) -> Result<(), RegistrationError> {
        let proto_agent: proto::Agent = agent.into();
        let request = proto::RegisterAgentRequest {
            agent: Some(proto_agent),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.register_agent(request).await })
            .map_err(|e| RegistrationError::Remote(e.to_string()))?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(RegistrationError::Remote(
                resp.error.unwrap_or_else(|| "unknown error".to_string()),
            ))
        }
    }

    fn get_agent(&self, id: &ResourceId) -> Option<Agent> {
        let request = proto::GetAgentRequest {
            id: Some(id.into()),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_agent(request).await })
            .ok()?;

        response.into_inner().agent.and_then(|a| a.try_into().ok())
    }

    fn get_agents(&self) -> Vec<Agent> {
        let request = proto::ListAgentsRequest {};

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.list_agents(request).await });

        match response {
            Ok(resp) => resp
                .into_inner()
                .agents
                .into_iter()
                .filter_map(|a| a.try_into().ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    fn get_agent_by_name(&self, name: &str) -> Option<Agent> {
        let request = proto::GetAgentByNameRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_agent_by_name(request).await })
            .ok()?;

        response.into_inner().agent.and_then(|a| a.try_into().ok())
    }

    fn agent_id(&self, name: &str) -> Option<ResourceId> {
        // Query the server — only it knows its registry_id.
        let request = proto::GetAgentByNameRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_agent_by_name(request).await })
            .ok()?;

        let agent = response.into_inner().agent?;
        Some(agent.id?.into())
    }

    fn select_runtime(&self, agent: &Agent) -> Option<RuntimeType> {
        // Runtime type is declared on the agent — no scheme parsing needed
        Some(agent.runtime)
    }

    // --- Model operations ---

    fn register_model(&self, model: Model) -> Result<(), RegistrationError> {
        let proto_model: proto::Model = model.into();
        let request = proto::RegisterModelRequest {
            model: Some(proto_model),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.register_model(request).await })
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(RegistrationError::Persistence(
                resp.error
                    .unwrap_or_else(|| "unknown server error".to_string()),
            ))
        }
    }

    fn get_model(&self, name: &str) -> Option<Model> {
        let request = proto::GetModelRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_model(request).await })
            .ok()?;

        response.into_inner().model.and_then(|m| m.try_into().ok())
    }

    fn get_models(&self) -> Vec<Model> {
        let request = proto::ListModelsRequest {};

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.list_models(request).await });

        match response {
            Ok(resp) => resp
                .into_inner()
                .models
                .into_iter()
                .filter_map(|m| m.try_into().ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    fn get_model_by_path(&self, path: &ResourceId) -> Option<Model> {
        // Get all models and find by path
        self.get_models()
            .into_iter()
            .find(|m| &m.model_path == path)
    }

    fn model_id(&self, name: &str) -> ResourceId {
        ResourceId::new(format!("model://{name}"))
    }

    fn delete_model(&self, name: &str) -> Result<bool, RegistrationError> {
        let request = proto::DeleteModelRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.delete_model(request).await })
            .map_err(|e| RegistrationError::Persistence(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            return Err(RegistrationError::Persistence(error));
        }
        Ok(resp.deleted)
    }

    fn delete_agent(&self, name: &str) -> Result<bool, RegistrationError> {
        let request = proto::DeleteAgentRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.delete_agent(request).await })
            .map_err(|e| RegistrationError::Remote(e.to_string()))?;

        let resp = response.into_inner();
        if let Some(error) = resp.error {
            return Err(RegistrationError::Remote(error));
        }
        Ok(resp.deleted)
    }

    // --- Job operations ---

    fn create_job(
        &self,
        submission_id: SubmissionId,
        agent_id: ResourceId,
        input: String,
    ) -> JobId {
        let request = proto::CreateJobRequest {
            submission_id: Some(submission_id.into()),
            agent_id: Some(agent_id.into()),
            input,
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.create_job(request).await });

        match response {
            Ok(resp) => resp.into_inner().job_id.map_or_else(
                || JobId::from_string("error".to_string()),
                std::convert::Into::into,
            ),
            Err(_) => JobId::from_string("error".to_string()),
        }
    }

    fn get_job(&self, id: &JobId) -> Option<Job> {
        let request = proto::GetJobRequest {
            id: Some(id.into()),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_job(request).await })
            .ok()?;

        response.into_inner().job.and_then(|j| j.try_into().ok())
    }

    fn update_job_status(&self, id: &JobId, status: JobStatus) {
        // Extract output from Completed/Failed status
        let output = match &status {
            JobStatus::Completed(result) => Some(result.clone()),
            JobStatus::Failed(error) => Some(error.clone()),
            _ => None,
        };

        let request = proto::UpdateJobStatusRequest {
            id: Some(id.into()),
            status: proto::JobStatus::from(status).into(),
            output,
        };

        let mut client = self.client.clone();
        let _ = self
            .runtime
            .block_on(async { client.update_job_status(request).await });
    }

    fn pending_jobs(&self) -> Vec<Job> {
        let request = proto::ListPendingJobsRequest {};

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.list_pending_jobs(request).await });

        match response {
            Ok(resp) => resp
                .into_inner()
                .jobs
                .into_iter()
                .filter_map(|j| j.try_into().ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    // --- Fleet operations ---

    fn register_fleet(&self, fleet: Fleet) -> Result<(), RegistrationError> {
        let proto_fleet: proto::Fleet = fleet.into();
        let request = proto::RegisterFleetRequest {
            fleet: Some(proto_fleet),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.register_fleet(request).await })
            .map_err(|e| RegistrationError::Remote(e.to_string()))?;

        let resp = response.into_inner();
        if resp.success {
            Ok(())
        } else {
            Err(RegistrationError::Remote(
                resp.error.unwrap_or_else(|| "unknown error".to_string()),
            ))
        }
    }

    fn get_fleet(&self, name: &str) -> Option<Fleet> {
        let request = proto::GetFleetRequest {
            name: name.to_string(),
        };

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.get_fleet(request).await })
            .ok()?;

        response.into_inner().fleet.and_then(|f| f.try_into().ok())
    }

    fn get_fleets(&self) -> Vec<Fleet> {
        let request = proto::ListFleetsRequest {};

        let mut client = self.client.clone();
        let response = self
            .runtime
            .block_on(async { client.list_fleets(request).await });

        match response {
            Ok(resp) => resp
                .into_inner()
                .fleets
                .into_iter()
                .filter_map(|f| f.try_into().ok())
                .collect(),
            Err(_) => vec![],
        }
    }

    // --- Capability registration (no-op for client, server manages these) ---

    fn register_runtime(&self, _runtime_type: RuntimeType) {
        // No-op: capabilities are managed server-side
    }

    fn register_object_storage(&self, _storage_type: ObjectStorageType) {
        // No-op
    }

    fn register_vector_storage(&self, _storage_type: VectorStorageType) {
        // No-op
    }

    fn register_inference_engine(&self, _engine_type: Provider) {
        // No-op
    }

    fn register_embedding_engine(&self, _engine_type: Provider) {
        // No-op
    }

    // --- Capability queries (would need server RPCs, stub for now) ---

    fn has_object_storage(&self, _storage_type: ObjectStorageType) -> bool {
        true // Assume available; real impl needs server RPC
    }

    fn has_vector_storage(&self, _storage_type: VectorStorageType) -> bool {
        true
    }

    fn has_inference_engine(&self, _engine_type: Provider) -> bool {
        true
    }

    fn has_embedding_engine(&self, _engine_type: Provider) -> bool {
        true
    }
}
