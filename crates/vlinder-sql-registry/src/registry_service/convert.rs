//! Conversions between domain types and protobuf types.

use std::collections::HashMap;
use std::str::FromStr;

use super::proto;
use vlinder_core::domain::{
    Agent, Fleet, Job, JobId, JobStatus, Model, ModelType, MountConfig, Protocol, Provider,
    Requirements, ResourceId, RuntimeType, ServiceConfig, ServiceType, SubmissionId,
};

// =============================================================================
// ResourceId
// =============================================================================

impl From<ResourceId> for proto::ResourceId {
    fn from(id: ResourceId) -> Self {
        Self {
            uri: id.to_string(),
        }
    }
}

impl From<proto::ResourceId> for ResourceId {
    fn from(id: proto::ResourceId) -> Self {
        ResourceId::new(&id.uri)
    }
}

impl From<&ResourceId> for proto::ResourceId {
    fn from(id: &ResourceId) -> Self {
        Self {
            uri: id.to_string(),
        }
    }
}

// =============================================================================
// JobId
// =============================================================================

impl From<JobId> for proto::JobId {
    fn from(id: JobId) -> Self {
        Self {
            id: id.as_str().to_string(),
        }
    }
}

impl From<proto::JobId> for JobId {
    fn from(id: proto::JobId) -> Self {
        JobId::from_string(id.id)
    }
}

impl From<&JobId> for proto::JobId {
    fn from(id: &JobId) -> Self {
        Self {
            id: id.as_str().to_string(),
        }
    }
}

// =============================================================================
// SubmissionId (ADR 044)
// =============================================================================

impl From<SubmissionId> for proto::SubmissionId {
    fn from(id: SubmissionId) -> Self {
        Self {
            id: id.as_str().to_string(),
        }
    }
}

impl From<proto::SubmissionId> for SubmissionId {
    fn from(id: proto::SubmissionId) -> Self {
        SubmissionId::from(id.id)
    }
}

impl From<&SubmissionId> for proto::SubmissionId {
    fn from(id: &SubmissionId) -> Self {
        Self {
            id: id.as_str().to_string(),
        }
    }
}

// =============================================================================
// Agent
// =============================================================================

impl From<Agent> for proto::Agent {
    fn from(agent: Agent) -> Self {
        Self {
            name: agent.name,
            description: agent.description,
            id: Some(agent.id.into()),
            services: agent
                .requirements
                .services
                .into_iter()
                .map(|(st, cfg)| proto::ServiceEntry {
                    service_type: proto::ServiceType::from(st).into(),
                    provider: proto::Provider::from(cfg.provider).into(),
                    protocol: proto::Protocol::from(cfg.protocol).into(),
                    models: cfg.models,
                })
                .collect(),
            object_storage: agent.object_storage.map(|r| proto::ObjectStorageConfig {
                resource_id: Some(r.into()),
            }),
            vector_storage: agent.vector_storage.map(|r| proto::VectorStorageConfig {
                resource_id: Some(r.into()),
                dimensions: 0,
            }),
            runtime: agent.runtime.as_str().to_string(),
            executable: agent.executable,
            models: agent
                .requirements
                .models
                .into_iter()
                .map(|(alias, name)| proto::ModelAlias { alias, uri: name })
                .collect(),
            mounts: agent
                .requirements
                .mounts
                .into_iter()
                .map(|(name, cfg)| proto::MountEntry {
                    name,
                    s3: cfg.s3,
                    path: cfg.path,
                    endpoint: cfg.endpoint,
                    secret: cfg.secret,
                })
                .collect(),
        }
    }
}

impl TryFrom<proto::Agent> for Agent {
    type Error = String;

    fn try_from(agent: proto::Agent) -> Result<Self, Self::Error> {
        let mut services = HashMap::new();
        for entry in agent.services {
            let st: ServiceType = proto::ServiceType::try_from(entry.service_type)
                .map_err(|_| "invalid service type")?
                .try_into()?;
            let provider: Provider = proto::Provider::try_from(entry.provider)
                .map_err(|_| "invalid provider")?
                .try_into()?;
            let protocol: Protocol = proto::Protocol::try_from(entry.protocol)
                .map_err(|_| "invalid protocol")?
                .try_into()?;
            services.insert(
                st,
                ServiceConfig {
                    provider,
                    protocol,
                    models: entry.models,
                },
            );
        }

        let runtime = RuntimeType::from_str(&agent.runtime)
            .map_err(|_| format!("unknown runtime: {}", agent.runtime))?;

        Ok(Self {
            name: agent.name,
            description: agent.description,
            id: agent.id.ok_or("missing agent id")?.into(),
            runtime,
            executable: agent.executable,
            requirements: Requirements {
                models: agent.models.into_iter().map(|m| (m.alias, m.uri)).collect(),
                services,
                mounts: agent
                    .mounts
                    .into_iter()
                    .map(|m| {
                        (
                            m.name,
                            MountConfig {
                                s3: m.s3,
                                path: m.path,
                                endpoint: m.endpoint,
                                secret: m.secret,
                            },
                        )
                    })
                    .collect(),
            },
            object_storage: agent
                .object_storage
                .and_then(|cfg| cfg.resource_id)
                .map(std::convert::Into::into),
            vector_storage: agent
                .vector_storage
                .and_then(|cfg| cfg.resource_id)
                .map(std::convert::Into::into),
            source: None,
            prompts: None,
            image_digest: None,
            public_key: None,
        })
    }
}

// =============================================================================
// Fleet
// =============================================================================

impl From<Fleet> for proto::Fleet {
    fn from(fleet: Fleet) -> Self {
        Self {
            id: Some(fleet.id.into()),
            name: fleet.name,
            entry: Some(fleet.entry.into()),
            agents: fleet
                .agents
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        }
    }
}

impl TryFrom<proto::Fleet> for Fleet {
    type Error = String;

    fn try_from(fleet: proto::Fleet) -> Result<Self, Self::Error> {
        Ok(Self {
            id: fleet.id.ok_or("missing fleet id")?.into(),
            name: fleet.name,
            entry: fleet.entry.ok_or("missing fleet entry")?.into(),
            agents: fleet
                .agents
                .into_iter()
                .map(std::convert::Into::into)
                .collect(),
        })
    }
}

// =============================================================================
// Model
// =============================================================================

impl From<Model> for proto::Model {
    fn from(model: Model) -> Self {
        Self {
            id: Some(model.id.into()),
            name: model.name,
            model_type: proto::ModelType::from(model.model_type).into(),
            provider: proto::Provider::from(model.provider).into(),
            model_path: Some(model.model_path.into()),
            digest: model.digest,
        }
    }
}

impl TryFrom<proto::Model> for Model {
    type Error = String;

    fn try_from(model: proto::Model) -> Result<Self, Self::Error> {
        Ok(Self {
            id: model.id.ok_or("missing model id")?.into(),
            name: model.name,
            model_type: proto::ModelType::try_from(model.model_type)
                .map_err(|_| "invalid model type")?
                .into(),
            provider: proto::Provider::try_from(model.provider)
                .map_err(|_| "invalid provider")?
                .try_into()?,
            model_path: model.model_path.ok_or("missing model path")?.into(),
            digest: model.digest,
        })
    }
}

impl From<ModelType> for proto::ModelType {
    fn from(t: ModelType) -> Self {
        match t {
            ModelType::Inference => proto::ModelType::Inference,
            ModelType::Embedding => proto::ModelType::Embedding,
        }
    }
}

impl From<proto::ModelType> for ModelType {
    fn from(t: proto::ModelType) -> Self {
        match t {
            proto::ModelType::Inference | proto::ModelType::Unspecified => ModelType::Inference,
            proto::ModelType::Embedding => ModelType::Embedding,
        }
    }
}

// =============================================================================
// Job
// =============================================================================

impl From<Job> for proto::Job {
    fn from(job: Job) -> Self {
        let (status, output) = match job.status {
            JobStatus::Pending => (proto::JobStatus::Pending, None),
            JobStatus::Running => (proto::JobStatus::Running, None),
            JobStatus::Completed(s) => (proto::JobStatus::Completed, Some(s)),
            JobStatus::Failed(s) => (proto::JobStatus::Failed, Some(s)),
        };

        Self {
            id: Some(job.id.into()),
            submission_id: Some(job.submission_id.into()),
            agent_id: Some(job.agent_id.into()),
            input: job.input,
            status: status.into(),
            output,
        }
    }
}

impl TryFrom<proto::Job> for Job {
    type Error = String;

    fn try_from(job: proto::Job) -> Result<Self, Self::Error> {
        let proto_status =
            proto::JobStatus::try_from(job.status).map_err(|_| "invalid job status")?;

        let status = match proto_status {
            proto::JobStatus::Pending | proto::JobStatus::Unspecified => JobStatus::Pending,
            proto::JobStatus::Running => JobStatus::Running,
            proto::JobStatus::Completed => JobStatus::Completed(job.output.unwrap_or_default()),
            proto::JobStatus::Failed => JobStatus::Failed(job.output.unwrap_or_default()),
        };

        Ok(Self {
            id: job.id.ok_or("missing job id")?.into(),
            submission_id: job.submission_id.ok_or("missing submission id")?.into(),
            agent_id: job.agent_id.ok_or("missing agent id")?.into(),
            input: job.input,
            status,
        })
    }
}

impl From<JobStatus> for proto::JobStatus {
    fn from(s: JobStatus) -> Self {
        match s {
            JobStatus::Pending => proto::JobStatus::Pending,
            JobStatus::Running => proto::JobStatus::Running,
            JobStatus::Completed(_) => proto::JobStatus::Completed,
            JobStatus::Failed(_) => proto::JobStatus::Failed,
        }
    }
}

impl From<proto::JobStatus> for JobStatus {
    fn from(s: proto::JobStatus) -> Self {
        match s {
            proto::JobStatus::Pending | proto::JobStatus::Unspecified => JobStatus::Pending,
            proto::JobStatus::Running => JobStatus::Running,
            proto::JobStatus::Completed => JobStatus::Completed(String::new()),
            proto::JobStatus::Failed => JobStatus::Failed(String::new()),
        }
    }
}

// =============================================================================
// ServiceType
// =============================================================================

impl From<ServiceType> for proto::ServiceType {
    fn from(t: ServiceType) -> Self {
        match t {
            ServiceType::Infer => proto::ServiceType::Infer,
            ServiceType::Embed => proto::ServiceType::Embed,
            ServiceType::Kv => proto::ServiceType::Kv,
            ServiceType::Vec => proto::ServiceType::Vec,
            ServiceType::Sql => proto::ServiceType::Sql,
        }
    }
}

impl TryFrom<proto::ServiceType> for ServiceType {
    type Error = String;

    fn try_from(t: proto::ServiceType) -> Result<Self, Self::Error> {
        match t {
            proto::ServiceType::Infer => Ok(ServiceType::Infer),
            proto::ServiceType::Embed => Ok(ServiceType::Embed),
            proto::ServiceType::Kv => Ok(ServiceType::Kv),
            proto::ServiceType::Vec => Ok(ServiceType::Vec),
            proto::ServiceType::Sql => Ok(ServiceType::Sql),
            proto::ServiceType::Unspecified => Err("unspecified service type".into()),
        }
    }
}

// =============================================================================
// Provider
// =============================================================================

impl From<Provider> for proto::Provider {
    fn from(p: Provider) -> Self {
        match p {
            Provider::OpenRouter => proto::Provider::Openrouter,
            Provider::Ollama => proto::Provider::Ollama,
        }
    }
}

impl TryFrom<proto::Provider> for Provider {
    type Error = String;

    fn try_from(p: proto::Provider) -> Result<Self, Self::Error> {
        match p {
            proto::Provider::Openrouter => Ok(Provider::OpenRouter),
            proto::Provider::Ollama => Ok(Provider::Ollama),
            proto::Provider::Unspecified => Err("unspecified provider".into()),
        }
    }
}

// =============================================================================
// Protocol
// =============================================================================

impl From<Protocol> for proto::Protocol {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::OpenAi => proto::Protocol::Openai,
            Protocol::Anthropic => proto::Protocol::Anthropic,
        }
    }
}

impl TryFrom<proto::Protocol> for Protocol {
    type Error = String;

    fn try_from(p: proto::Protocol) -> Result<Self, Self::Error> {
        match p {
            proto::Protocol::Openai => Ok(Protocol::OpenAi),
            proto::Protocol::Anthropic => Ok(Protocol::Anthropic),
            proto::Protocol::Unspecified => Err("unspecified protocol".into()),
        }
    }
}
