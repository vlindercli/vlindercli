use std::collections::HashMap;

use vlinder_core::domain::{
    Agent, Job, JobId, JobStatus, Model, ModelType, Protocol, Provider, Requirements, ResourceId,
    RuntimeType, ServiceConfig, ServiceType, SubmissionId,
};
use vlinder_sql_registry::registry_service::proto;

// ============================================================================
// ResourceId round-trip
// ============================================================================

#[test]
fn resource_id_round_trip() {
    let original = ResourceId::new("http://127.0.0.1:9000/agents/echo");
    let proto_id: proto::ResourceId = original.clone().into();
    let back: ResourceId = proto_id.into();
    assert_eq!(original, back);
}

#[test]
fn resource_id_ref_to_proto() {
    let id = ResourceId::new("memory://test");
    let proto_id: proto::ResourceId = (&id).into();
    assert_eq!(proto_id.uri, "memory://test");
}

// ============================================================================
// JobId round-trip
// ============================================================================

#[test]
fn job_id_round_trip() {
    let original = JobId::from_string("http://localhost:9000/jobs/abc-123".to_string());
    let proto_id: proto::JobId = original.into();
    assert_eq!(proto_id.id, "http://localhost:9000/jobs/abc-123");
    let back: JobId = proto_id.into();
    assert_eq!(back.as_str(), "http://localhost:9000/jobs/abc-123");
}

// ============================================================================
// SubmissionId round-trip
// ============================================================================

#[test]
fn submission_id_round_trip() {
    let original = SubmissionId::from("sha-abc123".to_string());
    let proto_id: proto::SubmissionId = original.clone().into();
    assert_eq!(proto_id.id, "sha-abc123");
    let back: SubmissionId = proto_id.into();
    assert_eq!(back.as_str(), "sha-abc123");
}

// ============================================================================
// Agent round-trip
// ============================================================================

#[test]
fn agent_domain_to_proto_preserves_fields() {
    let mut services = HashMap::new();
    services.insert(
        ServiceType::Infer,
        ServiceConfig {
            provider: Provider::Ollama,
            protocol: Protocol::OpenAi,
            models: vec!["phi3:latest".to_string()],
        },
    );

    let agent = Agent {
        name: "echo".to_string(),
        description: "Echoes input".to_string(),
        source: None,
        id: ResourceId::new("http://localhost:9000/agents/echo"),
        runtime: RuntimeType::Container,
        executable: "localhost/echo:latest".to_string(),
        image_digest: None,
        public_key: None,
        requirements: Requirements {
            models: HashMap::from([("phi3".to_string(), "phi3".to_string())]),
            services,
            mounts: HashMap::new(),
            mcp: Vec::new(),
        },
        prompts: None,

        object_storage: Some(ResourceId::new("sqlite:///data/obj.db")),
        vector_storage: None,
    };

    let proto_agent: proto::Agent = agent.into();
    assert_eq!(proto_agent.name, "echo");
    assert_eq!(proto_agent.description, "Echoes input");
    assert_eq!(proto_agent.runtime, "container");
    assert_eq!(proto_agent.executable, "localhost/echo:latest");
    assert_eq!(proto_agent.services.len(), 1);
    assert_eq!(
        proto_agent.services[0].service_type,
        proto::ServiceType::Infer as i32
    );
    assert_eq!(
        proto_agent.services[0].provider,
        proto::Provider::Ollama as i32
    );
    assert_eq!(
        proto_agent.services[0].protocol,
        proto::Protocol::Openai as i32
    );
    assert!(proto_agent.object_storage.is_some());
    assert!(proto_agent.vector_storage.is_none());
}

#[test]
fn agent_proto_to_domain_round_trip() {
    let proto_agent = proto::Agent {
        name: "echo".to_string(),
        description: "Echoes input".to_string(),
        id: Some(proto::ResourceId {
            uri: "http://localhost:9000/agents/echo".to_string(),
        }),
        runtime: "container".to_string(),
        executable: "localhost/echo:latest".to_string(),
        services: vec![proto::ServiceEntry {
            service_type: proto::ServiceType::Infer as i32,
            provider: proto::Provider::Openrouter as i32,
            protocol: proto::Protocol::Anthropic as i32,
            models: vec!["anthropic/claude-3.5-sonnet".to_string()],
        }],

        models: vec![],
        mounts: vec![],
        object_storage: None,
        vector_storage: None,
    };

    let agent: Agent = proto_agent.try_into().unwrap();
    assert_eq!(agent.name, "echo");
    assert_eq!(agent.runtime, RuntimeType::Container);
    let infer = &agent.requirements.services[&ServiceType::Infer];
    assert_eq!(infer.provider, Provider::OpenRouter);
    assert_eq!(infer.protocol, Protocol::Anthropic);
    assert_eq!(infer.models, vec!["anthropic/claude-3.5-sonnet"]);
}

#[test]
fn agent_models_survive_proto_round_trip() {
    let mut services = HashMap::new();
    services.insert(
        ServiceType::Infer,
        ServiceConfig {
            provider: Provider::Ollama,
            protocol: Protocol::OpenAi,
            models: vec!["phi3:latest".to_string()],
        },
    );

    let agent = Agent {
        name: "thinker".to_string(),
        description: "Thinks".to_string(),
        source: None,
        id: ResourceId::new("http://localhost:9000/agents/thinker"),
        runtime: RuntimeType::Container,
        executable: "localhost/thinker:latest".to_string(),
        image_digest: None,
        public_key: None,
        requirements: Requirements {
            models: HashMap::from([
                ("inference_model".to_string(), "phi3:latest".to_string()),
                ("embedding_model".to_string(), "nomic-embed".to_string()),
            ]),
            services,
            mounts: HashMap::new(),
            mcp: Vec::new(),
        },
        prompts: None,

        object_storage: None,
        vector_storage: None,
    };

    let proto_agent: proto::Agent = agent.into();
    let back: Agent = proto_agent.try_into().unwrap();

    assert_eq!(
        back.requirements.models.len(),
        2,
        "models lost in proto round-trip"
    );
    assert_eq!(
        back.requirements
            .models
            .get("inference_model")
            .map(std::string::String::as_str),
        Some("phi3:latest"),
    );
    assert_eq!(
        back.requirements
            .models
            .get("embedding_model")
            .map(std::string::String::as_str),
        Some("nomic-embed"),
    );
}

#[test]
fn agent_proto_missing_id_fails() {
    let proto_agent = proto::Agent {
        name: "bad".to_string(),
        description: "Missing ID".to_string(),
        id: None,
        runtime: "container".to_string(),
        executable: "x".to_string(),
        services: vec![],

        models: vec![],
        mounts: vec![],
        object_storage: None,
        vector_storage: None,
    };

    let result: Result<Agent, String> = proto_agent.try_into();
    assert!(result.is_err());
}

// ============================================================================
// Model round-trip
// ============================================================================

#[test]
fn model_domain_to_proto() {
    let model = Model {
        id: ResourceId::new("http://localhost:9000/models/phi3"),
        name: "phi3".to_string(),
        model_type: ModelType::Inference,
        provider: Provider::Ollama,
        model_path: ResourceId::new("ollama://localhost:11434/phi3:latest"),
        digest: "sha256:abc".to_string(),
    };

    let proto_model: proto::Model = model.into();
    assert_eq!(proto_model.name, "phi3");
    assert_eq!(proto_model.digest, "sha256:abc");
    assert_eq!(proto_model.provider, proto::Provider::Ollama as i32);
    assert_eq!(proto_model.model_type, proto::ModelType::Inference as i32);
}

#[test]
fn model_proto_to_domain_round_trip() {
    let proto_model = proto::Model {
        id: Some(proto::ResourceId {
            uri: "http://localhost/models/phi3".to_string(),
        }),
        name: "phi3".to_string(),
        model_type: proto::ModelType::Embedding as i32,
        provider: proto::Provider::Ollama as i32,
        model_path: Some(proto::ResourceId {
            uri: "ollama://localhost/phi3".to_string(),
        }),
        digest: "sha256:abc".to_string(),
    };

    let model: Model = proto_model.try_into().unwrap();
    assert_eq!(model.name, "phi3");
    assert_eq!(model.model_type, ModelType::Embedding);
    assert_eq!(model.provider, Provider::Ollama);
}

// ============================================================================
// Provider round-trip
// ============================================================================

#[test]
fn provider_round_trips() {
    let cases = [
        (Provider::Ollama, proto::Provider::Ollama),
        (Provider::OpenRouter, proto::Provider::Openrouter),
    ];
    for (domain, proto_val) in cases {
        let converted: proto::Provider = domain.into();
        assert_eq!(converted, proto_val);
        let back: Provider = converted.try_into().unwrap();
        assert_eq!(back, domain);
    }
}

// ============================================================================
// Job round-trip
// ============================================================================

#[test]
fn job_completed_round_trip() {
    let job = Job {
        id: JobId::from_string("http://localhost/jobs/1".to_string()),
        submission_id: SubmissionId::from("sha-abc".to_string()),
        agent_id: ResourceId::new("http://localhost/agents/echo"),
        input: "hello".to_string(),
        status: JobStatus::Completed("world".to_string()),
    };

    let proto_job: proto::Job = job.into();
    assert_eq!(proto_job.status, proto::JobStatus::Completed as i32);
    assert_eq!(proto_job.output, Some("world".to_string()));
    assert_eq!(proto_job.input, "hello");

    let back: Job = proto_job.try_into().unwrap();
    assert!(matches!(back.status, JobStatus::Completed(s) if s == "world"));
}

#[test]
fn job_failed_preserves_error_message() {
    let job = Job {
        id: JobId::from_string("http://localhost/jobs/2".to_string()),
        submission_id: SubmissionId::from("sha-def".to_string()),
        agent_id: ResourceId::new("http://localhost/agents/echo"),
        input: "test".to_string(),
        status: JobStatus::Failed("timeout".to_string()),
    };

    let proto_job: proto::Job = job.into();
    assert_eq!(proto_job.status, proto::JobStatus::Failed as i32);
    assert_eq!(proto_job.output, Some("timeout".to_string()));
}

#[test]
fn job_pending_has_no_output() {
    let job = Job {
        id: JobId::from_string("http://localhost/jobs/3".to_string()),
        submission_id: SubmissionId::from("sha-ghi".to_string()),
        agent_id: ResourceId::new("http://localhost/agents/echo"),
        input: "pending".to_string(),
        status: JobStatus::Pending,
    };

    let proto_job: proto::Job = job.into();
    assert_eq!(proto_job.status, proto::JobStatus::Pending as i32);
    assert_eq!(proto_job.output, None);
}
