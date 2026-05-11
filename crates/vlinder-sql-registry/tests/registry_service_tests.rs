//! Integration tests for gRPC registry service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use vlinder_core::domain::{
    Agent, InMemoryDagStore, InMemoryRegistry, InMemorySecretStore, MessageQueue, Registry,
    RegistryRepository, Requirements, RuntimeType, SecretStore, SubmissionId,
};
use vlinder_core::queue::InMemoryQueue;
use vlinder_sql_registry::registry_service::{GrpcRegistryClient, RegistryServer};

fn test_secret_store() -> Arc<dyn SecretStore> {
    Arc::new(InMemorySecretStore::new())
}

fn empty_requirements() -> Requirements {
    Requirements {
        models: HashMap::new(),
        services: HashMap::new(),
        mounts: HashMap::new(),
        mcp: Vec::new(),
    }
}

/// Start a gRPC server on a random port as a background task, return the address.
async fn start_server_background(registry: Arc<dyn Registry>) -> SocketAddr {
    use tokio::sync::oneshot;
    use tonic::transport::Server;

    let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
    let repo: Arc<dyn RegistryRepository> = Arc::new(InMemoryDagStore::new());

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound_addr = listener.local_addr().unwrap();

        tx.send(bound_addr).unwrap();

        let service = RegistryServer::new(registry, queue, repo).into_service();
        Server::builder()
            .add_service(service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    rx.await.unwrap()
}

#[tokio::test]
async fn grpc_register_and_get_agent() {
    // Create in-memory registry and register Container runtime
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone()).await;

    // Create gRPC client
    let client = GrpcRegistryClient::connect_async(&format!("http://{addr}"))
        .await
        .unwrap();

    // Register an agent via gRPC
    let agent = Agent {
        name: "test-agent".to_string(),
        description: "A test agent".to_string(),
        id: Agent::placeholder_id("test-agent"),
        runtime: RuntimeType::Container,
        executable: "localhost/test-agent:latest".to_string(),
        requirements: empty_requirements(),
        object_storage: None,
        vector_storage: None,

        source: None,
        image_digest: None,
        public_key: None,
    };
    client.register_agent(agent).await.unwrap();

    // Retrieve via gRPC using registry-assigned ID
    let agent_id = registry.agent_id("test-agent").await.unwrap();
    let retrieved = client.get_agent(&agent_id).await;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test-agent");
}

#[tokio::test]
async fn grpc_list_agents() {
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone()).await;
    let client = GrpcRegistryClient::connect_async(&format!("http://{addr}"))
        .await
        .unwrap();

    // Register two agents
    for i in 0..2 {
        let name = format!("agent-{i}");
        let agent = Agent {
            name: name.clone(),
            description: "Test".to_string(),
            id: Agent::placeholder_id(&name),
            runtime: RuntimeType::Container,
            executable: format!("localhost/agent-{i}:latest"),
            requirements: empty_requirements(),
            object_storage: None,
            vector_storage: None,

            source: None,
            image_digest: None,
            public_key: None,
        };
        client.register_agent(agent).await.unwrap();
    }

    let agents = client.get_agents().await;
    assert_eq!(agents.len(), 2);
}

#[tokio::test]
async fn grpc_job_lifecycle() {
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone()).await;
    let client = GrpcRegistryClient::connect_async(&format!("http://{addr}"))
        .await
        .unwrap();

    // Register an agent first
    let agent = Agent {
        name: "job-test-agent".to_string(),
        description: "Test".to_string(),
        id: Agent::placeholder_id("job-test-agent"),
        runtime: RuntimeType::Container,
        executable: "localhost/job-test-agent:latest".to_string(),
        requirements: empty_requirements(),
        object_storage: None,
        vector_storage: None,

        source: None,
        image_digest: None,
        public_key: None,
    };
    client.register_agent(agent).await.unwrap();

    // Create a job via gRPC using registry-assigned ID
    let agent_id = registry.agent_id("job-test-agent").await.unwrap();
    let job_id = client
        .create_job(SubmissionId::new(), agent_id, "hello".to_string())
        .await;

    // Job should be pending
    let pending = client.pending_jobs().await;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].input, "hello");

    // Get specific job
    let job = client.get_job(&job_id).await;
    assert!(job.is_some());
    assert_eq!(job.unwrap().input, "hello");
}
