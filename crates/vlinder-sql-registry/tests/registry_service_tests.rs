//! Integration tests for gRPC registry service.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

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
    }
}

/// Start a gRPC server on a random port in a background thread, return the address.
fn start_server_background(registry: Arc<dyn Registry>) -> SocketAddr {
    use tonic::transport::Server;

    let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
    let repo: Arc<dyn RegistryRepository> = Arc::new(InMemoryDagStore::new());

    // Use a channel to communicate the bound address
    let (tx, rx) = std::sync::mpsc::channel();

    thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
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
    });

    // Wait for server to start
    let addr = rx.recv().unwrap();
    thread::sleep(Duration::from_millis(100));
    addr
}

#[test]
fn grpc_register_and_get_agent() {
    // Create in-memory registry and register Container runtime
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone());

    // Create gRPC client
    let client = GrpcRegistryClient::connect(&format!("http://{addr}")).unwrap();

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
        prompts: None,
        image_digest: None,
        public_key: None,
    };

    client.register_agent(agent).unwrap();

    // Retrieve via gRPC using registry-assigned ID
    let agent_id = registry.agent_id("test-agent").unwrap();
    let retrieved = client.get_agent(&agent_id);
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().name, "test-agent");
}

#[test]
fn grpc_list_agents() {
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone());
    let client = GrpcRegistryClient::connect(&format!("http://{addr}")).unwrap();

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
            prompts: None,
            image_digest: None,
            public_key: None,
        };
        client.register_agent(agent).unwrap();
    }

    let agents = client.get_agents();
    assert_eq!(agents.len(), 2);
}

#[test]
fn grpc_job_lifecycle() {
    let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
    registry.register_runtime(RuntimeType::Container);
    let addr = start_server_background(registry.clone());
    let client = GrpcRegistryClient::connect(&format!("http://{addr}")).unwrap();

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
        prompts: None,
        image_digest: None,
        public_key: None,
    };
    client.register_agent(agent).unwrap();

    // Create a job via gRPC using registry-assigned ID
    let agent_id = registry.agent_id("job-test-agent").unwrap();
    let job_id = client.create_job(SubmissionId::new(), agent_id, "hello".to_string());

    // Job should be pending
    let pending = client.pending_jobs();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].input, "hello");

    // Get specific job
    let job = client.get_job(&job_id);
    assert!(job.is_some());
    assert_eq!(job.unwrap().input, "hello");
}
