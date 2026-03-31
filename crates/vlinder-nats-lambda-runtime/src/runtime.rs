//! `LambdaRuntime` — manages AWS Lambda functions for Lambda-typed agents.
//!
//! Reconciliation loop mirrors `ContainerRuntime` (pool.rs):
//! query registry → remove orphan functions → deploy missing agents.

use std::collections::HashMap;
use std::sync::Arc;

use vlinder_core::domain::{
    Agent, AgentName, AgentState, AgentStatus, CompleteMessage, DagNodeId, DataMessageKind,
    DataRoutingKey, MessageId, MessageQueue, Registry, RegistryRepository, ResourceId, Runtime,
    RuntimeDiagnostics, RuntimeType,
};

use crate::config::LambdaRuntimeConfig;
use crate::lambda_client::{AwsLambdaClient, CreateFunctionRequest, LambdaClient, LambdaError};

/// A deployed Lambda function: IAM role + Lambda function.
#[allow(dead_code)]
struct DeployedFunction {
    role_arn: String,
    function_arn: String,
}

/// Orchestrates AWS Lambda functions for Lambda-typed agents.
///
/// Maps agent names to deployed functions. Each agent gets:
/// - An IAM role `vlinder-agent-{name}` with zero permissions
/// - A Lambda function `vlinder-{name}` running the agent's ECR image
pub struct LambdaRuntime {
    id: ResourceId,
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    repo: Arc<dyn RegistryRepository>,
    functions: HashMap<String, DeployedFunction>,
    config: LambdaRuntimeConfig,
    client: Box<dyn LambdaClient>,
}

impl LambdaRuntime {
    /// Create a new Lambda runtime connected to the given registry.
    ///
    /// Returns `Err` if the AWS client cannot be initialized (bad region, no creds).
    pub fn new(
        config: &LambdaRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
    ) -> Result<Self, LambdaError> {
        let client = AwsLambdaClient::new(&config.region)?;
        Ok(Self::with_client(
            config,
            registry,
            repo,
            queue,
            Box::new(client),
        ))
    }

    /// Create a runtime with an injected client (for testing).
    fn with_client(
        config: &LambdaRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
        client: Box<dyn LambdaClient>,
    ) -> Self {
        let registry_id = ResourceId::new(&config.registry_addr);
        let id = ResourceId::new(format!(
            "{}/runtimes/{}",
            registry_id.as_str(),
            RuntimeType::Lambda.as_str()
        ));

        Self {
            id,
            queue,
            registry,
            repo,
            functions: HashMap::new(),
            config: config.clone(),
            client,
        }
    }

    /// Reconcile deployed functions with registry state.
    ///
    /// 1. Query registry for Lambda agents.
    /// 2. Remove orphan functions (deployed but no longer in registry).
    /// 3. Deploy missing agents (in registry but not deployed).
    ///
    /// Returns true if the function count changed.
    fn ensure_functions(&mut self) -> bool {
        let agents = self.registry.get_agents_by_runtime(RuntimeType::Lambda);
        let desired: HashMap<String, &Agent> = agents.iter().map(|a| (a.name.clone(), a)).collect();

        let before = self.functions.len();

        // Remove orphans: deployed but not in registry.
        let orphans: Vec<String> = self
            .functions
            .keys()
            .filter(|name| !desired.contains_key(*name))
            .cloned()
            .collect();

        for name in orphans {
            tracing::info!(agent = name.as_str(), "Removing orphan Lambda function");
            self.undeploy(&name);
        }

        for (name, agent) in &desired {
            let state = self.repo.get_agent_state(name).ok().flatten();
            let status = state.as_ref().map(|s| &s.status);

            match status {
                // Deploying: create Lambda function, transition to Live or Failed
                Some(AgentStatus::Deploying) if !self.functions.contains_key(name) => {
                    tracing::info!(agent = name.as_str(), "Deploying Lambda function");
                    match self.deploy(name, agent) {
                        Ok(()) => {
                            let live = AgentState::registered(AgentName::new(name))
                                .transition(AgentStatus::Live, None);
                            if let Err(e) = self.repo.append_agent_state(&live) {
                                tracing::warn!(error = %e, "Failed to set Live state");
                            }
                            tracing::info!(agent = name.as_str(), "Lambda function deployed: Live");
                        }
                        Err(e) => {
                            let failed = AgentState::registered(AgentName::new(name))
                                .transition(AgentStatus::Failed, Some(e.to_string()));
                            if let Err(e2) = self.repo.append_agent_state(&failed) {
                                tracing::warn!(error = %e2, "Failed to set Failed state");
                            }
                            tracing::error!(
                                agent = name.as_str(),
                                error = %e,
                                "Failed to deploy Lambda function"
                            );
                        }
                    }
                }

                // Deleting: tear down function, delete from registry, transition to Deleted
                Some(AgentStatus::Deleting) => {
                    tracing::info!(agent = name.as_str(), "Tearing down Lambda function");
                    self.undeploy(name);
                    // Soft delete: agent row stays in registry, state says Deleted
                    let deleted = AgentState::registered(AgentName::new(name))
                        .transition(AgentStatus::Deleted, None);
                    if let Err(e) = self.repo.append_agent_state(&deleted) {
                        tracing::warn!(error = %e, "Failed to set Deleted state");
                    }
                    tracing::info!(agent = name.as_str(), "Lambda function torn down: Deleted");
                }

                // Live or other states: nothing to do
                _ => {}
            }
        }

        self.functions.len() != before
    }

    /// Deploy a single agent as a Lambda function.
    ///
    /// Passes platform URLs as environment variables so the lambda adapter
    /// inside the container can connect back to NATS, registry, and state.
    fn deploy(&mut self, name: &str, agent: &Agent) -> Result<(), LambdaError> {
        let role_name = format!("vlinder-agent-{name}");
        let function_name = format!("vlinder-{name}");

        let role_arn = self.client.create_role(&role_name)?;

        // IAM is eventually consistent — Lambda can't assume a role that was
        // just created. Wait for IAM to propagate before creating the function.
        std::thread::sleep(std::time::Duration::from_secs(10));

        let mut env_vars: Vec<(&str, &str)> = vec![
            ("VLINDER_AGENT", &agent.name),
            ("VLINDER_NATS_URL", &self.config.nats_url),
            ("VLINDER_REGISTRY_URL", &self.config.registry_addr),
            ("VLINDER_STATE_URL", &self.config.state_url),
        ];
        if let Some(ref secret_url) = self.config.secret_url {
            env_vars.push(("VLINDER_SECRET_URL", secret_url));
        }

        let function_arn = self.client.create_function(&CreateFunctionRequest {
            function_name: &function_name,
            ecr_image_uri: &agent.executable,
            role_arn: &role_arn,
            memory_mb: self.config.memory_mb,
            timeout_secs: self.config.timeout_secs,
            env_vars: &env_vars,
            vpc_subnet_ids: &self.config.vpc_subnet_ids,
            vpc_security_group_ids: &self.config.vpc_security_group_ids,
        })?;

        self.functions.insert(
            name.to_string(),
            DeployedFunction {
                role_arn,
                function_arn,
            },
        );

        tracing::info!(
            agent = name,
            function = function_name.as_str(),
            "Lambda function deployed"
        );

        Ok(())
    }

    /// Poll queue for invocations and dispatch to Lambda functions.
    ///
    /// Serializes the `LambdaInvokePayload` (key + `InvokeMessage`) as the Lambda payload. The adapter
    /// inside the container deserializes it, runs the `ProviderServer`, and
    /// sends complete to NATS — the daemon only needs to handle invoke-level
    /// failures (Lambda itself couldn't run).
    fn dispatch_invocations(&self) {
        for name in self.functions.keys() {
            let agent_id = AgentName::new(name);

            // Receive invoke (ADR 121 — data plane).
            if let Ok((key, invoke, ack)) = self.queue.receive_invoke(&agent_id) {
                let _ = ack();
                let function_name = format!("vlinder-{name}");

                let DataMessageKind::Invoke { harness, agent, .. } = &key.kind else {
                    continue;
                };

                let json_payload = serde_json::json!({ "key": key, "msg": invoke });
                let json_bytes =
                    serde_json::to_vec(&json_payload).unwrap_or_else(|_| b"{}".to_vec());

                match self.client.invoke_function(&function_name, &json_bytes) {
                    Ok(_) => {
                        tracing::info!(
                            event = "lambda.invoke_ok",
                            agent = name.as_str(),
                            function = function_name.as_str(),
                            "Lambda invocation succeeded"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            event = "lambda.invoke_failed",
                            agent = name.as_str(),
                            error = %e,
                            "Lambda invocation failed"
                        );
                        let complete_key = DataRoutingKey {
                            session: key.session.clone(),
                            branch: key.branch,
                            submission: key.submission.clone(),
                            kind: DataMessageKind::Complete {
                                agent: agent.clone(),
                                harness: *harness,
                            },
                        };
                        let complete = CompleteMessage {
                            id: MessageId::new(),
                            dag_id: DagNodeId::root(),
                            state: None,
                            diagnostics: RuntimeDiagnostics::placeholder(0),
                            payload: format!("[error] Lambda invoke failed: {e}").into_bytes(),
                        };
                        let _ = self.queue.send_complete(complete_key, complete);
                    }
                }
            }
        }
    }

    /// Tear down a deployed function: delete Lambda first, then IAM role.
    fn undeploy(&mut self, name: &str) {
        let function_name = format!("vlinder-{name}");
        let role_name = format!("vlinder-agent-{name}");

        self.client.delete_function(&function_name);
        self.client.delete_role(&role_name);

        self.functions.remove(name);
    }
}

impl Runtime for LambdaRuntime {
    fn id(&self) -> &ResourceId {
        &self.id
    }

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Lambda
    }

    fn tick(&mut self) -> bool {
        let changed = self.ensure_functions();
        self.dispatch_invocations();
        changed
    }

    fn shutdown(&mut self) {
        let names: Vec<String> = self.functions.keys().cloned().collect();
        for name in names {
            tracing::info!(agent = name.as_str(), "Shutting down Lambda function");
            self.undeploy(&name);
        }
    }
}

impl Drop for LambdaRuntime {
    fn drop(&mut self) {
        if !self.functions.is_empty() {
            self.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashSet;
    use vlinder_core::domain::InMemorySecretStore;
    use vlinder_core::queue::InMemoryQueue;

    // ── Mock client ─────────────────────────────────────────────────

    struct MockLambdaClient {
        roles: RefCell<HashSet<String>>,
        functions: RefCell<HashSet<String>>,
        last_vpc_subnet_ids: RefCell<Vec<String>>,
        last_vpc_security_group_ids: RefCell<Vec<String>>,
    }

    impl MockLambdaClient {
        fn new() -> Self {
            Self {
                roles: RefCell::new(HashSet::new()),
                functions: RefCell::new(HashSet::new()),
                last_vpc_subnet_ids: RefCell::new(vec![]),
                last_vpc_security_group_ids: RefCell::new(vec![]),
            }
        }
    }

    impl LambdaClient for MockLambdaClient {
        fn check_connectivity(&self) -> Result<(), LambdaError> {
            Ok(())
        }

        fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
            self.roles.borrow_mut().insert(role_name.to_string());
            Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
        }

        fn delete_role(&self, role_name: &str) {
            self.roles.borrow_mut().remove(role_name);
        }

        fn create_function(&self, req: &CreateFunctionRequest) -> Result<String, LambdaError> {
            self.functions
                .borrow_mut()
                .insert(req.function_name.to_string());
            *self.last_vpc_subnet_ids.borrow_mut() = req.vpc_subnet_ids.to_vec();
            *self.last_vpc_security_group_ids.borrow_mut() = req.vpc_security_group_ids.to_vec();
            Ok(format!(
                "arn:aws:lambda:us-east-1:123456789012:function:{}",
                req.function_name
            ))
        }

        fn get_function(
            &self,
            function_name: &str,
        ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
            if self.functions.borrow().contains(function_name) {
                Ok(Some(crate::lambda_client::FunctionInfo {
                    function_arn: format!(
                        "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                    ),
                }))
            } else {
                Ok(None)
            }
        }

        fn delete_function(&self, function_name: &str) {
            self.functions.borrow_mut().remove(function_name);
        }

        fn invoke_function(
            &self,
            _function_name: &str,
            payload: &[u8],
        ) -> Result<Vec<u8>, LambdaError> {
            // Echo: return the payload as-is.
            Ok(payload.to_vec())
        }
    }

    /// Mock client where `invoke_function` always fails (deploy succeeds).
    struct FailingLambdaClient;

    impl LambdaClient for FailingLambdaClient {
        fn check_connectivity(&self) -> Result<(), LambdaError> {
            Ok(())
        }

        fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
            Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
        }

        fn delete_role(&self, _role_name: &str) {}

        fn create_function(&self, req: &CreateFunctionRequest) -> Result<String, LambdaError> {
            Ok(format!(
                "arn:aws:lambda:us-east-1:123456789012:function:{}",
                req.function_name
            ))
        }

        fn get_function(
            &self,
            function_name: &str,
        ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
            Ok(Some(crate::lambda_client::FunctionInfo {
                function_arn: format!(
                    "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                ),
            }))
        }

        fn delete_function(&self, _function_name: &str) {}

        fn invoke_function(
            &self,
            _function_name: &str,
            _payload: &[u8],
        ) -> Result<Vec<u8>, LambdaError> {
            Err(LambdaError::Aws("simulated Lambda failure".to_string()))
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────

    fn test_config() -> LambdaRuntimeConfig {
        LambdaRuntimeConfig {
            registry_addr: "http://127.0.0.1:9090".to_string(),
            region: "us-east-1".to_string(),
            memory_mb: 512,
            timeout_secs: 300,
            nats_url: "nats://localhost:4222".to_string(),
            state_url: "http://127.0.0.1:9092".to_string(),
            secret_url: None,
            vpc_subnet_ids: vec![],
            vpc_security_group_ids: vec![],
        }
    }

    fn test_registry() -> Arc<dyn Registry> {
        use vlinder_core::domain::InMemoryRegistry;
        let secret_store = Arc::new(InMemorySecretStore::new());
        let registry = Arc::new(InMemoryRegistry::new(secret_store));
        registry.register_runtime(RuntimeType::Lambda);
        registry
    }

    fn make_lambda_agent(name: &str) -> Agent {
        Agent::from_toml(&format!(
            r#"
            name = "{name}"
            description = "Test Lambda agent"
            runtime = "lambda"
            executable = "123456789012.dkr.ecr.us-east-1.amazonaws.com/{name}:latest"

            [requirements]
            "#
        ))
        .unwrap()
    }

    fn test_repo() -> Arc<dyn RegistryRepository> {
        Arc::new(vlinder_core::domain::InMemoryDagStore::new())
    }

    /// Set an agent to `Deploying` state so the runtime will provision it.
    fn set_deploying(repo: &dyn RegistryRepository, name: &str) {
        let state =
            AgentState::registered(AgentName::new(name)).transition(AgentStatus::Deploying, None);
        repo.append_agent_state(&state).unwrap();
    }

    fn make_runtime(
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
    ) -> LambdaRuntime {
        let config = test_config();
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        LambdaRuntime::with_client(
            &config,
            registry,
            repo,
            queue,
            Box::new(MockLambdaClient::new()),
        )
    }

    // ── Tests ───────────────────────────────────────────────────────

    #[test]
    fn runtime_id_format() {
        let registry = test_registry();
        let runtime = make_runtime(registry, test_repo());

        assert_eq!(
            runtime.id().as_str(),
            "http://127.0.0.1:9090/runtimes/lambda"
        );
        assert_eq!(runtime.runtime_type(), RuntimeType::Lambda);
    }

    #[test]
    fn tick_returns_false_when_no_agents() {
        let registry = test_registry();
        let mut runtime = make_runtime(registry, test_repo());

        assert!(!runtime.tick());
        assert!(runtime.functions.is_empty());
    }

    #[test]
    fn tick_deploys_new_agent() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let mut runtime = make_runtime(registry, repo);

        // First tick: deploys the agent → count changed.
        assert!(runtime.tick());
        assert_eq!(runtime.functions.len(), 1);
        assert!(runtime.functions.contains_key("echo"));

        // Second tick: no change (now Live).
        assert!(!runtime.tick());
    }

    #[test]
    fn tick_removes_orphan() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let mut runtime = make_runtime(registry.clone(), repo);
        runtime.tick(); // deploys
        assert_eq!(runtime.functions.len(), 1);

        // Remove from registry → next tick undeploys.
        registry.delete_agent("echo").unwrap();
        assert!(runtime.tick());
        assert!(runtime.functions.is_empty());
    }

    #[test]
    fn shutdown_undeploys_all() {
        let registry = test_registry();
        let repo = test_repo();
        registry.register_agent(make_lambda_agent("alpha")).unwrap();
        registry.register_agent(make_lambda_agent("beta")).unwrap();
        set_deploying(&*repo, "alpha");
        set_deploying(&*repo, "beta");

        let mut runtime = make_runtime(registry, repo);
        runtime.tick();
        assert_eq!(runtime.functions.len(), 2);

        runtime.shutdown();
        assert!(runtime.functions.is_empty());
    }

    #[test]
    fn deploy_creates_role_and_function_with_correct_names() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let mock = MockLambdaClient::new();
        let config = test_config();
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let mut runtime =
            LambdaRuntime::with_client(&config, registry, repo, queue, Box::new(mock));

        runtime.tick();

        let deployed = &runtime.functions["echo"];
        assert!(deployed.role_arn.contains("vlinder-agent-echo"));
        assert!(deployed.function_arn.contains("vlinder-echo"));
    }

    #[test]
    fn invoke_dispatches_to_lambda_consumes_from_queue() {
        use vlinder_core::domain::{
            BranchId, DagNodeId, DataMessageKind, DataRoutingKey, HarnessType, InvokeDiagnostics,
            InvokeMessage, MessageId, SessionId, SubmissionId,
        };

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let config = test_config();
        let mut runtime = LambdaRuntime::with_client(
            &config,
            registry,
            repo,
            queue.clone(),
            Box::new(MockLambdaClient::new()),
        );

        // Deploy first.
        runtime.tick();
        assert_eq!(runtime.functions.len(), 1);

        // Enqueue an invoke message.
        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Grpc,
                runtime: RuntimeType::Lambda,
                agent: AgentName::new("echo"),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: DagNodeId::root(),
            payload: b"hello lambda".to_vec(),
        };
        queue.send_invoke(key, msg).unwrap();

        // Tick — should dispatch the invocation.
        runtime.tick();

        // The invoke should be consumed.
        let agent_id = AgentName::new("echo");
        assert!(
            queue.receive_invoke(&agent_id).is_err(),
            "invoke should have been consumed from the queue"
        );
    }

    #[test]
    fn invoke_failure_sends_error_complete() {
        use vlinder_core::domain::{
            BranchId, DagNodeId, DataMessageKind, DataRoutingKey, HarnessType, InvokeDiagnostics,
            InvokeMessage, MessageId, SessionId, SubmissionId,
        };

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let config = test_config();

        // Use a failing mock client.
        let mut runtime = LambdaRuntime::with_client(
            &config,
            registry,
            repo,
            queue.clone(),
            Box::new(FailingLambdaClient),
        );

        // Deploy first (create_role/create_function succeed on FailingLambdaClient).
        runtime.tick();
        assert_eq!(runtime.functions.len(), 1);

        // Enqueue an invoke message.
        let submission = SubmissionId::new();
        let key = DataRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: submission.clone(),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Grpc,
                runtime: RuntimeType::Lambda,
                agent: AgentName::new("echo"),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "test".to_string(),
            },
            dag_parent: DagNodeId::root(),
            payload: b"hello".to_vec(),
        };
        queue.send_invoke(key, msg).unwrap();

        // Tick — invoke_function fails, so daemon sends error complete.
        runtime.tick();

        let (_key, complete, ack) = queue
            .receive_complete(&submission, HarnessType::Grpc, &AgentName::new("echo"))
            .expect("should receive error complete from daemon");
        ack().unwrap();
        let payload_str = String::from_utf8_lossy(&complete.payload);
        assert!(
            payload_str.contains("[error]"),
            "expected error payload, got: {payload_str}"
        );
    }

    #[test]
    fn deploy_passes_vpc_config_to_client() {
        use std::sync::{Arc as StdArc, Mutex};

        #[derive(Default)]
        struct CapturedVpc {
            subnet_ids: Vec<String>,
            security_group_ids: Vec<String>,
        }

        struct CapturingClient {
            captured: StdArc<Mutex<CapturedVpc>>,
        }

        impl LambdaClient for CapturingClient {
            fn check_connectivity(&self) -> Result<(), LambdaError> {
                Ok(())
            }
            fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
                Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
            }
            fn delete_role(&self, _: &str) {}
            fn create_function(&self, req: &CreateFunctionRequest) -> Result<String, LambdaError> {
                let mut c = self.captured.lock().unwrap();
                c.subnet_ids = req.vpc_subnet_ids.to_vec();
                c.security_group_ids = req.vpc_security_group_ids.to_vec();
                Ok(format!(
                    "arn:aws:lambda:us-east-1:123456789012:function:{}",
                    req.function_name
                ))
            }
            fn get_function(
                &self,
                function_name: &str,
            ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
                Ok(Some(crate::lambda_client::FunctionInfo {
                    function_arn: format!(
                        "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                    ),
                }))
            }
            fn delete_function(&self, _: &str) {}
            fn invoke_function(&self, _: &str, p: &[u8]) -> Result<Vec<u8>, LambdaError> {
                Ok(p.to_vec())
            }
        }

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).unwrap();
        set_deploying(&*repo, "echo");

        let captured = StdArc::new(Mutex::new(CapturedVpc::default()));

        let mut config = test_config();
        config.vpc_subnet_ids = vec!["subnet-aaa".to_string(), "subnet-bbb".to_string()];
        config.vpc_security_group_ids = vec!["sg-xxx".to_string()];

        let client = CapturingClient {
            captured: captured.clone(),
        };
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let mut runtime =
            LambdaRuntime::with_client(&config, registry, repo, queue, Box::new(client));

        runtime.tick();

        let c = captured.lock().unwrap();
        assert_eq!(c.subnet_ids, vec!["subnet-aaa", "subnet-bbb"]);
        assert_eq!(c.security_group_ids, vec!["sg-xxx"]);
    }
}
