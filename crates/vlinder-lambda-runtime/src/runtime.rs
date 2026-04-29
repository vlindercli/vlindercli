//! `LambdaRuntime` — manages AWS Lambda functions for Lambda-typed agents.
//!
//! Reconciliation loop mirrors `ContainerRuntime` (pool.rs):
//! query registry → remove orphan functions → deploy missing agents.
//!
//! Invoke dispatch is fanned out: each deployed agent owns a background
//! tokio task that long-polls `receive_invoke` for its own name. A single
//! serialized dispatch loop would take `N × expires_timeout` per cycle
//! (same starvation class as the worker `tick()` fix shipped in steps
//! 07-10), so ticks are reconcile-only and invokes race the child
//! cancellation token instead.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use vlinder_core::domain::{
    Agent, AgentName, AgentStatus, CompleteMessage, DagNodeId, DataMessageKind, DataRoutingKey,
    MessageId, MessageQueue, QueueError, ReadinessCheck, Registry, RegistryRepository, ResourceId,
    Runtime, RuntimeDiagnostics, RuntimeType,
};

use async_trait::async_trait;

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
/// - A background task long-polling `receive_invoke(&agent_id)`
pub struct LambdaRuntime {
    id: ResourceId,
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    repo: Arc<dyn RegistryRepository>,
    functions: HashMap<String, DeployedFunction>,
    invoke_tasks: HashMap<String, (JoinHandle<()>, CancellationToken)>,
    shutdown: CancellationToken,
    config: LambdaRuntimeConfig,
    client: Arc<dyn LambdaClient + Send + Sync>,
}

impl LambdaRuntime {
    /// Create a new Lambda runtime connected to the given registry (async).
    ///
    /// Returns `Err` if the AWS client cannot be initialized (bad region, no creds).
    pub async fn new_async(
        config: &LambdaRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
    ) -> Result<Self, LambdaError> {
        let client = AwsLambdaClient::new_async(&config.region).await?;
        Ok(Self::with_client(
            config,
            registry,
            repo,
            queue,
            Arc::new(client),
        ))
    }

    /// Create a runtime with an injected client (for testing).
    fn with_client(
        config: &LambdaRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
        client: Arc<dyn LambdaClient + Send + Sync>,
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
            invoke_tasks: HashMap::new(),
            shutdown: CancellationToken::new(),
            config: config.clone(),
            client,
        }
    }

    /// Reconcile deployed functions with registry state.
    async fn ensure_functions(&mut self) -> bool {
        let agents = self
            .registry
            .get_agents_by_runtime(RuntimeType::Lambda)
            .await;
        let desired: HashMap<String, &Agent> = agents.iter().map(|a| (a.name.clone(), a)).collect();
        let before = self.functions.len();

        self.stop_orphan_functions(&desired).await;

        for (name, agent) in &desired {
            let status = self.repo.get_derived_status(name).ok().flatten();
            match status.as_ref() {
                Some(AgentStatus::Deploying) => self.ensure_deployed(name, agent).await,
                Some(AgentStatus::Deleting) => self.ensure_deleted(name).await,
                _ => {}
            }
        }

        self.functions.len() != before
    }

    /// Stop functions that are deployed but no longer in the registry.
    async fn stop_orphan_functions(&mut self, desired: &HashMap<String, &Agent>) {
        let orphans: Vec<String> = self
            .functions
            .keys()
            .filter(|name| !desired.contains_key(*name))
            .cloned()
            .collect();

        for name in orphans {
            tracing::info!(agent = name.as_str(), "Removing orphan Lambda function");
            self.undeploy(&name).await;
        }
    }

    /// Ensure a Lambda function is deployed and mark readiness.
    async fn ensure_deployed(&mut self, name: &str, agent: &Agent) {
        if self.functions.contains_key(name) {
            return;
        }

        tracing::info!(agent = name, "Deploying Lambda function");
        match self.deploy(name, agent).await {
            Ok(()) => {
                let agent_name = AgentName::new(name);
                let check =
                    ReadinessCheck::pending(agent_name.clone(), RuntimeType::Lambda.as_str())
                        .ready();
                let _ = self.repo.append_readiness_check(&check);
                tracing::info!(agent = name, "Lambda function deployed: Live");

                self.spawn_invoke_task(&agent_name);
            }
            Err(e) => {
                let agent_name = AgentName::new(name);
                let check = ReadinessCheck::pending(agent_name, RuntimeType::Lambda.as_str())
                    .failed(e.to_string());
                let _ = self.repo.append_readiness_check(&check);
                tracing::error!(agent = name, error = %e, "Failed to deploy Lambda function");
            }
        }
    }

    /// Tear down a Lambda function and mark deleted.
    async fn ensure_deleted(&mut self, name: &str) {
        if self.functions.contains_key(name) {
            tracing::info!(agent = name, "Tearing down Lambda function");
            self.undeploy(name).await;
        }
        let agent_name = AgentName::new(name);
        let check = ReadinessCheck::pending(agent_name, RuntimeType::Lambda.as_str()).deleted();
        let _ = self.repo.append_readiness_check(&check);
        tracing::info!(agent = name, "Lambda function deleted");
    }

    /// Deploy a single agent as a Lambda function.
    ///
    /// Passes platform URLs as environment variables so the lambda adapter
    /// inside the container can connect back to NATS, registry, and state.
    async fn deploy(&mut self, name: &str, agent: &Agent) -> Result<(), LambdaError> {
        let role_name = format!("vlinder-agent-{name}");
        let function_name = format!("vlinder-{name}");

        let role_arn = self.client.create_role(&role_name).await?;

        // IAM is eventually consistent — Lambda can't assume a role that was
        // just created. Wait for IAM to propagate before creating the function.
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        let mut env_vars: Vec<(&str, &str)> = vec![
            ("VLINDER_AGENT", &agent.name),
            ("VLINDER_QUEUE_BACKEND", &self.config.queue_backend),
            ("VLINDER_REGISTRY_URL", &self.config.registry_addr),
            ("VLINDER_STATE_URL", &self.config.state_url),
        ];
        if self.config.queue_backend == "amqp" {
            env_vars.push(("VLINDER_AMQP_URL", &self.config.amqp_url));
        } else {
            env_vars.push(("VLINDER_NATS_URL", &self.config.nats_url));
        }
        if let Some(ref secret_url) = self.config.secret_url {
            env_vars.push(("VLINDER_SECRET_URL", secret_url));
        }

        let function_arn = self
            .client
            .create_function(&CreateFunctionRequest {
                function_name: &function_name,
                ecr_image_uri: &agent.executable,
                role_arn: &role_arn,
                memory_mb: self.config.memory_mb,
                timeout_secs: self.config.timeout_secs,
                env_vars: &env_vars,
                vpc_subnet_ids: &self.config.vpc_subnet_ids,
                vpc_security_group_ids: &self.config.vpc_security_group_ids,
            })
            .await?;

        // Wait for the function to become Active — Lambda needs time to
        // download the container image from ECR.
        self.client.wait_for_active(&function_name).await?;

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

    /// Spawn a background task that long-polls `receive_invoke(&agent_id)`
    /// and dispatches each invoke to the deployed Lambda function.
    ///
    /// The task owns cloned `Arc`s of the queue and client plus a child
    /// cancellation token; it exits when `undeploy` or `shutdown` cancels
    /// that token. Timeouts from the queue are swallowed (normal long-poll
    /// behavior); other receive errors are logged and the loop retries.
    pub(crate) fn spawn_invoke_task(&mut self, agent_name: &AgentName) {
        let token = self.shutdown.child_token();
        let child = token.clone();
        let queue = Arc::clone(&self.queue);
        let client = Arc::clone(&self.client);
        let agent_id = agent_name.clone();
        let function_name = format!("vlinder-{}", agent_name.as_str());

        let handle = tokio::spawn(async move {
            loop {
                let received = tokio::select! {
                    () = child.cancelled() => break,
                    result = queue.receive_invoke(&agent_id) => result,
                };

                let (key, invoke, ack) = match received {
                    Ok(tuple) => tuple,
                    Err(QueueError::Timeout) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(e) => {
                        tracing::warn!(
                            agent = agent_id.as_str(),
                            error = %e,
                            "Lambda invoke receive error"
                        );
                        continue;
                    }
                };
                let _ = ack().await;

                let DataMessageKind::Invoke { harness, agent, .. } = &key.kind else {
                    continue;
                };

                let json_payload = serde_json::json!({ "key": key, "msg": invoke });
                let json_bytes =
                    serde_json::to_vec(&json_payload).unwrap_or_else(|_| b"{}".to_vec());

                match client.invoke_function(&function_name, &json_bytes).await {
                    Ok(_) => {
                        tracing::info!(
                            event = "lambda.invoke_ok",
                            agent = agent_id.as_str(),
                            function = function_name.as_str(),
                            "Lambda invocation succeeded"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            event = "lambda.invoke_failed",
                            agent = agent_id.as_str(),
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
                            content: None,
                            tool_calls: None,
                            payload: format!("[error] Lambda invoke failed: {e}").into_bytes(),
                        };
                        let _ = queue.send_complete(complete_key, complete).await;
                    }
                }
            }
        });

        self.invoke_tasks
            .insert(agent_name.as_str().to_string(), (handle, token));
    }

    /// Tear down a deployed function: cancel its invoke task, delete
    /// Lambda, then IAM role. Task cancellation happens first so the task
    /// can't call `invoke_function` on a function that's about to disappear.
    async fn undeploy(&mut self, name: &str) {
        if let Some((handle, token)) = self.invoke_tasks.remove(name) {
            token.cancel();
            let _ = handle.await;
        }

        let function_name = format!("vlinder-{name}");
        let role_name = format!("vlinder-agent-{name}");

        self.client.delete_function(&function_name).await;
        self.client.delete_role(&role_name).await;

        self.functions.remove(name);
    }
}

#[async_trait]
impl Runtime for LambdaRuntime {
    fn id(&self) -> &ResourceId {
        &self.id
    }

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Lambda
    }

    async fn tick(&mut self) -> bool {
        self.ensure_functions().await
    }

    async fn shutdown(&mut self) {
        // Drain invoke tasks first — pending invokes lose their dispatcher
        // but haven't been acked yet, so the queue will redeliver to
        // whoever picks up next. Tearing down Lambda before tasks stop
        // would race: a task could invoke a function mid-deletion.
        let tasks: Vec<_> = self.invoke_tasks.drain().collect();
        for (_, (handle, token)) in tasks {
            token.cancel();
            let _ = handle.await;
        }

        let names: Vec<String> = self.functions.keys().cloned().collect();
        for name in names {
            tracing::info!(agent = name.as_str(), "Shutting down Lambda function");
            self.undeploy(&name).await;
        }
    }
}

impl Drop for LambdaRuntime {
    fn drop(&mut self) {
        if !self.functions.is_empty() {
            tracing::warn!(
                count = self.functions.len(),
                "LambdaRuntime dropped with active functions — AWS resources not cleaned up. \
                 Call shutdown() before dropping."
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Notify;
    use vlinder_core::domain::InMemorySecretStore;
    use vlinder_core::queue::InMemoryQueue;

    // ── Mock client ─────────────────────────────────────────────────

    struct MockLambdaClient {
        roles: Mutex<HashSet<String>>,
        functions: Mutex<HashSet<String>>,
        last_vpc_subnet_ids: Mutex<Vec<String>>,
        last_vpc_security_group_ids: Mutex<Vec<String>>,
        invoke_called: Arc<Notify>,
    }

    impl MockLambdaClient {
        fn new() -> Self {
            Self {
                roles: Mutex::new(HashSet::new()),
                functions: Mutex::new(HashSet::new()),
                last_vpc_subnet_ids: Mutex::new(vec![]),
                last_vpc_security_group_ids: Mutex::new(vec![]),
                invoke_called: Arc::new(Notify::new()),
            }
        }

        fn invoke_notify(&self) -> Arc<Notify> {
            Arc::clone(&self.invoke_called)
        }
    }

    #[async_trait]
    impl LambdaClient for MockLambdaClient {
        async fn check_connectivity(&self) -> Result<(), LambdaError> {
            Ok(())
        }

        async fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
            self.roles.lock().unwrap().insert(role_name.to_string());
            Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
        }

        async fn delete_role(&self, role_name: &str) {
            self.roles.lock().unwrap().remove(role_name);
        }

        async fn create_function(
            &self,
            req: &CreateFunctionRequest<'_>,
        ) -> Result<String, LambdaError> {
            self.functions
                .lock()
                .unwrap()
                .insert(req.function_name.to_string());
            *self.last_vpc_subnet_ids.lock().unwrap() = req.vpc_subnet_ids.to_vec();
            *self.last_vpc_security_group_ids.lock().unwrap() = req.vpc_security_group_ids.to_vec();
            Ok(format!(
                "arn:aws:lambda:us-east-1:123456789012:function:{}",
                req.function_name
            ))
        }

        async fn get_function(
            &self,
            function_name: &str,
        ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
            if self.functions.lock().unwrap().contains(function_name) {
                Ok(Some(crate::lambda_client::FunctionInfo {
                    function_arn: format!(
                        "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                    ),
                    state: "Active".to_string(),
                }))
            } else {
                Ok(None)
            }
        }

        async fn delete_function(&self, function_name: &str) {
            self.functions.lock().unwrap().remove(function_name);
        }

        async fn invoke_function(
            &self,
            _function_name: &str,
            payload: &[u8],
        ) -> Result<Vec<u8>, LambdaError> {
            self.invoke_called.notify_one();
            Ok(payload.to_vec())
        }
    }

    /// Mock client where `invoke_function` always fails (deploy succeeds).
    struct FailingLambdaClient;

    #[async_trait]
    impl LambdaClient for FailingLambdaClient {
        async fn check_connectivity(&self) -> Result<(), LambdaError> {
            Ok(())
        }

        async fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
            Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
        }

        async fn delete_role(&self, _role_name: &str) {}

        async fn create_function(
            &self,
            req: &CreateFunctionRequest<'_>,
        ) -> Result<String, LambdaError> {
            Ok(format!(
                "arn:aws:lambda:us-east-1:123456789012:function:{}",
                req.function_name
            ))
        }

        async fn get_function(
            &self,
            function_name: &str,
        ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
            Ok(Some(crate::lambda_client::FunctionInfo {
                function_arn: format!(
                    "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                ),
                state: "Active".to_string(),
            }))
        }

        async fn delete_function(&self, _function_name: &str) {}

        async fn invoke_function(
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
            queue_backend: "nats".to_string(),
            nats_url: "nats://localhost:4222".to_string(),
            amqp_url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
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
        let check = ReadinessCheck::pending(AgentName::new(name), RuntimeType::Lambda.as_str());
        repo.append_readiness_check(&check).unwrap();
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
            Arc::new(MockLambdaClient::new()),
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

    #[tokio::test]
    async fn tick_returns_false_when_no_agents() {
        let registry = test_registry();
        let mut runtime = make_runtime(registry, test_repo());

        assert!(!runtime.tick().await);
        assert!(runtime.functions.is_empty());
    }

    #[tokio::test]
    async fn tick_deploys_new_agent() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
        set_deploying(&*repo, "echo");

        let mut runtime = make_runtime(registry, repo);

        // First tick: deploys the agent → count changed.
        assert!(runtime.tick().await);
        assert_eq!(runtime.functions.len(), 1);
        assert!(runtime.functions.contains_key("echo"));
        assert!(runtime.invoke_tasks.contains_key("echo"));

        // Second tick: no change (now Live).
        assert!(!runtime.tick().await);

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn tick_removes_orphan() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
        set_deploying(&*repo, "echo");

        let mut runtime = make_runtime(registry.clone(), repo);
        runtime.tick().await; // deploys + spawns task
        assert_eq!(runtime.functions.len(), 1);
        assert!(runtime.invoke_tasks.contains_key("echo"));

        // Remove from registry → next tick undeploys, which cancels the task.
        registry.delete_agent("echo").await.unwrap();
        assert!(runtime.tick().await);
        assert!(runtime.functions.is_empty());
        assert!(runtime.invoke_tasks.is_empty());
    }

    #[tokio::test]
    async fn shutdown_undeploys_all() {
        let registry = test_registry();
        let repo = test_repo();
        registry
            .register_agent(make_lambda_agent("alpha"))
            .await
            .unwrap();
        registry
            .register_agent(make_lambda_agent("beta"))
            .await
            .unwrap();
        set_deploying(&*repo, "alpha");
        set_deploying(&*repo, "beta");

        let mut runtime = make_runtime(registry, repo);
        runtime.tick().await;
        assert_eq!(runtime.functions.len(), 2);
        assert_eq!(runtime.invoke_tasks.len(), 2);

        runtime.shutdown().await;
        assert!(runtime.functions.is_empty());
        assert!(runtime.invoke_tasks.is_empty());
    }

    #[tokio::test]
    async fn deploy_creates_role_and_function_with_correct_names() {
        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
        set_deploying(&*repo, "echo");

        let mock = MockLambdaClient::new();
        let config = test_config();
        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let mut runtime =
            LambdaRuntime::with_client(&config, registry, repo, queue, Arc::new(mock));

        runtime.tick().await;

        let deployed = &runtime.functions["echo"];
        assert!(deployed.role_arn.contains("vlinder-agent-echo"));
        assert!(deployed.function_arn.contains("vlinder-echo"));

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn invoke_dispatches_to_lambda_consumes_from_queue() {
        use vlinder_core::domain::{
            BranchId, DagNodeId, DataMessageKind, DataRoutingKey, HarnessType, InvokeDiagnostics,
            InvokeMessage, MessageId, SessionId, SubmissionId,
        };

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
        set_deploying(&*repo, "echo");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let config = test_config();
        let mock = MockLambdaClient::new();
        let notify = mock.invoke_notify();
        let mut runtime =
            LambdaRuntime::with_client(&config, registry, repo, queue.clone(), Arc::new(mock));

        // Deploy — spawns the per-agent invoke task.
        runtime.tick().await;
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
            history: vec![],
            current_input: vec![vlinder_core::domain::Message::User {
                content: "hello lambda".to_string(),
            }],
        };
        queue.send_invoke(key, msg).await.unwrap();

        // Wait until the task's invoke_function call fires.
        tokio::time::timeout(Duration::from_secs(5), notify.notified())
            .await
            .expect("invoke_function should have been called within 5s");

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn invoke_failure_sends_error_complete() {
        use vlinder_core::domain::{
            BranchId, DagNodeId, DataMessageKind, DataRoutingKey, HarnessType, InvokeDiagnostics,
            InvokeMessage, MessageId, SessionId, SubmissionId,
        };

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
        set_deploying(&*repo, "echo");

        let queue: Arc<dyn MessageQueue + Send + Sync> = Arc::new(InMemoryQueue::new());
        let config = test_config();

        // Use a failing mock client.
        let mut runtime = LambdaRuntime::with_client(
            &config,
            registry,
            repo,
            queue.clone(),
            Arc::new(FailingLambdaClient),
        );

        // Deploy — spawns the invoke task.
        runtime.tick().await;
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
            history: vec![],
            current_input: vec![vlinder_core::domain::Message::User {
                content: "hello".to_string(),
            }],
        };
        queue.send_invoke(key, msg).await.unwrap();

        // Task picks up the invoke, invoke_function fails, error-complete gets sent.
        // InMemoryQueue::receive_complete returns Poll::Ready(Timeout) synchronously,
        // so poll-with-yield until the spawned task has a chance to produce the complete.
        let poll = async {
            loop {
                match queue
                    .receive_complete(&submission, HarnessType::Grpc, &AgentName::new("echo"))
                    .await
                {
                    Ok(v) => break v,
                    Err(QueueError::Timeout) => tokio::task::yield_now().await,
                    Err(e) => panic!("unexpected receive_complete error: {e}"),
                }
            }
        };
        let (_key, complete, ack) = tokio::time::timeout(Duration::from_secs(5), poll)
            .await
            .expect("error complete should arrive within 5s");
        ack().await.unwrap();
        let payload_str = String::from_utf8_lossy(&complete.payload);
        assert!(
            payload_str.contains("[error]"),
            "expected error payload, got: {payload_str}"
        );

        runtime.shutdown().await;
    }

    #[tokio::test]
    async fn deploy_passes_vpc_config_to_client() {
        use std::sync::{Arc as StdArc, Mutex};

        #[derive(Default)]
        struct CapturedVpc {
            subnet_ids: Vec<String>,
            security_group_ids: Vec<String>,
        }

        struct CapturingClient {
            captured: StdArc<Mutex<CapturedVpc>>,
        }

        #[async_trait]
        impl LambdaClient for CapturingClient {
            async fn check_connectivity(&self) -> Result<(), LambdaError> {
                Ok(())
            }
            async fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
                Ok(format!("arn:aws:iam::123456789012:role/{role_name}"))
            }
            async fn delete_role(&self, _: &str) {}
            async fn create_function(
                &self,
                req: &CreateFunctionRequest<'_>,
            ) -> Result<String, LambdaError> {
                let mut c = self.captured.lock().unwrap();
                c.subnet_ids = req.vpc_subnet_ids.to_vec();
                c.security_group_ids = req.vpc_security_group_ids.to_vec();
                Ok(format!(
                    "arn:aws:lambda:us-east-1:123456789012:function:{}",
                    req.function_name
                ))
            }
            async fn get_function(
                &self,
                function_name: &str,
            ) -> Result<Option<crate::lambda_client::FunctionInfo>, LambdaError> {
                Ok(Some(crate::lambda_client::FunctionInfo {
                    function_arn: format!(
                        "arn:aws:lambda:us-east-1:123456789012:function:{function_name}"
                    ),
                    state: "Active".to_string(),
                }))
            }
            async fn delete_function(&self, _: &str) {}
            async fn invoke_function(&self, _: &str, p: &[u8]) -> Result<Vec<u8>, LambdaError> {
                Ok(p.to_vec())
            }
        }

        let registry = test_registry();
        let repo = test_repo();
        let agent = make_lambda_agent("echo");
        registry.register_agent(agent).await.unwrap();
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
            LambdaRuntime::with_client(&config, registry, repo, queue, Arc::new(client));

        runtime.tick().await;

        {
            let c = captured.lock().unwrap();
            assert_eq!(c.subnet_ids, vec!["subnet-aaa", "subnet-bbb"]);
            assert_eq!(c.security_group_ids, vec!["sg-xxx"]);
        }

        runtime.shutdown().await;
    }
}
