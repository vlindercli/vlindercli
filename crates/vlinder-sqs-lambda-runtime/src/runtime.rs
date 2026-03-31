//! `SqsLambdaRuntime` — manages AWS Lambda functions with push-based
//! SQS invoke delivery (ADR 125).
//!
//! Unlike `vlinder-nats-lambda-runtime`, this runtime does not poll the queue.
//! SQS event source mappings deliver invokes directly to Lambda as events.
//! The daemon only manages the lifecycle: deploy, undeploy, reconcile.

use std::collections::HashMap;
use std::sync::Arc;

use vlinder_core::domain::{
    Agent, AgentName, AgentState, AgentStatus, MessageQueue, Registry, RegistryRepository,
    ResourceId, Runtime, RuntimeType,
};

use crate::config::SqsLambdaRuntimeConfig;
use crate::lambda_client::{AwsSqsLambdaClient, CreateFunctionRequest, LambdaClient, LambdaError};

/// A deployed Lambda function with its SQS event source mapping.
#[allow(dead_code)]
struct DeployedFunction {
    role_arn: String,
    function_arn: String,
    /// SQS event source mapping UUID — deleted on undeploy.
    event_source_mapping_uuid: String,
}

/// Orchestrates AWS Lambda functions for Lambda-typed agents with SQS dispatch.
pub struct SqsLambdaRuntime {
    id: ResourceId,
    #[allow(dead_code)]
    queue: Arc<dyn MessageQueue + Send + Sync>,
    registry: Arc<dyn Registry>,
    repo: Arc<dyn RegistryRepository>,
    functions: HashMap<String, DeployedFunction>,
    config: SqsLambdaRuntimeConfig,
    client: Box<dyn LambdaClient>,
}

impl SqsLambdaRuntime {
    /// Create a new SQS Lambda runtime.
    pub fn new(
        config: &SqsLambdaRuntimeConfig,
        registry: Arc<dyn Registry>,
        repo: Arc<dyn RegistryRepository>,
        queue: Arc<dyn MessageQueue + Send + Sync>,
    ) -> Result<Self, LambdaError> {
        let client = AwsSqsLambdaClient::new(&config.region)?;
        let registry_id = ResourceId::new(&config.registry_addr);
        let id = ResourceId::new(format!(
            "{}/runtimes/{}",
            registry_id.as_str(),
            RuntimeType::Lambda.as_str()
        ));

        Ok(Self {
            id,
            queue,
            registry,
            repo,
            functions: HashMap::new(),
            config: config.clone(),
            client: Box::new(client),
        })
    }

    /// Reconcile deployed functions with registry state.
    fn ensure_functions(&mut self) -> bool {
        let agents = self.registry.get_agents_by_runtime(RuntimeType::Lambda);
        let desired: HashMap<String, &Agent> = agents.iter().map(|a| (a.name.clone(), a)).collect();

        let before = self.functions.len();

        // Remove orphans
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
                Some(AgentStatus::Deploying) if !self.functions.contains_key(name) => {
                    tracing::info!(agent = name.as_str(), "Deploying Lambda function (SQS)");
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

                Some(AgentStatus::Deleting) => {
                    tracing::info!(agent = name.as_str(), "Tearing down Lambda function");
                    self.undeploy(name);
                    let deleted = AgentState::registered(AgentName::new(name))
                        .transition(AgentStatus::Deleted, None);
                    if let Err(e) = self.repo.append_agent_state(&deleted) {
                        tracing::warn!(error = %e, "Failed to set Deleted state");
                    }
                    tracing::info!(agent = name.as_str(), "Lambda function torn down: Deleted");
                }

                _ => {}
            }
        }

        self.functions.len() != before
    }

    /// Deploy: IAM role → Lambda function → SQS event source mapping.
    fn deploy(&mut self, name: &str, agent: &Agent) -> Result<(), LambdaError> {
        let role_name = format!("vlinder-agent-{name}");
        let function_name = format!("vlinder-{name}");

        let role_arn = self.client.create_role(&role_name)?;

        // IAM eventual consistency — Lambda can't assume a role just created.
        std::thread::sleep(std::time::Duration::from_secs(10));

        let env_vars: Vec<(&str, &str)> = vec![
            ("VLINDER_AGENT", &agent.name),
            ("VLINDER_QUEUE_BACKEND", "sqs"),
            ("VLINDER_SQS_REGION", &self.config.region),
            ("VLINDER_SQS_QUEUE_PREFIX", &self.config.sqs_queue_prefix),
            ("VLINDER_REGISTRY_URL", &self.config.registry_addr),
            ("VLINDER_STATE_URL", &self.config.state_url),
        ];

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

        // Wire SQS invoke queue → Lambda function
        let invoke_queue_name = format!("{}-invoke-{name}", self.config.sqs_queue_prefix);
        let queue_url = self.client.get_queue_url(&invoke_queue_name)?;
        let queue_arn = self.client.get_queue_arn(&queue_url)?;
        let mapping_uuid = self
            .client
            .create_event_source_mapping(&function_name, &queue_arn)?;

        self.functions.insert(
            name.to_string(),
            DeployedFunction {
                role_arn,
                function_arn,
                event_source_mapping_uuid: mapping_uuid,
            },
        );

        tracing::info!(
            agent = name,
            function = function_name.as_str(),
            "Lambda function deployed with SQS event source mapping"
        );

        Ok(())
    }

    /// Undeploy: event source mapping → Lambda → IAM role.
    fn undeploy(&mut self, name: &str) {
        if let Some(deployed) = self.functions.get(name) {
            self.client
                .delete_event_source_mapping(&deployed.event_source_mapping_uuid);
        }

        let function_name = format!("vlinder-{name}");
        let role_name = format!("vlinder-agent-{name}");

        self.client.delete_function(&function_name);
        self.client.delete_role(&role_name);

        self.functions.remove(name);
    }
}

impl Runtime for SqsLambdaRuntime {
    fn id(&self) -> &ResourceId {
        &self.id
    }

    fn runtime_type(&self) -> RuntimeType {
        RuntimeType::Lambda
    }

    fn tick(&mut self) -> bool {
        // Push dispatch: no polling loop. Only lifecycle reconciliation.
        self.ensure_functions()
    }

    fn shutdown(&mut self) {
        let names: Vec<String> = self.functions.keys().cloned().collect();
        for name in names {
            tracing::info!(agent = name.as_str(), "Shutting down Lambda function");
            self.undeploy(&name);
        }
    }
}
