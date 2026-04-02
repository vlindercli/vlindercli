//! Configuration for the Lambda runtime.

/// Configuration for the Lambda runtime.
///
/// One worker = one region. The worker manages IAM roles and Lambda functions
/// for all agents assigned to `RuntimeType::Lambda`.
///
/// Platform URLs (nats, state, secret) are passed as environment variables
/// to Lambda functions so the adapter inside the container can connect back
/// to the platform.
#[derive(Clone, Debug)]
pub struct LambdaRuntimeConfig {
    /// Registry gRPC address for agent discovery.
    pub registry_addr: String,
    /// AWS region for Lambda functions (e.g. "us-east-1").
    pub region: String,
    /// Memory allocation for Lambda functions in MB.
    pub memory_mb: i32,
    /// Execution timeout for Lambda functions in seconds.
    pub timeout_secs: i32,
    /// NATS URL for the lambda adapter to connect back to the platform.
    pub nats_url: String,
    /// State service gRPC address for the lambda adapter.
    pub state_url: String,
    /// Secret store gRPC address (optional) for the lambda adapter.
    pub secret_url: Option<String>,
    /// VPC subnet IDs for Lambda ENI placement.
    /// Empty vec means no VPC config (Lambda runs in AWS-managed networking).
    pub vpc_subnet_ids: Vec<String>,
    /// VPC security group IDs for Lambda ENIs.
    /// Empty vec means no VPC config.
    pub vpc_security_group_ids: Vec<String>,
}
