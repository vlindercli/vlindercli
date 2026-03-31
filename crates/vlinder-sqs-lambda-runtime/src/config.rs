//! Configuration for the SQS Lambda runtime.

/// Configuration for the SQS Lambda runtime.
///
/// One worker = one region. The worker manages IAM roles, Lambda functions,
/// and SQS event source mappings for all agents assigned to `RuntimeType::Lambda`.
#[derive(Clone, Debug)]
pub struct SqsLambdaRuntimeConfig {
    /// Registry gRPC address for agent discovery.
    pub registry_addr: String,
    /// AWS region for Lambda functions and SQS queues.
    pub region: String,
    /// Memory allocation for Lambda functions in MB.
    pub memory_mb: i32,
    /// Execution timeout for Lambda functions in seconds.
    pub timeout_secs: i32,
    /// SQS queue name prefix (for deriving invoke queue names).
    pub sqs_queue_prefix: String,
    /// State service gRPC address for the lambda adapter.
    pub state_url: String,
    /// VPC subnet IDs for Lambda ENI placement.
    /// Empty vec means no VPC config (Lambda runs in AWS-managed networking).
    pub vpc_subnet_ids: Vec<String>,
    /// VPC security group IDs for Lambda ENIs.
    /// Empty vec means no VPC config.
    pub vpc_security_group_ids: Vec<String>,
}
