//! Lambda + SQS client abstraction — trait + AWS SDK implementation.
//!
//! Extends the Lambda lifecycle (IAM roles, function CRUD) with SQS
//! event source mapping management for push-based invoke delivery.

use std::fmt;

// ── Error type ──────────────────────────────────────────────────────

/// Lambda operation failure.
#[derive(Debug)]
pub enum LambdaError {
    /// Any AWS SDK or API error.
    Aws(String),
}

impl fmt::Display for LambdaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LambdaError::Aws(msg) => write!(f, "{msg}"),
        }
    }
}

// ── Request types ───────────────────────────────────────────────────

/// Parameters for creating a Lambda function.
pub(crate) struct CreateFunctionRequest<'a> {
    pub function_name: &'a str,
    pub ecr_image_uri: &'a str,
    pub role_arn: &'a str,
    pub memory_mb: i32,
    pub timeout_secs: i32,
    pub env_vars: &'a [(&'a str, &'a str)],
    pub vpc_subnet_ids: &'a [String],
    pub vpc_security_group_ids: &'a [String],
}

// ── Trait ────────────────────────────────────────────────────────────

/// Client abstraction over AWS Lambda + IAM + SQS.
pub(crate) trait LambdaClient: Send {
    /// Create an IAM role for a Lambda function.
    /// Returns the role ARN. Idempotent.
    fn create_role(&self, role_name: &str) -> Result<String, LambdaError>;

    /// Delete an IAM role. Fire-and-forget.
    fn delete_role(&self, role_name: &str);

    /// Create a Lambda function from an ECR image.
    /// Returns the function ARN. Idempotent.
    fn create_function(&self, req: &CreateFunctionRequest) -> Result<String, LambdaError>;

    /// Delete a Lambda function. Fire-and-forget.
    fn delete_function(&self, function_name: &str);

    /// Create an SQS event source mapping: queue → Lambda function.
    /// Returns the mapping UUID. Idempotent.
    fn create_event_source_mapping(
        &self,
        function_name: &str,
        sqs_queue_arn: &str,
    ) -> Result<String, LambdaError>;

    /// Delete an SQS event source mapping by UUID. Fire-and-forget.
    fn delete_event_source_mapping(&self, uuid: &str);

    /// Get the ARN of an SQS queue by URL.
    fn get_queue_arn(&self, queue_url: &str) -> Result<String, LambdaError>;

    /// Get the URL of an SQS queue by name.
    fn get_queue_url(&self, queue_name: &str) -> Result<String, LambdaError>;
}

// ── AWS SDK implementation ──────────────────────────────────────────

/// Real AWS Lambda + IAM + SQS client backed by the AWS SDK.
pub(crate) struct AwsSqsLambdaClient {
    rt: tokio::runtime::Runtime,
    lambda: aws_sdk_lambda::Client,
    iam: aws_sdk_iam::Client,
    sqs: aws_sdk_sqs::Client,
}

const LAMBDA_TRUST_POLICY: &str = include_str!("lambda-trust-policy.json");
const LAMBDA_PERMISSIONS_POLICY: &str = include_str!("lambda-permissions-policy.json");

impl AwsSqsLambdaClient {
    pub fn new(region: &str) -> Result<Self, LambdaError> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| LambdaError::Aws(format!("failed to create tokio runtime: {e}")))?;

        let (lambda, iam, sqs) = rt.block_on(async {
            let region = aws_config::Region::new(region.to_string());
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(region)
                .load()
                .await;

            let lambda = aws_sdk_lambda::Client::new(&config);
            let iam = aws_sdk_iam::Client::new(&config);
            let sqs = aws_sdk_sqs::Client::new(&config);
            (lambda, iam, sqs)
        });

        Ok(Self {
            rt,
            lambda,
            iam,
            sqs,
        })
    }
}

impl LambdaClient for AwsSqsLambdaClient {
    fn create_role(&self, role_name: &str) -> Result<String, LambdaError> {
        self.rt.block_on(async {
            let arn = match self
                .iam
                .create_role()
                .role_name(role_name)
                .assume_role_policy_document(LAMBDA_TRUST_POLICY)
                .send()
                .await
            {
                Ok(output) => {
                    let arn = output
                        .role()
                        .map(|r| r.arn().to_string())
                        .unwrap_or_default();
                    tracing::info!(role = role_name, arn = arn.as_str(), "Created IAM role");
                    arn
                }
                Err(sdk_err) => {
                    if is_entity_already_exists(&sdk_err) {
                        let get = self
                            .iam
                            .get_role()
                            .role_name(role_name)
                            .send()
                            .await
                            .map_err(|e| {
                                LambdaError::Aws(format!(
                                    "create_role (get existing): {}",
                                    format_sdk_error(&e)
                                ))
                            })?;
                        get.role().map(|r| r.arn().to_string()).unwrap_or_default()
                    } else {
                        return Err(LambdaError::Aws(format!(
                            "create_role: {}",
                            format_sdk_error(&sdk_err)
                        )));
                    }
                }
            };

            self.iam
                .put_role_policy()
                .role_name(role_name)
                .policy_name("vlinder-lambda-permissions")
                .policy_document(LAMBDA_PERMISSIONS_POLICY)
                .send()
                .await
                .map_err(|e| {
                    LambdaError::Aws(format!("put_role_policy: {}", format_sdk_error(&e)))
                })?;

            Ok(arn)
        })
    }

    fn delete_role(&self, role_name: &str) {
        self.rt.block_on(async {
            let _ = self
                .iam
                .delete_role_policy()
                .role_name(role_name)
                .policy_name("vlinder-lambda-permissions")
                .send()
                .await;

            match self.iam.delete_role().role_name(role_name).send().await {
                Ok(_) => tracing::info!(role = role_name, "Deleted IAM role"),
                Err(e) => tracing::warn!(
                    role = role_name,
                    error = %format_sdk_error(&e),
                    "Failed to delete IAM role"
                ),
            }
        });
    }

    fn create_function(&self, req: &CreateFunctionRequest) -> Result<String, LambdaError> {
        self.rt.block_on(async {
            let mut env_map = std::collections::HashMap::new();
            for (k, v) in req.env_vars {
                env_map.insert(k.to_string(), v.to_string());
            }
            let environment = aws_sdk_lambda::types::Environment::builder()
                .set_variables(Some(env_map))
                .build();

            let code = aws_sdk_lambda::types::FunctionCode::builder()
                .image_uri(req.ecr_image_uri)
                .build();

            let mut builder = self
                .lambda
                .create_function()
                .function_name(req.function_name)
                .role(req.role_arn)
                .code(code)
                .package_type(aws_sdk_lambda::types::PackageType::Image)
                .architectures(aws_sdk_lambda::types::Architecture::Arm64)
                .memory_size(req.memory_mb)
                .timeout(req.timeout_secs)
                .environment(environment);

            if !req.vpc_subnet_ids.is_empty() || !req.vpc_security_group_ids.is_empty() {
                let vpc_config = aws_sdk_lambda::types::VpcConfig::builder()
                    .set_subnet_ids(Some(req.vpc_subnet_ids.to_vec()))
                    .set_security_group_ids(Some(req.vpc_security_group_ids.to_vec()))
                    .build();
                builder = builder.vpc_config(vpc_config);
            }

            match builder.send().await {
                Ok(output) => {
                    let arn = output.function_arn().unwrap_or_default().to_string();
                    tracing::info!(
                        function = req.function_name,
                        arn = arn.as_str(),
                        "Created Lambda function"
                    );
                    Ok(arn)
                }
                Err(sdk_err) => {
                    if is_resource_conflict(&sdk_err) {
                        tracing::debug!(
                            function = req.function_name,
                            "Lambda function already exists"
                        );
                        Ok(String::new())
                    } else {
                        Err(LambdaError::Aws(format!(
                            "create_function: {}",
                            format_sdk_error(&sdk_err)
                        )))
                    }
                }
            }
        })
    }

    fn delete_function(&self, function_name: &str) {
        let result = self.rt.block_on(async {
            self.lambda
                .delete_function()
                .function_name(function_name)
                .send()
                .await
        });
        match result {
            Ok(_) => tracing::info!(function = function_name, "Deleted Lambda function"),
            Err(e) => tracing::warn!(
                function = function_name,
                error = %format_sdk_error(&e),
                "Failed to delete Lambda function"
            ),
        }
    }

    fn create_event_source_mapping(
        &self,
        function_name: &str,
        sqs_queue_arn: &str,
    ) -> Result<String, LambdaError> {
        self.rt.block_on(async {
            match self
                .lambda
                .create_event_source_mapping()
                .function_name(function_name)
                .event_source_arn(sqs_queue_arn)
                .batch_size(1)
                .enabled(true)
                .send()
                .await
            {
                Ok(output) => {
                    let uuid = output.uuid().unwrap_or_default().to_string();
                    tracing::info!(
                        function = function_name,
                        queue_arn = sqs_queue_arn,
                        uuid = uuid.as_str(),
                        "Created SQS event source mapping"
                    );
                    Ok(uuid)
                }
                Err(sdk_err) => {
                    if is_resource_conflict(&sdk_err) {
                        tracing::debug!(
                            function = function_name,
                            "Event source mapping already exists"
                        );
                        Ok(String::new())
                    } else {
                        Err(LambdaError::Aws(format!(
                            "create_event_source_mapping: {}",
                            format_sdk_error(&sdk_err)
                        )))
                    }
                }
            }
        })
    }

    fn delete_event_source_mapping(&self, uuid: &str) {
        if uuid.is_empty() {
            return;
        }
        let result = self.rt.block_on(async {
            self.lambda
                .delete_event_source_mapping()
                .uuid(uuid)
                .send()
                .await
        });
        match result {
            Ok(_) => tracing::info!(uuid = uuid, "Deleted event source mapping"),
            Err(e) => tracing::warn!(
                uuid = uuid,
                error = %format_sdk_error(&e),
                "Failed to delete event source mapping"
            ),
        }
    }

    fn get_queue_arn(&self, queue_url: &str) -> Result<String, LambdaError> {
        self.rt.block_on(async {
            let attrs = self
                .sqs
                .get_queue_attributes()
                .queue_url(queue_url)
                .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
                .send()
                .await
                .map_err(|e| {
                    LambdaError::Aws(format!("get_queue_arn: {}", format_sdk_error(&e)))
                })?;

            attrs
                .attributes()
                .and_then(|a| a.get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn))
                .cloned()
                .ok_or_else(|| LambdaError::Aws("queue ARN not found".into()))
        })
    }

    fn get_queue_url(&self, queue_name: &str) -> Result<String, LambdaError> {
        self.rt.block_on(async {
            self.sqs
                .get_queue_url()
                .queue_name(queue_name)
                .send()
                .await
                .map_err(|e| LambdaError::Aws(format!("get_queue_url: {}", format_sdk_error(&e))))?
                .queue_url
                .ok_or_else(|| LambdaError::Aws(format!("queue {queue_name} has no URL")))
        })
    }
}

// ── SDK error helpers ───────────────────────────────────────────────

fn format_sdk_error<E: std::error::Error>(err: &E) -> String {
    let mut parts = vec![err.to_string()];
    let mut source = err.source();
    while let Some(e) = source {
        parts.push(e.to_string());
        source = e.source();
    }
    parts.join(": ")
}

fn is_entity_already_exists<E: std::fmt::Debug>(err: &aws_sdk_iam::error::SdkError<E>) -> bool {
    format!("{err:?}").contains("EntityAlreadyExists")
}

fn is_resource_conflict<E: std::fmt::Debug>(err: &aws_sdk_lambda::error::SdkError<E>) -> bool {
    format!("{err:?}").contains("ResourceConflictException")
}
