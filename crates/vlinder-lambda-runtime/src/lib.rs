//! Lambda runtime — executes agents as AWS Lambda functions.
//!
//! Mirrors `vlinder-podman-runtime` but targets AWS Lambda as the compute
//! backend. Manages IAM roles and Lambda functions for agents assigned to
//! `RuntimeType::Lambda`.

mod config;
mod lambda_client;
mod runtime;

pub use config::LambdaRuntimeConfig;
pub use lambda_client::LambdaError;
pub use runtime::LambdaRuntime;
