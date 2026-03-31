//! SQS Lambda runtime — executes agents as AWS Lambda functions with
//! push-based invoke delivery via SQS event source mappings (ADR 125).
//!
//! Unlike `vlinder-nats-lambda-runtime` (which polls NATS and calls the
//! Lambda API), this runtime wires SQS queues directly to Lambda functions.
//! No polling loop — SQS delivers invokes as events.

mod config;
mod lambda_client;
mod runtime;

pub use config::SqsLambdaRuntimeConfig;
pub use lambda_client::LambdaError;
pub use runtime::SqsLambdaRuntime;
