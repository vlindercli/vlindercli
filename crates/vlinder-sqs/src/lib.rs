//! SQS-backed message queue (ADR 125).
//!
//! Sync facade over the async AWS SDK, mirroring the `NatsQueue` pattern.

mod envelope;
mod queue;
mod routing;

pub use queue::{SqsConfig, SqsQueue};
