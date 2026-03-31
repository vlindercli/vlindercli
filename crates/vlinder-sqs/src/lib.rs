//! SQS-backed message queue (ADR 125).
//!
//! Provides the same sync facade as `vlinder-nats`: an internal tokio runtime
//! bridges the async AWS SDK to the synchronous `MessageQueue` trait.

mod queue;
mod routing;

pub use queue::{SqsConfig, SqsQueue};
