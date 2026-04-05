//! AMQP 0-9-1 queue backend (ADR 126).
//!
//! Implements `MessageQueue` using a topic exchange with routing keys.
//! Compatible with any AMQP 0-9-1 broker (`LavinMQ`, `RabbitMQ`, Amazon MQ).

mod connect;
mod queue;
#[allow(dead_code)] // parse/binding functions used by tests and receive path (step 3)
mod routing;

pub use connect::AmqpConfig;
pub use queue::AmqpQueue;
