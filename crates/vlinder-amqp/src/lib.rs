//! AMQP 0-9-1 queue backend (ADR 126).
//!
//! Implements `MessageQueue` using a topic exchange with routing keys.
//! Compatible with any AMQP 0-9-1 broker (`LavinMQ`, `RabbitMQ`, Amazon MQ).

mod connect;
#[allow(dead_code)]
mod routing;

pub use connect::AmqpConfig;
