//! Service workers - queue-based message handlers.
//!
//! These workers listen on queues and route requests to
//! storage/inference backends.

pub mod dag;
