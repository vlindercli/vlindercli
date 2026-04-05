//! Doltgres-backed SQL provider — postgres wire protocol proxy with version control.

mod connection;
pub mod sidecar;
pub mod wire;
mod worker;

pub use connection::{TcpConnectionFactory, TcpDoltConnection};
pub use worker::{dolt_branch_name, DoltConnection, DoltConnectionFactory, DoltError, DoltWorker};

/// The virtual hostname the sidecar will serve for dolt.
pub const HOSTNAME: &str = "postgres.vlinder.local";
