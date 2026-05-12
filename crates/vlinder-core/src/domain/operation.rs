//! Operation — typed enum for the operation dimension of the routing triple.
//!
//! Part of the (service, backend, operation) routing triple (ADR 043).
//! Each service defines which operations it supports:
//! - Kv: Get, Put, List, Delete
//! - Vec: Store, Search, Delete
//! - Infer: Run, Chat, Generate
//! - Embed: Run

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// The operation being performed on a service.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Get,
    Put,
    List,
    Delete,
    Store,
    Search,
    Run,
    Chat,
    Generate,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Operation::Get => "get",
            Operation::Put => "put",
            Operation::List => "list",
            Operation::Delete => "delete",
            Operation::Store => "store",
            Operation::Search => "search",
            Operation::Run => "run",
            Operation::Chat => "chat",
            Operation::Generate => "generate",
        }
    }
}

impl FromStr for Operation {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "get" => Ok(Operation::Get),
            "put" => Ok(Operation::Put),
            "list" => Ok(Operation::List),
            "delete" => Ok(Operation::Delete),
            "store" => Ok(Operation::Store),
            "search" => Ok(Operation::Search),
            "run" => Ok(Operation::Run),
            "chat" => Ok(Operation::Chat),
            "generate" => Ok(Operation::Generate),
            _ => Err(format!("unknown operation: {s}")),
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Service operation for V2 service dispatch — arbitrary string
/// enabling tool names ("`charge_money`"), service-specific
/// operations, and per-operation observability.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceOperation(pub String);

impl ServiceOperation {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ServiceOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for ServiceOperation {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod service_operation_tests {
    use super::*;

    #[test]
    fn round_trip() {
        let op = ServiceOperation::new("test");
        assert_eq!(op.as_str(), "test");
        assert_eq!(op.0, "test");
    }

    #[test]
    fn display() {
        let op = ServiceOperation::new("echo");
        assert_eq!(format!("{op}"), "echo");
    }

    #[test]
    fn as_ref() {
        let op = ServiceOperation::new("foo");
        let s: &str = op.as_ref();
        assert_eq!(s, "foo");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_round_trips() {
        let ops = [
            Operation::Get,
            Operation::Put,
            Operation::List,
            Operation::Delete,
            Operation::Store,
            Operation::Search,
            Operation::Run,
            Operation::Chat,
            Operation::Generate,
        ];
        for op in ops {
            assert_eq!(Operation::from_str(op.as_str()), Ok(op));
        }
    }

    #[test]
    fn unknown_returns_none() {
        assert!(Operation::from_str("unknown").is_err());
        assert!(Operation::from_str("").is_err());
    }

    #[test]
    fn serde_round_trip() {
        let op = Operation::Store;
        let json = serde_json::to_string(&op).unwrap();
        assert_eq!(json, "\"store\"");
        let parsed: Operation = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, op);
    }

    #[test]
    fn display_matches_as_str() {
        assert_eq!(format!("{}", Operation::Run), "run");
        assert_eq!(format!("{}", Operation::Delete), "delete");
    }
}
