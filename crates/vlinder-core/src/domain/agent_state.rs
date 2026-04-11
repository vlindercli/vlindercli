//! Agent deployment lifecycle (ADR 121).

use std::fmt;
use std::str::FromStr;

/// Deployment lifecycle status tag.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentStatus {
    /// Manifest accepted, queued for deployment.
    Registered,
    /// Worker is provisioning (pulling image, creating Lambda, etc.).
    Deploying,
    /// Ready to receive invocations.
    Live,
    /// Deployment failed.
    Failed,
    /// Teardown in progress.
    Deleting,
    /// Fully removed from infrastructure.
    Deleted,
}

impl AgentStatus {
    /// Wire-format string (stored in `SQLite`).
    pub fn as_str(&self) -> &'static str {
        match self {
            AgentStatus::Registered => "registered",
            AgentStatus::Deploying => "deploying",
            AgentStatus::Live => "live",
            AgentStatus::Failed => "failed",
            AgentStatus::Deleting => "deleting",
            AgentStatus::Deleted => "deleted",
        }
    }
}

impl fmt::Display for AgentStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AgentStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "registered" => Ok(AgentStatus::Registered),
            "deploying" => Ok(AgentStatus::Deploying),
            "live" => Ok(AgentStatus::Live),
            "failed" => Ok(AgentStatus::Failed),
            "deleting" => Ok(AgentStatus::Deleting),
            "deleted" => Ok(AgentStatus::Deleted),
            _ => Err(format!("unknown agent status: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trip() {
        for status in [
            AgentStatus::Registered,
            AgentStatus::Deploying,
            AgentStatus::Live,
            AgentStatus::Failed,
            AgentStatus::Deleting,
            AgentStatus::Deleted,
        ] {
            let s = status.as_str();
            let parsed = AgentStatus::from_str(s).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn unknown_status_errors() {
        assert!(AgentStatus::from_str("bogus").is_err());
    }

    #[test]
    fn display_format() {
        assert_eq!(format!("{}", AgentStatus::Live), "live");
        assert_eq!(format!("{}", AgentStatus::Failed), "failed");
    }
}
