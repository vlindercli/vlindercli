//! Agent deployment lifecycle (ADR 121).
//!
//! `AgentState` is a domain object representing the current deployment
//! status of an agent. It is created on each state transition and stored
//! separately from the `Agent` definition (which is a pure manifest).
//! The `agent` field connects it to its `Agent` via `AgentName`.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};

use super::AgentName;

/// Current deployment state of an agent.
///
/// Created on each lifecycle transition. The `agent_states` table holds
/// the latest state; the infra DAG chain records the full history.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentState {
    pub agent: AgentName,
    pub status: AgentStatus,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
}

impl AgentState {
    /// Create an initial state for a newly registered agent.
    pub fn registered(agent: AgentName) -> Self {
        Self {
            agent,
            status: AgentStatus::Registered,
            updated_at: Utc::now(),
            error: None,
        }
    }

    /// Transition to a new status.
    #[must_use]
    pub fn transition(&self, status: AgentStatus, error: Option<String>) -> Self {
        Self {
            agent: self.agent.clone(),
            status,
            updated_at: Utc::now(),
            error,
        }
    }
}

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

    fn agent() -> AgentName {
        AgentName::new("todoapp")
    }

    #[test]
    fn registered_creates_initial_state() {
        let state = AgentState::registered(agent());
        assert_eq!(state.agent, agent());
        assert_eq!(state.status, AgentStatus::Registered);
        assert!(state.error.is_none());
    }

    #[test]
    fn transition_preserves_agent() {
        let initial = AgentState::registered(agent());
        let deploying = initial.transition(AgentStatus::Deploying, None);
        assert_eq!(deploying.agent, agent());
        assert_eq!(deploying.status, AgentStatus::Deploying);
        assert!(deploying.error.is_none());
    }

    #[test]
    fn transition_to_failed_carries_error() {
        let initial = AgentState::registered(agent());
        let failed =
            initial.transition(AgentStatus::Failed, Some("connection refused".to_string()));
        assert_eq!(failed.status, AgentStatus::Failed);
        assert_eq!(failed.error.as_deref(), Some("connection refused"));
    }

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
