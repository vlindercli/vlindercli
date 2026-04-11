//! Readiness checks for multi-worker operations.
//!
//! When an agent is deployed, multiple workers must complete their part
//! (compute, storage, etc.). Each worker appends a `ReadinessCheck` when
//! it starts (pending) and when it finishes (ready/failed). The agent's
//! readiness is derived from the latest check per worker.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};

use super::AgentName;

/// Status of a single worker's readiness check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadinessStatus {
    Pending,
    Ready,
    Failed,
    Deleting,
    Deleted,
}

impl ReadinessStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ReadinessStatus::Pending => "pending",
            ReadinessStatus::Ready => "ready",
            ReadinessStatus::Failed => "failed",
            ReadinessStatus::Deleting => "deleting",
            ReadinessStatus::Deleted => "deleted",
        }
    }
}

impl fmt::Display for ReadinessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReadinessStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(ReadinessStatus::Pending),
            "ready" => Ok(ReadinessStatus::Ready),
            "failed" => Ok(ReadinessStatus::Failed),
            "deleting" => Ok(ReadinessStatus::Deleting),
            "deleted" => Ok(ReadinessStatus::Deleted),
            _ => Err(format!("unknown readiness status: {s}")),
        }
    }
}

/// A readiness check for one worker's contribution to an agent deploy.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadinessCheck {
    pub agent: AgentName,
    pub worker: String,
    pub status: ReadinessStatus,
    pub updated_at: DateTime<Utc>,
    pub error: Option<String>,
}

impl ReadinessCheck {
    /// Create a pending check for a worker.
    pub fn pending(agent: AgentName, worker: &str) -> Self {
        Self {
            agent,
            worker: worker.to_string(),
            status: ReadinessStatus::Pending,
            updated_at: Utc::now(),
            error: None,
        }
    }

    /// Mark this check as ready.
    #[must_use]
    pub fn ready(&self) -> Self {
        Self {
            agent: self.agent.clone(),
            worker: self.worker.clone(),
            status: ReadinessStatus::Ready,
            updated_at: Utc::now(),
            error: None,
        }
    }

    /// Mark this check as failed.
    #[must_use]
    pub fn failed(&self, error: String) -> Self {
        Self {
            agent: self.agent.clone(),
            worker: self.worker.clone(),
            status: ReadinessStatus::Failed,
            updated_at: Utc::now(),
            error: Some(error),
        }
    }

    /// Mark this check as deleting.
    #[must_use]
    pub fn deleting(&self) -> Self {
        Self {
            agent: self.agent.clone(),
            worker: self.worker.clone(),
            status: ReadinessStatus::Deleting,
            updated_at: Utc::now(),
            error: None,
        }
    }

    /// Mark this check as deleted.
    #[must_use]
    pub fn deleted(&self) -> Self {
        Self {
            agent: self.agent.clone(),
            worker: self.worker.clone(),
            status: ReadinessStatus::Deleted,
            updated_at: Utc::now(),
            error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_check() {
        let check = ReadinessCheck::pending(AgentName::new("todoapp"), "container");
        assert_eq!(check.status, ReadinessStatus::Pending);
        assert_eq!(check.worker, "container");
        assert!(check.error.is_none());
    }

    #[test]
    fn transition_to_ready() {
        let pending = ReadinessCheck::pending(AgentName::new("todoapp"), "lambda");
        let ready = pending.ready();
        assert_eq!(ready.status, ReadinessStatus::Ready);
        assert_eq!(ready.worker, "lambda");
        assert!(ready.error.is_none());
    }

    #[test]
    fn transition_to_failed() {
        let pending = ReadinessCheck::pending(AgentName::new("todoapp"), "s3files");
        let failed = pending.failed("timeout".to_string());
        assert_eq!(failed.status, ReadinessStatus::Failed);
        assert_eq!(failed.error.as_deref(), Some("timeout"));
    }

    #[test]
    fn transition_to_deleting() {
        let ready = ReadinessCheck::pending(AgentName::new("todoapp"), "container").ready();
        let deleting = ready.deleting();
        assert_eq!(deleting.status, ReadinessStatus::Deleting);
        assert!(deleting.error.is_none());
    }

    #[test]
    fn transition_to_deleted() {
        let deleting = ReadinessCheck::pending(AgentName::new("todoapp"), "container")
            .ready()
            .deleting();
        let deleted = deleting.deleted();
        assert_eq!(deleted.status, ReadinessStatus::Deleted);
    }

    #[test]
    fn status_round_trip() {
        for status in [
            ReadinessStatus::Pending,
            ReadinessStatus::Ready,
            ReadinessStatus::Failed,
            ReadinessStatus::Deleting,
            ReadinessStatus::Deleted,
        ] {
            let s = status.as_str();
            let parsed = ReadinessStatus::from_str(s).unwrap();
            assert_eq!(parsed, status);
        }
    }
}
