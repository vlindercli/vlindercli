//! Queue name derivation from routing dimensions.
//!
//! One queue per message type. Every queue has a paired DLQ.

use vlinder_core::domain::{AgentName, ServiceBackend};

// Infra plane
pub(crate) fn deploy_queue(prefix: &str) -> String {
    format!("{prefix}-deploy")
}

pub(crate) fn delete_queue(prefix: &str) -> String {
    format!("{prefix}-delete")
}

// Session plane
pub(crate) fn fork_queue(prefix: &str) -> String {
    format!("{prefix}-fork")
}

pub(crate) fn promote_queue(prefix: &str) -> String {
    format!("{prefix}-promote")
}

// Data plane — per service backend
pub(crate) fn request_queue(prefix: &str, service: ServiceBackend) -> String {
    format!(
        "{prefix}-request-{}-{}",
        service.service_type().as_str(),
        service.backend_str()
    )
}

// Data plane — per agent
pub(crate) fn invoke_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-invoke-{}", agent.as_str())
}

pub(crate) fn complete_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-complete-{}", agent.as_str())
}

pub(crate) fn response_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-response-{}", agent.as_str())
}

pub(crate) fn dlq_name(queue_name: &str) -> String {
    format!("{queue_name}-dlq")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_queue_names() {
        assert_eq!(deploy_queue("dev-vlinder"), "dev-vlinder-deploy");
        assert_eq!(delete_queue("dev-vlinder"), "dev-vlinder-delete");
        assert_eq!(fork_queue("vlinder"), "vlinder-fork");
        assert_eq!(promote_queue("vlinder"), "vlinder-promote");
    }

    #[test]
    fn agent_queue_names() {
        let agent = AgentName::new("todoapp");
        assert_eq!(invoke_queue("vlinder", &agent), "vlinder-invoke-todoapp");
        assert_eq!(
            complete_queue("vlinder", &agent),
            "vlinder-complete-todoapp"
        );
        assert_eq!(
            response_queue("vlinder", &agent),
            "vlinder-response-todoapp"
        );
    }

    #[test]
    fn dlq_suffix() {
        assert_eq!(dlq_name("vlinder-deploy"), "vlinder-deploy-dlq");
    }
}
