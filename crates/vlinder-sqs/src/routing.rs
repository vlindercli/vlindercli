//! Routing: maps routing dimensions to SQS queue names (ADR 125).
//!
//! Static queues (cluster-scoped):
//!   {prefix}-deploy      (infra plane)
//!   {prefix}-delete      (infra plane)
//!   {prefix}-fork        (session plane)
//!   {prefix}-promote     (session plane)
//!   {prefix}-request-{svc_type}-{backend}
//!
//! Dynamic queues (per-agent, created at deploy):
//!   {prefix}-invoke-{agent}
//!   {prefix}-complete-{agent}
//!   {prefix}-response-{agent}
//!
//! Every queue has a paired DLQ: {name}-dlq

use vlinder_core::domain::{AgentName, ServiceBackend};

/// SQS queue name for deploy-agent messages.
pub(crate) fn deploy_queue(prefix: &str) -> String {
    format!("{prefix}-deploy")
}

/// SQS queue name for delete-agent messages.
pub(crate) fn delete_queue(prefix: &str) -> String {
    format!("{prefix}-delete")
}

/// SQS queue name for fork messages.
pub(crate) fn fork_queue(prefix: &str) -> String {
    format!("{prefix}-fork")
}

/// SQS queue name for promote messages.
pub(crate) fn promote_queue(prefix: &str) -> String {
    format!("{prefix}-promote")
}

/// Derive the SQS queue name for a service request queue.
pub(crate) fn request_queue(prefix: &str, service: ServiceBackend) -> String {
    format!(
        "{prefix}-request-{}-{}",
        service.service_type().as_str(),
        service.backend_str()
    )
}

/// Derive the SQS queue name for agent invoke messages.
pub(crate) fn invoke_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-invoke-{}", agent.as_str())
}

/// Derive the SQS queue name for agent complete messages.
pub(crate) fn complete_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-complete-{}", agent.as_str())
}

/// Derive the SQS queue name for agent response messages.
pub(crate) fn response_queue(prefix: &str, agent: &AgentName) -> String {
    format!("{prefix}-response-{}", agent.as_str())
}

/// Derive the DLQ name for any queue.
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
        assert_eq!(
            dlq_name("vlinder-invoke-todoapp"),
            "vlinder-invoke-todoapp-dlq"
        );
    }
}
