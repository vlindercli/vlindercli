use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use vlinder_core::domain::{
    MessageId, MessageQueue, QueueError, Registry, ResponseV2, ServiceBackendV2, ServiceOperation,
    SvcMessageKind, SvcResponseDiagnostics, SvcRoutingKey,
};
use vlinder_nats::NatsQueue;

use crate::mcp_client::call_mcp_tool;

/// Build response key from request key (swap `SvcRequest` → `SvcResponse`).
fn response_key_from_request(req_key: &SvcRoutingKey) -> SvcRoutingKey {
    let kind = match &req_key.kind {
        SvcMessageKind::SvcRequest {
            agent,
            service,
            operation,
            sequence,
        } => SvcMessageKind::SvcResponse {
            agent: agent.clone(),
            service: service.clone(),
            operation: operation.clone(),
            sequence: *sequence,
        },
        SvcMessageKind::SvcResponse { .. } => req_key.kind.clone(),
    };
    SvcRoutingKey {
        session: req_key.session.clone(),
        branch: req_key.branch,
        submission: req_key.submission.clone(),
        kind,
    }
}

/// Extract the provider name from a routing key.
fn provider_from_key(key: &SvcRoutingKey) -> String {
    match &key.kind {
        SvcMessageKind::SvcRequest {
            service: ServiceBackendV2::Mcp(p),
            ..
        }
        | SvcMessageKind::SvcResponse {
            service: ServiceBackendV2::Mcp(p),
            ..
        } => p.clone(),
    }
}

pub async fn run_mcp_worker(queue: NatsQueue, registry: Arc<dyn Registry>) -> Result<()> {
    loop {
        let (key, req, ack) = match queue.receive_svc_request_mcp().await {
            Ok(result) => result,
            Err(QueueError::Timeout) => continue,
            Err(e) => {
                tracing::warn!(error = %e, "MCP worker receive error");
                continue;
            }
        };

        let _ = ack().await;

        let provider_name = provider_from_key(&key);
        let agent_name = match &key.kind {
            SvcMessageKind::SvcRequest { agent, .. } => agent.as_str().to_string(),
            SvcMessageKind::SvcResponse { .. } => String::new(),
        };
        let operation = match &key.kind {
            SvcMessageKind::SvcRequest { operation, .. } => operation.clone(),
            SvcMessageKind::SvcResponse { .. } => ServiceOperation::new("unknown"),
        };

        // Resolve the MCP server URL from the agent's config in the registry.
        let server_url = if agent_name.is_empty() {
            None
        } else {
            registry
                .get_agent_by_name(&agent_name)
                .await
                .and_then(|a| a.requirements.mcp.get(&provider_name).cloned())
                .map(|cfg| cfg.url)
        };

        let arguments: Value = serde_json::from_slice(&req.payload).unwrap_or_default();

        let result = if let Some(url) = server_url {
            call_mcp_tool(&url, &operation, arguments.clone()).await
        } else {
            let msg = if provider_name.is_empty() {
                "MCP worker: no provider in routing key".to_string()
            } else {
                format!("MCP worker: agent '{agent_name}' has no MCP provider '{provider_name}'")
            };
            tracing::warn!("{msg}");
            Err(anyhow::anyhow!("{msg}"))
        };

        let payload = match result {
            Ok(text) => text.into_bytes(),
            Err(e) => e.to_string().into_bytes(),
        };

        let response_key = response_key_from_request(&key);
        let tool_name = match &key.kind {
            SvcMessageKind::SvcRequest { operation, .. } => operation.as_str().to_string(),
            SvcMessageKind::SvcResponse { .. } => "unknown".to_string(),
        };
        let content_bytes = payload.len() as u64;
        let resp = ResponseV2 {
            id: MessageId::new(),
            dag_id: req.dag_id.clone(),
            correlation_id: req.id,
            state: req.state.clone(),
            diagnostics: SvcResponseDiagnostics {
                server: provider_name,
                tool: tool_name,
                round_trip_ms: 0, // deferred: real timing not yet wired
                content_bytes,
            },
            payload,
        };

        queue.send_svc_response(response_key, resp).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{AgentName, BranchId, Sequence, SessionId, SubmissionId};

    fn test_request_key(provider: &str, operation: &str) -> SvcRoutingKey {
        SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcRequest {
                agent: AgentName::new("test_agent"),
                service: ServiceBackendV2::Mcp(provider.to_string()),
                operation: ServiceOperation::new(operation),
                sequence: Sequence::first(),
            },
        }
    }

    #[test]
    fn provider_from_key_request() {
        let key = test_request_key("brave", "search");
        assert_eq!(provider_from_key(&key), "brave".to_string());
    }

    #[test]
    fn provider_from_key_response() {
        let key = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcResponse {
                agent: AgentName::new("test_agent"),
                service: ServiceBackendV2::Mcp("jira".to_string()),
                operation: ServiceOperation::new("get_issue"),
                sequence: Sequence::first(),
            },
        };
        assert_eq!(provider_from_key(&key), "jira".to_string());
    }

    #[test]
    fn provider_from_key_empty() {
        let key = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcResponse {
                agent: AgentName::new("test_agent"),
                service: ServiceBackendV2::Mcp(String::new()),
                operation: ServiceOperation::new("echo"),
                sequence: Sequence::first(),
            },
        };
        assert_eq!(provider_from_key(&key), String::new());
    }

    #[test]
    fn response_key_from_request_swaps_kind() {
        let req_key = test_request_key("jira", "get_issue");
        let resp_key = response_key_from_request(&req_key);

        assert_eq!(resp_key.session, req_key.session);
        assert_eq!(resp_key.branch, req_key.branch);
        assert_eq!(resp_key.submission, req_key.submission);

        let SvcMessageKind::SvcResponse {
            agent,
            service,
            operation,
            sequence,
        } = &resp_key.kind
        else {
            panic!("expected SvcResponse kind");
        };
        assert_eq!(agent.as_str(), "test_agent");
        assert_eq!(service, &ServiceBackendV2::Mcp("jira".to_string()));
        assert_eq!(operation.as_str(), "get_issue");
        assert_eq!(*sequence, Sequence::first());
    }

    #[test]
    fn response_key_from_response_key_is_noop() {
        let key = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcResponse {
                agent: AgentName::new("test_agent"),
                service: ServiceBackendV2::Mcp("brave".to_string()),
                operation: ServiceOperation::new("search"),
                sequence: Sequence::first(),
            },
        };
        let result = response_key_from_request(&key);
        assert_eq!(result.kind, key.kind);
    }
}
