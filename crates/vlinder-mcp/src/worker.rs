use anyhow::Result;
use vlinder_core::domain::{
    MessageId, MessageQueue, ResponseV2, ServiceBackendV2, ServiceOperation, SvcMessageKind,
    SvcResponseDiagnostics, SvcRoutingKey,
};
use vlinder_nats::NatsQueue;

use crate::mcp_client::call_mcp_tool;

/// Map MCP provider from routing key to the npx package name.
fn mcp_server_package(key: &SvcRoutingKey) -> String {
    let provider = match &key.kind {
        SvcMessageKind::SvcRequest {
            service: ServiceBackendV2::Mcp(p),
            ..
        } => p.as_str(),
        SvcMessageKind::SvcResponse { .. } => "unknown",
    };
    match provider {
        "server-everything" => "@modelcontextprotocol/server-everything".to_string(),
        "unknown" => "unknown".to_string(),
        _ => format!("@modelcontextprotocol/server-{provider}"),
    }
}

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

pub async fn run_mcp_worker(queue: NatsQueue) -> Result<()> {
    loop {
        let (key, req, ack) = queue
            .receive_svc_request_mcp()
            .await
            .map_err(|e| anyhow::anyhow!("receive error: {e}"))?;

        let _ = ack().await;

        let server_package = mcp_server_package(&key);
        let operation = match &key.kind {
            SvcMessageKind::SvcRequest { operation, .. } => operation.clone(),
            SvcMessageKind::SvcResponse { .. } => ServiceOperation::new("unknown"),
        };

        let result = call_mcp_tool(&server_package, &operation, req.arguments.clone()).await;

        let (content, is_error) = match result {
            Ok(text) => (text, false),
            Err(e) => (e.to_string(), true),
        };

        let response_key = response_key_from_request(&key);
        let tool_name = match &key.kind {
            SvcMessageKind::SvcRequest { operation, .. } => operation.as_str().to_string(),
            SvcMessageKind::SvcResponse { .. } => "unknown".to_string(),
        };
        let content_bytes = content.len() as u64;
        let resp = ResponseV2 {
            id: MessageId::new(),
            dag_id: req.dag_id.clone(),
            correlation_id: req.id,
            state: req.state.clone(),
            diagnostics: SvcResponseDiagnostics {
                server: server_package,
                tool: tool_name,
                round_trip_ms: 0, // deferred: real timing not yet wired
                content_bytes,
                is_error,
            },
            content,
            is_error,
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
    fn mcp_server_package_known_provider() {
        let key = test_request_key("server-everything", "echo");
        assert_eq!(
            mcp_server_package(&key),
            "@modelcontextprotocol/server-everything"
        );
    }

    #[test]
    fn mcp_server_package_unknown_provider() {
        let key = test_request_key("brave", "search");
        assert_eq!(
            mcp_server_package(&key),
            "@modelcontextprotocol/server-brave"
        );
    }

    #[test]
    fn mcp_server_package_response_key_returns_unknown() {
        let key = SvcRoutingKey {
            session: SessionId::new(),
            branch: BranchId::from(1),
            submission: SubmissionId::new(),
            kind: SvcMessageKind::SvcResponse {
                agent: AgentName::new("test_agent"),
                service: ServiceBackendV2::Mcp("server-everything".to_string()),
                operation: ServiceOperation::new("echo"),
                sequence: Sequence::first(),
            },
        };
        assert_eq!(mcp_server_package(&key), "unknown");
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
