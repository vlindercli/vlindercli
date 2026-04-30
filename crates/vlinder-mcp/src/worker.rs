use anyhow::Result;
#[allow(unused_imports)]
use vlinder_core::domain::{
    MessageId, MessageQueue, RequestV2, ResponseV2, ServiceBackendV2, ServiceOperation,
    SvcMessageKind, SvcRoutingKey,
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
        _ => "unknown",
    };
    match provider {
        "server-everything" => "@modelcontextprotocol/server-everything".to_string(),
        _ => format!("@modelcontextprotocol/server-{provider}"),
    }
}

/// Build response key from request key (swap `SvcRequest` → `SvcResponse`).
#[allow(clippy::match_wildcard_for_single_variants)]
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
        _ => req_key.kind.clone(),
    };
    SvcRoutingKey {
        session: req_key.session.clone(),
        branch: req_key.branch,
        submission: req_key.submission.clone(),
        kind,
    }
}

#[allow(clippy::match_wildcard_for_single_variants)]
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
            _ => ServiceOperation::new("unknown"),
        };

        let result = call_mcp_tool(&server_package, &operation, req.arguments.clone()).await;

        let (content, is_error) = match result {
            Ok(text) => (text, false),
            Err(e) => (e.to_string(), true),
        };

        let response_key = response_key_from_request(&key);
        let resp = ResponseV2 {
            id: MessageId::new(),
            dag_id: req.dag_id.clone(),
            correlation_id: req.id,
            content,
            is_error,
        };

        queue.send_svc_response(response_key, resp).await?;
    }
}
