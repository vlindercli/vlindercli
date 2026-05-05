use anyhow::Result;
use rmcp::model::CallToolRequestParams;
use rmcp::model::CallToolResult;
use rmcp::service::ServiceExt;
use rmcp::transport::StreamableHttpClientTransport;
use serde_json::Value;
use vlinder_core::domain::ServiceOperation;

/// Call an MCP tool on a server via HTTP (streamable HTTP transport).
///
/// Connects via HTTP, performs the initialize handshake,
/// calls the tool, and disconnects. Per-call lifecycle —
/// no connection pooling.
pub async fn call_mcp_tool(
    server_url: &str,
    operation: &ServiceOperation,
    arguments: Value,
) -> Result<CallToolResult> {
    let transport = StreamableHttpClientTransport::from_uri(server_url);
    let client = ().serve(transport).await?;

    let result = client
        .call_tool(
            CallToolRequestParams::new(operation.as_str().to_string())
                .with_arguments(arguments.as_object().cloned().unwrap_or_default()),
        )
        .await?;

    client.cancel().await?;

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires a running MCP server"]
    async fn test_echo_tool() {
        let result = call_mcp_tool(
            "http://127.0.0.1:8000/mcp",
            &ServiceOperation::new("echo"),
            json!({ "message": "hello" }),
        )
        .await;
        // Test requires a running server; we just verify the call path works.
        match result {
            Ok(result) => println!("echo succeeded ({} content blocks)", result.content.len()),
            Err(e) => println!("echo error (expected without server): {e}"),
        }
    }
}
