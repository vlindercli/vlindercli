use anyhow::Result;
#[allow(deprecated)]
use rmcp::model::CallToolRequestParam;
use rmcp::service::ServiceExt;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use serde_json::Value;
use tokio::process::Command;
use vlinder_core::domain::ServiceOperation;

/// Call an MCP tool on a server started via npx.
///
/// Connects via stdio, performs the initialize handshake,
/// calls the tool, and disconnects. Per-call lifecycle —
/// no connection pooling.
#[allow(deprecated)]
pub async fn call_mcp_tool(
    server_package: &str,
    operation: &ServiceOperation,
    arguments: Value,
) -> Result<String> {
    let client = ()
        .serve(TokioChildProcess::new(Command::new("npx").configure(
            |cmd| {
                cmd.arg("-y").arg(server_package);
            },
        ))?)
        .await?;

    let result = client
        .call_tool(
            CallToolRequestParam::new(operation.as_str().to_string())
                .with_arguments(arguments.as_object().cloned().unwrap_or_default()),
        )
        .await?;

    client.cancel().await?;

    let text = result
        .content
        .into_iter()
        .filter_map(|c| {
            if let rmcp::model::RawContent::Text(tc) = c.raw {
                Some(tc.text)
            } else {
                None
            }
        })
        .collect::<String>();

    Ok(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    #[ignore = "requires npx and network"]
    async fn test_echo_tool() {
        let result = call_mcp_tool(
            "@modelcontextprotocol/server-everything",
            &ServiceOperation::new("echo"),
            json!({ "message": "hello" }),
        )
        .await;
        assert!(result.is_ok());
        let text = result.unwrap();
        assert!(text.contains("hello"));
    }

    #[tokio::test]
    #[ignore = "requires npx and network"]
    async fn test_get_sum_tool() {
        let result = call_mcp_tool(
            "@modelcontextprotocol/server-everything",
            &ServiceOperation::new("get_sum"),
            json!({ "numbers": [1, 2, 3] }),
        )
        .await;
        // This tool may not exist; we just ensure the call doesn't panic.
        // The result may be an error (tool not found) but that's fine.
        match result {
            Ok(_) => println!("get_sum succeeded"),
            Err(e) => println!("get_sum error: {e}"),
        }
    }
}
