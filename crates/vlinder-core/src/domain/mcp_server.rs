//! MCP server domain type with resolved tools.

use serde::Serialize;

use super::mcp_manifest::McpManifest;
use super::resource_id::ResourceId;

/// An MCP server with resolved tools, ready for use.
#[derive(Clone, Debug, Serialize)]
pub struct McpServer {
    /// Registry-assigned identity: `<registry_id>/mcp-servers/<name>`.
    /// Set by the registry during registration.
    pub id: ResourceId,
    pub name: String,
    pub url: String,
    /// Tool definitions as opaque JSON values (OpenAI-compatible format for now).
    pub tools: Vec<serde_json::Value>,
}

impl McpServer {
    /// Create a placeholder ID for MCP servers not yet registered.
    ///
    /// Registry replaces this with `<registry_id>/mcp-servers/<name>` during registration.
    pub fn placeholder_id(name: &str) -> ResourceId {
        ResourceId::new(format!("pending-registration://mcp-servers/{name}"))
    }

    /// Create an MCP server from a manifest with loaded tools JSON.
    pub fn from_manifest(manifest: McpManifest, tools: Vec<serde_json::Value>) -> Self {
        McpServer {
            id: Self::placeholder_id(&manifest.name),
            name: manifest.name,
            url: manifest.url,
            tools,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::mcp_manifest::McpManifest;

    #[test]
    fn placeholder_id_has_pending_scheme() {
        let id = McpServer::placeholder_id("brave");
        assert_eq!(id.as_str(), "pending-registration://mcp-servers/brave");
    }

    #[test]
    fn from_manifest_uses_name_and_url() {
        let manifest = McpManifest {
            name: "brave".to_string(),
            url: "http://0.0.0.0:8080/mcp".to_string(),
            tools_file: "brave_tools.json".to_string(),
        };
        let tools = vec![serde_json::json!({ "type": "function" })];
        let server = McpServer::from_manifest(manifest, tools);

        assert_eq!(server.name, "brave");
        assert_eq!(server.url, "http://0.0.0.0:8080/mcp");
        assert_eq!(server.tools.len(), 1);
        assert_eq!(server.tools[0], serde_json::json!({ "type": "function" }));
        assert!(server.id.as_str().contains("pending-registration"));
    }

    #[test]
    fn from_manifest_empty_tools() {
        let manifest = McpManifest {
            name: "empty".to_string(),
            url: "http://localhost:9999/mcp".to_string(),
            tools_file: "empty_tools.json".to_string(),
        };
        let server = McpServer::from_manifest(manifest, Vec::new());

        assert!(server.tools.is_empty());
    }
}
