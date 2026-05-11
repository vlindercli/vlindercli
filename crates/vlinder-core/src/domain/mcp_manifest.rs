//! MCP manifest as read from MCP TOML files.

use serde::Deserialize;
use std::path::Path;

/// MCP manifest as read from an `<agent_dir>/mcp/<name>.toml` file.
#[derive(Clone, Debug, Deserialize)]
pub struct McpManifest {
    pub name: String,
    pub url: String,
    /// Path to the JSON file containing tool definitions.
    /// Resolved relative to the manifest directory.
    pub tools_file: String,
}

impl McpManifest {
    /// Load an MCP manifest from a file path.
    pub fn load(path: &Path) -> Result<McpManifest, ParseError> {
        let content = std::fs::read_to_string(path)?;
        let manifest: McpManifest = toml::from_str(&content)?;
        Ok(manifest)
    }

    /// Load the tools JSON file, resolving `tools_file` relative to the manifest directory.
    pub fn load_tools(&self, manifest_dir: &Path) -> Result<Vec<serde_json::Value>, ParseError> {
        let tools_path = if Path::new(&self.tools_file).is_absolute() {
            Path::new(&self.tools_file).to_path_buf()
        } else {
            manifest_dir.join(&self.tools_file)
        };

        let content = std::fs::read_to_string(&tools_path)?;
        let tools: Vec<serde_json::Value> =
            serde_json::from_str(&content).map_err(|e| ParseError::Json(e.to_string()))?;
        Ok(tools)
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum ParseError {
    Io(std::io::Error),
    Toml(String),
    Json(String),
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        ParseError::Io(e)
    }
}

impl From<toml::de::Error> for ParseError {
    fn from(e: toml::de::Error) -> Self {
        ParseError::Toml(e.to_string())
    }
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::Io(e) => write!(f, "{e}"),
            ParseError::Toml(e) | ParseError::Json(e) => write!(f, "{e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_manifest() {
        let toml = r#"
            name = "brave"
            url = "http://0.0.0.0:8080/mcp"
            tools_file = "brave_tools.json"
        "#;

        let manifest: McpManifest = toml::from_str(toml).unwrap();
        assert_eq!(manifest.name, "brave");
        assert_eq!(manifest.url, "http://0.0.0.0:8080/mcp");
        assert_eq!(manifest.tools_file, "brave_tools.json");
    }

    #[test]
    fn parse_manifest_missing_name_fails() {
        let toml = r#"
            url = "http://0.0.0.0:8080/mcp"
            tools_file = "tools.json"
        "#;

        let result: Result<McpManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn parse_manifest_missing_url_fails() {
        let toml = r#"
            name = "brave"
            tools_file = "tools.json"
        "#;

        let result: Result<McpManifest, _> = toml::from_str(toml);
        assert!(result.is_err());
    }

    #[test]
    fn load_tools_from_file() {
        let dir = tempfile::tempdir().unwrap();
        let tools_json = r#"[
            {"type": "function", "function": {"name": "brave_web_search"}}
        ]"#;
        std::fs::write(dir.path().join("brave_tools.json"), tools_json).unwrap();

        let manifest = McpManifest {
            name: "brave".to_string(),
            url: "http://0.0.0.0:8080/mcp".to_string(),
            tools_file: "brave_tools.json".to_string(),
        };

        let tools = manifest.load_tools(dir.path()).unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(
            tools[0]["function"]["name"].as_str().unwrap(),
            "brave_web_search"
        );
    }

    #[test]
    fn load_tools_missing_file_fails() {
        let manifest = McpManifest {
            name: "brave".to_string(),
            url: "http://0.0.0.0:8080/mcp".to_string(),
            tools_file: "nonexistent.json".to_string(),
        };

        let dir = tempfile::tempdir().unwrap();
        let result = manifest.load_tools(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn load_manifest_from_path() {
        let dir = tempfile::tempdir().unwrap();
        let toml = r#"
            name = "brave"
            url = "http://0.0.0.0:8080/mcp"
            tools_file = "brave_tools.json"
        "#;
        let manifest_path = dir.path().join("brave.toml");
        std::fs::write(&manifest_path, toml).unwrap();

        let manifest = McpManifest::load(&manifest_path).unwrap();
        assert_eq!(manifest.name, "brave");
        assert_eq!(manifest.url, "http://0.0.0.0:8080/mcp");
    }

    #[test]
    fn load_manifest_invalid_toml_fails() {
        let result: Result<McpManifest, _> = toml::from_str("not valid toml {{{}}}");
        assert!(result.is_err());
    }
}
