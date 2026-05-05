use std::path::Path;
use std::sync::Arc;

use clap::Subcommand;

use crate::config::CliConfig;
use vlinder_core::domain::{McpManifest, McpServer, Registry};

/// Load an MCP server from a TOML manifest and register it with the registry.
pub async fn load_and_register_mcp_server(
    path: &Path,
    registry: &dyn Registry,
) -> Result<McpServer, String> {
    let manifest = McpManifest::load(path)
        .map_err(|e| format!("Failed to load MCP manifest '{}': {}", path.display(), e))?;

    let manifest_dir = path.parent().unwrap_or(Path::new("."));
    let tools = manifest
        .load_tools(manifest_dir)
        .map_err(|e| format!("Failed to load tools JSON: {e}"))?;

    let server = McpServer::from_manifest(manifest, tools);
    registry
        .register_mcp_server(server.clone())
        .await
        .map_err(|e| format!("Failed to register MCP server: {e}"))?;
    Ok(server)
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum McpCommand {
    /// Add an MCP server from a TOML manifest
    Add {
        /// Path to the MCP manifest TOML file
        path: String,
    },

    /// List registered MCP servers
    List,

    /// Remove a registered MCP server
    Remove {
        /// MCP server name to remove
        name: String,
    },
}

pub async fn execute(cmd: McpCommand) {
    let config = CliConfig::load();

    match cmd {
        McpCommand::Add { path } => {
            let registry = open_registry(&config).await;
            let Some(registry) = registry else { return };

            match load_and_register_mcp_server(Path::new(&path), &*registry).await {
                Ok(server) => {
                    println!("Added MCP server '{}':", server.name);
                    println!("  URL:   {}", server.url);
                    println!("  Tools: {}", server.tools.len());
                }
                Err(e) => {
                    eprintln!("{e}");
                }
            }
        }
        McpCommand::List => {
            let registry = open_registry(&config).await;
            let Some(registry) = registry else { return };

            let servers = registry.get_mcp_servers().await;
            if servers.is_empty() {
                println!(
                    "No MCP servers registered yet. Use 'vlinder mcp add <path.toml>' to add one."
                );
                return;
            }
            println!("Registered MCP servers:");
            for server in servers {
                println!(
                    "  {} (URL: {}, {} tools)",
                    server.name,
                    server.url,
                    server.tools.len()
                );
            }
        }
        McpCommand::Remove { name } => {
            let registry = open_registry(&config).await;
            let Some(registry) = registry else { return };

            match registry.delete_mcp_server(&name).await {
                Ok(true) => println!("Removed MCP server '{name}'"),
                Ok(false) => println!("MCP server '{name}' not found"),
                Err(e) => eprintln!("Failed to remove MCP server: {e}"),
            }
        }
    }
}

async fn open_registry(config: &CliConfig) -> Option<Arc<dyn Registry>> {
    super::connect::open_registry(config).await
}
