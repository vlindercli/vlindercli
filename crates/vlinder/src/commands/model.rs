use std::path::Path;
use std::sync::Arc;

use clap::Subcommand;

use crate::config::CliConfig;
use vlinder_catalog::catalog_service::{ping_catalog_service_async, GrpcCatalogClient};
use vlinder_core::domain::{CatalogService, Model, Registry};

/// Load a model from a TOML manifest and register it with the registry.
/// Used by `model add <path.toml>` and by `agent deploy` auto-discovery.
pub async fn load_and_register_model(
    path: &Path,
    registry: &dyn Registry,
) -> Result<Model, String> {
    let model = Model::load(path)
        .map_err(|e| format!("Failed to load model manifest '{}': {}", path.display(), e))?;
    registry
        .register_model(model.clone())
        .await
        .map_err(|e| format!("Failed to register model: {e}"))?;
    Ok(model)
}

#[derive(Subcommand, Debug, PartialEq)]
pub enum ModelCommand {
    /// Add a model from a catalog
    Add {
        /// Model name (e.g., "llama3", "nomic-embed-text")
        name: String,

        /// Catalog to use
        #[arg(short, long, default_value = "ollama")]
        catalog: String,

        /// Ollama endpoint (overrides config)
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// List models available in a catalog
    Available {
        /// Filter models by name (substring match)
        filter: Option<String>,

        /// Catalog to query (default: all)
        #[arg(short, long, default_value = "all")]
        catalog: String,

        /// Ollama endpoint (overrides config)
        #[arg(long)]
        endpoint: Option<String>,
    },

    /// List registered models (added via `model add`)
    List,

    /// Remove a registered model
    Remove {
        /// Model name to remove
        name: String,
    },
}

pub fn execute(cmd: ModelCommand) {
    let rt = tokio::runtime::Runtime::new().expect("Failed to create tokio runtime");
    let config = CliConfig::load();

    match cmd {
        ModelCommand::Add {
            name,
            catalog,
            endpoint: _,
        } => {
            let registry = open_registry(&config);
            let Some(registry) = registry else { return };

            let model = if Path::new(&name)
                .extension()
                .is_some_and(|ext| ext == "toml")
            {
                match rt.block_on(load_and_register_model(Path::new(&name), &*registry)) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("{e}");
                        return;
                    }
                }
            } else {
                let Some(model) = rt.block_on(resolve_from_catalog(&name, &catalog, &config))
                else {
                    return;
                };
                if let Err(e) = rt.block_on(registry.register_model(model.clone())) {
                    eprintln!("Failed to register model: {e}");
                    return;
                }
                model
            };

            println!("Added model '{}':", model.name);
            println!("  Type:   {:?}", model.model_type);
            println!("  Engine: {:?}", model.provider);
            println!("  Path:   {}", model.model_path);
        }
        ModelCommand::Available {
            filter,
            ref catalog,
            endpoint: _,
        } => rt.block_on(list_available(catalog, filter.as_deref(), &config)),
        ModelCommand::List => {
            let registry = open_registry(&config);
            let Some(registry) = registry else { return };

            let models = rt.block_on(registry.get_models());
            if models.is_empty() {
                println!("No models registered yet. Use 'vlinder model add <name>' to add models.");
                return;
            }
            println!("Registered models:");
            for model in models {
                println!(
                    "  {} ({:?}, {:?})",
                    model.name, model.model_type, model.provider
                );
            }
        }
        ModelCommand::Remove { name } => {
            let registry = open_registry(&config);
            let Some(registry) = registry else { return };

            match rt.block_on(registry.delete_model(&name)) {
                Ok(true) => println!("Removed model '{name}'"),
                Ok(false) => println!("Model '{name}' not found"),
                Err(e) => eprintln!("Failed to remove model: {e}"),
            }
        }
    }
}

fn open_registry(config: &CliConfig) -> Option<Arc<dyn Registry>> {
    super::connect::open_registry(config)
}

/// Connect to the daemon's gRPC catalog service.
async fn open_catalog_service(config: &CliConfig) -> Option<GrpcCatalogClient> {
    let catalog_addr = if config.daemon.catalog_addr.starts_with("http://")
        || config.daemon.catalog_addr.starts_with("https://")
    {
        config.daemon.catalog_addr.clone()
    } else {
        format!("http://{}", config.daemon.catalog_addr)
    };

    if ping_catalog_service_async(&catalog_addr).await.is_none() {
        eprintln!("Cannot reach catalog service at {catalog_addr}. Is the daemon running?");
        return None;
    }

    match GrpcCatalogClient::connect_async(&catalog_addr).await {
        Ok(client) => Some(client),
        Err(e) => {
            eprintln!("Failed to connect to catalog service: {e}");
            None
        }
    }
}

/// Resolve a model by name from a catalog backend (Ollama, `OpenRouter`).
async fn resolve_from_catalog(name: &str, catalog: &str, config: &CliConfig) -> Option<Model> {
    let service = open_catalog_service(config).await?;

    match service.resolve(catalog, name).await {
        Ok(m) => Some(m),
        Err(e) => {
            eprintln!("Failed to resolve model '{name}': {e}");
            None
        }
    }
}

async fn list_available(catalog_name: &str, filter: Option<&str>, config: &CliConfig) {
    let Some(service) = open_catalog_service(config).await else {
        return;
    };

    let available_catalogs = service.catalogs().await;
    let catalogs: Vec<&str> = if catalog_name == "all" {
        available_catalogs
            .iter()
            .map(std::string::String::as_str)
            .collect()
    } else if available_catalogs.iter().any(|c| c == catalog_name) {
        vec![catalog_name]
    } else {
        let names = available_catalogs.join(", ");
        eprintln!("Unknown catalog: {catalog_name}. Available: all, {names}");
        return;
    };

    println!("Tip: use --catalog <name> to pick a catalog, or pass a filter to narrow results.");
    println!();

    for name in &catalogs {
        match service.list(name).await {
            Ok(models) => {
                let filtered: Vec<_> = if let Some(q) = filter {
                    let q = q.to_lowercase();
                    models
                        .into_iter()
                        .filter(|m| m.name.to_lowercase().contains(&q))
                        .collect()
                } else {
                    models
                };

                if filtered.is_empty() {
                    continue;
                }

                println!("{} ({} models):", name, filtered.len());
                for model in &filtered {
                    let size = model.size.as_deref().unwrap_or_default();
                    let detail = if size.is_empty() {
                        String::new()
                    } else {
                        format!(" ({size})")
                    };
                    println!("  {}{}", model.name, detail);
                }
                println!();
            }
            Err(e) => {
                eprintln!("  {name} — failed to list: {e}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use vlinder_core::domain::{InMemoryRegistry, InMemorySecretStore, Provider};

    fn test_registry() -> Arc<InMemoryRegistry> {
        let store = Arc::new(InMemorySecretStore::new());
        Arc::new(InMemoryRegistry::new(store))
    }

    /// Write a model TOML that uses a URI `model_path` (no local file needed).
    fn write_model_toml(dir: &Path, filename: &str, name: &str, provider: &str, model_path: &str) {
        let models_dir = dir.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        let content = format!(
            "name = \"{name}\"\ntype = \"inference\"\nprovider = \"{provider}\"\nmodel_path = \"{model_path}\"\n"
        );
        std::fs::write(models_dir.join(filename), content).unwrap();
    }

    // ========================================================================
    // load_and_register_model
    // ========================================================================

    #[tokio::test]
    async fn load_and_register_model_registers_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);

        let model =
            load_and_register_model(&dir.path().join("models/claude-sonnet.toml"), &*registry)
                .await
                .unwrap();

        assert_eq!(model.name, "claude-sonnet");
        // Verify it's actually in the registry
        assert!(registry.get_model("claude-sonnet").await.is_some());
    }

    #[tokio::test]
    async fn load_and_register_model_fails_on_missing_file() {
        let registry = test_registry();
        let result =
            load_and_register_model(Path::new("/nonexistent/model.toml"), &*registry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_and_register_model_fails_when_engine_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        // Don't register OpenRouter engine — should fail at register_model

        let result =
            load_and_register_model(&dir.path().join("models/claude-sonnet.toml"), &*registry)
                .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Failed to register model"));
    }

    #[tokio::test]
    async fn load_and_register_model_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        write_model_toml(
            dir.path(),
            "claude-sonnet.toml",
            "claude-sonnet",
            "openrouter",
            "openrouter://anthropic/claude-sonnet-4",
        );

        let registry = test_registry();
        registry.register_inference_engine(Provider::OpenRouter);

        let path = dir.path().join("models/claude-sonnet.toml");
        let model1 = load_and_register_model(&path, &*registry).await.unwrap();
        let model2 = load_and_register_model(&path, &*registry).await.unwrap();

        assert_eq!(model1.name, model2.name);
        // Still only one model in registry
        assert_eq!(registry.get_models().await.len(), 1);
    }

    // ========================================================================
    // CLI parsing
    // ========================================================================

    #[test]
    fn parses_add_command() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ModelCommand,
        }

        let cli = TestCli::try_parse_from(["test", "add", "llama3"]).unwrap();
        match cli.cmd {
            ModelCommand::Add { name, catalog, .. } => {
                assert_eq!(name, "llama3");
                assert_eq!(catalog, "ollama");
            }
            _ => panic!("Expected Add command"),
        }
    }

    #[test]
    fn parses_list_command() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ModelCommand,
        }

        let cli = TestCli::try_parse_from(["test", "list"]).unwrap();
        assert!(matches!(cli.cmd, ModelCommand::List));
    }

    #[test]
    fn parses_available_command() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ModelCommand,
        }

        let cli = TestCli::try_parse_from(["test", "available"]).unwrap();
        match cli.cmd {
            ModelCommand::Available {
                catalog, filter, ..
            } => {
                assert_eq!(catalog, "all");
                assert_eq!(filter, None);
            }
            _ => panic!("Expected Available command"),
        }
    }

    #[test]
    fn parses_available_with_filter() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ModelCommand,
        }

        let cli = TestCli::try_parse_from(["test", "available", "claude"]).unwrap();
        match cli.cmd {
            ModelCommand::Available {
                filter, catalog, ..
            } => {
                assert_eq!(filter, Some("claude".to_string()));
                assert_eq!(catalog, "all");
            }
            _ => panic!("Expected Available command"),
        }
    }

    #[test]
    fn parses_available_with_catalog_and_filter() {
        use clap::Parser;

        #[derive(Parser)]
        struct TestCli {
            #[command(subcommand)]
            cmd: ModelCommand,
        }

        let cli =
            TestCli::try_parse_from(["test", "available", "--catalog", "openrouter", "claude"])
                .unwrap();
        match cli.cmd {
            ModelCommand::Available {
                filter, catalog, ..
            } => {
                assert_eq!(filter, Some("claude".to_string()));
                assert_eq!(catalog, "openrouter");
            }
            _ => panic!("Expected Available command"),
        }
    }
}
