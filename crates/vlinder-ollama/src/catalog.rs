//! Ollama model catalog - queries Ollama API for available models.

use async_trait::async_trait;
use serde::Deserialize;

use vlinder_core::domain::{
    CatalogError, Model, ModelCatalog, ModelInfo, ModelType, Provider, ResourceId,
};

/// Catalog that queries Ollama's API for available models.
pub struct OllamaCatalog {
    endpoint: String,
}

impl OllamaCatalog {
    /// Create a new Ollama catalog.
    ///
    /// - `endpoint`: Ollama server URL (e.g., `<http://localhost:11434>`)
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
        }
    }
}

#[async_trait]
impl ModelCatalog for OllamaCatalog {
    async fn resolve(&self, name: &str) -> Result<Model, CatalogError> {
        // Verify model exists by listing and checking
        let models = self.list().await?;
        let info = models
            .iter()
            .find(|m| m.name == name || m.name.starts_with(&format!("{name}:")))
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;

        // Determine model type from name heuristics
        let model_type = if name.contains("embed") {
            ModelType::Embedding
        } else {
            ModelType::Inference
        };

        let host = self.endpoint.trim_start_matches("http://");
        let digest = info
            .digest
            .clone()
            .ok_or_else(|| CatalogError::Parse("missing digest from Ollama API".to_string()))?;

        Ok(Model {
            id: Model::placeholder_id(&info.name),
            name: info.name.clone(),
            model_type,
            provider: Provider::Ollama,
            model_path: ResourceId::new(format!("ollama://{}/{}", host, info.name)),
            digest,
        })
    }

    async fn list(&self) -> Result<Vec<ModelInfo>, CatalogError> {
        let url = format!("{}/api/tags", self.endpoint);

        let body: TagsResponse = reqwest::get(&url)
            .await
            .map_err(|e| CatalogError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| CatalogError::Parse(e.to_string()))?;

        Ok(body
            .models
            .into_iter()
            .map(|m| ModelInfo {
                name: m.name,
                size: Some(format_size(m.size)),
                modified: Some(m.modified_at),
                digest: Some(format!("sha256:{}", m.digest)),
            })
            .collect())
    }

    async fn available(&self, name: &str) -> bool {
        self.list()
            .await
            .map(|models| {
                models
                    .iter()
                    .any(|m| m.name == name || m.name.starts_with(&format!("{name}:")))
            })
            .unwrap_or(false)
    }
}

#[allow(clippy::cast_precision_loss)] // display-only: sub-byte precision irrelevant
fn format_size(bytes: u64) -> String {
    const GB: u64 = 1024 * 1024 * 1024;
    const MB: u64 = 1024 * 1024;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else {
        format!("{:.0} MB", bytes as f64 / MB as f64)
    }
}

#[derive(Deserialize)]
struct TagsResponse {
    models: Vec<OllamaModel>,
}

#[derive(Deserialize)]
struct OllamaModel {
    name: String,
    size: u64,
    modified_at: String,
    digest: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_catalog_with_endpoint() {
        let catalog = OllamaCatalog::new("http://localhost:11434");
        assert_eq!(catalog.endpoint, "http://localhost:11434");
    }
}
