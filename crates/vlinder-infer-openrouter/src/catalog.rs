//! `OpenRouter` model catalog - queries `OpenRouter` API for available models.

use async_trait::async_trait;
use serde::Deserialize;

use vlinder_core::domain::{
    CatalogError, Model, ModelCatalog, ModelInfo, ModelType, Provider, ResourceId,
};

/// Catalog that queries `OpenRouter`'s API for available models.
pub struct OpenRouterCatalog {
    endpoint: String,
    api_key: String,
}

impl OpenRouterCatalog {
    /// Create a new `OpenRouter` catalog.
    ///
    /// - `endpoint`: `OpenRouter` API URL (e.g., `<https://openrouter.ai/api/v1>`)
    /// - `api_key`: `OpenRouter` API key for authentication
    pub fn new(endpoint: impl Into<String>, api_key: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            api_key: api_key.into(),
        }
    }
}

#[async_trait]
impl ModelCatalog for OpenRouterCatalog {
    async fn resolve(&self, name: &str) -> Result<Model, CatalogError> {
        let models = self.list().await?;
        let info = models
            .iter()
            .find(|m| m.name == name)
            .ok_or_else(|| CatalogError::NotFound(name.to_string()))?;

        let model_type = if name.contains("embed") {
            ModelType::Embedding
        } else {
            ModelType::Inference
        };

        Ok(Model {
            id: Model::placeholder_id(&info.name),
            name: info.name.clone(),
            model_type,
            provider: Provider::OpenRouter,
            model_path: ResourceId::new(format!("openrouter://{}", info.name)),
            digest: String::new(),
        })
    }

    async fn list(&self) -> Result<Vec<ModelInfo>, CatalogError> {
        let url = format!("{}/models", self.endpoint);

        let client = reqwest::Client::new();
        let mut request = client.get(&url);
        if !self.api_key.is_empty() {
            request = request.header("Authorization", format!("Bearer {}", self.api_key));
        }

        let body: ModelsResponse = request
            .send()
            .await
            .map_err(|e| CatalogError::Network(e.to_string()))?
            .json()
            .await
            .map_err(|e| CatalogError::Parse(e.to_string()))?;

        Ok(body
            .data
            .into_iter()
            .map(|m| ModelInfo {
                name: m.id,
                size: None,
                modified: None,
                digest: None,
            })
            .collect())
    }

    async fn available(&self, name: &str) -> bool {
        self.list()
            .await
            .map(|models| models.iter().any(|m| m.name == name))
            .unwrap_or(false)
    }
}

#[derive(Deserialize)]
struct ModelsResponse {
    data: Vec<OpenRouterModel>,
}

#[derive(Deserialize)]
struct OpenRouterModel {
    id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_catalog_with_endpoint() {
        let catalog = OpenRouterCatalog::new("https://openrouter.ai/api/v1", "test-key");
        assert_eq!(catalog.endpoint, "https://openrouter.ai/api/v1");
        assert_eq!(catalog.api_key, "test-key");
    }
}
