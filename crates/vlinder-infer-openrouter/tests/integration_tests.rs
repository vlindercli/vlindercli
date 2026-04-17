//! Integration tests that require an `OpenRouter` API key.
//!
//! These tests skip gracefully if `VLINDER_OPENROUTER_API_KEY` is not set.

use vlinder_core::domain::{ModelCatalog, Provider};
use vlinder_infer_openrouter::OpenRouterCatalog;

/// Return the API key if set, or print a skip message and return None.
fn openrouter_key_or_skip() -> Option<String> {
    match std::env::var("VLINDER_OPENROUTER_API_KEY") {
        Ok(key) if !key.is_empty() => Some(key),
        _ => {
            eprintln!("VLINDER_OPENROUTER_API_KEY not set — skipping");
            None
        }
    }
}

#[tokio::test]
#[ignore = "requires OpenRouter API key"]
async fn lists_models_from_openrouter() {
    let Some(key) = openrouter_key_or_skip() else {
        return;
    };
    let catalog = OpenRouterCatalog::new("https://openrouter.ai/api/v1", &key);
    let models = catalog.list().await;
    assert!(models.is_ok());
    assert!(!models.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires OpenRouter API key"]
async fn resolves_model_from_openrouter() {
    let Some(key) = openrouter_key_or_skip() else {
        return;
    };
    let catalog = OpenRouterCatalog::new("https://openrouter.ai/api/v1", &key);
    let model = catalog.resolve("anthropic/claude-sonnet-4").await;
    assert!(model.is_ok());
    let model = model.unwrap();
    assert_eq!(model.provider, Provider::OpenRouter);
    assert!(model.id.as_str().starts_with("pending-registration://"));
}
