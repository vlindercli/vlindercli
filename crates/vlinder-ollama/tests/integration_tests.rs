//! Integration tests that require a running Ollama server.

use vlinder_core::domain::{ModelCatalog, Provider};
use vlinder_ollama::OllamaCatalog;

#[tokio::test]
#[ignore = "requires running Ollama server"]
async fn lists_models_from_ollama() {
    let catalog = OllamaCatalog::new("http://localhost:11434");
    let models = catalog.list().await;
    assert!(models.is_ok());
}

#[tokio::test]
#[ignore = "requires running Ollama server"]
async fn resolves_model_from_ollama() {
    let catalog = OllamaCatalog::new("http://localhost:11434");
    let model = catalog.resolve("phi3").await;
    assert!(model.is_ok());
    let model = model.unwrap();
    assert_eq!(model.provider, Provider::Ollama);
    assert!(model.id.as_str().starts_with("pending-registration://"));
}
