//! Ollama provider — declares the hostname and routes
//! for the Ollama backend, and the worker that processes
//! inference and embedding requests.
//!
//! Routes are scoped to the capabilities an agent declares:
//!
//! Inference:
//! - `/v1/chat/completions` — OpenAI-compatible inference (`Operation::Run`)
//! - `/api/chat` — Ollama native chat (`Operation::Chat`)
//! - `/api/generate` — Ollama native generate (`Operation::Generate`)
//!
//! Embedding:
//! - `/api/embed` — Ollama native embed (`Operation::Run`)

#[cfg(feature = "worker")]
mod catalog;
mod types;
#[cfg(feature = "worker")]
mod worker;

#[cfg(feature = "toolcall")]
mod tool_call_parser;

#[cfg(feature = "worker")]
pub use catalog::OllamaCatalog;

pub use types::{
    OllamaChatRequest, OllamaChatResponse, OllamaEmbedRequest, OllamaEmbedResponse,
    OllamaGenerateRequest, OllamaGenerateResponse,
};
#[cfg(feature = "worker")]
pub use worker::OllamaWorker;

#[cfg(feature = "toolcall")]
pub use tool_call_parser::OpenAiToolCallParser;

use async_openai::types::chat::{CreateChatCompletionRequest, CreateChatCompletionResponse};
use vlinder_core::domain::{
    EmbeddingBackendType, HttpMethod, InferenceBackendType, Operation, ProviderHost, ProviderRoute,
    ServiceBackend,
};

/// The virtual hostname the sidecar will serve for Ollama.
pub const HOSTNAME: &str = "ollama.vlinder.local";

/// Build the provider host declaration for Ollama.
///
/// Only includes routes for capabilities the agent actually declared.
/// Panics if both `infer` and `embed` are false (caller should not
/// create a host with zero routes).
pub fn provider_host(infer: bool, embed: bool) -> ProviderHost {
    assert!(
        infer || embed,
        "provider_host() called with no capabilities"
    );

    let mut routes = Vec::new();

    if infer {
        let backend = ServiceBackend::Infer(InferenceBackendType::Ollama);
        routes.push(ProviderRoute::new::<
            CreateChatCompletionRequest,
            CreateChatCompletionResponse,
        >(
            HttpMethod::Post,
            "/v1/chat/completions",
            backend,
            Operation::Run,
        ));
        routes.push(ProviderRoute::new::<OllamaChatRequest, OllamaChatResponse>(
            HttpMethod::Post,
            "/api/chat",
            backend,
            Operation::Chat,
        ));
        routes.push(ProviderRoute::new::<
            OllamaGenerateRequest,
            OllamaGenerateResponse,
        >(
            HttpMethod::Post,
            "/api/generate",
            backend,
            Operation::Generate,
        ));
    }

    if embed {
        let backend = ServiceBackend::Embed(EmbeddingBackendType::Ollama);
        routes.push(
            ProviderRoute::new::<OllamaEmbedRequest, OllamaEmbedResponse>(
                HttpMethod::Post,
                "/api/embed",
                backend,
                Operation::Run,
            ),
        );
    }

    ProviderHost::new(HOSTNAME, routes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hostname_is_ollama_vlinder_local() {
        assert_eq!(HOSTNAME, "ollama.vlinder.local");
    }

    #[test]
    fn provider_host_has_correct_hostname() {
        let host = provider_host(true, true);
        assert_eq!(host.hostname, "ollama.vlinder.local");
    }

    // --- Route counts per capability ---

    #[test]
    fn both_capabilities_has_four_routes() {
        let host = provider_host(true, true);
        assert_eq!(host.routes.len(), 4);
    }

    #[test]
    fn infer_only_has_three_routes() {
        let host = provider_host(true, false);
        assert_eq!(host.routes.len(), 3);
    }

    #[test]
    fn embed_only_has_one_route() {
        let host = provider_host(false, true);
        assert_eq!(host.routes.len(), 1);
    }

    #[test]
    #[should_panic(expected = "no capabilities")]
    fn no_capabilities_panics() {
        provider_host(false, false);
    }

    // --- Infer-only: no embed route ---

    #[test]
    fn infer_only_has_no_embed_route() {
        let host = provider_host(true, false);
        assert!(host.routes.iter().all(|r| r.path != "/api/embed"));
    }

    // --- Embed-only: no infer routes ---

    #[test]
    fn embed_only_has_no_infer_routes() {
        let host = provider_host(false, true);
        assert!(host.routes.iter().all(|r| r.path != "/v1/chat/completions"));
        assert!(host.routes.iter().all(|r| r.path != "/api/chat"));
        assert!(host.routes.iter().all(|r| r.path != "/api/generate"));
    }

    // --- /v1/chat/completions (OpenAI-compatible) ---

    #[test]
    fn openai_route_is_post_chat_completions() {
        let host = provider_host(true, false);
        let route = &host.routes[0];
        assert_eq!(route.method, HttpMethod::Post);
        assert_eq!(route.path, "/v1/chat/completions");
    }

    #[test]
    fn openai_route_has_correct_backend_and_operation() {
        let host = provider_host(true, false);
        let route = &host.routes[0];
        assert_eq!(
            route.service_backend,
            ServiceBackend::Infer(InferenceBackendType::Ollama)
        );
        assert_eq!(route.operation, Operation::Run);
    }

    #[test]
    fn openai_route_rejects_invalid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[0];
        assert!((route.validate_request)(b"not json").is_err());
    }

    #[test]
    fn openai_route_accepts_valid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[0];
        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        assert!((route.validate_request)(&serde_json::to_vec(&body).unwrap()).is_ok());
    }

    // --- /api/chat (Ollama native) ---

    #[test]
    fn chat_route_is_post_api_chat() {
        let host = provider_host(true, false);
        let route = &host.routes[1];
        assert_eq!(route.method, HttpMethod::Post);
        assert_eq!(route.path, "/api/chat");
    }

    #[test]
    fn chat_route_has_correct_backend_and_operation() {
        let host = provider_host(true, false);
        let route = &host.routes[1];
        assert_eq!(
            route.service_backend,
            ServiceBackend::Infer(InferenceBackendType::Ollama)
        );
        assert_eq!(route.operation, Operation::Chat);
    }

    #[test]
    fn chat_route_rejects_invalid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[1];
        assert!((route.validate_request)(b"not json").is_err());
    }

    #[test]
    fn chat_route_accepts_valid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[1];
        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        assert!((route.validate_request)(&serde_json::to_vec(&body).unwrap()).is_ok());
    }

    // --- /api/generate (Ollama native) ---

    #[test]
    fn generate_route_is_post_api_generate() {
        let host = provider_host(true, false);
        let route = &host.routes[2];
        assert_eq!(route.method, HttpMethod::Post);
        assert_eq!(route.path, "/api/generate");
    }

    #[test]
    fn generate_route_has_correct_backend_and_operation() {
        let host = provider_host(true, false);
        let route = &host.routes[2];
        assert_eq!(
            route.service_backend,
            ServiceBackend::Infer(InferenceBackendType::Ollama)
        );
        assert_eq!(route.operation, Operation::Generate);
    }

    #[test]
    fn generate_route_rejects_invalid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[2];
        assert!((route.validate_request)(b"not json").is_err());
    }

    #[test]
    fn generate_route_accepts_valid_request() {
        let host = provider_host(true, false);
        let route = &host.routes[2];
        let body = serde_json::json!({
            "model": "llama3.2",
            "prompt": "Why is the sky blue?"
        });
        assert!((route.validate_request)(&serde_json::to_vec(&body).unwrap()).is_ok());
    }

    #[test]
    fn generate_route_rejects_chat_request() {
        let host = provider_host(true, false);
        let route = &host.routes[2];
        // chat-shaped payload should fail: has messages, no prompt
        let body = serde_json::json!({
            "model": "llama3.2",
            "messages": [{"role": "user", "content": "hello"}]
        });
        assert!((route.validate_request)(&serde_json::to_vec(&body).unwrap()).is_err());
    }

    // --- /api/embed (Ollama native) ---

    #[test]
    fn embed_route_is_post_api_embed() {
        let host = provider_host(false, true);
        let route = &host.routes[0];
        assert_eq!(route.method, HttpMethod::Post);
        assert_eq!(route.path, "/api/embed");
    }

    #[test]
    fn embed_route_has_correct_backend_and_operation() {
        let host = provider_host(false, true);
        let route = &host.routes[0];
        assert_eq!(
            route.service_backend,
            ServiceBackend::Embed(EmbeddingBackendType::Ollama)
        );
        assert_eq!(route.operation, Operation::Run);
    }

    #[test]
    fn embed_route_rejects_invalid_request() {
        let host = provider_host(false, true);
        let route = &host.routes[0];
        assert!((route.validate_request)(b"not json").is_err());
    }

    #[test]
    fn embed_route_accepts_valid_request() {
        let host = provider_host(false, true);
        let route = &host.routes[0];
        let body = serde_json::json!({
            "model": "nomic-embed-text",
            "input": "hello world"
        });
        assert!((route.validate_request)(&serde_json::to_vec(&body).unwrap()).is_ok());
    }
}
