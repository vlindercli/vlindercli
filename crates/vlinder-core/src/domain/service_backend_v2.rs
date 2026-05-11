//! V2 service backend — independent of V1 `ServiceBackend`.
//!
//! Grows as each service type migrates from V1 to V2.
//! Today: only MCP. When kv migrates, add `Kv(ObjectStorageType)`.
//! When infer migrates, add `Infer(InferenceBackendType)`.
//! Each migration is one variant, one worker update, one subscription change.

/// V2 service backend — used by the harness-mediated dispatch path.
///
/// Unlike V1's `ServiceBackend` (closed enum, `Copy`), V2 is an
/// open-ended enum that grows incrementally via strangler fig.
/// It is NOT `Copy` — `Mcp(String)` can't be Copy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum ServiceBackendV2 {
    /// MCP service backend. The String is the provider name
    /// ("brave", "jira", "server-everything"). Transport
    /// (stdio vs HTTP) is the worker's concern, not a
    /// routing dimension.
    Mcp(String),
}

impl ServiceBackendV2 {
    /// The service type string for this backend (NATs subject dimension).
    /// Returns `&'static str` to avoid depending on V1's `ServiceType` enum.
    pub fn service_type_str(&self) -> &'static str {
        match self {
            ServiceBackendV2::Mcp(_) => "mcp",
        }
    }

    /// The backend identifier string for this backend (NATs subject dimension).
    pub fn backend_str(&self) -> &str {
        match self {
            ServiceBackendV2::Mcp(provider) => provider.as_str(),
        }
    }

    /// Construct from service type + backend strings (NATs subject parsing).
    pub fn from_parts(service_type: &str, backend: &str) -> Option<Self> {
        match service_type {
            "mcp" => Some(Self::Mcp(backend.to_string())),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_type_str_mcp() {
        let backend = ServiceBackendV2::Mcp("server-everything".to_string());
        assert_eq!(backend.service_type_str(), "mcp");
    }

    #[test]
    fn backend_str_mcp() {
        let backend = ServiceBackendV2::Mcp("brave".to_string());
        assert_eq!(backend.backend_str(), "brave");
    }

    #[test]
    fn from_parts_mcp() {
        let backend = ServiceBackendV2::from_parts("mcp", "jira").unwrap();
        assert_eq!(backend, ServiceBackendV2::Mcp("jira".to_string()));
    }

    #[test]
    fn from_parts_unknown_service_type() {
        let backend = ServiceBackendV2::from_parts("unknown", "foo");
        assert!(backend.is_none());
    }

    #[test]
    fn serde_round_trip() {
        let original = ServiceBackendV2::Mcp("server-everything".to_string());
        let json = serde_json::to_string(&original).unwrap();
        let back: ServiceBackendV2 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn equality_and_hashing() {
        use std::collections::HashSet;
        let a = ServiceBackendV2::Mcp("a".to_string());
        let b = ServiceBackendV2::Mcp("a".to_string());
        let c = ServiceBackendV2::Mcp("c".to_string());
        assert_eq!(a, b);
        assert_ne!(a, c);
        let mut set = HashSet::new();
        set.insert(a);
        assert!(set.contains(&b));
        assert!(!set.contains(&c));
    }
}
