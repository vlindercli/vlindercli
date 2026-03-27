//! Diagnostics types for message observability (ADR 071).
//!
//! Each message type carries diagnostics specific to its emitter.
//! The type system encodes what each platform component guarantees —
//! no shared `Diagnostics` struct with optional fields.
//!
//! | Message   | Diagnostics type       | Emitter                  |
//! |-----------|------------------------|--------------------------|
//! | Invoke    | InvokeDiagnostics      | Harness                  |
//! | Request   | RequestDiagnostics     | Provider server        |
//! | Response  | ServiceDiagnostics     | Service workers          |
//! | Complete  | RuntimeDiagnostics     | Runtime                  |

use serde::{Deserialize, Serialize};

use super::container_id::ContainerId;
use super::image_digest::ImageDigest;
use super::image_ref::ImageRef;
use super::operation::Operation;
use super::service_type::ServiceType;

// ============================================================================
// InvokeDiagnostics — Harness
// ============================================================================

/// Diagnostics emitted by the harness when creating an invoke message.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InvokeDiagnostics {
    /// Harness version (e.g., `env!("CARGO_PKG_VERSION")`).
    pub harness_version: String,
}

// ============================================================================
// RequestDiagnostics — Provider server
// ============================================================================

/// Diagnostics emitted by the bridge when intercepting an agent SDK call.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RequestDiagnostics {
    /// Sequence number within the submission.
    pub sequence: u32,
    /// The bridge endpoint the agent called (e.g., "/infer", "/kv/get").
    pub endpoint: String,
    /// Size of the agent's request body in bytes.
    pub request_bytes: u64,
    /// Timestamp when the bridge received the call (Unix millis).
    pub received_at_ms: u64,
}

// ============================================================================
// ServiceDiagnostics — Service workers (Response)
// ============================================================================

/// Diagnostics emitted by a service worker after processing a request.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ServiceDiagnostics {
    /// Which platform service handled this request.
    pub service: ServiceType,
    /// Backend identifier: "ollama", "openrouter", "sqlite", "memory".
    pub backend: String,
    /// Execution time in milliseconds.
    pub duration_ms: u64,
    /// Service-specific metrics.
    pub metrics: ServiceMetrics,
}

/// Service-specific metrics — the type system enforces which metrics
/// each service kind reports.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ServiceMetrics {
    Inference {
        tokens_input: u32,
        tokens_output: u32,
        model: String,
    },
    Embedding {
        dimensions: u32,
        model: String,
    },
    Storage {
        operation: Operation,
        bytes_transferred: u64,
    },
}

impl Default for ServiceDiagnostics {
    fn default() -> Self {
        Self::placeholder()
    }
}

impl ServiceDiagnostics {
    /// Placeholder diagnostics for when real service metadata is not available.
    ///
    /// Used during NATS deserialization as a fallback for messages from
    /// older senders that don't yet include diagnostics headers.
    pub fn placeholder() -> Self {
        Self {
            service: ServiceType::Kv,
            backend: "unknown".to_string(),
            duration_ms: 0,
            metrics: ServiceMetrics::Storage {
                operation: Operation::Get,
                bytes_transferred: 0,
            },
        }
    }

    /// Convenience constructor for storage service workers (kv, vec).
    pub fn storage(
        service: ServiceType,
        backend: impl Into<String>,
        operation: Operation,
        bytes: u64,
        duration_ms: u64,
    ) -> Self {
        Self {
            service,
            backend: backend.into(),
            duration_ms,
            metrics: ServiceMetrics::Storage {
                operation,
                bytes_transferred: bytes,
            },
        }
    }
}

// ============================================================================
// RuntimeDiagnostics — Runtime (Complete)
// ============================================================================

/// Diagnostics emitted by the runtime on task completion.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RuntimeDiagnostics {
    /// Agent's stderr stream, extracted from the HTTP response.
    /// Stored as a separate binary file in the git tree, not in diagnostics.toml.
    #[serde(skip)]
    pub stderr: Vec<u8>,
    /// Runtime-specific metadata (container, lambda, etc.).
    pub runtime: RuntimeInfo,
    /// Wall-clock execution time in milliseconds.
    pub duration_ms: u64,
    /// Health observation at completion time.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub health: Option<HealthSnapshot>,
}

/// Runtime-specific metadata — populated entirely by the platform.
///
/// Each variant carries the fields meaningful for that runtime type.
/// Mirrors the `ServiceMetrics` pattern (tagged enum per backend).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum RuntimeInfo {
    Container {
        /// Podman engine version (e.g., "5.3.1").
        engine_version: String,
        /// OCI image reference (e.g., "localhost/echo-agent:latest").
        image_ref: Option<ImageRef>,
        /// Image digest (e.g., "sha256:a80c4f17..."), if resolved.
        image_digest: Option<ImageDigest>,
        /// Container ID for this execution.
        container_id: ContainerId,
    },
    Lambda {
        /// Lambda function name (e.g., "vlinder-echo-agent").
        function_name: String,
        /// AWS region (e.g., "us-east-1").
        region: String,
    },
}

impl Default for RuntimeDiagnostics {
    fn default() -> Self {
        Self::placeholder(0)
    }
}

impl RuntimeDiagnostics {
    /// Placeholder diagnostics for when real runtime metadata is not yet available.
    pub fn placeholder(duration_ms: u64) -> Self {
        Self {
            stderr: Vec::new(),
            runtime: RuntimeInfo::Container {
                engine_version: "unknown".to_string(),
                image_ref: None,
                image_digest: None,
                container_id: ContainerId::unknown(),
            },
            duration_ms,
            health: None,
        }
    }
}

// ============================================================================
// HealthSnapshot / HealthWindow — Agent container health
// ============================================================================

/// A single health check observation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct HealthSnapshot {
    /// Unix timestamp in milliseconds when the check was performed.
    pub timestamp_ms: u64,
    /// Round-trip latency of the health check in milliseconds.
    pub latency_ms: u64,
    /// HTTP status code returned (0 if connection failed).
    pub status_code: u16,
}

/// Sliding window of health snapshots, evicted by age.
pub struct HealthWindow {
    snapshots: std::collections::VecDeque<HealthSnapshot>,
    /// Maximum age in milliseconds — snapshots older than this are evicted.
    max_age_ms: u64,
}

impl HealthWindow {
    /// Create a new window with the given maximum age.
    pub fn new(max_age_ms: u64) -> Self {
        Self {
            snapshots: std::collections::VecDeque::new(),
            max_age_ms,
        }
    }

    /// Push a snapshot and evict any that are older than the window.
    pub fn push(&mut self, snapshot: HealthSnapshot) {
        let cutoff = snapshot.timestamp_ms.saturating_sub(self.max_age_ms);
        while self
            .snapshots
            .front()
            .is_some_and(|s| s.timestamp_ms < cutoff)
        {
            self.snapshots.pop_front();
        }
        self.snapshots.push_back(snapshot);
    }

    /// Return all snapshots with timestamp >= `since_ms`.
    pub fn since(&self, since_ms: u64) -> Vec<HealthSnapshot> {
        self.snapshots
            .iter()
            .filter(|s| s.timestamp_ms >= since_ms)
            .cloned()
            .collect()
    }

    /// Whether the most recent snapshot indicates a healthy agent.
    pub fn is_healthy(&self) -> bool {
        self.latest().is_some_and(|s| s.status_code == 200)
    }

    /// Number of snapshots currently in the window.
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }

    /// Whether the window is empty.
    pub fn is_empty(&self) -> bool {
        self.snapshots.is_empty()
    }

    /// The most recent snapshot, if any.
    pub fn latest(&self) -> Option<&HealthSnapshot> {
        self.snapshots.back()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invoke_diagnostics_json_round_trip() {
        let diag = InvokeDiagnostics {
            harness_version: "0.1.0".to_string(),
        };
        let json = serde_json::to_string(&diag).unwrap();
        let back: InvokeDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn invoke_diagnostics_toml_round_trip() {
        let diag = InvokeDiagnostics {
            harness_version: "0.1.0".to_string(),
        };
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        let back: InvokeDiagnostics = toml::from_str(&toml_str).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn request_diagnostics_json_round_trip() {
        let diag = RequestDiagnostics {
            sequence: 1,
            endpoint: "/infer".to_string(),
            request_bytes: 1024,
            received_at_ms: 1_700_000_000_000,
        };
        let json = serde_json::to_string(&diag).unwrap();
        let back: RequestDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn request_diagnostics_toml_round_trip() {
        let diag = RequestDiagnostics {
            sequence: 1,
            endpoint: "/infer".to_string(),
            request_bytes: 1024,
            received_at_ms: 1_700_000_000_000,
        };
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        let back: RequestDiagnostics = toml::from_str(&toml_str).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn service_diagnostics_inference_json_round_trip() {
        let diag = ServiceDiagnostics {
            service: ServiceType::Infer,
            backend: "ollama".to_string(),
            duration_ms: 1800,
            metrics: ServiceMetrics::Inference {
                tokens_input: 512,
                tokens_output: 908,
                model: "phi3:latest".to_string(),
            },
        };
        let json = serde_json::to_string(&diag).unwrap();
        let back: ServiceDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn service_diagnostics_inference_toml_round_trip() {
        let diag = ServiceDiagnostics {
            service: ServiceType::Infer,
            backend: "ollama".to_string(),
            duration_ms: 1800,
            metrics: ServiceMetrics::Inference {
                tokens_input: 512,
                tokens_output: 908,
                model: "phi3:latest".to_string(),
            },
        };
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        let back: ServiceDiagnostics = toml::from_str(&toml_str).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn service_diagnostics_embedding_json_round_trip() {
        let diag = ServiceDiagnostics {
            service: ServiceType::Embed,
            backend: "ollama".to_string(),
            duration_ms: 200,
            metrics: ServiceMetrics::Embedding {
                dimensions: 768,
                model: "nomic-embed-text".to_string(),
            },
        };
        let json = serde_json::to_string(&diag).unwrap();
        let back: ServiceDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn service_diagnostics_storage_json_round_trip() {
        let diag = ServiceDiagnostics::storage(ServiceType::Kv, "sqlite", Operation::Put, 2048, 5);
        let json = serde_json::to_string(&diag).unwrap();
        let back: ServiceDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn service_diagnostics_storage_toml_round_trip() {
        let diag = ServiceDiagnostics::storage(ServiceType::Kv, "sqlite", Operation::Get, 512, 2);
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        let back: ServiceDiagnostics = toml::from_str(&toml_str).unwrap();
        assert_eq!(diag, back);
    }

    #[test]
    fn container_diagnostics_json_round_trip() {
        // stderr is #[serde(skip)] — stored as a separate binary blob (ADR 078).
        // Round-trip only preserves the non-skipped fields.
        let diag = RuntimeDiagnostics {
            stderr: b"INFO: loaded model".to_vec(),
            runtime: RuntimeInfo::Container {
                engine_version: "5.3.1".to_string(),
                image_ref: Some(ImageRef::parse("localhost/echo-agent:latest").unwrap()),
                image_digest: Some(ImageDigest::parse("sha256:abc123").unwrap()),
                container_id: ContainerId::new("def456"),
            },
            duration_ms: 2300,
            health: None,
        };
        let json = serde_json::to_string(&diag).unwrap();
        assert!(!json.contains("stderr"), "stderr should not appear in JSON");
        let back: RuntimeDiagnostics = serde_json::from_str(&json).unwrap();
        assert!(
            back.stderr.is_empty(),
            "stderr defaults to empty on deserialize"
        );
        assert_eq!(back.runtime, diag.runtime);
        assert_eq!(back.duration_ms, diag.duration_ms);
    }

    #[test]
    fn container_diagnostics_toml_round_trip() {
        // stderr is #[serde(skip)] — stored as a separate binary blob (ADR 078).
        let diag = RuntimeDiagnostics {
            stderr: b"WARN: truncated".to_vec(),
            runtime: RuntimeInfo::Container {
                engine_version: "5.3.1".to_string(),
                image_ref: Some(ImageRef::parse("localhost/support-agent:latest").unwrap()),
                image_digest: Some(ImageDigest::parse("sha256:a80c4f17").unwrap()),
                container_id: ContainerId::new("abc123def456"),
            },
            duration_ms: 2300,
            health: None,
        };
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        assert!(
            !toml_str.contains("stderr"),
            "stderr should not appear in TOML"
        );
        let back: RuntimeDiagnostics = toml::from_str(&toml_str).unwrap();
        assert!(
            back.stderr.is_empty(),
            "stderr defaults to empty on deserialize"
        );
        assert_eq!(back.runtime, diag.runtime);
        assert_eq!(back.duration_ms, diag.duration_ms);
    }

    #[test]
    fn runtime_diagnostics_placeholder() {
        let diag = RuntimeDiagnostics::placeholder(100);
        assert!(diag.stderr.is_empty());
        assert_eq!(diag.duration_ms, 100);
        match &diag.runtime {
            RuntimeInfo::Container { engine_version, .. } => {
                assert_eq!(engine_version, "unknown");
            }
            other @ RuntimeInfo::Lambda { .. } => panic!("expected Container, got {other:?}"),
        }
    }

    #[test]
    fn lambda_diagnostics_json_round_trip() {
        let diag = RuntimeDiagnostics {
            stderr: Vec::new(),
            runtime: RuntimeInfo::Lambda {
                function_name: "vlinder-echo-agent".to_string(),
                region: "us-east-1".to_string(),
            },
            duration_ms: 450,
            health: None,
        };
        let json = serde_json::to_string(&diag).unwrap();
        let back: RuntimeDiagnostics = serde_json::from_str(&json).unwrap();
        assert_eq!(back.runtime, diag.runtime);
        assert_eq!(back.duration_ms, 450);
    }

    #[test]
    fn health_window_push_and_evict() {
        let mut w = HealthWindow::new(1000); // 1 second window
        w.push(HealthSnapshot {
            timestamp_ms: 100,
            latency_ms: 5,
            status_code: 200,
        });
        w.push(HealthSnapshot {
            timestamp_ms: 500,
            latency_ms: 3,
            status_code: 200,
        });
        assert_eq!(w.len(), 2);

        // Push one at 1200 — the first (100) is now older than 1000ms window
        w.push(HealthSnapshot {
            timestamp_ms: 1200,
            latency_ms: 4,
            status_code: 200,
        });
        assert_eq!(w.len(), 2); // 100 evicted, 500 and 1200 remain
    }

    #[test]
    fn health_window_since() {
        let mut w = HealthWindow::new(10000);
        w.push(HealthSnapshot {
            timestamp_ms: 100,
            latency_ms: 5,
            status_code: 200,
        });
        w.push(HealthSnapshot {
            timestamp_ms: 200,
            latency_ms: 3,
            status_code: 200,
        });
        w.push(HealthSnapshot {
            timestamp_ms: 300,
            latency_ms: 4,
            status_code: 500,
        });

        let slice = w.since(200);
        assert_eq!(slice.len(), 2);
        assert_eq!(slice[0].timestamp_ms, 200);
        assert_eq!(slice[1].timestamp_ms, 300);
    }

    #[test]
    fn health_window_is_healthy() {
        let mut w = HealthWindow::new(10000);
        assert!(!w.is_healthy()); // empty

        w.push(HealthSnapshot {
            timestamp_ms: 100,
            latency_ms: 5,
            status_code: 200,
        });
        assert!(w.is_healthy());

        w.push(HealthSnapshot {
            timestamp_ms: 200,
            latency_ms: 5,
            status_code: 0,
        });
        assert!(!w.is_healthy()); // connection failed
    }

    #[test]
    fn health_snapshot_json_round_trip() {
        let snap = HealthSnapshot {
            timestamp_ms: 1_700_000_000_000,
            latency_ms: 12,
            status_code: 200,
        };
        let json = serde_json::to_string(&snap).unwrap();
        let back: HealthSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, back);
    }

    #[test]
    fn lambda_diagnostics_toml_round_trip() {
        let diag = RuntimeDiagnostics {
            stderr: Vec::new(),
            runtime: RuntimeInfo::Lambda {
                function_name: "vlinder-echo-agent".to_string(),
                region: "us-east-1".to_string(),
            },
            duration_ms: 450,
            health: None,
        };
        let toml_str = toml::to_string_pretty(&diag).unwrap();
        let back: RuntimeDiagnostics = toml::from_str(&toml_str).unwrap();
        assert_eq!(back.runtime, diag.runtime);
        assert_eq!(back.duration_ms, 450);
    }
}
