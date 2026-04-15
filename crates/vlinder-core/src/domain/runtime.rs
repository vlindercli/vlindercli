//! Runtime trait - agent execution protocol.
//!
//! Defines how agents are registered and executed.

use super::ResourceId;
use async_trait::async_trait;

// ============================================================================
// Runtime Type (compile-time supported runtimes)
// ============================================================================

/// Supported runtime types.
///
/// This is a compile-time enum - adding a new runtime type requires code changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RuntimeType {
    /// OCI container runtime (Podman)
    Container,
    /// AWS Lambda runtime
    Lambda,
}

impl std::fmt::Display for RuntimeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl RuntimeType {
    /// String representation for URI construction.
    pub fn as_str(&self) -> &'static str {
        match self {
            RuntimeType::Container => "container",
            RuntimeType::Lambda => "lambda",
        }
    }
}

impl std::str::FromStr for RuntimeType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "container" => Ok(RuntimeType::Container),
            "lambda" => Ok(RuntimeType::Lambda),
            _ => Err(format!("unknown runtime type: {s}")),
        }
    }
}

// ============================================================================
// Runtime Trait
// ============================================================================

/// Executes agents in response to queue messages.
///
/// The runtime:
/// - Has a unique id (`<registry>/runtimes/<type>`)
/// - Discovers agents from Registry
/// - Polls their input queues
/// - Executes agent code on message arrival
/// - Sends responses to reply queues
#[async_trait(?Send)]
pub trait Runtime {
    /// Unique identifier for this runtime instance.
    /// Format: `<registry_id>/runtimes/<runtime_type>`
    fn id(&self) -> &ResourceId;

    /// The type of runtime (Container, etc.)
    fn runtime_type(&self) -> RuntimeType;

    /// Process agent work. Returns true if work was done.
    async fn tick(&mut self) -> bool;

    /// Release all resources held by this runtime.
    ///
    /// Called on graceful shutdown. Implementations should stop running
    /// agents and clean up any external resources (containers, connections, etc).
    async fn shutdown(&mut self);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock runtime for testing.
    struct MockRuntime {
        id: ResourceId,
        work_available: bool,
    }

    impl MockRuntime {
        fn new() -> Self {
            Self {
                id: ResourceId::new("http://test/runtimes/mock"),
                work_available: false,
            }
        }

        fn set_work_available(&mut self, available: bool) {
            self.work_available = available;
        }
    }

    #[async_trait(?Send)]
    impl Runtime for MockRuntime {
        fn id(&self) -> &ResourceId {
            &self.id
        }

        fn runtime_type(&self) -> RuntimeType {
            RuntimeType::Container
        }

        async fn tick(&mut self) -> bool {
            if self.work_available {
                self.work_available = false;
                true
            } else {
                false
            }
        }

        async fn shutdown(&mut self) {}
    }

    #[tokio::test]
    async fn mock_runtime_implements_trait() {
        let mut runtime: Box<dyn Runtime> = Box::new(MockRuntime::new());

        // No work available
        assert!(!runtime.tick().await);
    }

    #[tokio::test]
    async fn runtime_trait_is_object_safe() {
        // Can use Runtime as a trait object
        async fn use_runtime(runtime: &mut dyn Runtime) {
            runtime.tick().await;
        }

        let mut mock = MockRuntime::new();
        mock.set_work_available(true);
        use_runtime(&mut mock).await;

        // Work was consumed
        assert!(!mock.work_available);
    }
}
