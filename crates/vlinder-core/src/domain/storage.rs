//! Storage domain types.
//!
//! Enums describing available storage implementations.
//! Used for registry tracking and queue routing.

use serde::Deserialize;

// ============================================================================
// ObjectStorageType (available implementations)
// ============================================================================

/// Available object storage implementations.
///
/// Registered with the Registry to track what backends are available.
/// Follows the same pattern as `RuntimeType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, Deserialize)]
pub enum ObjectStorageType {
    /// SQLite-backed storage.
    Sqlite,
    /// In-memory storage (for testing).
    InMemory,
    /// S3-backed storage (versioned via S3 object versioning).
    S3,
}

impl ObjectStorageType {
    /// String representation for queue routing.
    pub fn as_str(&self) -> &'static str {
        match self {
            ObjectStorageType::Sqlite => "sqlite",
            ObjectStorageType::InMemory => "memory",
            ObjectStorageType::S3 => "s3",
        }
    }

    /// Determine storage type from URI scheme.
    pub fn from_scheme(scheme: Option<&str>) -> Option<Self> {
        match scheme {
            Some("sqlite") => Some(ObjectStorageType::Sqlite),
            Some("memory") => Some(ObjectStorageType::InMemory),
            Some("s3") => Some(ObjectStorageType::S3),
            _ => None,
        }
    }
}

// ============================================================================
// VectorStorageType (available implementations)
// ============================================================================

/// Available vector storage implementations.
///
/// Registered with the Registry to track what backends are available.
/// Follows the same pattern as `RuntimeType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize, Deserialize)]
pub enum VectorStorageType {
    /// SQLite-backed storage with sqlite-vec extension.
    SqliteVec,
    /// In-memory storage (for testing).
    InMemory,
}

impl VectorStorageType {
    /// String representation for queue routing.
    pub fn as_str(&self) -> &'static str {
        match self {
            VectorStorageType::SqliteVec => "sqlite-vec",
            VectorStorageType::InMemory => "memory",
        }
    }

    /// Determine storage type from URI scheme.
    pub fn from_scheme(scheme: Option<&str>) -> Option<Self> {
        match scheme {
            Some("sqlite") => Some(VectorStorageType::SqliteVec),
            Some("memory") => Some(VectorStorageType::InMemory),
            _ => None,
        }
    }
}
