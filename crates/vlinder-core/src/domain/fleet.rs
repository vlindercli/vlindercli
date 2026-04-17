use std::collections::HashSet;

use super::fleet_manifest::{FleetManifest, ParseError};
use super::registry::Registry;
use super::resource_id::ResourceId;

/// A fleet is a composition boundary for agents.
///
/// Stored in the registry after deployment. References agents by their
/// registry-issued `ResourceId`s — names are resolved via the registry
/// during construction.
///
/// See ADR 022 for the manifest format.
#[derive(Clone, Debug, PartialEq)]
pub struct Fleet {
    pub id: ResourceId,
    pub name: String,
    pub entry: ResourceId,
    pub agents: HashSet<ResourceId>,
}

impl Fleet {
    /// Build a Fleet from a manifest, resolving agent names via the registry.
    ///
    /// All agents referenced in the manifest must already be registered.
    pub async fn from_manifest(
        manifest: FleetManifest,
        registry: &dyn Registry,
    ) -> Result<Fleet, LoadError> {
        let entry_id = registry.agent_id(&manifest.entry).await.ok_or_else(|| {
            LoadError::Validation(format!(
                "entry agent '{}' is not registered",
                manifest.entry
            ))
        })?;

        let mut agents = HashSet::with_capacity(manifest.agents.len());
        for name in manifest.agents.keys() {
            let id = registry.agent_id(name).await.ok_or_else(|| {
                LoadError::Validation(format!("agent '{name}' is not registered"))
            })?;
            agents.insert(id);
        }

        Ok(Fleet {
            id: Self::placeholder_id(&manifest.name),
            name: manifest.name,
            entry: entry_id,
            agents,
        })
    }

    /// Placeholder ID before registry assigns a real one.
    pub fn placeholder_id(name: &str) -> ResourceId {
        ResourceId::new(format!("pending-registration://fleets/{name}"))
    }
}

#[derive(Debug)]
pub enum LoadError {
    Io(std::io::Error),
    Parse(String),
    Validation(String),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LoadError::Io(e) => write!(f, "IO error: {e}"),
            LoadError::Parse(s) => write!(f, "parse error: {s}"),
            LoadError::Validation(s) => write!(f, "validation error: {s}"),
        }
    }
}

impl From<std::io::Error> for LoadError {
    fn from(e: std::io::Error) -> Self {
        LoadError::Io(e)
    }
}

impl From<ParseError> for LoadError {
    fn from(e: ParseError) -> Self {
        match e {
            ParseError::Io(e) => LoadError::Io(e),
            ParseError::Toml(s) => LoadError::Parse(s),
            ParseError::Validation(s) => LoadError::Validation(s),
        }
    }
}
