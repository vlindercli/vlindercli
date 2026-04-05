//! Service type — identifies which platform service handles a request.
//!
//! The routing triple (`ServiceType`, Backend, Operation) determines which
//! worker processes a request. `ServiceType` is the first dimension.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Platform service type.
///
/// Each variant corresponds to a service subsystem with its own workers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceType {
    /// Object storage (key-value).
    Kv,
    /// Vector storage (embeddings).
    Vec,
    /// LLM inference.
    Infer,
    /// Text embedding.
    Embed,
    /// SQL storage (e.g. Doltgres).
    Sql,
}

impl ServiceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ServiceType::Kv => "kv",
            ServiceType::Vec => "vec",
            ServiceType::Infer => "infer",
            ServiceType::Embed => "embed",
            ServiceType::Sql => "sql",
        }
    }
}

impl FromStr for ServiceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "kv" => Ok(ServiceType::Kv),
            "vec" => Ok(ServiceType::Vec),
            "infer" => Ok(ServiceType::Infer),
            "embed" => Ok(ServiceType::Embed),
            "sql" => Ok(ServiceType::Sql),
            _ => Err(format!("unknown service type: {s}")),
        }
    }
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_str_values() {
        assert_eq!(ServiceType::Kv.as_str(), "kv");
        assert_eq!(ServiceType::Vec.as_str(), "vec");
        assert_eq!(ServiceType::Infer.as_str(), "infer");
        assert_eq!(ServiceType::Embed.as_str(), "embed");
    }

    #[test]
    fn from_str_round_trip() {
        for st in [
            ServiceType::Kv,
            ServiceType::Vec,
            ServiceType::Infer,
            ServiceType::Embed,
        ] {
            assert_eq!(ServiceType::from_str(st.as_str()), Ok(st));
        }
    }

    #[test]
    fn from_str_unknown() {
        assert!(ServiceType::from_str("unknown").is_err());
        assert!(ServiceType::from_str("").is_err());
    }

    #[test]
    fn display() {
        assert_eq!(format!("{}", ServiceType::Kv), "kv");
        assert_eq!(format!("{}", ServiceType::Infer), "infer");
    }

    #[test]
    fn json_round_trip() {
        let st = ServiceType::Kv;
        let json = serde_json::to_string(&st).unwrap();
        assert_eq!(json, r#""kv""#);
        let back: ServiceType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, st);
    }

    #[test]
    fn toml_round_trip() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Wrapper {
            service: ServiceType,
        }

        let w = Wrapper {
            service: ServiceType::Vec,
        };
        let toml_str = toml::to_string(&w).unwrap();
        let back: Wrapper = toml::from_str(&toml_str).unwrap();
        assert_eq!(w, back);
    }
}
