//! Message types for queue communication (ADR 044).
//!
//! Typed messages with explicit fields for observability:
//! - `InvokeMessage`: Harness → Runtime (start a submission, ADR 121)
//! - `RequestMessage`: Runtime → Service (agent calls a service)
//! - `ResponseMessage`: Service → Runtime (service replies)
//! - `CompleteMessage`: Runtime → Harness (submission finished)
//! - `ForkMessage`: CLI → Platform (create a timeline fork)
//! - `SessionStartMessage`: CLI → Platform (create a conversation session)

/// Serde helper: encode `Vec<u8>` as a base64 string for JSON-friendly transport.
pub(crate) mod base64_serde {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let encoded = String::deserialize(d)?;
        STANDARD.decode(&encoded).map_err(serde::de::Error::custom)
    }
}

pub mod complete;
pub mod fork;
pub mod identity;
pub mod invoke;
pub mod promote;
pub mod request;
pub mod response;
pub mod session_start;

// Re-export everything at the module level for backwards compatibility.
pub use complete::CompleteMessage;
pub use fork::ForkMessageV2;
pub use identity::{
    BranchId, DagNodeId, HarnessType, Instance, MessageId, Sequence, SequenceCounter, SessionId,
    StateHash, SubmissionId,
};
pub use invoke::InvokeMessage;
pub use promote::PromoteMessageV2;
pub use request::RequestMessage;
pub use response::ResponseMessage;
pub use session_start::SessionStartMessageV2;

/// Protocol version stamped on every message at construction time.
pub const PROTOCOL_VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    // All tests in this module previously tested DelegateMessage and
    // DelegateReplyMessage, which have been removed. Complete and invoke
    // message types are now tested via their own modules (complete.rs,
    // invoke.rs) and the data-plane routing tests in routing_key.rs.
}
