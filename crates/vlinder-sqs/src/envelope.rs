//! JSON envelope for SQS message transport.
//!
//! SQS has no subject-based routing, so the routing key is bundled
//! with the message in a JSON envelope.

use serde::{Deserialize, Serialize};

use vlinder_core::domain::QueueError;

#[derive(Serialize, Deserialize)]
pub(crate) struct Envelope<K, M> {
    pub key: K,
    pub msg: M,
}

impl<K: Serialize, M: Serialize> Envelope<K, M> {
    pub fn to_json(&self) -> Result<String, QueueError> {
        serde_json::to_string(self)
            .map_err(|e| QueueError::SendFailed(format!("serialize envelope: {e}")))
    }
}

pub(crate) fn from_json<K, M>(body: &str) -> Result<Envelope<K, M>, QueueError>
where
    K: for<'de> Deserialize<'de>,
    M: for<'de> Deserialize<'de>,
{
    serde_json::from_str(body)
        .map_err(|e| QueueError::ReceiveFailed(format!("deserialize envelope: {e}")))
}
