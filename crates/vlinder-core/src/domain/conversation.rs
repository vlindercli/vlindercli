//! Conversation domain types — message roles and content.
//!
//! Industry-standard convention: each message has a role indicating who
//! spoke, and content shaped by that role.

use serde::{Deserialize, Serialize};

/// A participant's contribution to the conversation, with provenance.
///
/// Industry-standard convention: each message has a role indicating who
/// spoke, and content shaped by that role.
///
/// This type belongs to the conversation bounded context, not the
/// transport context (`InvokeMessage`, `CompleteMessage`).
///
/// The `Agent` variant currently only carries `content`. `Tool_calls`
/// support will be added in a subsequent branch.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "role")]
pub enum Message {
    #[serde(rename = "user")]
    User { content: String },
    #[serde(rename = "agent")]
    Agent { content: String },
}
