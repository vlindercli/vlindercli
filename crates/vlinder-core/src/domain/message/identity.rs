//! Identity types carried by messages.
//!
//! Small newtypes for message dimensions: who, what, when, where.

use std::fmt;
use uuid::Uuid;

use serde::{Deserialize, Serialize};

// --- MessageId ---

/// Unique identifier for a message.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MessageId(String);

impl MessageId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for MessageId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// --- ToolCallId ---

/// Unique identifier for a tool call within a turn.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ToolCallId(String);

impl ToolCallId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for ToolCallId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for ToolCallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ToolCallId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// --- DagNodeId (ADR 067) ---

/// Content-addressed hash identifying a single DAG node.
///
/// Value is SHA-256(payload || `parent_hash` || `message_type` || diagnostics).
/// Used as the primary key of the Merkle chain — every node's `parent_hash`
/// references another `DagNodeId` (or is empty for the root).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DagNodeId(String);

impl DagNodeId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns true if this is the empty/root sentinel (no parent).
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The empty sentinel used for root nodes (no parent).
    pub fn root() -> Self {
        Self(String::new())
    }
}

impl fmt::Display for DagNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for DagNodeId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// --- SubmissionId (ADR 044, ADR 054) ---

/// Unique identifier for a user-initiated submission.
///
/// Minted once per user turn (in `Harness::run_agent`) and carried through
/// every cycle of the invoke/reinvoke loop — so all messages belonging to
/// one user request share a single `SubmissionId`.
///
/// Value is a UUID (v4).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubmissionId(String);

impl SubmissionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Create a unique `SubmissionId` for a new user action.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

impl Default for SubmissionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SubmissionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for SubmissionId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// --- SessionId (ADR 054) ---

/// Unique identifier for a conversation session.
///
/// Format: UUID. Groups multiple submissions into a conversation.
/// Created by the CLI when the REPL starts.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SessionId(String);

impl SessionId {
    /// Create a new session ID as a UUID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Derive a human-readable petname from this session's UUID.
    ///
    /// Uses the first 4 hex chars of the UUID as a suffix:
    /// `a3f2b1c4-...` → `elegant-knuth-a3f2`
    pub fn petname(&self) -> String {
        use petname::{Generator, Petnames};

        let id = self.as_str();
        let suffix = &id[..4.min(id.len())];

        let petnames = Petnames::default();
        let mut rng = rand::thread_rng();
        let name = petnames
            .generate(&mut rng, 2, "-")
            .unwrap_or_else(|| "unnamed-session".to_string());
        format!("{name}-{suffix}")
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl PartialEq<&str> for SessionId {
    fn eq(&self, other: &&str) -> bool {
        self.0 == *other
    }
}

impl Default for SessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for SessionId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Uuid::parse_str(&s).map_err(|_| format!("invalid session ID (expected UUID): {s}"))?;
        Ok(Self(s))
    }
}

// --- Instance (ADR 115, 116) ---

/// A named service instance scoped to an agent.
///
/// Identifies a specific store the agent can write to (e.g. "kv", "vec").
/// Post-ADR 115, agents declare named instances in their manifest.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Instance(String);

impl Instance {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Instance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for Instance {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for Instance {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

// --- StateHash (ADR 116) ---

/// Content-addressed hash of a single store's state.
///
/// Produced by the store worker after a write operation.
/// Empty string means the store has no state (initial/empty).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StateHash(String);

impl StateHash {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn empty() -> Self {
        Self(String::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Display for StateHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for StateHash {
    fn from(s: String) -> Self {
        Self(s)
    }
}

// --- ExternalSessionId ---

/// User-supplied external identifier for a session.
///
/// Customers can bring their own ID (e.g., `ticket-123`, `user_42`).
/// Allowed characters: `[a-zA-Z0-9_-]`, length 1-128.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExternalSessionId(String);

impl ExternalSessionId {
    pub fn new(s: &str) -> Result<Self, String> {
        if s.is_empty() {
            return Err("external session ID must not be empty".into());
        }
        if s.len() > 128 {
            return Err("external session ID must be ≤128 chars".into());
        }
        if !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "external session ID contains invalid chars (allowed: a-z A-Z 0-9 _ -): {s}"
            ));
        }
        Ok(Self(s.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ExternalSessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// --- BranchId ---

/// Database primary key identifying a branch within a session.
///
/// Wraps the integer ID from the `branches` table. Carried on messages
/// and stored in `Branch.id`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BranchId(i64);

impl BranchId {
    pub fn as_i64(&self) -> i64 {
        self.0
    }
}

impl fmt::Display for BranchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for BranchId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

// --- Sequence (ADR 044) ---

/// Sequence number for ordering interactions within a submission.
///
/// Starts at 1 and increments for each service request.
/// Used to reconstruct the order of events when debugging.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sequence(u32);

impl Sequence {
    /// Create the first sequence number (1).
    pub fn first() -> Self {
        Self(1)
    }

    /// Get the next sequence number.
    #[must_use]
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    /// Get the raw sequence number.
    pub fn as_u32(&self) -> u32 {
        self.0
    }
}

/// Thread-safe counter that allocates sequential Sequence values.
///
/// Each call to `next()` returns the current value and advances.
/// Starts at 1 (the first valid sequence number).
pub struct SequenceCounter(std::sync::Mutex<Sequence>);

impl SequenceCounter {
    /// Create a counter starting at sequence 1.
    pub fn new() -> Self {
        Self(std::sync::Mutex::new(Sequence::first()))
    }

    /// Allocate the next sequence number.
    pub fn next(&self) -> Sequence {
        let mut current = self.0.lock().unwrap();
        let seq = *current;
        *current = current.next();
        seq
    }

    /// Reset the counter back to sequence 1.
    pub fn reset(&self) {
        *self.0.lock().unwrap() = Sequence::first();
    }
}

impl Default for SequenceCounter {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Sequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for Sequence {
    fn from(n: u32) -> Self {
        Self(n)
    }
}

// --- HarnessType (ADR 044) ---

/// The type of entry point that initiated a submission.
///
/// Each variant represents a concrete harness implementation.
/// Used to route completion messages back to the correct harness type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HarnessType {
    /// Command-line interface harness
    Cli,
    /// Web API harness
    Web,
    /// REST API harness
    Api,
    /// `WhatsApp` integration harness
    Whatsapp,
    /// gRPC remote harness
    Grpc,
}

impl HarnessType {
    /// Get the string representation for use in subjects.
    pub fn as_str(&self) -> &'static str {
        match self {
            HarnessType::Cli => "cli",
            HarnessType::Web => "web",
            HarnessType::Api => "api",
            HarnessType::Whatsapp => "whatsapp",
            HarnessType::Grpc => "grpc",
        }
    }
}

impl std::str::FromStr for HarnessType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cli" => Ok(HarnessType::Cli),
            "web" => Ok(HarnessType::Web),
            "api" => Ok(HarnessType::Api),
            "whatsapp" => Ok(HarnessType::Whatsapp),
            "grpc" => Ok(HarnessType::Grpc),
            _ => Err(format!("unknown harness type: {s}")),
        }
    }
}

impl fmt::Display for HarnessType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::str::FromStr;

    // --- SubmissionId tests ---

    #[test]
    fn submission_id_from_string() {
        let id = SubmissionId::from("a1b2c3d".to_string());
        assert_eq!(id.as_str(), "a1b2c3d");
    }

    #[test]
    fn submission_id_display() {
        let id = SubmissionId::from("a1b2c3d".to_string());
        assert_eq!(format!("{id}"), "a1b2c3d");
    }

    #[test]
    fn submission_id_equality_and_hashing() {
        let id1 = SubmissionId::from("a1b2c3d".to_string());
        let id2 = SubmissionId::from("a1b2c3d".to_string());
        let id3 = SubmissionId::from("b3c4d5e".to_string());

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1.clone());
        assert!(set.contains(&id2));
        assert!(!set.contains(&id3));
    }

    // --- SessionId tests ---

    #[test]
    fn session_id_generates_unique_ids() {
        let id1 = SessionId::new();
        let id2 = SessionId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn session_id_is_uuid() {
        let id = SessionId::new();
        let s = id.as_str();
        // Should be a valid UUID (8-4-4-4-12 hex with dashes)
        assert_eq!(s.len(), 36, "UUID should be 36 chars, got: {s}");
        assert!(
            uuid::Uuid::parse_str(s).is_ok(),
            "should be a valid UUID: {s}"
        );
    }

    #[test]
    fn session_petname_uses_uuid_prefix() {
        let id = SessionId::new();
        let name = id.petname();
        // Format: "adjective-noun-hex4" e.g. "elegant-knuth-a3f2"
        let suffix = &id.as_str()[..4];
        assert!(
            name.ends_with(suffix),
            "petname '{name}' should end with UUID prefix '{suffix}'"
        );
    }

    #[test]
    fn session_id_equality_and_hashing() {
        let id1 = SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        let id2 = SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        let id3 = SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1.clone());
        assert!(set.contains(&id2));
        assert!(!set.contains(&id3));
    }

    #[test]
    fn session_id_from_string() {
        let id = SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        assert_eq!(id.as_str(), "d4761d76-dee4-4ebf-9df4-43b52efa4f78");
    }

    #[test]
    fn session_id_display() {
        let id = SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        assert_eq!(format!("{id}"), "d4761d76-dee4-4ebf-9df4-43b52efa4f78");
    }

    // --- BranchId tests (ADR 093) ---

    #[test]
    fn branch_id_main_is_one() {
        let id = BranchId::from(1);
        assert_eq!(format!("{id}"), "1");
    }

    #[test]
    fn branch_id_display() {
        let id = BranchId::from(1);
        assert_eq!(format!("{id}"), "1");
    }

    #[test]
    fn branch_id_from_i64() {
        let id = BranchId::from(42i64);
        assert_eq!(id.as_i64(), 42);
    }

    #[test]
    fn branch_id_equality_and_hashing() {
        let id1 = BranchId::from(3i64);
        let id2 = BranchId::from(3i64);
        let id3 = BranchId::from(5i64);

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);

        let mut set = HashSet::new();
        set.insert(id1);
        assert!(set.contains(&id2));
        assert!(!set.contains(&id3));
    }

    // --- Sequence tests ---

    #[test]
    fn sequence_first_is_one() {
        let seq = Sequence::first();
        assert_eq!(seq.as_u32(), 1);
    }

    #[test]
    fn sequence_next_increments() {
        let seq1 = Sequence::first();
        let seq2 = seq1.next();
        let seq3 = seq2.next();

        assert_eq!(seq1.as_u32(), 1);
        assert_eq!(seq2.as_u32(), 2);
        assert_eq!(seq3.as_u32(), 3);
    }

    #[test]
    fn sequence_display_format() {
        let seq = Sequence::from(42);
        assert_eq!(format!("{seq}"), "42");
    }

    #[test]
    fn sequence_from_u32() {
        let seq = Sequence::from(5);
        assert_eq!(seq.as_u32(), 5);
    }

    #[test]
    fn sequence_equality() {
        let seq1 = Sequence::from(3);
        let seq2 = Sequence::from(3);
        let seq3 = Sequence::from(4);

        assert_eq!(seq1, seq2);
        assert_ne!(seq1, seq3);
    }

    // --- HarnessType tests ---

    #[test]
    fn harness_type_as_str() {
        assert_eq!(HarnessType::Cli.as_str(), "cli");
        assert_eq!(HarnessType::Web.as_str(), "web");
        assert_eq!(HarnessType::Api.as_str(), "api");
        assert_eq!(HarnessType::Whatsapp.as_str(), "whatsapp");
        assert_eq!(HarnessType::Grpc.as_str(), "grpc");
    }

    #[test]
    fn harness_type_from_str_round_trips() {
        let types = [
            HarnessType::Cli,
            HarnessType::Web,
            HarnessType::Api,
            HarnessType::Whatsapp,
            HarnessType::Grpc,
        ];
        for ht in types {
            assert_eq!(HarnessType::from_str(ht.as_str()), Ok(ht));
        }
    }

    #[test]
    fn harness_type_from_str_unknown_returns_none() {
        assert!(HarnessType::from_str("unknown").is_err());
        assert!(HarnessType::from_str("").is_err());
    }

    #[test]
    fn harness_type_display() {
        assert_eq!(format!("{}", HarnessType::Cli), "cli");
        assert_eq!(format!("{}", HarnessType::Web), "web");
        assert_eq!(format!("{}", HarnessType::Api), "api");
        assert_eq!(format!("{}", HarnessType::Whatsapp), "whatsapp");
        assert_eq!(format!("{}", HarnessType::Grpc), "grpc");
    }

    #[test]
    fn harness_type_equality() {
        assert_eq!(HarnessType::Cli, HarnessType::Cli);
        assert_ne!(HarnessType::Cli, HarnessType::Web);
    }

    #[test]
    fn harness_type_is_copy() {
        let id = HarnessType::Cli;
        let id2 = id;
        assert_eq!(id, id2);
    }

    #[test]
    fn harness_type_hashable() {
        let mut set = HashSet::new();
        set.insert(HarnessType::Cli);
        set.insert(HarnessType::Web);

        assert!(set.contains(&HarnessType::Cli));
        assert!(set.contains(&HarnessType::Web));
        assert!(!set.contains(&HarnessType::Api));
    }

    mod external_session_id_tests {
        use super::ExternalSessionId;

        #[test]
        fn valid_simple() {
            assert!(ExternalSessionId::new("ticket-123").is_ok());
        }

        #[test]
        fn valid_underscore() {
            assert!(ExternalSessionId::new("user_42").is_ok());
        }

        #[test]
        fn valid_uuid() {
            let uuid = uuid::Uuid::new_v4().to_string();
            assert!(ExternalSessionId::new(&uuid).is_ok());
        }

        #[test]
        fn reject_empty() {
            assert!(ExternalSessionId::new("").is_err());
        }

        #[test]
        fn reject_dot() {
            assert!(ExternalSessionId::new("bad.id").is_err());
        }

        #[test]
        fn reject_star() {
            assert!(ExternalSessionId::new("bad*id").is_err());
        }

        #[test]
        fn reject_space() {
            assert!(ExternalSessionId::new("bad id").is_err());
        }

        #[test]
        fn reject_too_long() {
            let long = "a".repeat(129);
            assert!(ExternalSessionId::new(&long).is_err());
        }

        #[test]
        fn accept_max_length() {
            let max = "a".repeat(128);
            assert!(ExternalSessionId::new(&max).is_ok());
        }
    }
}
