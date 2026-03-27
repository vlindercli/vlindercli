//! `SqliteDagStore` — `SQLite`-backed persistence for the Merkle DAG (ADR 067).
//!
//! Domain types (`DagNode`, `DagStore`, `MessageType`, `hash_dag_node`) live
//! in `vlinder_core::domain`. This module provides the `SQLite` implementation.

use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use rusqlite::Connection;

use vlinder_core::domain::session::Session;
use vlinder_core::domain::{
    Branch, BranchId, DagNode, DagNodeId, DagStore, MessageType, ObservableMessage, SessionId,
    SessionSummary,
};

/// SQLite-backed `DagStore`.
pub struct SqliteDagStore {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteDagStore {
    /// Open (or create) a DAG store at the given path.
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create dag store directory: {e}"))?;
        }

        let conn = Connection::open(path).map_err(|e| format!("failed to open dag store: {e}"))?;

        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS dag_nodes (
                 hash TEXT PRIMARY KEY,
                 parent_hash TEXT NOT NULL,
                 message_type TEXT NOT NULL,
                 sender TEXT NOT NULL,
                 receiver TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 submission_id TEXT NOT NULL,
                 payload BLOB NOT NULL,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 stderr BLOB NOT NULL DEFAULT x'',
                 created_at TEXT NOT NULL,
                 state TEXT,
                 protocol_version TEXT NOT NULL DEFAULT '',
                 checkpoint TEXT,
                 operation TEXT,
                 message_blob TEXT,
                 branch_id INTEGER NOT NULL DEFAULT 0,
                 snapshot TEXT NOT NULL DEFAULT '{}'
             );
             CREATE INDEX IF NOT EXISTS idx_dag_nodes_session
                 ON dag_nodes (session_id, created_at);
             CREATE INDEX IF NOT EXISTS idx_dag_nodes_parent
                 ON dag_nodes (parent_hash);
             CREATE INDEX IF NOT EXISTS idx_dag_nodes_timeline
                 ON dag_nodes (branch_id, message_type, created_at);
             -- Typed message tables (ADR 122). Each holds domain-specific
             -- fields; routing and Merkle fields stay in dag_nodes.
             CREATE TABLE IF NOT EXISTS invoke_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 harness TEXT NOT NULL,
                 runtime TEXT NOT NULL,
                 agent TEXT NOT NULL,
                 message_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS complete_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 harness TEXT NOT NULL,
                 message_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS request_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 service TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 message_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL,
                 checkpoint TEXT
             );
             CREATE TABLE IF NOT EXISTS response_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 service TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 message_id TEXT NOT NULL,
                 correlation_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL,
                 status_code INTEGER NOT NULL DEFAULT 200,
                 checkpoint TEXT
             );
             CREATE TABLE IF NOT EXISTS delegate_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 caller TEXT NOT NULL,
                 target TEXT NOT NULL,
                 nonce TEXT NOT NULL,
                 message_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS repair_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 harness TEXT NOT NULL,
                 service TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 message_id TEXT NOT NULL,
                 dag_parent TEXT NOT NULL,
                 checkpoint TEXT NOT NULL,
                 state TEXT,
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS fork_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 branch_name TEXT NOT NULL,
                 fork_point TEXT NOT NULL,
                 message_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS promote_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 message_id TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS branches (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL,
                 session_id TEXT NOT NULL DEFAULT '',
                 fork_point TEXT,
                 head TEXT,
                 created_at TEXT NOT NULL,
                 broken_at TEXT,
                 UNIQUE(name, session_id)
             );
             CREATE INDEX IF NOT EXISTS idx_branches_session
                 ON branches (session_id);
             CREATE TABLE IF NOT EXISTS sessions (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 agent_name TEXT NOT NULL,
                 default_branch INTEGER NOT NULL DEFAULT 1,
                 created_at TEXT NOT NULL
             );",
        )
        .map_err(|e| format!("failed to initialize dag store: {e}"))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// Construct a `Branch` from a `SQLite` row.
///
/// Expects columns in order: `id`, `name`, `session_id`, `fork_point`, `head`,
/// `created_at`, `broken_at`.
fn row_to_branch(row: &rusqlite::Row) -> Result<Branch, rusqlite::Error> {
    let created_at_str: String = row.get(5)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    let broken_at_str: Option<String> = row.get(6)?;
    let broken_at = broken_at_str.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    });
    Ok(Branch {
        id: BranchId::from(row.get::<_, i64>(0)?),
        name: row.get(1)?,
        session_id: SessionId::try_from(row.get::<_, String>(2)?).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, e.into())
        })?,
        fork_point: row.get::<_, Option<String>>(3)?.map(DagNodeId::from),
        head: row.get::<_, Option<String>>(4)?.map(DagNodeId::from),
        created_at,
        broken_at,
    })
}

/// Construct a `Session` from a `SQLite` row.
///
/// Expects columns in order: `id`, `name`, `agent_name`, `default_branch`, `created_at`.
fn row_to_session(row: &rusqlite::Row) -> Result<Session, rusqlite::Error> {
    let id: String = row.get(0)?;
    let name: String = row.get(1)?;
    let agent: String = row.get(2)?;
    let default_branch = BranchId::from(row.get::<_, i64>(3)?);
    let created_at_str: String = row.get(4)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    Ok(Session {
        id: SessionId::try_from(id).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, e.into())
        })?,
        name,
        agent,
        default_branch,
        created_at,
    })
}

/// Construct a `DagNode` from a `SQLite` row.
///
/// Expects columns in order: `hash`, `parent_hash`, `message_type`, `created_at`,
/// `message_blob`, `payload`, `snapshot`.
fn row_to_dag_node(row: &rusqlite::Row) -> Result<DagNode, rusqlite::Error> {
    let msg_type_str: String = row.get(2)?;
    let msg_type = msg_type_str
        .parse::<MessageType>()
        .unwrap_or(MessageType::Complete);
    let created_at_str: String = row.get(3)?;
    let created_at = DateTime::parse_from_rfc3339(&created_at_str)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    let blob: String = row.get(4)?;
    let payload: Vec<u8> = row.get(5)?;
    let snapshot_json: String = row.get(6)?;
    let state: vlinder_core::domain::Snapshot = serde_json::from_str(&snapshot_json)
        .unwrap_or_else(|_| vlinder_core::domain::Snapshot::empty());
    let session = SessionId::try_from(row.get::<_, String>(7)?).unwrap_or_else(|_| {
        SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string()).unwrap()
    });
    let branch = vlinder_core::domain::BranchId::from(row.get::<_, i64>(8)?);
    let submission = vlinder_core::domain::SubmissionId::from(row.get::<_, String>(9)?);
    let protocol_version: String = row.get(10)?;

    // Empty or v2 blob = typed table row. Callers use get_invoke_node for content.
    if blob.is_empty() || serde_json::from_str::<vlinder_core::domain::DataPlane>(&blob).is_ok() {
        return Ok(DagNode {
            id: DagNodeId::from(row.get::<_, String>(0)?),
            parent_id: DagNodeId::from(row.get::<_, String>(1)?),
            created_at,
            state,
            msg_type,
            session,
            submission,
            branch,
            protocol_version,
            message: None,
        });
    }

    {
        let mut message: ObservableMessage = serde_json::from_str(&blob).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                format!("invalid message_blob JSON: {e}").into(),
            )
        })?;
        if !payload.is_empty() {
            message.set_payload(payload);
        }
        Ok(DagNode {
            id: DagNodeId::from(row.get::<_, String>(0)?),
            parent_id: DagNodeId::from(row.get::<_, String>(1)?),
            created_at,
            state,
            msg_type,
            session,
            submission,
            branch,
            protocol_version,
            message: Some(message),
        })
    }
}

/// Insert into the appropriate typed table based on message content.
///
/// For v1 messages: extracts from `ObservableMessage` variants.
fn insert_typed_node(conn: &Connection, node: &DagNode) -> Result<(), rusqlite::Error> {
    use vlinder_core::domain::ObservableMessage;

    let hash = node.id.as_str();

    let Some(msg) = &node.message else {
        return Ok(());
    };

    match msg {
        ObservableMessage::Repair(m) => {
            conn.execute(
                "INSERT OR IGNORE INTO repair_nodes (dag_hash, agent, harness, service, operation, sequence, message_id, dag_parent, checkpoint, state, payload)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![hash, m.agent_name.as_str(), m.harness.as_str(), m.service.to_string(), m.operation.as_str(), m.sequence.as_u32(), m.id.as_str(), m.dag_parent.as_str(), &m.checkpoint, m.state.as_deref(), &m.payload],
            )?;
        }
        ObservableMessage::Fork(m) => {
            conn.execute(
                "INSERT OR IGNORE INTO fork_nodes (dag_hash, agent, branch_name, fork_point, message_id)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![hash, m.agent_name.as_str(), &m.branch_name, m.fork_point.as_str(), m.id.as_str()],
            )?;
        }
        ObservableMessage::Promote(m) => {
            conn.execute(
                "INSERT OR IGNORE INTO promote_nodes (dag_hash, agent, message_id)
                 VALUES (?1, ?2, ?3)",
                rusqlite::params![hash, m.agent_name.as_str(), m.id.as_str()],
            )?;
        }
    }

    Ok(())
}

/// Column list for queries that return full `DagNode`s.
const DAG_NODE_COLUMNS: &str =
    "hash, parent_hash, message_type, created_at, message_blob, payload, snapshot, session_id, branch_id, submission_id, protocol_version";

impl DagStore for SqliteDagStore {
    fn insert_node(&self, node: &DagNode) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");

        let msg = node
            .message
            .as_ref()
            .expect("insert_node: message must be present");
        let (from, to) = msg.sender_receiver();
        let message_blob = serde_json::to_string(msg)
            .map_err(|e| format!("serialize message_blob failed: {e}"))?;
        let diagnostics_json = msg.diagnostics_json();
        let stderr = msg.stderr().to_vec();
        let state = msg.state().map(str::to_string);
        let checkpoint = msg.checkpoint().map(str::to_string);
        let operation = msg.operation().map(str::to_string);

        let snapshot_json = serde_json::to_string(&node.state)
            .map_err(|e| format!("serialize snapshot failed: {e}"))?;

        conn.execute(
            "INSERT OR IGNORE INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, operation, message_blob, branch_id, snapshot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                node.id.as_str(),
                node.parent_id.as_str(),
                node.message_type().as_str(),
                from,
                to,
                node.session_id().as_str(),
                node.submission_id().as_str(),
                node.payload(),
                diagnostics_json,
                stderr,
                node.created_at.to_rfc3339(),
                state,
                node.protocol_version(),
                checkpoint,
                operation,
                message_blob,
                node.branch_id().as_i64(),
                snapshot_json,
            ],
        ).map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        // Write to typed table (ADR 122).
        insert_typed_node(&conn, node).map_err(|e| format!("insert typed node failed: {e}"))?;

        Ok(())
    }

    fn insert_invoke_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::DataRoutingKey,
        msg: &vlinder_core::domain::InvokeMessage,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let vlinder_core::domain::DataMessageKind::Invoke {
            harness,
            runtime,
            agent,
        } = &key.kind
        else {
            return Err("insert_invoke_node: expected Invoke key".into());
        };
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();

        conn.execute(
            "INSERT OR IGNORE INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, operation, message_blob, branch_id, snapshot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                dag_id.as_str(),
                parent_id.as_str(),
                "invoke",
                harness.as_str(),
                agent.to_string(),
                key.session.as_str(),
                key.submission.as_str(),
                &msg.payload,
                &diagnostics_json,
                Vec::<u8>::new(),
                created_at.to_rfc3339(),
                msg.state.as_deref(),
                "v1",
                None::<String>,
                None::<String>,
                "",
                key.branch.as_i64(),
                &snapshot_json,
            ],
        )
        .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        conn.execute(
            "INSERT OR IGNORE INTO invoke_nodes (dag_hash, harness, runtime, agent, message_id, state, diagnostics, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![
                dag_id.as_str(),
                harness.as_str(),
                runtime.as_str(),
                agent.as_str(),
                msg.id.as_str(),
                msg.state.as_deref(),
                &diagnostics_json,
                &msg.payload,
            ],
        )
        .map_err(|e| format!("insert invoke_nodes failed: {e}"))?;

        Ok(())
    }

    fn insert_complete_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: BranchId,
        agent: &vlinder_core::domain::AgentName,
        harness: vlinder_core::domain::HarnessType,
        msg: &vlinder_core::domain::CompleteMessage,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();

        conn.execute(
            "INSERT OR IGNORE INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, operation, message_blob, branch_id, snapshot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                dag_id.as_str(),
                parent_id.as_str(),
                "complete",
                agent.to_string(),
                harness.as_str(),
                session.as_str(),
                submission.as_str(),
                &msg.payload,
                &diagnostics_json,
                Vec::<u8>::new(),
                created_at.to_rfc3339(),
                msg.state.as_deref(),
                "v1",
                None::<String>,
                None::<String>,
                "",
                branch.as_i64(),
                &snapshot_json,
            ],
        )
        .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        conn.execute(
            "INSERT OR IGNORE INTO complete_nodes (dag_hash, agent, harness, message_id, state, diagnostics, payload)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params![
                dag_id.as_str(),
                agent.as_str(),
                harness.as_str(),
                msg.id.as_str(),
                msg.state.as_deref(),
                &diagnostics_json,
                &msg.payload,
            ],
        )
        .map_err(|e| format!("insert complete_nodes failed: {e}"))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_request_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: vlinder_core::domain::ServiceBackend,
        operation: vlinder_core::domain::Operation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::RequestMessage,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let service_str = service.to_string();
        let op_str = operation.as_str();

        conn.execute(
            "INSERT OR IGNORE INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, operation, message_blob, branch_id, snapshot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                dag_id.as_str(),
                parent_id.as_str(),
                "request",
                agent.to_string(),
                &service_str,
                session.as_str(),
                submission.as_str(),
                &msg.payload,
                &diagnostics_json,
                Vec::<u8>::new(),
                created_at.to_rfc3339(),
                msg.state.as_deref(),
                "v1",
                msg.checkpoint.as_deref(),
                op_str,
                "",
                branch.as_i64(),
                &snapshot_json,
            ],
        )
        .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        conn.execute(
            "INSERT OR IGNORE INTO request_nodes (dag_hash, agent, service, operation, sequence, message_id, state, diagnostics, payload, checkpoint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            rusqlite::params![
                dag_id.as_str(),
                agent.as_str(),
                &service_str,
                op_str,
                sequence.as_u32(),
                msg.id.as_str(),
                msg.state.as_deref(),
                &diagnostics_json,
                &msg.payload,
                msg.checkpoint.as_deref(),
            ],
        )
        .map_err(|e| format!("insert request_nodes failed: {e}"))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn insert_response_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: vlinder_core::domain::ServiceBackend,
        operation: vlinder_core::domain::Operation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::ResponseMessage,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let service_str = service.to_string();
        let op_str = operation.as_str();

        conn.execute(
            "INSERT OR IGNORE INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, operation, message_blob, branch_id, snapshot)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
            rusqlite::params![
                dag_id.as_str(),
                parent_id.as_str(),
                "response",
                &service_str,
                agent.to_string(),
                session.as_str(),
                submission.as_str(),
                &msg.payload,
                &diagnostics_json,
                Vec::<u8>::new(),
                created_at.to_rfc3339(),
                msg.state.as_deref(),
                "v1",
                msg.checkpoint.as_deref(),
                op_str,
                "",
                branch.as_i64(),
                &snapshot_json,
            ],
        )
        .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        conn.execute(
            "INSERT OR IGNORE INTO response_nodes (dag_hash, agent, service, operation, sequence, message_id, correlation_id, state, diagnostics, payload, status_code, checkpoint)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            rusqlite::params![
                dag_id.as_str(),
                agent.as_str(),
                &service_str,
                op_str,
                sequence.as_u32(),
                msg.id.as_str(),
                msg.correlation_id.as_str(),
                msg.state.as_deref(),
                &diagnostics_json,
                &msg.payload,
                msg.status_code,
                msg.checkpoint.as_deref(),
            ],
        )
        .map_err(|e| format!("insert response_nodes failed: {e}"))?;

        Ok(())
    }

    fn get_node(&self, hash: &DagNodeId) -> Result<Option<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let sql = format!("SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE hash = ?1");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("get_node prepare failed: {e}"))?;

        let result = stmt
            .query_row(rusqlite::params![hash.as_str()], row_to_dag_node)
            .optional()
            .map_err(|e| format!("get_node query failed: {e}"))?;

        Ok(result)
    }

    fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let pattern = format!("{prefix}%");

        // Count matches first to detect ambiguity
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM dag_nodes WHERE hash LIKE ?1",
                rusqlite::params![pattern],
                |row| row.get(0),
            )
            .map_err(|e| format!("get_node_by_prefix count failed: {e}"))?;

        match count {
            0 => Ok(None),
            1 => {
                let sql = format!("SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE hash LIKE ?1");
                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| format!("get_node_by_prefix prepare failed: {e}"))?;

                let node = stmt
                    .query_row(rusqlite::params![pattern], row_to_dag_node)
                    .map_err(|e| format!("get_node_by_prefix query failed: {e}"))?;

                Ok(Some(node))
            }
            n => Err(format!("ambiguous hash prefix '{prefix}': {n} matches")),
        }
    }

    fn get_session_nodes(&self, session_id: &SessionId) -> Result<Vec<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let sql = format!(
            "SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE session_id = ?1 ORDER BY created_at"
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("get_session_nodes prepare failed: {e}"))?;

        let rows = stmt
            .query_map(rusqlite::params![session_id.as_str()], row_to_dag_node)
            .map_err(|e| format!("get_session_nodes query failed: {e}"))?;

        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row.map_err(|e| format!("get_session_nodes row failed: {e}"))?);
        }
        Ok(nodes)
    }

    fn get_children(&self, parent_hash: &DagNodeId) -> Result<Vec<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let sql = format!("SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE parent_hash = ?1");
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("get_children prepare failed: {e}"))?;

        let rows = stmt
            .query_map(rusqlite::params![parent_hash.as_str()], row_to_dag_node)
            .map_err(|e| format!("get_children query failed: {e}"))?;

        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row.map_err(|e| format!("get_children row failed: {e}"))?);
        }
        Ok(nodes)
    }

    // -------------------------------------------------------------------------
    // Branch methods
    // -------------------------------------------------------------------------

    fn create_branch(
        &self,
        name: &str,
        session_id: &SessionId,
        fork_point: Option<&DagNodeId>,
    ) -> Result<BranchId, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        conn.execute(
            "INSERT INTO branches (name, session_id, fork_point, created_at)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![
                name,
                session_id.as_str(),
                fork_point.map(vlinder_core::domain::DagNodeId::as_str),
                Utc::now().to_rfc3339()
            ],
        )
        .map_err(|e| format!("create_branch failed: {e}"))?;
        Ok(BranchId::from(conn.last_insert_rowid()))
    }

    fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, session_id, fork_point, head, created_at, broken_at
                 FROM branches WHERE name = ?1",
            )
            .map_err(|e| format!("get_branch_by_name prepare failed: {e}"))?;

        stmt.query_row(rusqlite::params![name], row_to_branch)
            .optional()
            .map_err(|e| format!("get_branch_by_name query failed: {e}"))
    }

    fn get_branch(&self, id: BranchId) -> Result<Option<Branch>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, session_id, fork_point, head, created_at, broken_at
                 FROM branches WHERE id = ?1",
            )
            .map_err(|e| format!("get_branch prepare failed: {e}"))?;

        stmt.query_row(rusqlite::params![id.as_i64()], row_to_branch)
            .optional()
            .map_err(|e| format!("get_branch query failed: {e}"))
    }

    fn list_sessions(&self) -> Result<Vec<SessionSummary>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT
                session_id,
                MIN(CASE WHEN message_type = 'invoke' THEN receiver END) AS agent_name,
                MIN(created_at) AS started_at,
                COUNT(CASE WHEN message_type IN ('invoke', 'complete') THEN 1 END) AS msg_count,
                (SELECT message_type FROM dag_nodes d2
                 WHERE d2.session_id = dag_nodes.session_id
                 ORDER BY created_at DESC LIMIT 1) AS last_type
            FROM dag_nodes
            GROUP BY session_id
            ORDER BY started_at DESC",
            )
            .map_err(|e| format!("list_sessions prepare failed: {e}"))?;

        let rows = stmt
            .query_map([], |row| {
                let session_id: String = row.get(0)?;
                let agent_name: Option<String> = row.get(1)?;
                let started_at_str: String = row.get(2)?;
                let message_count: usize = row.get(3)?;
                let last_type: Option<String> = row.get(4)?;

                let started_at = DateTime::parse_from_rfc3339(&started_at_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_default();
                let is_open = last_type.as_deref() != Some("complete");

                Ok(SessionSummary {
                    session_id: SessionId::try_from(session_id).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            e.into(),
                        )
                    })?,
                    agent_name: agent_name.unwrap_or_default(),
                    started_at,
                    message_count,
                    is_open,
                })
            })
            .map_err(|e| format!("list_sessions query failed: {e}"))?;

        let mut summaries = Vec::new();
        for row in rows {
            summaries.push(row.map_err(|e| format!("list_sessions row failed: {e}"))?);
        }
        Ok(summaries)
    }

    fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let sql = format!(
            "SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE submission_id = ?1 ORDER BY created_at"
        );
        let mut stmt = conn
            .prepare(&sql)
            .map_err(|e| format!("get_nodes_by_submission prepare failed: {e}"))?;

        let rows = stmt
            .query_map([submission_id], row_to_dag_node)
            .map_err(|e| format!("get_nodes_by_submission query failed: {e}"))?;

        let mut nodes = Vec::new();
        for row in rows {
            nodes.push(row.map_err(|e| format!("get_nodes_by_submission row failed: {e}"))?);
        }
        Ok(nodes)
    }

    fn get_invoke_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::DataRoutingKey,
            vlinder_core::domain::InvokeMessage,
        )>,
        String,
    > {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT i.harness, i.runtime, i.agent, i.message_id, i.state, i.diagnostics, i.payload,
                        d.session_id, d.submission_id, d.branch_id, d.parent_hash
                 FROM invoke_nodes i
                 JOIN dag_nodes d ON d.hash = i.dag_hash
                 WHERE i.dag_hash = ?1",
            )
            .map_err(|e| format!("get_invoke_node prepare failed: {e}"))?;

        let result = stmt
            .query_row(rusqlite::params![dag_hash.as_str()], |row| {
                let harness_str: String = row.get(0)?;
                let runtime_str: String = row.get(1)?;
                let agent_str: String = row.get(2)?;
                let message_id: String = row.get(3)?;
                let state: Option<String> = row.get(4)?;
                let diagnostics_blob: Vec<u8> = row.get(5)?;
                let payload: Vec<u8> = row.get(6)?;
                let session_id: String = row.get(7)?;
                let submission_id: String = row.get(8)?;
                let branch: i64 = row.get(9)?;
                let parent_hash: String = row.get(10)?;

                let harness: vlinder_core::domain::HarnessType =
                    harness_str.parse().map_err(|e: String| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            e.into(),
                        )
                    })?;
                let runtime: vlinder_core::domain::RuntimeType =
                    runtime_str.parse().map_err(|e: String| {
                        rusqlite::Error::FromSqlConversionFailure(
                            1,
                            rusqlite::types::Type::Text,
                            e.into(),
                        )
                    })?;

                let key = vlinder_core::domain::DataRoutingKey {
                    session: SessionId::try_from(session_id).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Text,
                            e.into(),
                        )
                    })?,
                    branch: BranchId::from(branch),
                    submission: vlinder_core::domain::SubmissionId::from(submission_id),
                    kind: vlinder_core::domain::DataMessageKind::Invoke {
                        harness,
                        runtime,
                        agent: vlinder_core::domain::AgentName::new(agent_str),
                    },
                };

                let diagnostics: vlinder_core::domain::InvokeDiagnostics =
                    serde_json::from_slice(&diagnostics_blob).unwrap_or_else(|_| {
                        vlinder_core::domain::InvokeDiagnostics {
                            harness_version: String::new(),
                        }
                    });

                let msg = vlinder_core::domain::InvokeMessage {
                    id: vlinder_core::domain::MessageId::from(message_id),
                    dag_id: dag_hash.clone(),
                    state,
                    diagnostics,
                    dag_parent: DagNodeId::from(parent_hash),
                    payload,
                };

                Ok((key, msg))
            })
            .optional()
            .map_err(|e| format!("get_invoke_node query failed: {e}"))?;

        Ok(result)
    }

    fn get_complete_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::CompleteMessage>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT c.agent, c.harness, c.message_id, c.state, c.diagnostics, c.payload
                 FROM complete_nodes c
                 WHERE c.dag_hash = ?1",
            )
            .map_err(|e| format!("get_complete_node prepare failed: {e}"))?;

        let result = stmt
            .query_row(rusqlite::params![dag_hash.as_str()], |row| {
                let _agent_str: String = row.get(0)?;
                let _harness_str: String = row.get(1)?;
                let message_id: String = row.get(2)?;
                let state: Option<String> = row.get(3)?;
                let diagnostics_blob: Vec<u8> = row.get(4)?;
                let payload: Vec<u8> = row.get(5)?;

                let diagnostics: vlinder_core::domain::RuntimeDiagnostics =
                    serde_json::from_slice(&diagnostics_blob).unwrap_or_else(|_| {
                        vlinder_core::domain::RuntimeDiagnostics::placeholder(0)
                    });

                Ok(vlinder_core::domain::CompleteMessage {
                    id: vlinder_core::domain::MessageId::from(message_id),
                    dag_id: dag_hash.clone(),
                    state,
                    diagnostics,
                    payload,
                })
            })
            .optional()
            .map_err(|e| format!("get_complete_node query failed: {e}"))?;

        Ok(result)
    }

    fn get_request_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::RequestMessage>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT r.message_id, r.state, r.diagnostics, r.payload, r.checkpoint
                 FROM request_nodes r
                 WHERE r.dag_hash = ?1",
            )
            .map_err(|e| format!("get_request_node prepare failed: {e}"))?;

        let result = stmt
            .query_row(rusqlite::params![dag_hash.as_str()], |row| {
                let message_id: String = row.get(0)?;
                let state: Option<String> = row.get(1)?;
                let diagnostics_blob: Vec<u8> = row.get(2)?;
                let payload: Vec<u8> = row.get(3)?;
                let checkpoint: Option<String> = row.get(4)?;

                let diagnostics: vlinder_core::domain::RequestDiagnostics =
                    serde_json::from_slice(&diagnostics_blob).unwrap_or_else(|_| {
                        vlinder_core::domain::RequestDiagnostics {
                            sequence: 0,
                            endpoint: String::new(),
                            request_bytes: 0,
                            received_at_ms: 0,
                        }
                    });

                Ok(vlinder_core::domain::RequestMessage {
                    id: vlinder_core::domain::MessageId::from(message_id),
                    dag_id: dag_hash.clone(),
                    state,
                    diagnostics,
                    payload,
                    checkpoint,
                })
            })
            .optional()
            .map_err(|e| format!("get_request_node query failed: {e}"))?;

        Ok(result)
    }

    fn get_response_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::ResponseMessage>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT r.message_id, r.correlation_id, r.state, r.diagnostics, r.payload, r.status_code, r.checkpoint
                 FROM response_nodes r
                 WHERE r.dag_hash = ?1",
            )
            .map_err(|e| format!("get_response_node prepare failed: {e}"))?;

        let result = stmt
            .query_row(rusqlite::params![dag_hash.as_str()], |row| {
                let message_id: String = row.get(0)?;
                let correlation_id: String = row.get(1)?;
                let state: Option<String> = row.get(2)?;
                let diagnostics_blob: Vec<u8> = row.get(3)?;
                let payload: Vec<u8> = row.get(4)?;
                let status_code: u16 = row.get(5)?;
                let checkpoint: Option<String> = row.get(6)?;

                let diagnostics: vlinder_core::domain::ServiceDiagnostics =
                    serde_json::from_slice(&diagnostics_blob).unwrap_or_else(|_| {
                        vlinder_core::domain::ServiceDiagnostics::storage(
                            vlinder_core::domain::ServiceType::Kv,
                            "unknown",
                            vlinder_core::domain::Operation::Get,
                            0,
                            0,
                        )
                    });

                Ok(vlinder_core::domain::ResponseMessage {
                    id: vlinder_core::domain::MessageId::from(message_id),
                    dag_id: dag_hash.clone(),
                    correlation_id: vlinder_core::domain::MessageId::from(correlation_id),
                    state,
                    diagnostics,
                    payload,
                    status_code,
                    checkpoint,
                })
            })
            .optional()
            .map_err(|e| format!("get_response_node query failed: {e}"))?;

        Ok(result)
    }

    fn get_branches_for_session(&self, session_id: &SessionId) -> Result<Vec<Branch>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, session_id, fork_point, head, created_at, broken_at
                 FROM branches
                 WHERE session_id = ?1
                 ORDER BY created_at",
            )
            .map_err(|e| format!("get_branches_for_session prepare failed: {e}"))?;

        let rows = stmt
            .query_map(rusqlite::params![session_id.as_str()], row_to_branch)
            .map_err(|e| format!("get_branches_for_session query failed: {e}"))?;

        let mut branches = Vec::new();
        for row in rows {
            branches.push(row.map_err(|e| format!("get_branches_for_session row failed: {e}"))?);
        }
        Ok(branches)
    }

    fn latest_node_on_branch(
        &self,
        branch_id: BranchId,
        message_type: Option<MessageType>,
    ) -> Result<Option<DagNode>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let branch_id_str = branch_id.to_string();

        if let Some(mt) = message_type {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE branch_id = ?1 AND message_type = ?2 ORDER BY created_at DESC LIMIT 1"
                ))
                .map_err(|e| format!("latest_node_on_branch prepare failed: {e}"))?;

            stmt.query_row(
                rusqlite::params![branch_id_str, mt.as_str()],
                row_to_dag_node,
            )
            .optional()
            .map_err(|e| format!("latest_node_on_branch query failed: {e}"))
        } else {
            let mut stmt = conn
                .prepare(&format!(
                    "SELECT {DAG_NODE_COLUMNS} FROM dag_nodes WHERE branch_id = ?1 ORDER BY created_at DESC LIMIT 1"
                ))
                .map_err(|e| format!("latest_node_on_branch prepare failed: {e}"))?;

            stmt.query_row(rusqlite::params![branch_id_str], row_to_dag_node)
                .optional()
                .map_err(|e| format!("latest_node_on_branch query failed: {e}"))
        }
    }

    fn rename_branch(&self, id: BranchId, new_name: &str) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = conn
            .execute(
                "UPDATE branches SET name = ?1 WHERE id = ?2",
                rusqlite::params![new_name, id.as_i64()],
            )
            .map_err(|e| format!("rename_branch failed: {e}"))?;
        if rows == 0 {
            return Err(format!("branch {id} not found"));
        }
        Ok(())
    }

    fn seal_branch(
        &self,
        id: BranchId,
        broken_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = conn
            .execute(
                "UPDATE branches SET broken_at = ?1 WHERE id = ?2",
                rusqlite::params![broken_at.to_rfc3339(), id.as_i64()],
            )
            .map_err(|e| format!("seal_branch failed: {e}"))?;
        if rows == 0 {
            return Err(format!("branch {id} not found"));
        }
        Ok(())
    }

    // -------------------------------------------------------------------------
    // Session CRUD
    // -------------------------------------------------------------------------

    fn update_session_default_branch(
        &self,
        session_id: &SessionId,
        branch_id: BranchId,
    ) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = conn
            .execute(
                "UPDATE sessions SET default_branch = ?1 WHERE id = ?2",
                rusqlite::params![branch_id.as_i64(), session_id.as_str()],
            )
            .map_err(|e| format!("update_session_default_branch failed: {e}"))?;
        if rows == 0 {
            return Err(format!("session {session_id} not found"));
        }
        Ok(())
    }

    fn create_session(&self, session: &Session) -> Result<(), String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        conn.execute(
            "INSERT OR IGNORE INTO sessions (id, name, agent_name, default_branch, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                session.id.as_str(),
                session.name,
                session.agent,
                session.default_branch.as_i64(),
                session.created_at.to_rfc3339(),
            ],
        )
        .map_err(|e| format!("create_session failed: {e}"))?;
        Ok(())
    }

    fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, agent_name, default_branch, created_at
                 FROM sessions WHERE id = ?1",
            )
            .map_err(|e| format!("get_session prepare failed: {e}"))?;

        stmt.query_row(rusqlite::params![session_id.as_str()], row_to_session)
            .optional()
            .map_err(|e| format!("get_session query failed: {e}"))
    }

    fn get_session_by_name(&self, name: &str) -> Result<Option<Session>, String> {
        let conn = self.conn.lock().expect("db connection lock poisoned");
        let mut stmt = conn
            .prepare(
                "SELECT id, name, agent_name, default_branch, created_at
                 FROM sessions WHERE name = ?1",
            )
            .map_err(|e| format!("get_session_by_name prepare failed: {e}"))?;

        stmt.query_row(rusqlite::params![name], row_to_session)
            .optional()
            .map_err(|e| format!("get_session_by_name query failed: {e}"))
    }
}

/// Trait extension for rusqlite optional queries.
trait OptionalExt<T> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error>;
}

impl<T> OptionalExt<T> for Result<T, rusqlite::Error> {
    fn optional(self) -> Result<Option<T>, rusqlite::Error> {
        match self {
            Ok(val) => Ok(Some(val)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::workers::dag::build_dag_node;
    use vlinder_core::domain::{
        AgentName, BranchId, HarnessType, Operation, RepairMessage, Sequence, ServiceBackend,
        Snapshot, SubmissionId,
    };

    fn test_store() -> SqliteDagStore {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        SqliteDagStore::open(tmp.path()).unwrap()
    }

    fn sess() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }

    fn sub() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }

    /// Build a test `ObservableMessage` using `RepairMessage`.
    fn make_repair(payload: &[u8], session: SessionId) -> ObservableMessage {
        ObservableMessage::Repair(RepairMessage::new(
            BranchId::from(1),
            sub(),
            session,
            AgentName::new("agent-a"),
            HarnessType::Cli,
            DagNodeId::root(),
            "on_error".to_string(),
            ServiceBackend::Kv(vlinder_core::domain::ObjectStorageType::Sqlite),
            Operation::Get,
            Sequence::first(),
            payload.to_vec(),
            None,
        ))
    }

    /// Build a test `DagNode` with a valid message.
    fn test_node(payload: &[u8], parent: &DagNodeId) -> DagNode {
        let msg = make_repair(payload, sess());
        build_dag_node(&msg, parent, &Snapshot::empty())
    }

    #[test]
    fn round_trip_insert_get() {
        let store = test_store();
        let node = test_node(b"hello", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        let retrieved = store.get_node(&node.id).unwrap().unwrap();

        assert_eq!(retrieved.id, node.id);
        assert_eq!(retrieved.parent_id, node.parent_id);
    }

    #[test]
    fn get_node_returns_none_for_unknown() {
        let store = test_store();
        assert_eq!(
            store
                .get_node(&DagNodeId::from("nonexistent".to_string()))
                .unwrap(),
            None
        );
    }

    #[test]
    fn idempotent_insert() {
        let store = test_store();
        let node = test_node(b"data", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        store.insert_node(&node).unwrap(); // No error

        let retrieved = store.get_node(&node.id).unwrap().unwrap();
        assert_eq!(retrieved.id, node.id);
    }

    #[test]
    fn get_children() {
        let store = test_store();

        let parent = test_node(b"parent", &DagNodeId::root());

        let child_msg = make_repair(b"child", sess());
        let mut child = build_dag_node(&child_msg, &parent.id, &Snapshot::empty());
        child.created_at = chrono::TimeZone::with_ymd_and_hms(&Utc, 2025, 1, 1, 0, 1, 0).unwrap();

        store.insert_node(&parent).unwrap();
        store.insert_node(&child).unwrap();

        let children = store.get_children(&parent.id).unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, child.id);

        // Root has one child (the parent node, whose parent_id is root)
        let root_children = store.get_children(&DagNodeId::root()).unwrap();
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].id, parent.id);
    }

    #[test]
    fn different_sessions_are_isolated() {
        let store = test_store();

        let sess1 =
            SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        let sess2 =
            SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();

        let msg_a = make_repair(b"a", sess1.clone());
        let node_a = build_dag_node(&msg_a, &DagNodeId::root(), &Snapshot::empty());

        let msg_b = make_repair(b"b", sess2.clone());
        let node_b = build_dag_node(&msg_b, &DagNodeId::root(), &Snapshot::empty());

        store.insert_node(&node_a).unwrap();
        store.insert_node(&node_b).unwrap();

        let s1_nodes = store.get_session_nodes(&sess1).unwrap();
        assert_eq!(s1_nodes.len(), 1);
        assert_eq!(*s1_nodes[0].session_id(), sess1);

        let s2_nodes = store.get_session_nodes(&sess2).unwrap();
        assert_eq!(s2_nodes.len(), 1);
        assert_eq!(*s2_nodes[0].session_id(), sess2);
    }

    // ========================================================================
    // Timeline tests (ADR 093)
    // ========================================================================

    #[test]
    fn create_timeline_returns_auto_id() {
        let store = test_store();

        let session_id = sess();
        let fork = DagNodeId::from("abc123".to_string());
        let id = store
            .create_branch("repair-1", &session_id, Some(&fork))
            .unwrap();
        assert!(id.as_i64() >= 1);

        let tl = store.get_branch(id).unwrap().unwrap();
        assert_eq!(tl.name, "repair-1");
        assert_eq!(tl.session_id, session_id);
        assert_eq!(tl.fork_point, Some(DagNodeId::from("abc123".to_string())));
        assert!(tl.broken_at.is_none());
    }

    #[test]
    fn create_timeline_with_parent() {
        let store = test_store();

        let session_id = sess();
        let _parent_id = store.create_branch("main", &session_id, None).unwrap();
        let fork = DagNodeId::from("abc123".to_string());
        let fork_id = store
            .create_branch("repair-1", &session_id, Some(&fork))
            .unwrap();

        let tl = store.get_branch(fork_id).unwrap().unwrap();
        assert_eq!(tl.fork_point, Some(fork));
    }

    #[test]
    fn get_timeline_by_branch() {
        let store = test_store();
        let session_id = sess();
        store.create_branch("main", &session_id, None).unwrap();

        let tl = store.get_branch_by_name("main").unwrap().unwrap();
        assert_eq!(tl.session_id, session_id);

        assert!(store.get_branch_by_name("nonexistent").unwrap().is_none());
    }

    // ========================================================================
    // latest_node_on_branch tests
    // ========================================================================

    #[test]
    fn latest_node_on_branch_returns_none_for_empty() {
        let store = test_store();
        let result = store
            .latest_node_on_branch(BranchId::from(1), None)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn latest_node_on_branch_returns_most_recent() {
        let store = test_store();

        let node1 = test_node(b"first", &DagNodeId::root());
        store.insert_node(&node1).unwrap();

        let msg2 = make_repair(b"response", sess());
        let node2 = build_dag_node(&msg2, &node1.id, &Snapshot::empty());
        store.insert_node(&node2).unwrap();

        // No filter — returns the most recent
        let latest = store
            .latest_node_on_branch(BranchId::from(1), None)
            .unwrap()
            .unwrap();
        assert_eq!(latest.id, node2.id);
    }

    // ========================================================================
    // Session CRUD tests
    // ========================================================================

    #[test]
    fn create_and_get_session() {
        let store = test_store();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            "pensieve",
            BranchId::from(1),
        );

        store.create_session(&session).unwrap();

        let sid = SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap();
        let retrieved = store.get_session(&sid).unwrap().unwrap();
        assert_eq!(
            retrieved.id.as_str(),
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
        assert_eq!(retrieved.agent, "pensieve");
        assert_eq!(retrieved.name, session.name);
    }

    #[test]
    fn get_session_by_name() {
        let store = test_store();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            "pensieve",
            BranchId::from(1),
        );
        let name = session.name.clone();

        store.create_session(&session).unwrap();

        let retrieved = store.get_session_by_name(&name).unwrap().unwrap();
        assert_eq!(
            retrieved.id.as_str(),
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
        assert_eq!(retrieved.agent, "pensieve");
    }

    #[test]
    fn get_session_returns_none_for_unknown() {
        let store = test_store();
        let sid = SessionId::try_from("00000000-0000-0000-0000-000000000000".to_string()).unwrap();
        assert!(store.get_session(&sid).unwrap().is_none());
    }

    #[test]
    fn get_session_by_name_returns_none_for_unknown() {
        let store = test_store();
        assert!(store.get_session_by_name("nonexistent").unwrap().is_none());
    }

    #[test]
    fn create_session_is_idempotent() {
        let store = test_store();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            "pensieve",
            BranchId::from(1),
        );

        store.create_session(&session).unwrap();
        store.create_session(&session).unwrap(); // No error

        let sid = SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap();
        let retrieved = store.get_session(&sid).unwrap().unwrap();
        assert_eq!(retrieved.agent, "pensieve");
    }

    #[test]
    fn invalid_message_blob_returns_error() {
        let store = test_store();
        let conn = store.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO dag_nodes (hash, parent_hash, message_type, sender, receiver, session_id, submission_id, payload, diagnostics, stderr, created_at, state, protocol_version, checkpoint, message_blob)
             VALUES ('h1', '', 'bogus', 'cli', 'agent-a', 'sess-1', 'sub-1', x'', x'', x'', '2025-01-01T00:00:00Z', NULL, '', NULL, '{\"bad\": true}')",
            [],
        ).unwrap();
        drop(conn);

        let result = store.get_node(&DagNodeId::from("h1".to_string()));
        assert!(
            result.is_err(),
            "invalid message_blob should error, not silently default"
        );
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid message_blob JSON"),
            "error should mention invalid JSON, got: {err}"
        );
    }
}
