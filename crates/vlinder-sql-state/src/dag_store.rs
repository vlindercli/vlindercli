//! `SqliteDagStore` — `SQLite`-backed persistence for the Merkle DAG (ADR 067).
//!
//! Domain types (`DagNode`, `DagStore`, `MessageType`, `hash_dag_node`) live
//! in `vlinder_core::domain`. This module provides the `SQLite` implementation.

use std::path::Path;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use diesel::connection::SimpleConnection;
use diesel::prelude::*;
use diesel::sql_types::{Integer, Nullable, Text};
use diesel::sqlite::SqliteConnection;

use vlinder_core::domain::session::Session;
use vlinder_core::domain::{
    Branch, BranchId, DagNode, DagNodeId, DagStore, MessageType, SessionId, SessionSummary,
};

/// SQLite-backed `DagStore`.
pub struct SqliteDagStore {
    pub(crate) conn: Arc<Mutex<SqliteConnection>>,
}

impl SqliteDagStore {
    /// Open (or create) a DAG store at the given path.
    #[allow(clippy::too_many_lines)]
    pub fn open(path: &Path) -> Result<Self, String> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("failed to create dag store directory: {e}"))?;
        }

        let mut conn = SqliteConnection::establish(path.to_str().ok_or("invalid path")?)
            .map_err(|e| format!("failed to open dag store: {e}"))?;

        conn.batch_execute(
            "PRAGMA journal_mode=WAL;
             PRAGMA foreign_keys=ON;

             CREATE TABLE IF NOT EXISTS sessions (
                 id TEXT PRIMARY KEY,
                 name TEXT NOT NULL UNIQUE,
                 agent_name TEXT NOT NULL,
                 default_branch INTEGER NOT NULL DEFAULT 1,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS branches (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT NOT NULL,
                 session_id TEXT NOT NULL REFERENCES sessions(id),
                 fork_point TEXT,
                 head TEXT,
                 created_at TEXT NOT NULL,
                 broken_at TEXT,
                 UNIQUE(name, session_id)
             );
             CREATE INDEX IF NOT EXISTS idx_branches_session
                 ON branches (session_id);
             CREATE TABLE IF NOT EXISTS dag_nodes (
                 hash TEXT PRIMARY KEY,
                 parent_hash TEXT NOT NULL,
                 message_type TEXT NOT NULL,
                 session_id TEXT REFERENCES sessions(id),
                 submission_id TEXT,
                 branch_id INTEGER REFERENCES branches(id),
                 created_at TEXT NOT NULL,
                 protocol_version TEXT NOT NULL DEFAULT '',
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
                 message_id TEXT NOT NULL UNIQUE,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS complete_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 harness TEXT NOT NULL,
                 message_id TEXT NOT NULL UNIQUE,
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
                 message_id TEXT NOT NULL UNIQUE,
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
                 message_id TEXT NOT NULL UNIQUE,
                 correlation_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics BLOB NOT NULL DEFAULT x'',
                 payload BLOB NOT NULL,
                 status_code INTEGER NOT NULL DEFAULT 200,
                 checkpoint TEXT
             );
             CREATE TABLE IF NOT EXISTS fork_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 branch_name TEXT NOT NULL,
                 fork_point TEXT NOT NULL,
                 message_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS promote_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 message_id TEXT NOT NULL UNIQUE,
                 branch_id INTEGER REFERENCES branches(id)
             );

             -- Infra read model (ADR 121)
             CREATE TABLE IF NOT EXISTS agents (
                 name TEXT PRIMARY KEY,
                 description TEXT NOT NULL,
                 source TEXT,
                 runtime TEXT NOT NULL,
                 executable TEXT NOT NULL,
                 image_digest TEXT,
                 object_storage TEXT,
                 vector_storage TEXT,
                 requirements_json TEXT NOT NULL,
                 prompts_json TEXT,
                 public_key TEXT
             );
             CREATE TABLE IF NOT EXISTS models (
                 name TEXT PRIMARY KEY,
                 model_type TEXT NOT NULL,
                 provider TEXT NOT NULL,
                 model_path TEXT NOT NULL,
                 digest TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS deploy_agent_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent_name TEXT NOT NULL,
                 manifest_json TEXT NOT NULL,
                 message_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS delete_agent_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent_name TEXT NOT NULL,
                 message_id TEXT NOT NULL UNIQUE
             );
             CREATE TABLE IF NOT EXISTS agent_states (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 agent_name TEXT NOT NULL REFERENCES agents(name),
                 state TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 error TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_agent_states_name
                 ON agent_states (agent_name, updated_at);
             ",
        )
        .map_err(|e| format!("failed to initialize dag store: {e}"))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

/// Convert a Diesel `BranchRow` to the domain `Branch`.
fn branch_row_to_domain(r: crate::models::BranchRow) -> Branch {
    let created_at = DateTime::parse_from_rfc3339(&r.created_at)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    let broken_at = r.broken_at.and_then(|s| {
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .ok()
    });
    Branch {
        id: BranchId::from(r.id),
        name: r.name,
        session_id: SessionId::try_from(r.session_id).unwrap_or_else(|_| {
            SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string()).unwrap()
        }),
        fork_point: r.fork_point.map(DagNodeId::from),
        head: r.head.map(DagNodeId::from),
        created_at,
        broken_at,
    }
}

/// Convert a Diesel `SessionRow` to the domain `Session`.
fn session_row_to_domain(r: crate::models::SessionRow) -> Session {
    let created_at = DateTime::parse_from_rfc3339(&r.created_at)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    Session {
        id: SessionId::try_from(r.id).unwrap_or_else(|_| {
            SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string()).unwrap()
        }),
        name: r.name,
        agent: r.agent_name,
        default_branch: BranchId::from(r.default_branch),
        created_at,
    }
}

/// Convert a Diesel `DagNodeRow` to the domain `DagNode`.
fn dag_node_row_to_domain(r: crate::models::DagNodeRow) -> DagNode {
    let msg_type = r
        .message_type
        .parse::<MessageType>()
        .unwrap_or(MessageType::Complete);
    let created_at = DateTime::parse_from_rfc3339(&r.created_at)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default();
    let state: vlinder_core::domain::Snapshot = serde_json::from_str(&r.snapshot)
        .unwrap_or_else(|_| vlinder_core::domain::Snapshot::empty());
    let session = r
        .session_id
        .and_then(|s| SessionId::try_from(s).ok())
        .unwrap_or_else(|| {
            SessionId::try_from("00000000-0000-4000-8000-000000000000".to_string()).unwrap()
        });
    let branch = vlinder_core::domain::BranchId::from(r.branch_id.unwrap_or(0));
    let submission = vlinder_core::domain::SubmissionId::from(r.submission_id.unwrap_or_default());

    DagNode {
        id: DagNodeId::from(r.hash),
        parent_id: DagNodeId::from(r.parent_hash),
        created_at,
        state,
        msg_type,
        session,
        submission,
        branch,
        protocol_version: r.protocol_version,
    }
}

/// Row type for the `list_sessions` aggregate query.
#[derive(QueryableByName, Debug)]
struct SessionSummaryRow {
    #[diesel(sql_type = Text)]
    session_id: String,
    #[diesel(sql_type = Nullable<Text>)]
    agent_name: Option<String>,
    #[diesel(sql_type = Text)]
    started_at: String,
    #[diesel(sql_type = Integer)]
    msg_count: i32,
    #[diesel(sql_type = Nullable<Text>)]
    last_type: Option<String>,
}

impl DagStore for SqliteDagStore {
    fn insert_invoke_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::DataRoutingKey,
        msg: &vlinder_core::domain::InvokeMessage,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewInvokeNode};
        use crate::schema::{dag_nodes, invoke_nodes};

        let vlinder_core::domain::DataMessageKind::Invoke {
            harness,
            runtime,
            agent,
        } = &key.kind
        else {
            return Err("insert_invoke_node: expected Invoke key".into());
        };

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "invoke",
                session_id: Some(key.session.as_str()),
                submission_id: Some(key.submission.as_str()),
                branch_id: Some(key.branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(invoke_nodes::table)
            .values(&NewInvokeNode {
                dag_hash: dag_id.as_str(),
                harness: harness.as_str(),
                runtime: runtime.as_str(),
                agent: agent.as_str(),
                message_id: msg.id.as_str(),
                state: msg.state.as_deref(),
                diagnostics: &diagnostics_json,
                payload: &msg.payload,
            })
            .execute(&mut *conn)
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
        use crate::models::{NewCompleteNode, NewDagNode};
        use crate::schema::{complete_nodes, dag_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "complete",
                session_id: Some(session.as_str()),
                submission_id: Some(submission.as_str()),
                branch_id: Some(branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(complete_nodes::table)
            .values(&NewCompleteNode {
                dag_hash: dag_id.as_str(),
                agent: agent.as_str(),
                harness: harness.as_str(),
                message_id: msg.id.as_str(),
                state: msg.state.as_deref(),
                diagnostics: &diagnostics_json,
                payload: &msg.payload,
            })
            .execute(&mut *conn)
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
        use crate::models::{NewDagNode, NewRequestNode};
        use crate::schema::{dag_nodes, request_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();
        let service_str = service.to_string();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "request",
                session_id: Some(session.as_str()),
                submission_id: Some(submission.as_str()),
                branch_id: Some(branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(request_nodes::table)
            .values(&NewRequestNode {
                dag_hash: dag_id.as_str(),
                agent: agent.as_str(),
                service: &service_str,
                operation: operation.as_str(),
                sequence: i32::try_from(sequence.as_u32()).unwrap_or(0),
                message_id: msg.id.as_str(),
                state: msg.state.as_deref(),
                diagnostics: &diagnostics_json,
                payload: &msg.payload,
                checkpoint: msg.checkpoint.as_deref(),
            })
            .execute(&mut *conn)
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
        use crate::models::{NewDagNode, NewResponseNode};
        use crate::schema::{dag_nodes, response_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let diagnostics_json = serde_json::to_vec(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();
        let service_str = service.to_string();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "response",
                session_id: Some(session.as_str()),
                submission_id: Some(submission.as_str()),
                branch_id: Some(branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(response_nodes::table)
            .values(&NewResponseNode {
                dag_hash: dag_id.as_str(),
                agent: agent.as_str(),
                service: &service_str,
                operation: operation.as_str(),
                sequence: i32::try_from(sequence.as_u32()).unwrap_or(0),
                message_id: msg.id.as_str(),
                correlation_id: msg.correlation_id.as_str(),
                state: msg.state.as_deref(),
                diagnostics: &diagnostics_json,
                payload: &msg.payload,
                status_code: i32::from(msg.status_code),
                checkpoint: msg.checkpoint.as_deref(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert response_nodes failed: {e}"))?;

        Ok(())
    }

    fn get_node(&self, hash: &DagNodeId) -> Result<Option<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::DagNodeRow> = dag_nodes::table
            .find(hash.as_str())
            .select(crate::models::DagNodeRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_node query failed: {e}"))?;

        Ok(row.map(dag_node_row_to_domain))
    }

    fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let pattern = format!("{prefix}%");

        // Count matches first to detect ambiguity
        let count: i64 = dag_nodes::table
            .filter(dag_nodes::hash.like(&pattern))
            .count()
            .get_result(&mut *conn)
            .map_err(|e| format!("get_node_by_prefix count failed: {e}"))?;

        match count {
            0 => Ok(None),
            1 => {
                let row: crate::models::DagNodeRow = dag_nodes::table
                    .filter(dag_nodes::hash.like(&pattern))
                    .select(crate::models::DagNodeRow::as_select())
                    .first(&mut *conn)
                    .map_err(|e| format!("get_node_by_prefix query failed: {e}"))?;

                Ok(Some(dag_node_row_to_domain(row)))
            }
            n => Err(format!("ambiguous hash prefix '{prefix}': {n} matches")),
        }
    }

    fn get_session_nodes(&self, session_id: &SessionId) -> Result<Vec<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<crate::models::DagNodeRow> = dag_nodes::table
            .filter(dag_nodes::session_id.eq(session_id.as_str()))
            .order(dag_nodes::created_at.asc())
            .select(crate::models::DagNodeRow::as_select())
            .load(&mut *conn)
            .map_err(|e| format!("get_session_nodes query failed: {e}"))?;

        Ok(rows.into_iter().map(dag_node_row_to_domain).collect())
    }

    fn get_children(&self, parent_hash: &DagNodeId) -> Result<Vec<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<crate::models::DagNodeRow> = dag_nodes::table
            .filter(dag_nodes::parent_hash.eq(parent_hash.as_str()))
            .select(crate::models::DagNodeRow::as_select())
            .load(&mut *conn)
            .map_err(|e| format!("get_children query failed: {e}"))?;

        Ok(rows.into_iter().map(dag_node_row_to_domain).collect())
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
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let created_at_str = Utc::now().to_rfc3339();

        diesel::insert_into(branches::table)
            .values(&crate::models::NewBranch {
                name,
                session_id: session_id.as_str(),
                fork_point: fork_point.map(DagNodeId::as_str),
                created_at: &created_at_str,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("create_branch failed: {e}"))?;

        // Get the auto-incremented id
        let id: i64 = diesel::select(diesel::dsl::sql::<diesel::sql_types::BigInt>(
            "last_insert_rowid()",
        ))
        .get_result(&mut *conn)
        .map_err(|e| format!("create_branch last_insert_rowid failed: {e}"))?;

        Ok(BranchId::from(id))
    }

    fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String> {
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::BranchRow> = branches::table
            .filter(branches::name.eq(name))
            .select(crate::models::BranchRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_branch_by_name failed: {e}"))?;

        Ok(row.map(branch_row_to_domain))
    }

    fn get_branch(&self, id: BranchId) -> Result<Option<Branch>, String> {
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::BranchRow> = branches::table
            .find(id.as_i64())
            .select(crate::models::BranchRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_branch failed: {e}"))?;

        Ok(row.map(branch_row_to_domain))
    }

    fn list_sessions(&self) -> Result<Vec<SessionSummary>, String> {
        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<SessionSummaryRow> = diesel::sql_query(
            "SELECT
                d.session_id,
                s.agent_name AS agent_name,
                MIN(d.created_at) AS started_at,
                COUNT(CASE WHEN d.message_type IN ('invoke', 'complete') THEN 1 END) AS msg_count,
                (SELECT message_type FROM dag_nodes d2
                 WHERE d2.session_id = d.session_id
                 ORDER BY created_at DESC LIMIT 1) AS last_type
            FROM dag_nodes d
            JOIN sessions s ON s.id = d.session_id
            GROUP BY d.session_id
            ORDER BY started_at DESC",
        )
        .load(&mut *conn)
        .map_err(|e| format!("list_sessions query failed: {e}"))?;

        rows.into_iter()
            .map(|r| {
                let started_at = DateTime::parse_from_rfc3339(&r.started_at)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_default();
                let is_open = r.last_type.as_deref() != Some("complete");

                Ok(SessionSummary {
                    session_id: SessionId::try_from(r.session_id)
                        .map_err(|e| format!("invalid session_id: {e}"))?,
                    agent_name: r.agent_name.unwrap_or_default(),
                    started_at,
                    message_count: usize::try_from(r.msg_count).unwrap_or(0),
                    is_open,
                })
            })
            .collect()
    }

    fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<crate::models::DagNodeRow> = dag_nodes::table
            .filter(dag_nodes::submission_id.eq(submission_id))
            .order(dag_nodes::created_at.asc())
            .select(crate::models::DagNodeRow::as_select())
            .load(&mut *conn)
            .map_err(|e| format!("get_nodes_by_submission query failed: {e}"))?;

        Ok(rows.into_iter().map(dag_node_row_to_domain).collect())
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
        use crate::schema::{dag_nodes, invoke_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::InvokeNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
            String,
        )> = invoke_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(invoke_nodes::dag_hash)))
            .filter(invoke_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::InvokeNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
                dag_nodes::parent_hash,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_invoke_node failed: {e}"))?;

        let result = row.map(|(inv, session_id, submission_id, branch, parent_hash)| {
            let harness: vlinder_core::domain::HarnessType = inv
                .harness
                .parse()
                .unwrap_or(vlinder_core::domain::HarnessType::Cli);
            let runtime: vlinder_core::domain::RuntimeType = inv
                .runtime
                .parse()
                .unwrap_or(vlinder_core::domain::RuntimeType::Container);

            let key = vlinder_core::domain::DataRoutingKey {
                session: session_id
                    .and_then(|s| SessionId::try_from(s).ok())
                    .unwrap_or_else(SessionId::new),
                branch: BranchId::from(branch.unwrap_or(0)),
                submission: vlinder_core::domain::SubmissionId::from(
                    submission_id.unwrap_or_default(),
                ),
                kind: vlinder_core::domain::DataMessageKind::Invoke {
                    harness,
                    runtime,
                    agent: vlinder_core::domain::AgentName::new(inv.agent),
                },
            };

            let diagnostics: vlinder_core::domain::InvokeDiagnostics =
                serde_json::from_slice(&inv.diagnostics).unwrap_or_else(|_| {
                    vlinder_core::domain::InvokeDiagnostics {
                        harness_version: String::new(),
                    }
                });

            let msg = vlinder_core::domain::InvokeMessage {
                id: vlinder_core::domain::MessageId::from(inv.message_id),
                dag_id: dag_hash.clone(),
                state: inv.state,
                diagnostics,
                dag_parent: DagNodeId::from(parent_hash),
                payload: inv.payload,
            };

            (key, msg)
        });

        Ok(result)
    }

    fn get_complete_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::CompleteMessage>, String> {
        use crate::schema::complete_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::CompleteNodeRow> = complete_nodes::table
            .find(dag_hash.as_str())
            .select(crate::models::CompleteNodeRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_complete_node failed: {e}"))?;

        Ok(row.map(|r| {
            let diagnostics: vlinder_core::domain::RuntimeDiagnostics =
                serde_json::from_slice(&r.diagnostics)
                    .unwrap_or_else(|_| vlinder_core::domain::RuntimeDiagnostics::placeholder(0));
            vlinder_core::domain::CompleteMessage {
                id: vlinder_core::domain::MessageId::from(r.message_id),
                dag_id: dag_hash.clone(),
                state: r.state,
                diagnostics,
                payload: r.payload,
            }
        }))
    }

    fn get_request_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::RequestMessage>, String> {
        use crate::schema::request_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::RequestNodeRow> = request_nodes::table
            .find(dag_hash.as_str())
            .select(crate::models::RequestNodeRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_request_node failed: {e}"))?;

        Ok(row.map(|r| {
            let diagnostics: vlinder_core::domain::RequestDiagnostics =
                serde_json::from_slice(&r.diagnostics).unwrap_or_else(|_| {
                    vlinder_core::domain::RequestDiagnostics {
                        sequence: 0,
                        endpoint: String::new(),
                        request_bytes: 0,
                        received_at_ms: 0,
                    }
                });
            vlinder_core::domain::RequestMessage {
                id: vlinder_core::domain::MessageId::from(r.message_id),
                dag_id: dag_hash.clone(),
                state: r.state,
                diagnostics,
                payload: r.payload,
                checkpoint: r.checkpoint,
            }
        }))
    }

    fn get_response_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::ResponseMessage>, String> {
        use crate::schema::response_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::ResponseNodeRow> = response_nodes::table
            .find(dag_hash.as_str())
            .select(crate::models::ResponseNodeRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_response_node failed: {e}"))?;

        Ok(row.map(|r| {
            let diagnostics: vlinder_core::domain::ServiceDiagnostics =
                serde_json::from_slice(&r.diagnostics).unwrap_or_else(|_| {
                    vlinder_core::domain::ServiceDiagnostics::storage(
                        vlinder_core::domain::ServiceType::Kv,
                        "unknown",
                        vlinder_core::domain::Operation::Get,
                        0,
                        0,
                    )
                });
            vlinder_core::domain::ResponseMessage {
                id: vlinder_core::domain::MessageId::from(r.message_id),
                dag_id: dag_hash.clone(),
                correlation_id: vlinder_core::domain::MessageId::from(r.correlation_id),
                state: r.state,
                diagnostics,
                payload: r.payload,
                status_code: u16::try_from(r.status_code).unwrap_or(200),
                checkpoint: r.checkpoint,
            }
        }))
    }

    fn get_branches_for_session(&self, session_id: &SessionId) -> Result<Vec<Branch>, String> {
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows: Vec<crate::models::BranchRow> = branches::table
            .filter(branches::session_id.eq(session_id.as_str()))
            .order(branches::created_at.asc())
            .select(crate::models::BranchRow::as_select())
            .load(&mut *conn)
            .map_err(|e| format!("get_branches_for_session failed: {e}"))?;

        Ok(rows.into_iter().map(branch_row_to_domain).collect())
    }

    fn latest_node_on_branch(
        &self,
        branch_id: BranchId,
        message_type: Option<MessageType>,
    ) -> Result<Option<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let row: Option<crate::models::DagNodeRow> = if let Some(mt) = message_type {
            dag_nodes::table
                .filter(dag_nodes::branch_id.eq(branch_id.as_i64()))
                .filter(dag_nodes::message_type.eq(mt.as_str()))
                .order(dag_nodes::created_at.desc())
                .select(crate::models::DagNodeRow::as_select())
                .first(&mut *conn)
                .optional()
                .map_err(|e| format!("latest_node_on_branch query failed: {e}"))?
        } else {
            dag_nodes::table
                .filter(dag_nodes::branch_id.eq(branch_id.as_i64()))
                .order(dag_nodes::created_at.desc())
                .select(crate::models::DagNodeRow::as_select())
                .first(&mut *conn)
                .optional()
                .map_err(|e| format!("latest_node_on_branch query failed: {e}"))?
        };

        Ok(row.map(dag_node_row_to_domain))
    }

    fn rename_branch(&self, id: BranchId, new_name: &str) -> Result<(), String> {
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = diesel::update(branches::table.find(id.as_i64()))
            .set(branches::name.eq(new_name))
            .execute(&mut *conn)
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
        use crate::schema::branches;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = diesel::update(branches::table.find(id.as_i64()))
            .set(branches::broken_at.eq(Some(broken_at.to_rfc3339())))
            .execute(&mut *conn)
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
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let rows = diesel::update(sessions::table.find(session_id.as_str()))
            .set(sessions::default_branch.eq(branch_id.as_i64()))
            .execute(&mut *conn)
            .map_err(|e| format!("update_session_default_branch failed: {e}"))?;
        if rows == 0 {
            return Err(format!("session {session_id} not found"));
        }
        Ok(())
    }

    fn create_session(&self, session: &Session) -> Result<(), String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        diesel::insert_or_ignore_into(sessions::table)
            .values(&crate::models::NewSession {
                id: session.id.as_str(),
                name: &session.name,
                agent_name: &session.agent,
                default_branch: session.default_branch.as_i64(),
                created_at: &session.created_at.to_rfc3339(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("create_session failed: {e}"))?;
        Ok(())
    }

    fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>, String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::SessionRow> = sessions::table
            .find(session_id.as_str())
            .select(crate::models::SessionRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_session failed: {e}"))?;

        Ok(row.map(session_row_to_domain))
    }

    fn insert_fork_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::SessionRoutingKey,
        msg: &vlinder_core::domain::ForkMessage,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewForkNode};
        use crate::schema::{dag_nodes, fork_nodes};

        let vlinder_core::domain::SessionMessageKind::Fork { agent_name } = &key.kind else {
            return Err("insert_fork_node: expected Fork kind".into());
        };

        // Look up branch BEFORE locking conn (get_branch_by_name also locks conn)
        let branch_id = self
            .get_branch_by_name(&msg.branch_name)?
            .map_or(vlinder_core::domain::BranchId::from(1), |b| b.id);

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "fork",
                session_id: Some(key.session.as_str()),
                submission_id: Some(key.submission.as_str()),
                branch_id: Some(branch_id.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(fork_nodes::table)
            .values(&NewForkNode {
                dag_hash: dag_id.as_str(),
                agent: agent_name.as_str(),
                branch_name: &msg.branch_name,
                fork_point: msg.fork_point.as_str(),
                message_id: msg.id.as_str(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert fork_nodes failed: {e}"))?;

        Ok(())
    }

    fn insert_promote_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::SessionRoutingKey,
        msg: &vlinder_core::domain::PromoteMessage,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewPromoteNode};
        use crate::schema::{dag_nodes, promote_nodes};

        let vlinder_core::domain::SessionMessageKind::Promote { agent_name } = &key.kind else {
            return Err("insert_promote_node: expected Promote kind".into());
        };

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "promote",
                session_id: Some(key.session.as_str()),
                submission_id: Some(key.submission.as_str()),
                branch_id: Some(msg.branch_id.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(promote_nodes::table)
            .values(&NewPromoteNode {
                dag_hash: dag_id.as_str(),
                agent: agent_name.as_str(),
                message_id: msg.id.as_str(),
                branch_id: Some(msg.branch_id.as_i64()),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert promote_nodes failed: {e}"))?;

        Ok(())
    }

    fn insert_deploy_agent_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::InfraRoutingKey,
        msg: &vlinder_core::domain::DeployAgentMessage,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewDeployAgentNode};
        use crate::schema::{dag_nodes, deploy_agent_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = created_at.to_rfc3339();
        let manifest_json = serde_json::to_string(&msg.manifest)
            .map_err(|e| format!("serialize manifest failed: {e}"))?;

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "deploy-agent",
                session_id: None,
                submission_id: Some(key.submission.as_str()),
                branch_id: None,
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(deploy_agent_nodes::table)
            .values(&NewDeployAgentNode {
                dag_hash: dag_id.as_str(),
                agent_name: &msg.manifest.name,
                manifest_json: &manifest_json,
                message_id: msg.id.as_str(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert deploy_agent_nodes failed: {e}"))?;

        Ok(())
    }

    fn insert_delete_agent_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        key: &vlinder_core::domain::InfraRoutingKey,
        msg: &vlinder_core::domain::DeleteAgentMessage,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewDeleteAgentNode};
        use crate::schema::{dag_nodes, delete_agent_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_id.as_str(),
                message_type: "delete-agent",
                session_id: None,
                submission_id: Some(key.submission.as_str()),
                branch_id: None,
                created_at: &created_at_str,
                protocol_version: "v1",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(delete_agent_nodes::table)
            .values(&NewDeleteAgentNode {
                dag_hash: dag_id.as_str(),
                agent_name: msg.agent.as_str(),
                message_id: msg.id.as_str(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert delete_agent_nodes failed: {e}"))?;

        Ok(())
    }

    fn get_session_by_name(&self, name: &str) -> Result<Option<Session>, String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::SessionRow> = sessions::table
            .filter(sessions::name.eq(name))
            .select(crate::models::SessionRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_session_by_name failed: {e}"))?;

        Ok(row.map(session_row_to_domain))
    }
}

#[cfg(test)]
impl SqliteDagStore {
    /// Test-only helper: insert a raw `DagNode` into the chain index.
    fn insert_node(&self, node: &DagNode) -> Result<(), String> {
        use crate::models::NewDagNode;
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let snapshot_json = serde_json::to_string(&node.state)
            .map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = node.created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: node.id.as_str(),
                parent_hash: node.parent_id.as_str(),
                message_type: node.message_type().as_str(),
                session_id: Some(node.session_id().as_str()),
                submission_id: Some(node.submission_id().as_str()),
                branch_id: Some(node.branch_id().as_i64()),
                created_at: &created_at_str,
                protocol_version: node.protocol_version(),
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{hash_dag_node, BranchId, Snapshot, SubmissionId};

    fn test_store() -> (SqliteDagStore, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteDagStore::open(&path).unwrap();
        // Create session + default branch to satisfy FK constraints.
        let session = vlinder_core::domain::session::Session {
            id: sess(),
            name: "test-session".to_string(),
            agent: "agent-a".to_string(),
            default_branch: BranchId::from(1),
            created_at: Utc::now(),
        };
        store.create_session(&session).unwrap();
        store.create_branch("main", &sess(), None).unwrap();
        (store, dir)
    }

    fn sess() -> SessionId {
        SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap()
    }

    fn sub() -> SubmissionId {
        SubmissionId::from("sub-1".to_string())
    }

    /// Build a test `DagNode`.
    fn test_node(payload: &[u8], parent: &DagNodeId) -> DagNode {
        let id = hash_dag_node(payload, parent, &MessageType::Fork, &[], &sess());
        DagNode {
            id,
            parent_id: parent.clone(),
            created_at: Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Fork,
            session: sess(),
            submission: sub(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        }
    }

    #[test]
    fn round_trip_insert_get() {
        let (store, _dir) = test_store();
        let node = test_node(b"hello", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        let retrieved = store.get_node(&node.id).unwrap().unwrap();

        assert_eq!(retrieved.id, node.id);
        assert_eq!(retrieved.parent_id, node.parent_id);
    }

    #[test]
    fn get_node_returns_none_for_unknown() {
        let (store, _dir) = test_store();
        assert_eq!(
            store
                .get_node(&DagNodeId::from("nonexistent".to_string()))
                .unwrap(),
            None
        );
    }

    #[test]
    fn idempotent_insert() {
        let (store, _dir) = test_store();
        let node = test_node(b"data", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        store.insert_node(&node).unwrap(); // No error

        let retrieved = store.get_node(&node.id).unwrap().unwrap();
        assert_eq!(retrieved.id, node.id);
    }

    #[test]
    fn get_children() {
        let (store, _dir) = test_store();

        let parent = test_node(b"parent", &DagNodeId::root());

        let child_id = hash_dag_node(b"child", &parent.id, &MessageType::Fork, &[], &sess());
        let mut child = DagNode {
            id: child_id,
            parent_id: parent.id.clone(),
            created_at: Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Fork,
            session: sess(),
            submission: sub(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };
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
        let (store, _dir) = test_store();

        let sess1 =
            SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        let sess2 =
            SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();

        // Create second session + branch for FK constraints
        let session2 = vlinder_core::domain::session::Session {
            id: sess2.clone(),
            name: "test-session-2".to_string(),
            agent: "agent-b".to_string(),
            default_branch: BranchId::from(1),
            created_at: Utc::now(),
        };
        store.create_session(&session2).unwrap();
        store.create_branch("main", &sess2, None).unwrap();

        let id_a = hash_dag_node(b"a", &DagNodeId::root(), &MessageType::Fork, &[], &sess1);
        let node_a = DagNode {
            id: id_a,
            parent_id: DagNodeId::root(),
            created_at: Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Fork,
            session: sess1.clone(),
            submission: sub(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };

        let id_b = hash_dag_node(b"b", &DagNodeId::root(), &MessageType::Fork, &[], &sess2);
        let node_b = DagNode {
            id: id_b,
            parent_id: DagNodeId::root(),
            created_at: Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Fork,
            session: sess2.clone(),
            submission: sub(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };

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
        let (store, _dir) = test_store();

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
        let (store, _dir) = test_store();

        let session_id = sess();
        // "main" branch already created by test_store()
        let fork = DagNodeId::from("abc123".to_string());
        let fork_id = store
            .create_branch("repair-1", &session_id, Some(&fork))
            .unwrap();

        let tl = store.get_branch(fork_id).unwrap().unwrap();
        assert_eq!(tl.fork_point, Some(fork));
    }

    #[test]
    fn get_timeline_by_branch() {
        let (store, _dir) = test_store();
        let session_id = sess();
        // "main" branch already created by test_store()

        let tl = store.get_branch_by_name("main").unwrap().unwrap();
        assert_eq!(tl.session_id, session_id);

        assert!(store.get_branch_by_name("nonexistent").unwrap().is_none());
    }

    // ========================================================================
    // latest_node_on_branch tests
    // ========================================================================

    #[test]
    fn latest_node_on_branch_returns_none_for_empty() {
        let (store, _dir) = test_store();
        let result = store
            .latest_node_on_branch(BranchId::from(1), None)
            .unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn latest_node_on_branch_returns_most_recent() {
        let (store, _dir) = test_store();

        let node1 = test_node(b"first", &DagNodeId::root());
        store.insert_node(&node1).unwrap();

        let id2 = hash_dag_node(b"response", &node1.id, &MessageType::Fork, &[], &sess());
        let node2 = DagNode {
            id: id2,
            parent_id: node1.id.clone(),
            created_at: Utc::now(),
            state: Snapshot::empty(),
            msg_type: MessageType::Fork,
            session: sess(),
            submission: sub(),
            branch: BranchId::from(1),
            protocol_version: "v1".to_string(),
        };
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
        let (store, _dir) = test_store();
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
        let (store, _dir) = test_store();
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
        let (store, _dir) = test_store();
        let sid = SessionId::try_from("00000000-0000-0000-0000-000000000000".to_string()).unwrap();
        assert!(store.get_session(&sid).unwrap().is_none());
    }

    #[test]
    fn get_session_by_name_returns_none_for_unknown() {
        let (store, _dir) = test_store();
        assert!(store.get_session_by_name("nonexistent").unwrap().is_none());
    }

    #[test]
    fn create_session_is_idempotent() {
        let (store, _dir) = test_store();
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
    fn dag_node_row_with_message_blob_ignores_it() {
        // The old message_blob column has been removed from dag_nodes.
        // Verify a raw insert with only the slimmed columns round-trips.
        use diesel::connection::SimpleConnection;

        let (store, _dir) = test_store();
        let mut conn = store.conn.lock().unwrap();
        conn.batch_execute(
            "INSERT INTO dag_nodes (hash, parent_hash, message_type, session_id, submission_id, created_at, protocol_version, branch_id, snapshot)
             VALUES ('h1', '', 'fork', 'd4761d76-dee4-4ebf-9df4-43b52efa4f78', 'sub-1', '2025-01-01T00:00:00Z', '', 1, '{}')",
        ).unwrap();
        drop(conn);

        let result = store.get_node(&DagNodeId::from("h1".to_string()));
        assert!(result.is_ok());
        let node = result.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::Fork);
    }

    // ========================================================================
    // Infra plane insert tests
    // ========================================================================

    #[test]
    fn insert_deploy_agent_node_round_trip() {
        let (store, _dir) = test_store();

        let manifest = vlinder_core::domain::AgentManifest {
            name: "test-agent".to_string(),
            description: "Test".to_string(),
            source: None,
            runtime: "container".to_string(),
            executable: "localhost/test:latest".to_string(),
            requirements: vlinder_core::domain::RequirementsConfig {
                models: std::collections::HashMap::new(),
                services: std::collections::HashMap::new(),
                mounts: std::collections::HashMap::new(),
            },
            prompts: None,
            object_storage: None,
            vector_storage: None,
        };

        let msg = vlinder_core::domain::DeployAgentMessage::new(manifest);
        let key = vlinder_core::domain::InfraRoutingKey {
            submission: sub(),
            kind: vlinder_core::domain::InfraMessageKind::DeployAgent,
        };
        let dag_id = DagNodeId::from("deploy-hash-1".to_string());

        store
            .insert_deploy_agent_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &key,
                &msg,
            )
            .unwrap();

        let node = store.get_node(&dag_id).unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::DeployAgent);
        assert!(node.session.as_str().contains("00000000")); // nullable → default
    }

    #[test]
    fn insert_delete_agent_node_round_trip() {
        let (store, _dir) = test_store();

        let msg = vlinder_core::domain::DeleteAgentMessage::new(
            vlinder_core::domain::AgentName::new("echo"),
        );
        let key = vlinder_core::domain::InfraRoutingKey {
            submission: sub(),
            kind: vlinder_core::domain::InfraMessageKind::DeleteAgent,
        };
        let dag_id = DagNodeId::from("delete-hash-1".to_string());

        store
            .insert_delete_agent_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &key,
                &msg,
            )
            .unwrap();

        let node = store.get_node(&dag_id).unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::DeleteAgent);
    }
}
