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

use async_trait::async_trait;
use vlinder_core::domain::session::Session;
use vlinder_core::domain::{
    Branch, BranchId, DagNode, DagNodeId, DagStore, MessageType, ServiceBackendV2,
    ServiceOperation, SessionId, SessionSummary, SubmissionId, SvcMessageKind, SvcRoutingKey,
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
                 external_id TEXT NOT NULL,
                 name TEXT NOT NULL UNIQUE,
                 agent_name TEXT NOT NULL,
                 default_branch INTEGER NOT NULL DEFAULT 1,
                 created_at TEXT NOT NULL
             );
             CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_external_id ON sessions(external_id);
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
                 parent_hash TEXT REFERENCES dag_nodes(hash),
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
             CREATE TABLE IF NOT EXISTS svc_request_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 service_type TEXT NOT NULL,
                 service_backend TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 message_id TEXT NOT NULL UNIQUE,
                 tool_call_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics TEXT,
                 arguments BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS svc_response_nodes (
                 dag_hash TEXT PRIMARY KEY REFERENCES dag_nodes(hash),
                 agent TEXT NOT NULL,
                 service_type TEXT NOT NULL,
                 service_backend TEXT NOT NULL,
                 operation TEXT NOT NULL,
                 sequence INTEGER NOT NULL,
                 message_id TEXT NOT NULL UNIQUE,
                 correlation_id TEXT NOT NULL,
                 state TEXT,
                 diagnostics TEXT,
                 payload BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS mcp_servers (
                 name TEXT PRIMARY KEY,
                 url TEXT NOT NULL,
                 tools_json TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS readiness_checks (
                 id INTEGER PRIMARY KEY AUTOINCREMENT,
                 agent_name TEXT NOT NULL,
                 worker TEXT NOT NULL,
                 status TEXT NOT NULL,
                 updated_at TEXT NOT NULL,
                 error TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_readiness_checks_agent_worker
                 ON readiness_checks (agent_name, worker, updated_at);
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

/// Convert a Diesel `SessionRow` to the domain `Session`, returning an error
/// on corrupt data (invalid `id`, `external_id`, or `created_at`).
fn session_row_to_domain(r: crate::models::SessionRow) -> Result<Session, String> {
    let created_at = DateTime::parse_from_rfc3339(&r.created_at)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| format!("invalid created_at in session row: {e}"))?;
    let external_id = vlinder_core::domain::ExternalSessionId::new(&r.external_id)
        .map_err(|e| format!("invalid external_id in session row: {e}"))?;
    let id = SessionId::try_from(r.id).map_err(|e| format!("invalid id in session row: {e}"))?;
    Ok(Session {
        id,
        external_id,
        name: r.name,
        agent: r.agent_name,
        default_branch: BranchId::from(r.default_branch),
        created_at,
    })
}

/// Convert a `DagNodeId` to an `Option<&str>` for SQL storage.
/// Root nodes (empty string) become `None` for FK integrity.
fn parent_hash_for_sql(id: &DagNodeId) -> Option<&str> {
    let s = id.as_str();
    if s.is_empty() {
        None
    } else {
        Some(s)
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
        parent_id: r.parent_hash.map_or_else(DagNodeId::root, DagNodeId::from),
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

#[async_trait]
impl DagStore for SqliteDagStore {
    async fn insert_invoke_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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
                payload: &serde_json::to_vec(msg).unwrap_or_default(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert invoke_nodes failed: {e}"))?;

        Ok(())
    }

    async fn insert_complete_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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
                payload: &serde_json::to_vec(msg).unwrap_or_default(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert complete_nodes failed: {e}"))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_request_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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
    async fn insert_response_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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

    #[allow(clippy::too_many_arguments)]
    async fn insert_svc_request_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: ServiceBackendV2,
        operation: ServiceOperation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::RequestV2,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewSvcRequestNode};
        use crate::schema::{dag_nodes, svc_request_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let payload_bytes = msg.payload.clone();
        let state_json = msg.state.as_deref().map(String::from);
        let diagnostics_json = serde_json::to_string(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();
        let service_type = service.service_type_str();
        let service_backend = service.backend_str();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_hash_for_sql(parent_id),
                message_type: "svc_request",
                session_id: Some(session.as_str()),
                submission_id: Some(submission.as_str()),
                branch_id: Some(branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v2",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(svc_request_nodes::table)
            .values(&NewSvcRequestNode {
                dag_hash: dag_id.as_str(),
                agent: agent.as_str(),
                service_type,
                service_backend,
                operation: operation.as_str(),
                sequence: i32::try_from(sequence.as_u32()).unwrap_or(0),
                message_id: msg.id.as_str(),
                tool_call_id: msg.tool_call_id.as_str(),
                state: state_json.as_deref(),
                diagnostics: Some(&diagnostics_json),
                payload: &payload_bytes,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert svc_request_nodes failed: {e}"))?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn insert_svc_response_node(
        &self,
        dag_id: &DagNodeId,
        parent_id: &DagNodeId,
        created_at: chrono::DateTime<chrono::Utc>,
        state: &vlinder_core::domain::Snapshot,
        session: &SessionId,
        submission: &vlinder_core::domain::SubmissionId,
        branch: BranchId,
        agent: &vlinder_core::domain::AgentName,
        service: ServiceBackendV2,
        operation: ServiceOperation,
        sequence: vlinder_core::domain::Sequence,
        msg: &vlinder_core::domain::ResponseV2,
    ) -> Result<(), String> {
        use crate::models::{NewDagNode, NewSvcResponseNode};
        use crate::schema::{dag_nodes, svc_response_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let state_json = msg.state.as_deref().map(String::from);
        let diagnostics_json = serde_json::to_string(&msg.diagnostics).unwrap_or_default();
        let created_at_str = created_at.to_rfc3339();
        let service_type = service.service_type_str();
        let service_backend = service.backend_str();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_hash_for_sql(parent_id),
                message_type: "svc_response",
                session_id: Some(session.as_str()),
                submission_id: Some(submission.as_str()),
                branch_id: Some(branch.as_i64()),
                created_at: &created_at_str,
                protocol_version: "v2",
                snapshot: &snapshot_json,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert dag_nodes failed: {e}"))?;

        diesel::insert_or_ignore_into(svc_response_nodes::table)
            .values(&NewSvcResponseNode {
                dag_hash: dag_id.as_str(),
                agent: agent.as_str(),
                service_type,
                service_backend,
                operation: operation.as_str(),
                sequence: i32::try_from(sequence.as_u32()).unwrap_or(0),
                message_id: msg.id.as_str(),
                correlation_id: msg.correlation_id.as_str(),
                state: state_json.as_deref(),
                diagnostics: Some(&diagnostics_json),
                payload: &msg.payload,
            })
            .execute(&mut *conn)
            .map_err(|e| format!("insert svc_response_nodes failed: {e}"))?;

        Ok(())
    }

    async fn get_node(&self, hash: &DagNodeId) -> Result<Option<DagNode>, String> {
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

    async fn get_node_by_prefix(&self, prefix: &str) -> Result<Option<DagNode>, String> {
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

    async fn get_session_nodes(&self, session_id: &SessionId) -> Result<Vec<DagNode>, String> {
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

    async fn get_children(&self, parent_hash: &DagNodeId) -> Result<Vec<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let parent = parent_hash_for_sql(parent_hash);

        let rows: Vec<crate::models::DagNodeRow> = match parent {
            Some(h) => dag_nodes::table
                .filter(dag_nodes::parent_hash.eq(h))
                .select(crate::models::DagNodeRow::as_select())
                .load(&mut *conn)
                .map_err(|e| format!("get_children query failed: {e}"))?,
            None => dag_nodes::table
                .filter(dag_nodes::parent_hash.is_null())
                .select(crate::models::DagNodeRow::as_select())
                .load(&mut *conn)
                .map_err(|e| format!("get_children query failed: {e}"))?,
        };

        Ok(rows.into_iter().map(dag_node_row_to_domain).collect())
    }

    // -------------------------------------------------------------------------
    // Branch methods
    // -------------------------------------------------------------------------

    async fn create_branch(
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

    async fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, String> {
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

    async fn get_branch(&self, id: BranchId) -> Result<Option<Branch>, String> {
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

    async fn list_sessions(&self) -> Result<Vec<SessionSummary>, String> {
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

    async fn get_nodes_by_submission(&self, submission_id: &str) -> Result<Vec<DagNode>, String> {
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

    async fn get_invoke_node(
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
            Option<String>,
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

            let msg: vlinder_core::domain::InvokeMessage = serde_json::from_slice(&inv.payload)
                .unwrap_or_else(|_| vlinder_core::domain::InvokeMessage {
                    id: vlinder_core::domain::MessageId::from(inv.message_id.clone()),
                    dag_id: dag_hash.clone(),
                    state: inv.state.clone(),
                    diagnostics,
                    dag_parent: parent_hash.map_or_else(DagNodeId::root, DagNodeId::from),
                    current_input: vec![],
                });

            (key, msg)
        });

        Ok(result)
    }

    async fn get_complete_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::DataRoutingKey,
            vlinder_core::domain::CompleteMessage,
        )>,
        String,
    > {
        use crate::schema::{complete_nodes, dag_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::CompleteNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
        )> = complete_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(complete_nodes::dag_hash)))
            .filter(complete_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::CompleteNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_complete_node failed: {e}"))?;

        let result = row.map(|(r, session_id, submission_id, branch)| {
            let key = vlinder_core::domain::DataRoutingKey {
                session: session_id
                    .and_then(|s| SessionId::try_from(s).ok())
                    .unwrap_or_else(SessionId::new),
                branch: BranchId::from(branch.unwrap_or(0)),
                submission: vlinder_core::domain::SubmissionId::from(
                    submission_id.unwrap_or_default(),
                ),
                kind: vlinder_core::domain::DataMessageKind::Complete {
                    agent: vlinder_core::domain::AgentName::new(r.agent),
                    harness: r
                        .harness
                        .parse()
                        .unwrap_or(vlinder_core::domain::HarnessType::Cli),
                },
            };

            let diagnostics: vlinder_core::domain::RuntimeDiagnostics =
                serde_json::from_slice(&r.diagnostics)
                    .unwrap_or_else(|_| vlinder_core::domain::RuntimeDiagnostics::placeholder(0));
            let msg = serde_json::from_slice(&r.payload).unwrap_or(
                vlinder_core::domain::CompleteMessage {
                    id: vlinder_core::domain::MessageId::from(r.message_id),
                    dag_id: dag_hash.clone(),
                    dag_parent: vlinder_core::domain::DagNodeId::root(),
                    state: r.state,
                    diagnostics,
                    content: None,
                    tool_calls: None,
                    payload: vec![],
                },
            );
            (key, msg)
        });

        Ok(result)
    }

    async fn get_request_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::DataRoutingKey,
            vlinder_core::domain::RequestMessage,
        )>,
        String,
    > {
        use crate::schema::{dag_nodes, request_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::RequestNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
        )> = request_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(request_nodes::dag_hash)))
            .filter(request_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::RequestNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_request_node failed: {e}"))?;

        let result = row.map(|(r, session_id, submission_id, branch)| {
            let service: vlinder_core::domain::ServiceBackend =
                r.service
                    .parse()
                    .unwrap_or(vlinder_core::domain::ServiceBackend::Infer(
                        vlinder_core::domain::InferenceBackendType::OpenRouter,
                    ));
            let operation: vlinder_core::domain::Operation = r
                .operation
                .parse()
                .unwrap_or(vlinder_core::domain::Operation::Get);
            let key = vlinder_core::domain::DataRoutingKey {
                session: session_id
                    .and_then(|s| SessionId::try_from(s).ok())
                    .unwrap_or_else(SessionId::new),
                branch: BranchId::from(branch.unwrap_or(0)),
                submission: vlinder_core::domain::SubmissionId::from(
                    submission_id.unwrap_or_default(),
                ),
                kind: vlinder_core::domain::DataMessageKind::Request {
                    agent: vlinder_core::domain::AgentName::new(r.agent),
                    service,
                    operation,
                    sequence: vlinder_core::domain::Sequence::from(
                        u32::try_from(r.sequence).unwrap_or(0),
                    ),
                },
            };

            let diagnostics: vlinder_core::domain::RequestDiagnostics =
                serde_json::from_slice(&r.diagnostics).unwrap_or_else(|_| {
                    vlinder_core::domain::RequestDiagnostics {
                        sequence: 0,
                        endpoint: String::new(),
                        request_bytes: 0,
                        received_at_ms: 0,
                    }
                });
            let msg = vlinder_core::domain::RequestMessage {
                id: vlinder_core::domain::MessageId::from(r.message_id),
                dag_id: dag_hash.clone(),
                dag_parent: vlinder_core::domain::DagNodeId::root(),
                state: r.state,
                diagnostics,
                payload: r.payload,
                checkpoint: r.checkpoint,
            };
            (key, msg)
        });

        Ok(result)
    }

    async fn get_response_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::DataRoutingKey,
            vlinder_core::domain::ResponseMessage,
        )>,
        String,
    > {
        use crate::schema::{dag_nodes, response_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::ResponseNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
        )> = response_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(response_nodes::dag_hash)))
            .filter(response_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::ResponseNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_response_node failed: {e}"))?;

        let result = row.map(|(r, session_id, submission_id, branch)| {
            let service: vlinder_core::domain::ServiceBackend =
                r.service
                    .parse()
                    .unwrap_or(vlinder_core::domain::ServiceBackend::Infer(
                        vlinder_core::domain::InferenceBackendType::OpenRouter,
                    ));
            let operation: vlinder_core::domain::Operation = r
                .operation
                .parse()
                .unwrap_or(vlinder_core::domain::Operation::Get);
            let key = vlinder_core::domain::DataRoutingKey {
                session: session_id
                    .and_then(|s| SessionId::try_from(s).ok())
                    .unwrap_or_else(SessionId::new),
                branch: BranchId::from(branch.unwrap_or(0)),
                submission: vlinder_core::domain::SubmissionId::from(
                    submission_id.unwrap_or_default(),
                ),
                kind: vlinder_core::domain::DataMessageKind::Response {
                    agent: vlinder_core::domain::AgentName::new(r.agent),
                    service,
                    operation,
                    sequence: vlinder_core::domain::Sequence::from(
                        u32::try_from(r.sequence).unwrap_or(0),
                    ),
                },
            };

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
            let msg = vlinder_core::domain::ResponseMessage {
                id: vlinder_core::domain::MessageId::from(r.message_id),
                dag_id: dag_hash.clone(),
                dag_parent: vlinder_core::domain::DagNodeId::root(),
                correlation_id: vlinder_core::domain::MessageId::from(r.correlation_id),
                state: r.state,
                diagnostics,
                payload: r.payload,
                status_code: u16::try_from(r.status_code).unwrap_or(200),
                checkpoint: r.checkpoint,
            };
            (key, msg)
        });

        Ok(result)
    }

    async fn get_svc_request_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::SvcRoutingKey,
            vlinder_core::domain::RequestV2,
        )>,
        String,
    > {
        use crate::schema::{dag_nodes, svc_request_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::SvcRequestNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
        )> = svc_request_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(svc_request_nodes::dag_hash)))
            .filter(svc_request_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::SvcRequestNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_svc_request_node failed: {e}"))?;

        let result = row
            .map(
                |(r, session_id, submission_id, branch)| -> Result<
                    (
                        vlinder_core::domain::SvcRoutingKey,
                        vlinder_core::domain::RequestV2,
                    ),
                    String,
                > {
                    let service = ServiceBackendV2::from_parts(&r.service_type, &r.service_backend)
                        .ok_or_else(|| {
                            format!(
                        "get_svc_request_node: invalid ServiceBackendV2 (type={:?}, backend={:?})",
                        r.service_type, r.service_backend
                    )
                        })?;
                    let key = SvcRoutingKey {
                        session: session_id
                            .and_then(|s| SessionId::try_from(s).ok())
                            .unwrap_or_else(SessionId::new),
                        branch: BranchId::from(branch.unwrap_or(0)),
                        submission: vlinder_core::domain::SubmissionId::from(
                            submission_id.unwrap_or_default(),
                        ),
                        kind: SvcMessageKind::SvcRequest {
                            agent: vlinder_core::domain::AgentName::new(r.agent),
                            service,
                            operation: ServiceOperation::new(&r.operation),
                            sequence: vlinder_core::domain::Sequence::from(
                                u32::try_from(r.sequence).unwrap_or(0),
                            ),
                        },
                    };

                    let diagnostics: vlinder_core::domain::SvcRequestDiagnostics =
                        serde_json::from_slice(&r.diagnostics.unwrap_or_default().into_bytes())
                            .unwrap_or_default();
                    let msg = vlinder_core::domain::RequestV2 {
                        id: vlinder_core::domain::MessageId::from(r.message_id),
                        dag_id: dag_hash.clone(),
                        dag_parent: vlinder_core::domain::DagNodeId::root(),
                        tool_call_id: vlinder_core::domain::ToolCallId::from(r.tool_call_id),
                        state: r.state,
                        diagnostics,
                        payload: r.arguments,
                    };
                    Ok((key, msg))
                },
            )
            .transpose()?;
        Ok(result)
    }

    async fn get_svc_response_node(
        &self,
        dag_hash: &DagNodeId,
    ) -> Result<
        Option<(
            vlinder_core::domain::SvcRoutingKey,
            vlinder_core::domain::ResponseV2,
        )>,
        String,
    > {
        use crate::schema::{dag_nodes, svc_response_nodes};

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        #[allow(clippy::type_complexity)]
        let row: Option<(
            crate::models::SvcResponseNodeRow,
            Option<String>,
            Option<String>,
            Option<i64>,
        )> = svc_response_nodes::table
            .inner_join(dag_nodes::table.on(dag_nodes::hash.eq(svc_response_nodes::dag_hash)))
            .filter(svc_response_nodes::dag_hash.eq(dag_hash.as_str()))
            .select((
                crate::models::SvcResponseNodeRow::as_select(),
                dag_nodes::session_id,
                dag_nodes::submission_id,
                dag_nodes::branch_id,
            ))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_svc_response_node failed: {e}"))?;

        let result = row
            .map(
                |(r, session_id, submission_id, branch)| -> Result<
                    (
                        vlinder_core::domain::SvcRoutingKey,
                        vlinder_core::domain::ResponseV2,
                    ),
                    String,
                > {
                    let service = ServiceBackendV2::from_parts(&r.service_type, &r.service_backend)
                        .ok_or_else(|| {
                            format!(
                        "get_svc_response_node: invalid ServiceBackendV2 (type={:?}, backend={:?})",
                        r.service_type, r.service_backend
                    )
                        })?;
                    let key = SvcRoutingKey {
                        session: session_id
                            .and_then(|s| SessionId::try_from(s).ok())
                            .unwrap_or_else(SessionId::new),
                        branch: BranchId::from(branch.unwrap_or(0)),
                        submission: vlinder_core::domain::SubmissionId::from(
                            submission_id.unwrap_or_default(),
                        ),
                        kind: SvcMessageKind::SvcResponse {
                            agent: vlinder_core::domain::AgentName::new(r.agent),
                            service,
                            operation: ServiceOperation::new(&r.operation),
                            sequence: vlinder_core::domain::Sequence::from(
                                u32::try_from(r.sequence).unwrap_or(0),
                            ),
                        },
                    };

                    let diagnostics: vlinder_core::domain::SvcResponseDiagnostics =
                        serde_json::from_slice(&r.diagnostics.unwrap_or_default().into_bytes())
                            .unwrap_or_default();
                    let msg = vlinder_core::domain::ResponseV2 {
                        id: vlinder_core::domain::MessageId::from(r.message_id),
                        dag_id: dag_hash.clone(),
                        dag_parent: vlinder_core::domain::DagNodeId::root(),
                        correlation_id: vlinder_core::domain::MessageId::from(r.correlation_id),
                        state: r.state,
                        diagnostics,
                        payload: r.payload,
                    };
                    Ok((key, msg))
                },
            )
            .transpose()?;
        Ok(result)
    }

    async fn get_invoke_message(
        &self,
        dag_id: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::InvokeMessage>, String> {
        use crate::schema::invoke_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let row: Option<crate::models::InvokeNodeRow> = invoke_nodes::table
            .filter(invoke_nodes::dag_hash.eq(dag_id.as_str()))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_invoke_message failed: {e}"))?;

        let result = row.map(|r| {
            serde_json::from_slice(&r.payload).unwrap_or_else(|_| {
                vlinder_core::domain::InvokeMessage {
                    id: vlinder_core::domain::MessageId::from(r.message_id),
                    dag_id: dag_id.clone(),
                    state: r.state,
                    diagnostics: vlinder_core::domain::InvokeDiagnostics {
                        harness_version: String::new(),
                    },
                    dag_parent: DagNodeId::root(),
                    current_input: vec![],
                }
            })
        });

        Ok(result)
    }

    async fn get_complete_message(
        &self,
        dag_id: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::CompleteMessage>, String> {
        use crate::schema::complete_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let row: Option<crate::models::CompleteNodeRow> = complete_nodes::table
            .filter(complete_nodes::dag_hash.eq(dag_id.as_str()))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_complete_message failed: {e}"))?;

        let result = row.map(|r| {
            serde_json::from_slice(&r.payload).unwrap_or_else(|_| {
                vlinder_core::domain::CompleteMessage {
                    id: vlinder_core::domain::MessageId::from(r.message_id),
                    dag_id: dag_id.clone(),
                    dag_parent: DagNodeId::root(),
                    state: r.state,
                    diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(0),
                    content: None,
                    tool_calls: None,
                    payload: vec![],
                }
            })
        });

        Ok(result)
    }

    async fn get_request_v2(
        &self,
        dag_id: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::RequestV2>, String> {
        use crate::schema::svc_request_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let row: Option<crate::models::SvcRequestNodeRow> = svc_request_nodes::table
            .filter(svc_request_nodes::dag_hash.eq(dag_id.as_str()))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_request_v2 failed: {e}"))?;

        let result = row.map(|r| vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::from(r.message_id),
            dag_id: dag_id.clone(),
            dag_parent: DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::from(r.tool_call_id),
            state: r.state,
            diagnostics: serde_json::from_slice(&r.diagnostics.unwrap_or_default().into_bytes())
                .unwrap_or_default(),
            payload: r.arguments,
        });

        Ok(result)
    }

    async fn get_response_v2(
        &self,
        dag_id: &DagNodeId,
    ) -> Result<Option<vlinder_core::domain::ResponseV2>, String> {
        use crate::schema::svc_response_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let row: Option<crate::models::SvcResponseNodeRow> = svc_response_nodes::table
            .filter(svc_response_nodes::dag_hash.eq(dag_id.as_str()))
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_response_v2 failed: {e}"))?;

        let result = row.map(|r| vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::from(r.message_id),
            dag_id: dag_id.clone(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::from(r.correlation_id),
            state: r.state,
            diagnostics: serde_json::from_slice(&r.diagnostics.unwrap_or_default().into_bytes())
                .unwrap_or_default(),
            payload: r.payload,
        });

        Ok(result)
    }

    async fn get_branches_for_session(
        &self,
        session_id: &SessionId,
    ) -> Result<Vec<Branch>, String> {
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

    async fn latest_nodes_on_branch(
        &self,
        branch_id: BranchId,
        n: u32,
    ) -> Result<Vec<DagNode>, String> {
        use crate::schema::dag_nodes;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");

        let rows: Vec<crate::models::DagNodeRow> = dag_nodes::table
            .filter(dag_nodes::branch_id.eq(branch_id.as_i64()))
            .order(dag_nodes::created_at.desc())
            .limit(i64::from(n))
            .select(crate::models::DagNodeRow::as_select())
            .load(&mut *conn)
            .map_err(|e| format!("latest_nodes_on_branch query failed: {e}"))?;

        // Reverse to oldest-first (SQL returns newest-first via ORDER BY DESC)
        let mut nodes: Vec<DagNode> = rows.into_iter().map(dag_node_row_to_domain).collect();
        nodes.reverse();
        Ok(nodes)
    }

    async fn latest_node_on_branch(
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

    async fn rename_branch(&self, id: BranchId, new_name: &str) -> Result<(), String> {
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

    async fn seal_branch(
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

    async fn update_session_default_branch(
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

    async fn create_session(&self, session: &Session) -> Result<(), String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        diesel::insert_or_ignore_into(sessions::table)
            .values(&crate::models::NewSession {
                id: session.id.as_str(),
                external_id: session.external_id.as_str(),
                name: &session.name,
                agent_name: &session.agent,
                default_branch: session.default_branch.as_i64(),
                created_at: &session.created_at.to_rfc3339(),
            })
            .execute(&mut *conn)
            .map_err(|e| format!("create_session failed: {e}"))?;
        Ok(())
    }

    async fn get_session(&self, session_id: &SessionId) -> Result<Option<Session>, String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::SessionRow> = sessions::table
            .find(session_id.as_str())
            .select(crate::models::SessionRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_session failed: {e}"))?;

        Ok(row.map(session_row_to_domain).transpose()?)
    }

    async fn insert_fork_node(
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
            .get_branch_by_name(&msg.branch_name)
            .await?
            .map_or(vlinder_core::domain::BranchId::from(1), |b| b.id);

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let snapshot_json =
            serde_json::to_string(state).map_err(|e| format!("serialize snapshot failed: {e}"))?;
        let created_at_str = created_at.to_rfc3339();

        diesel::insert_or_ignore_into(dag_nodes::table)
            .values(&NewDagNode {
                hash: dag_id.as_str(),
                parent_hash: parent_hash_for_sql(parent_id),
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

    async fn insert_promote_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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

    async fn insert_deploy_agent_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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

    async fn insert_delete_agent_node(
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
                parent_hash: parent_hash_for_sql(parent_id),
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

    async fn get_session_by_name(&self, name: &str) -> Result<Option<Session>, String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::SessionRow> = sessions::table
            .filter(sessions::name.eq(name))
            .select(crate::models::SessionRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_session_by_name failed: {e}"))?;

        Ok(row.map(session_row_to_domain).transpose()?)
    }

    async fn get_session_by_external_id(
        &self,
        external_id: &vlinder_core::domain::ExternalSessionId,
    ) -> Result<Option<Session>, String> {
        use crate::schema::sessions;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        let row: Option<crate::models::SessionRow> = sessions::table
            .filter(sessions::external_id.eq(external_id.as_str()))
            .select(crate::models::SessionRow::as_select())
            .first(&mut *conn)
            .optional()
            .map_err(|e| format!("get_session_by_external_id failed: {e}"))?;

        Ok(row.map(session_row_to_domain).transpose()?)
    }

    async fn exists_in_submission(
        &self,
        submission: &SubmissionId,
        branch: BranchId,
        message_type: MessageType,
    ) -> Result<bool, String> {
        use crate::schema::dag_nodes;
        use diesel::dsl::exists;
        use diesel::select;

        let mut conn = self.conn.lock().expect("db connection lock poisoned");
        select(exists(
            dag_nodes::table
                .filter(dag_nodes::submission_id.eq(submission.as_str()))
                .filter(dag_nodes::branch_id.eq(branch.as_i64()))
                .filter(dag_nodes::message_type.eq(message_type.as_str())),
        ))
        .get_result(&mut *conn)
        .map_err(|e| format!("exists_in_submission failed: {e}"))
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
                parent_hash: parent_hash_for_sql(&node.parent_id),
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

    async fn test_store() -> (SqliteDagStore, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        let store = SqliteDagStore::open(&path).unwrap();
        // Create session + default branch to satisfy FK constraints.
        let external_id = vlinder_core::domain::ExternalSessionId::new("test-ext-id").unwrap();
        let session = vlinder_core::domain::session::Session {
            id: sess(),
            external_id,
            name: "test-session".to_string(),
            agent: "agent-a".to_string(),
            default_branch: BranchId::from(1),
            created_at: Utc::now(),
        };
        store.create_session(&session).await.unwrap();
        store.create_branch("main", &sess(), None).await.unwrap();
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

    #[tokio::test]
    async fn round_trip_insert_get() {
        let (store, _dir) = test_store().await;
        let node = test_node(b"hello", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        let retrieved = store.get_node(&node.id).await.unwrap().unwrap();

        assert_eq!(retrieved.id, node.id);
        assert_eq!(retrieved.parent_id, node.parent_id);
    }

    #[tokio::test]
    async fn get_node_returns_none_for_unknown() {
        let (store, _dir) = test_store().await;
        assert_eq!(
            store
                .get_node(&DagNodeId::from("nonexistent".to_string()))
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn idempotent_insert() {
        let (store, _dir) = test_store().await;
        let node = test_node(b"data", &DagNodeId::root());

        store.insert_node(&node).unwrap();
        store.insert_node(&node).unwrap(); // No error

        let retrieved = store.get_node(&node.id).await.unwrap().unwrap();
        assert_eq!(retrieved.id, node.id);
    }

    #[tokio::test]
    async fn get_children() {
        let (store, _dir) = test_store().await;

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

        let children = store.get_children(&parent.id).await.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].id, child.id);

        // Root has one child (the parent node, whose parent_id is root)
        let root_children = store.get_children(&DagNodeId::root()).await.unwrap();
        assert_eq!(root_children.len(), 1);
        assert_eq!(root_children[0].id, parent.id);
    }

    #[tokio::test]
    async fn different_sessions_are_isolated() {
        let (store, _dir) = test_store().await;

        let sess1 =
            SessionId::try_from("d4761d76-dee4-4ebf-9df4-43b52efa4f78".to_string()).unwrap();
        let sess2 =
            SessionId::try_from("e2660cff-33d6-4428-acca-2d297dcc1cad".to_string()).unwrap();

        // Create second session + branch for FK constraints
        let ext_id2 = vlinder_core::domain::ExternalSessionId::new("test-ext-id-2").unwrap();
        let session2 = vlinder_core::domain::session::Session {
            id: sess2.clone(),
            external_id: ext_id2,
            name: "test-session-2".to_string(),
            agent: "agent-b".to_string(),
            default_branch: BranchId::from(1),
            created_at: Utc::now(),
        };
        store.create_session(&session2).await.unwrap();
        store.create_branch("main", &sess2, None).await.unwrap();

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

        let s1_nodes = store.get_session_nodes(&sess1).await.unwrap();
        assert_eq!(s1_nodes.len(), 1);
        assert_eq!(*s1_nodes[0].session_id(), sess1);

        let s2_nodes = store.get_session_nodes(&sess2).await.unwrap();
        assert_eq!(s2_nodes.len(), 1);
        assert_eq!(*s2_nodes[0].session_id(), sess2);
    }

    // ========================================================================
    // Timeline tests (ADR 093)
    // ========================================================================

    #[tokio::test]
    async fn create_timeline_returns_auto_id() {
        let (store, _dir) = test_store().await;

        let session_id = sess();
        let fork = DagNodeId::from("abc123".to_string());
        let id = store
            .create_branch("repair-1", &session_id, Some(&fork))
            .await
            .unwrap();
        assert!(id.as_i64() >= 1);

        let tl = store.get_branch(id).await.unwrap().unwrap();
        assert_eq!(tl.name, "repair-1");
        assert_eq!(tl.session_id, session_id);
        assert_eq!(tl.fork_point, Some(DagNodeId::from("abc123".to_string())));
        assert!(tl.broken_at.is_none());
    }

    #[tokio::test]
    async fn create_timeline_with_parent() {
        let (store, _dir) = test_store().await;

        let session_id = sess();
        // "main" branch already created by test_store()
        let fork = DagNodeId::from("abc123".to_string());
        let fork_id = store
            .create_branch("repair-1", &session_id, Some(&fork))
            .await
            .unwrap();

        let tl = store.get_branch(fork_id).await.unwrap().unwrap();
        assert_eq!(tl.fork_point, Some(fork));
    }

    #[tokio::test]
    async fn get_timeline_by_branch() {
        let (store, _dir) = test_store().await;
        let session_id = sess();
        // "main" branch already created by test_store()

        let tl = store.get_branch_by_name("main").await.unwrap().unwrap();
        assert_eq!(tl.session_id, session_id);

        assert!(store
            .get_branch_by_name("nonexistent")
            .await
            .unwrap()
            .is_none());
    }

    // ========================================================================
    // latest_node_on_branch tests
    // ========================================================================

    #[tokio::test]
    async fn latest_node_on_branch_returns_none_for_empty() {
        let (store, _dir) = test_store().await;
        let result = store
            .latest_node_on_branch(BranchId::from(1), None)
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn latest_node_on_branch_returns_most_recent() {
        let (store, _dir) = test_store().await;

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
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.id, node2.id);
    }

    // ========================================================================
    // latest_nodes_on_branch tests
    // ========================================================================

    #[tokio::test]
    async fn latest_nodes_on_branch_returns_n_most_recent() {
        let (store, _dir) = test_store().await;
        let branch = BranchId::from(1);

        // Insert 5 nodes
        let mut parent = DagNodeId::root();
        for i in 0..5 {
            let id = hash_dag_node(
                format!("sql-test-{i}").as_bytes(),
                &parent,
                &MessageType::Fork,
                &[],
                &sess(),
            );
            let node = DagNode {
                id: id.clone(),
                parent_id: parent.clone(),
                created_at: Utc::now(),
                state: Snapshot::empty(),
                msg_type: MessageType::Fork,
                session: sess(),
                submission: sub(),
                branch,
                protocol_version: "v1".to_string(),
            };
            store.insert_node(&node).unwrap();
            parent = id;
            // Stagger timestamps so ordering is deterministic
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        // Fetch last 3 — oldest-first
        let result = store.latest_nodes_on_branch(branch, 3).await.unwrap();
        assert_eq!(result.len(), 3);
        // All belong to the right branch
        for node in &result {
            assert_eq!(*node.branch_id(), branch);
        }
        // Order is oldest-first
        assert!(result[0].created_at <= result[1].created_at);
        assert!(result[1].created_at <= result[2].created_at);
    }

    #[tokio::test]
    async fn latest_nodes_on_branch_n_larger_than_chain_returns_all() {
        let (store, _dir) = test_store().await;
        let branch = BranchId::from(1);

        let mut parent = DagNodeId::root();
        for i in 0..3 {
            let id = hash_dag_node(
                format!("sql-all-{i}").as_bytes(),
                &parent,
                &MessageType::Fork,
                &[],
                &sess(),
            );
            let node = DagNode {
                id: id.clone(),
                parent_id: parent.clone(),
                created_at: Utc::now(),
                state: Snapshot::empty(),
                msg_type: MessageType::Fork,
                session: sess(),
                submission: sub(),
                branch,
                protocol_version: "v1".to_string(),
            };
            store.insert_node(&node).unwrap();
            parent = id;
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let result = store.latest_nodes_on_branch(branch, 100).await.unwrap();
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn latest_nodes_on_branch_unknown_branch_returns_empty() {
        let (store, _dir) = test_store().await;
        let result = store
            .latest_nodes_on_branch(BranchId::from(999), 3)
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    // ========================================================================
    // Session CRUD tests
    // ========================================================================

    #[tokio::test]
    async fn create_and_get_session() {
        let (store, _dir) = test_store().await;
        let ext_id = vlinder_core::domain::ExternalSessionId::new("a1b2-ext").unwrap();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            ext_id.clone(),
            "pensieve",
            BranchId::from(1),
        );

        store.create_session(&session).await.unwrap();

        let sid = SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap();
        let retrieved = store.get_session(&sid).await.unwrap().unwrap();
        assert_eq!(
            retrieved.id.as_str(),
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
        assert_eq!(retrieved.agent, "pensieve");
        assert_eq!(retrieved.name, session.name);
        assert_eq!(retrieved.external_id, ext_id);
    }

    #[tokio::test]
    async fn get_session_by_name() {
        let (store, _dir) = test_store().await;
        let ext_id = vlinder_core::domain::ExternalSessionId::new("a1b2-name-ext").unwrap();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            ext_id.clone(),
            "pensieve",
            BranchId::from(1),
        );
        let name = session.name.clone();

        store.create_session(&session).await.unwrap();

        let retrieved = store.get_session_by_name(&name).await.unwrap().unwrap();
        assert_eq!(
            retrieved.id.as_str(),
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        );
        assert_eq!(retrieved.agent, "pensieve");
        assert_eq!(retrieved.external_id, ext_id);
    }

    #[tokio::test]
    async fn get_session_returns_none_for_unknown() {
        let (store, _dir) = test_store().await;
        let sid = SessionId::try_from("00000000-0000-0000-0000-000000000000".to_string()).unwrap();
        assert!(store.get_session(&sid).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn get_session_by_name_returns_none_for_unknown() {
        let (store, _dir) = test_store().await;
        assert!(store
            .get_session_by_name("nonexistent")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn create_session_is_idempotent() {
        let (store, _dir) = test_store().await;
        let ext_id = vlinder_core::domain::ExternalSessionId::new("idempotent-ext").unwrap();
        let session = Session::new(
            SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap(),
            ext_id.clone(),
            "pensieve",
            BranchId::from(1),
        );

        store.create_session(&session).await.unwrap();
        store.create_session(&session).await.unwrap(); // No error

        let sid = SessionId::try_from("a1b2c3d4-e5f6-7890-abcd-ef1234567890".to_string()).unwrap();
        let retrieved = store.get_session(&sid).await.unwrap().unwrap();
        assert_eq!(retrieved.agent, "pensieve");
        assert_eq!(retrieved.external_id, ext_id);
    }

    #[tokio::test]
    async fn dag_node_row_with_message_blob_ignores_it() {
        // The old message_blob column has been removed from dag_nodes.
        // Verify a raw insert with only the slimmed columns round-trips.
        use diesel::connection::SimpleConnection;

        let (store, _dir) = test_store().await;
        {
            let mut conn = store.conn.lock().unwrap();
            conn.batch_execute(
                "INSERT INTO dag_nodes (hash, parent_hash, message_type, session_id, submission_id, created_at, protocol_version, branch_id, snapshot)
                 VALUES ('h1', NULL, 'fork', 'd4761d76-dee4-4ebf-9df4-43b52efa4f78', 'sub-1', '2025-01-01T00:00:00Z', '', 1, '{}')",
            ).unwrap();
        }

        let result = store.get_node(&DagNodeId::from("h1".to_string())).await;
        assert!(result.is_ok());
        let node = result.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::Fork);
    }

    // ========================================================================
    // Infra plane insert tests
    // ========================================================================

    #[tokio::test]
    async fn insert_deploy_agent_node_round_trip() {
        let (store, _dir) = test_store().await;

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
                mcp: Vec::new(),
            },
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
            .await
            .unwrap();

        let node = store.get_node(&dag_id).await.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::DeployAgent);
        assert!(node.session.as_str().contains("00000000")); // nullable → default
    }

    #[tokio::test]
    async fn insert_delete_agent_node_round_trip() {
        let (store, _dir) = test_store().await;

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
            .await
            .unwrap();

        let node = store.get_node(&dag_id).await.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::DeleteAgent);
    }

    // ========================================================================
    // Idempotency guard (ADR 125)
    // ========================================================================

    #[tokio::test]
    async fn exists_in_submission_returns_false_when_empty() {
        let (store, _dir) = test_store().await;
        assert!(!store
            .exists_in_submission(&sub(), BranchId::from(1), MessageType::Complete)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_in_submission_returns_true_when_matching_node_exists() {
        let (store, _dir) = test_store().await;

        let mut node = test_node(b"payload", &DagNodeId::root());
        node.msg_type = MessageType::Complete;
        store.insert_node(&node).unwrap();

        assert!(store
            .exists_in_submission(&sub(), BranchId::from(1), MessageType::Complete)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_in_submission_returns_false_for_wrong_type() {
        let (store, _dir) = test_store().await;

        let mut node = test_node(b"payload", &DagNodeId::root());
        node.msg_type = MessageType::Invoke;
        store.insert_node(&node).unwrap();

        assert!(!store
            .exists_in_submission(&sub(), BranchId::from(1), MessageType::Complete)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_in_submission_returns_false_for_wrong_branch() {
        let (store, _dir) = test_store().await;

        let mut node = test_node(b"payload", &DagNodeId::root());
        node.msg_type = MessageType::Complete;
        store.insert_node(&node).unwrap();

        assert!(!store
            .exists_in_submission(&sub(), BranchId::from(2), MessageType::Complete)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn exists_in_submission_returns_false_for_wrong_submission() {
        let (store, _dir) = test_store().await;

        let mut node = test_node(b"payload", &DagNodeId::root());
        node.msg_type = MessageType::Complete;
        store.insert_node(&node).unwrap();

        let other_sub = SubmissionId::from("sub-other".to_string());
        assert!(!store
            .exists_in_submission(&other_sub, BranchId::from(1), MessageType::Complete)
            .await
            .unwrap());
    }

    // ========================================================================
    // V2 service nodes
    // ========================================================================

    #[tokio::test]
    async fn insert_svc_request_and_response_nodes_round_trip() {
        let (store, _dir) = test_store().await;

        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let service = ServiceBackendV2::Mcp("brave".to_string());
        let operation = ServiceOperation::new("echo");
        let sequence = vlinder_core::domain::Sequence::first();

        // Insert svc_request node
        let request_msg = vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
        };
        let request_dag_id = DagNodeId::from("svc-req-hash-1".to_string());

        store
            .insert_svc_request_node(
                &request_dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service.clone(),
                operation.clone(),
                sequence,
                &request_msg,
            )
            .await
            .unwrap();

        let node = store.get_node(&request_dag_id).await.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::SvcRequest);
        assert_eq!(node.protocol_version(), "v2");

        // Insert svc_response node
        let response_msg = vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
            payload: b"result".to_vec(),
        };
        let response_dag_id = DagNodeId::from("svc-res-hash-1".to_string());

        store
            .insert_svc_response_node(
                &response_dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence.next(),
                &response_msg,
            )
            .await
            .unwrap();

        let node = store.get_node(&response_dag_id).await.unwrap().unwrap();
        assert_eq!(node.message_type(), MessageType::SvcResponse);
        assert_eq!(node.protocol_version(), "v2");
    }

    // ------------------------------------------------------------------------
    // 4.1 — SqliteDagStore unit tests for 5 node getters
    // ------------------------------------------------------------------------
    // NOTE: V1 getters (Complete, Request, Response) use silent `unwrap_or()`
    // for all column-parsing fallbacks, so no loud-error test is possible
    // without first converting those to `ok_or_else` (out of scope). Only V2
    // getters (SvcRequest, SvcResponse) have loud-error paths after Commit 1.

    // Complete node getter

    #[tokio::test]
    async fn get_complete_node_happy_path() {
        let (store, _dir) = test_store().await;
        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let harness = vlinder_core::domain::HarnessType::Grpc;
        let dag_id = DagNodeId::from("complete-happy-1".to_string());

        let msg = vlinder_core::domain::CompleteMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some("done".to_string()),
            diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(100),
            content: Some("result".to_string()),
            tool_calls: None,
            payload: b"output".to_vec(),
        };

        store
            .insert_complete_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                harness,
                &msg,
            )
            .await
            .unwrap();

        let (key, out_msg) = store
            .get_complete_node(&dag_id)
            .await
            .expect("query must succeed")
            .expect("Some, not None");

        match key.kind {
            vlinder_core::domain::DataMessageKind::Complete {
                ref agent,
                ref harness,
            } => {
                assert_eq!(agent.as_str(), "test-agent");
                assert_eq!(harness.as_str(), "grpc");
            }
            other => panic!("expected Complete kind, got {other:?}"),
        }
        assert_eq!(key.session, session);
        assert_eq!(key.branch, branch);
        assert_eq!(out_msg.state.as_deref(), Some("done"));
    }

    #[tokio::test]
    async fn get_complete_node_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("does-not-exist".to_string());
        let out = store
            .get_complete_node(&id)
            .await
            .expect("query must succeed");
        assert!(out.is_none(), "expected None for missing row");
    }

    // Request node getter

    #[tokio::test]
    async fn get_request_node_happy_path() {
        let (store, _dir) = test_store().await;
        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let service = vlinder_core::domain::ServiceBackend::Infer(
            vlinder_core::domain::InferenceBackendType::OpenRouter,
        );
        let operation = vlinder_core::domain::Operation::Get;
        let sequence = vlinder_core::domain::Sequence::first();
        let dag_id = DagNodeId::from("request-happy-1".to_string());

        let msg = vlinder_core::domain::RequestMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: vlinder_core::domain::RequestDiagnostics {
                sequence: 1,
                endpoint: "infer".to_string(),
                request_bytes: 100,
                received_at_ms: 0,
            },
            payload: b"hello".to_vec(),
            checkpoint: None,
        };

        store
            .insert_request_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
            .unwrap();

        let (key, out_msg) = store
            .get_request_node(&dag_id)
            .await
            .expect("query must succeed")
            .expect("Some, not None");

        match key.kind {
            vlinder_core::domain::DataMessageKind::Request { ref agent, .. } => {
                assert_eq!(agent.as_str(), "test-agent");
            }
            other => panic!("expected Request kind, got {other:?}"),
        }
        assert_eq!(key.session, session);
        assert_eq!(key.branch, branch);
        assert_eq!(out_msg.id, msg.id);
    }

    #[tokio::test]
    async fn get_request_node_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("does-not-exist".to_string());
        let out = store
            .get_request_node(&id)
            .await
            .expect("query must succeed");
        assert!(out.is_none(), "expected None for missing row");
    }

    // Response node getter

    #[tokio::test]
    async fn get_response_node_happy_path() {
        let (store, _dir) = test_store().await;
        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let service = vlinder_core::domain::ServiceBackend::Infer(
            vlinder_core::domain::InferenceBackendType::OpenRouter,
        );
        let operation = vlinder_core::domain::Operation::Get;
        let sequence = vlinder_core::domain::Sequence::first();
        let dag_id = DagNodeId::from("response-happy-1".to_string());

        let msg = vlinder_core::domain::ResponseMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::new(),
            state: Some("done".to_string()),
            diagnostics: vlinder_core::domain::ServiceDiagnostics::storage(
                vlinder_core::domain::ServiceType::Kv,
                "test",
                vlinder_core::domain::Operation::Get,
                100,
                0,
            ),
            payload: b"result".to_vec(),
            status_code: 200,
            checkpoint: None,
        };

        store
            .insert_response_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service,
                operation,
                sequence,
                &msg,
            )
            .await
            .unwrap();

        let (key, out_msg) = store
            .get_response_node(&dag_id)
            .await
            .expect("query must succeed")
            .expect("Some, not None");

        match key.kind {
            vlinder_core::domain::DataMessageKind::Response { ref agent, .. } => {
                assert_eq!(agent.as_str(), "test-agent");
            }
            other => panic!("expected Response kind, got {other:?}"),
        }
        assert_eq!(key.session, session);
        assert_eq!(key.branch, branch);
        assert_eq!(out_msg.id, msg.id);
    }

    #[tokio::test]
    async fn get_response_node_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("does-not-exist".to_string());
        let out = store
            .get_response_node(&id)
            .await
            .expect("query must succeed");
        assert!(out.is_none(), "expected None for missing row");
    }

    // SvcRequest node getter

    #[tokio::test]
    async fn get_svc_request_node_happy_path() {
        let (store, _dir) = test_store().await;
        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let service = ServiceBackendV2::Mcp("brave".to_string());
        let operation = ServiceOperation::new("echo");
        let sequence = vlinder_core::domain::Sequence::first();
        let dag_id = DagNodeId::from("svc-req-happy-1".to_string());

        let msg = vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({ "key": "val" })).unwrap(),
        };

        store
            .insert_svc_request_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service.clone(),
                operation.clone(),
                sequence,
                &msg,
            )
            .await
            .unwrap();

        let (key, out_msg) = store
            .get_svc_request_node(&dag_id)
            .await
            .expect("query must succeed")
            .expect("Some, not None");

        match key.kind {
            vlinder_core::domain::SvcMessageKind::SvcRequest {
                ref agent,
                ref service,
                ..
            } => {
                assert_eq!(agent.as_str(), "test-agent");
                assert_eq!(service.backend_str(), "brave");
            }
            vlinder_core::domain::SvcMessageKind::SvcResponse { .. } => {
                panic!("expected SvcRequest kind")
            }
        }
        assert_eq!(key.session, session);
        assert_eq!(key.branch, branch);
        assert_eq!(out_msg.tool_call_id, msg.tool_call_id);
    }

    #[tokio::test]
    async fn get_svc_request_node_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("does-not-exist".to_string());
        let out = store
            .get_svc_request_node(&id)
            .await
            .expect("query must succeed");
        assert!(out.is_none(), "expected None for missing row");
    }

    #[tokio::test]
    async fn get_svc_request_node_returns_err_on_malformed_row() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("svc-req-bad-1".to_string());
        let branch_id = 1i64;

        // Insert a minimal dag_nodes row to satisfy FK
        {
            let mut conn = store.conn.lock().expect("db connection lock poisoned");
            diesel::insert_or_ignore_into(crate::schema::dag_nodes::table)
                .values(&crate::models::NewDagNode {
                    hash: dag_id.as_str(),
                    parent_hash: None,
                    message_type: "svc_request",
                    session_id: Some(sess().as_str()),
                    submission_id: Some(sub().as_str()),
                    branch_id: Some(branch_id),
                    created_at: &Utc::now().to_rfc3339(),
                    protocol_version: "v2",
                    snapshot: "{}",
                })
                .execute(&mut *conn)
                .unwrap();
        }

        // Insert a svc_request_nodes row with invalid service_backend
        {
            let mut conn = store.conn.lock().expect("db connection lock poisoned");
            diesel::insert_into(crate::schema::svc_request_nodes::table)
                .values(&crate::models::NewSvcRequestNode {
                    dag_hash: dag_id.as_str(),
                    agent: "test-agent",
                    service_type: "not-a-real-type",
                    service_backend: "brave",
                    operation: "echo",
                    sequence: 1,
                    message_id: "msg-1",
                    tool_call_id: "tc-1",
                    state: None,
                    diagnostics: Some("{}"),
                    payload: b"{}",
                })
                .execute(&mut *conn)
                .unwrap();
        }

        let err = store
            .get_svc_request_node(&dag_id)
            .await
            .expect_err("malformed row must surface as Err, not Ok(Some(_))");
        assert!(
            err.contains("get_svc_request_node"),
            "error must name the getter: {err}"
        );
        assert!(
            err.contains("not-a-real-type"),
            "error must contain the bad service_type: {err}"
        );
    }

    // SvcResponse node getter

    #[tokio::test]
    async fn get_svc_response_node_happy_path() {
        let (store, _dir) = test_store().await;
        let session = sess();
        let submission = sub();
        let branch = BranchId::from(1);
        let agent = vlinder_core::domain::AgentName::new("test-agent");
        let service = ServiceBackendV2::Mcp("brave".to_string());
        let operation = ServiceOperation::new("echo");
        let sequence = vlinder_core::domain::Sequence::first();
        let dag_id = DagNodeId::from("svc-res-happy-1".to_string());

        let msg = vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
            payload: b"result".to_vec(),
        };

        store
            .insert_svc_response_node(
                &dag_id,
                &DagNodeId::root(),
                Utc::now(),
                &Snapshot::empty(),
                &session,
                &submission,
                branch,
                &agent,
                service.clone(),
                operation.clone(),
                sequence,
                &msg,
            )
            .await
            .unwrap();

        let (key, out_msg) = store
            .get_svc_response_node(&dag_id)
            .await
            .expect("query must succeed")
            .expect("Some, not None");

        match key.kind {
            vlinder_core::domain::SvcMessageKind::SvcResponse {
                ref agent,
                ref service,
                ..
            } => {
                assert_eq!(agent.as_str(), "test-agent");
                assert_eq!(service.backend_str(), "brave");
            }
            vlinder_core::domain::SvcMessageKind::SvcRequest { .. } => {
                panic!("expected SvcResponse kind")
            }
        }
        assert_eq!(key.session, session);
        assert_eq!(key.branch, branch);
        assert_eq!(out_msg.correlation_id, msg.correlation_id);
    }

    #[tokio::test]
    async fn get_svc_response_node_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("does-not-exist".to_string());
        let out = store
            .get_svc_response_node(&id)
            .await
            .expect("query must succeed");
        assert!(out.is_none(), "expected None for missing row");
    }

    #[tokio::test]
    async fn get_svc_response_node_returns_err_on_malformed_row() {
        let (store, _dir) = test_store().await;
        let dag_id = "svc-res-bad-1".to_string();

        // Insert a minimal dag_nodes row to satisfy FK
        {
            let mut conn = store.conn.lock().expect("db connection lock poisoned");
            diesel::insert_or_ignore_into(crate::schema::dag_nodes::table)
                .values(&crate::models::NewDagNode {
                    hash: &dag_id,
                    parent_hash: None,
                    message_type: "svc_response",
                    session_id: Some(sess().as_str()),
                    submission_id: Some(sub().as_str()),
                    branch_id: Some(1i64),
                    created_at: &Utc::now().to_rfc3339(),
                    protocol_version: "v2",
                    snapshot: "{}",
                })
                .execute(&mut *conn)
                .unwrap();
        }

        // Insert a svc_response_nodes row with invalid service_backend
        {
            let mut conn = store.conn.lock().expect("db connection lock poisoned");
            diesel::insert_into(crate::schema::svc_response_nodes::table)
                .values(&crate::models::NewSvcResponseNode {
                    dag_hash: &dag_id,
                    agent: "test-agent",
                    service_type: "not-a-real-type",
                    service_backend: "brave",
                    operation: "echo",
                    sequence: 1,
                    message_id: "msg-1",
                    correlation_id: "corr-1",
                    state: None,
                    diagnostics: Some("{}"),
                    payload: b"{}",
                })
                .execute(&mut *conn)
                .unwrap();
        }

        let id = DagNodeId::from(dag_id);
        let err = store
            .get_svc_response_node(&id)
            .await
            .expect_err("malformed row must surface as Err, not Ok(Some(_))");
        assert!(
            err.contains("get_svc_response_node"),
            "error must name the getter: {err}"
        );
        assert!(
            err.contains("not-a-real-type"),
            "error must contain the bad service_type: {err}"
        );
    }

    // ========================================================================
    // get_invoke_message / get_complete_message / get_request_v2 / get_response_v2
    // ========================================================================

    #[tokio::test]
    async fn get_invoke_message_happy_path() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("invoke-msg-1".to_string());
        let msg = vlinder_core::domain::InvokeMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            state: Some("abc".to_string()),
            diagnostics: vlinder_core::domain::InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            current_input: vec![vlinder_core::domain::Message::User {
                content: "hello".to_string(),
            }],
        };

        store
            .insert_invoke_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &vlinder_core::domain::DataRoutingKey {
                    session: sess(),
                    branch: BranchId::from(1),
                    submission: sub(),
                    kind: vlinder_core::domain::DataMessageKind::Invoke {
                        harness: vlinder_core::domain::HarnessType::Grpc,
                        runtime: vlinder_core::domain::RuntimeType::Container,
                        agent: vlinder_core::domain::AgentName::new("test-agent"),
                    },
                },
                &msg,
            )
            .await
            .unwrap();

        let result = store
            .get_invoke_message(&dag_id)
            .await
            .expect("query must succeed");
        let out = result.expect("expected Some(InvokeMessage)");
        assert_eq!(out.current_input, msg.current_input);
        assert_eq!(out.state, msg.state);
        assert_eq!(out.diagnostics.harness_version, "0.1.0");
    }

    #[tokio::test]
    async fn get_invoke_message_returns_none_for_wrong_type() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("invoke-wrong-1".to_string());
        // Insert a complete node at this dag_id instead
        let complete_msg = vlinder_core::domain::CompleteMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: None,
            diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(0),
            content: None,
            tool_calls: None,
            payload: vec![],
        };
        store
            .insert_complete_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::HarnessType::Grpc,
                &complete_msg,
            )
            .await
            .unwrap();

        let result = store
            .get_invoke_message(&dag_id)
            .await
            .expect("query must succeed");
        assert!(result.is_none(), "expected None for wrong type");
    }

    #[tokio::test]
    async fn get_complete_message_happy_path() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("complete-msg-1".to_string());
        let msg = vlinder_core::domain::CompleteMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            state: Some("done".to_string()),
            diagnostics: vlinder_core::domain::RuntimeDiagnostics::placeholder(42),
            content: Some("result".to_string()),
            tool_calls: Some(vec![vlinder_core::domain::ToolCall {
                id: vlinder_core::domain::ToolCallId::new(),
                name: "test".to_string(),
                arguments: serde_json::json!({}),
            }]),
            payload: b"data".to_vec(),
        };

        store
            .insert_complete_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::HarnessType::Grpc,
                &msg,
            )
            .await
            .unwrap();

        let result = store
            .get_complete_message(&dag_id)
            .await
            .expect("query must succeed");
        let out = result.expect("expected Some(CompleteMessage)");
        assert_eq!(out.content, msg.content);
        assert_eq!(out.tool_calls, msg.tool_calls);
        assert_eq!(out.state, msg.state);
    }

    #[tokio::test]
    async fn get_complete_message_returns_none_for_wrong_type() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("complete-wrong-1".to_string());
        // Insert an invoke node at this dag_id
        let invoke_msg = vlinder_core::domain::InvokeMessage {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: vlinder_core::domain::InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            current_input: vec![],
        };
        store
            .insert_invoke_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &vlinder_core::domain::DataRoutingKey {
                    session: sess(),
                    branch: BranchId::from(1),
                    submission: sub(),
                    kind: vlinder_core::domain::DataMessageKind::Invoke {
                        harness: vlinder_core::domain::HarnessType::Grpc,
                        runtime: vlinder_core::domain::RuntimeType::Container,
                        agent: vlinder_core::domain::AgentName::new("test-agent"),
                    },
                },
                &invoke_msg,
            )
            .await
            .unwrap();

        let result = store
            .get_complete_message(&dag_id)
            .await
            .expect("query must succeed");
        assert!(result.is_none(), "expected None for wrong type");
    }

    #[tokio::test]
    async fn get_request_v2_happy_path() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("req-v2-1".to_string());
        let msg = vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::new(),
            state: Some("st".to_string()),
            diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
            payload: serde_json::to_vec(&serde_json::json!({"key": "val"})).unwrap(),
        };

        store
            .insert_svc_request_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::ServiceBackendV2::Mcp("brave".to_string()),
                vlinder_core::domain::ServiceOperation::new("echo"),
                vlinder_core::domain::Sequence::first(),
                &msg,
            )
            .await
            .unwrap();

        let result = store
            .get_request_v2(&dag_id)
            .await
            .expect("query must succeed");
        let out = result.expect("expected Some(RequestV2)");
        assert_eq!(out.tool_call_id, msg.tool_call_id);
        assert_eq!(out.payload, msg.payload);
        assert_eq!(out.state, msg.state);
    }

    #[tokio::test]
    async fn get_request_v2_returns_none_for_wrong_type() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("req-v2-wrong-1".to_string());
        // Insert a response node at this dag_id
        let svc_resp = vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
            payload: vec![],
        };
        store
            .insert_svc_response_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::ServiceBackendV2::Mcp("brave".to_string()),
                vlinder_core::domain::ServiceOperation::new("echo"),
                vlinder_core::domain::Sequence::first(),
                &svc_resp,
            )
            .await
            .unwrap();

        let result = store
            .get_request_v2(&dag_id)
            .await
            .expect("query must succeed");
        assert!(result.is_none(), "expected None for wrong type");
    }

    #[tokio::test]
    async fn get_response_v2_happy_path() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("resp-v2-1".to_string());
        let msg = vlinder_core::domain::ResponseV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            correlation_id: vlinder_core::domain::MessageId::new(),
            state: Some("final".to_string()),
            diagnostics: vlinder_core::domain::SvcResponseDiagnostics::default(),
            payload: b"result".to_vec(),
        };

        store
            .insert_svc_response_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::ServiceBackendV2::Mcp("brave".to_string()),
                vlinder_core::domain::ServiceOperation::new("echo"),
                vlinder_core::domain::Sequence::first(),
                &msg,
            )
            .await
            .unwrap();

        let result = store
            .get_response_v2(&dag_id)
            .await
            .expect("query must succeed");
        let out = result.expect("expected Some(ResponseV2)");
        assert_eq!(out.correlation_id, msg.correlation_id);
        assert_eq!(out.payload, msg.payload);
        assert_eq!(out.state, msg.state);
    }

    #[tokio::test]
    async fn get_response_v2_returns_none_for_wrong_type() {
        let (store, _dir) = test_store().await;
        let dag_id = DagNodeId::from("resp-v2-wrong-1".to_string());
        // Insert a svc_request node at this dag_id
        let svc_req = vlinder_core::domain::RequestV2 {
            id: vlinder_core::domain::MessageId::new(),
            dag_id: DagNodeId::root(),
            dag_parent: DagNodeId::root(),
            tool_call_id: vlinder_core::domain::ToolCallId::new(),
            state: None,
            diagnostics: vlinder_core::domain::SvcRequestDiagnostics::default(),
            payload: vec![],
        };
        store
            .insert_svc_request_node(
                &dag_id,
                &DagNodeId::root(),
                chrono::Utc::now(),
                &Snapshot::empty(),
                &sess(),
                &sub(),
                BranchId::from(1),
                &vlinder_core::domain::AgentName::new("test-agent"),
                vlinder_core::domain::ServiceBackendV2::Mcp("brave".to_string()),
                vlinder_core::domain::ServiceOperation::new("echo"),
                vlinder_core::domain::Sequence::first(),
                &svc_req,
            )
            .await
            .unwrap();

        let result = store
            .get_response_v2(&dag_id)
            .await
            .expect("query must succeed");
        assert!(result.is_none(), "expected None for wrong type");
    }

    #[tokio::test]
    async fn get_invoke_message_returns_none_when_missing() {
        let (store, _dir) = test_store().await;
        let id = DagNodeId::from("nonexistent-invoke".to_string());
        let result = store
            .get_invoke_message(&id)
            .await
            .expect("query must succeed");
        assert!(result.is_none(), "expected None for missing id");
    }
}
