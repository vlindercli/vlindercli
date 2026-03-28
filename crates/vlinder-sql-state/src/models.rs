//! Diesel models — Rust structs that map to database rows.
//!
//! `Queryable` structs are returned from SELECT queries.
//! `Insertable` structs are used for INSERT operations.
//!
//! Diesel validates at compile time that field types match the schema.

use diesel::prelude::*;

use crate::schema::{
    branches, complete_nodes, dag_nodes, fork_nodes, invoke_nodes, promote_nodes, request_nodes,
    response_nodes, sessions,
};

// ============================================================================
// dag_nodes — the Merkle chain index
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = dag_nodes)]
pub struct DagNodeRow {
    pub hash: String,
    pub parent_hash: String,
    pub message_type: String,
    pub sender: String,
    pub receiver: String,
    pub session_id: String,
    pub submission_id: String,
    pub payload: Vec<u8>,
    pub diagnostics: Vec<u8>,
    pub stderr: Vec<u8>,
    pub created_at: String,
    pub state: Option<String>,
    pub protocol_version: String,
    pub checkpoint: Option<String>,
    pub operation: Option<String>,
    pub message_blob: Option<String>,
    pub branch_id: i64,
    pub snapshot: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = dag_nodes)]
pub struct NewDagNode<'a> {
    pub hash: &'a str,
    pub parent_hash: &'a str,
    pub message_type: &'a str,
    pub sender: &'a str,
    pub receiver: &'a str,
    pub session_id: &'a str,
    pub submission_id: &'a str,
    pub payload: &'a [u8],
    pub diagnostics: &'a [u8],
    pub stderr: &'a [u8],
    pub created_at: &'a str,
    pub state: Option<&'a str>,
    pub protocol_version: &'a str,
    pub checkpoint: Option<&'a str>,
    pub operation: Option<&'a str>,
    pub message_blob: Option<&'a str>,
    pub branch_id: i64,
    pub snapshot: &'a str,
}

// ============================================================================
// invoke_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = invoke_nodes)]
pub struct InvokeNodeRow {
    pub dag_hash: String,
    pub harness: String,
    pub runtime: String,
    pub agent: String,
    pub message_id: String,
    pub state: Option<String>,
    pub diagnostics: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = invoke_nodes)]
pub struct NewInvokeNode<'a> {
    pub dag_hash: &'a str,
    pub harness: &'a str,
    pub runtime: &'a str,
    pub agent: &'a str,
    pub message_id: &'a str,
    pub state: Option<&'a str>,
    pub diagnostics: &'a [u8],
    pub payload: &'a [u8],
}

// ============================================================================
// complete_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = complete_nodes)]
pub struct CompleteNodeRow {
    pub dag_hash: String,
    pub agent: String,
    pub harness: String,
    pub message_id: String,
    pub state: Option<String>,
    pub diagnostics: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = complete_nodes)]
pub struct NewCompleteNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub harness: &'a str,
    pub message_id: &'a str,
    pub state: Option<&'a str>,
    pub diagnostics: &'a [u8],
    pub payload: &'a [u8],
}

// ============================================================================
// request_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = request_nodes)]
pub struct RequestNodeRow {
    pub dag_hash: String,
    pub agent: String,
    pub service: String,
    pub operation: String,
    pub sequence: i32,
    pub message_id: String,
    pub state: Option<String>,
    pub diagnostics: Vec<u8>,
    pub payload: Vec<u8>,
    pub checkpoint: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = request_nodes)]
pub struct NewRequestNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub service: &'a str,
    pub operation: &'a str,
    pub sequence: i32,
    pub message_id: &'a str,
    pub state: Option<&'a str>,
    pub diagnostics: &'a [u8],
    pub payload: &'a [u8],
    pub checkpoint: Option<&'a str>,
}

// ============================================================================
// response_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = response_nodes)]
pub struct ResponseNodeRow {
    pub dag_hash: String,
    pub agent: String,
    pub service: String,
    pub operation: String,
    pub sequence: i32,
    pub message_id: String,
    pub correlation_id: String,
    pub state: Option<String>,
    pub diagnostics: Vec<u8>,
    pub payload: Vec<u8>,
    pub status_code: i32,
    pub checkpoint: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = response_nodes)]
pub struct NewResponseNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub service: &'a str,
    pub operation: &'a str,
    pub sequence: i32,
    pub message_id: &'a str,
    pub correlation_id: &'a str,
    pub state: Option<&'a str>,
    pub diagnostics: &'a [u8],
    pub payload: &'a [u8],
    pub status_code: i32,
    pub checkpoint: Option<&'a str>,
}

// ============================================================================
// fork_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = fork_nodes)]
pub struct ForkNodeRow {
    pub dag_hash: String,
    pub agent: String,
    pub branch_name: String,
    pub fork_point: String,
    pub message_id: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = fork_nodes)]
pub struct NewForkNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub branch_name: &'a str,
    pub fork_point: &'a str,
    pub message_id: &'a str,
}

// ============================================================================
// promote_nodes
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = promote_nodes)]
pub struct PromoteNodeRow {
    pub dag_hash: String,
    pub agent: String,
    pub message_id: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = promote_nodes)]
pub struct NewPromoteNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub message_id: &'a str,
}

// ============================================================================
// branches
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = branches)]
pub struct BranchRow {
    pub id: i64,
    pub name: String,
    pub session_id: String,
    pub fork_point: Option<String>,
    pub head: Option<String>,
    pub created_at: String,
    pub broken_at: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = branches)]
pub struct NewBranch<'a> {
    pub name: &'a str,
    pub session_id: &'a str,
    pub fork_point: Option<&'a str>,
    pub created_at: &'a str,
}

// ============================================================================
// sessions
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = sessions)]
pub struct SessionRow {
    pub id: String,
    pub name: String,
    pub agent_name: String,
    pub default_branch: i64,
    pub created_at: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = sessions)]
pub struct NewSession<'a> {
    pub id: &'a str,
    pub name: &'a str,
    pub agent_name: &'a str,
    pub default_branch: i64,
    pub created_at: &'a str,
}
