//! Diesel models — Rust structs that map to database rows.
//!
//! `Queryable` structs are returned from SELECT queries.
//! `Insertable` structs are used for INSERT operations.
//!
//! Diesel validates at compile time that field types match the schema.

use diesel::prelude::*;

use crate::schema::{
    agent_states, agents, branches, complete_nodes, dag_nodes, delete_agent_nodes,
    deploy_agent_nodes, fork_nodes, invoke_nodes, models, promote_nodes, request_nodes,
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
    pub session_id: Option<String>,
    pub submission_id: Option<String>,
    pub branch_id: Option<i64>,
    pub created_at: String,
    pub protocol_version: String,
    pub snapshot: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = dag_nodes)]
pub struct NewDagNode<'a> {
    pub hash: &'a str,
    pub parent_hash: &'a str,
    pub message_type: &'a str,
    pub session_id: Option<&'a str>,
    pub submission_id: Option<&'a str>,
    pub branch_id: Option<i64>,
    pub created_at: &'a str,
    pub protocol_version: &'a str,
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
    pub branch_id: Option<i64>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = promote_nodes)]
pub struct NewPromoteNode<'a> {
    pub dag_hash: &'a str,
    pub agent: &'a str,
    pub message_id: &'a str,
    pub branch_id: Option<i64>,
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

// ============================================================================
// agents (infra read model)
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = agents)]
pub struct AgentRow {
    pub name: String,
    pub description: String,
    pub source: Option<String>,
    pub runtime: String,
    pub executable: String,
    pub image_digest: Option<String>,
    pub object_storage: Option<String>,
    pub vector_storage: Option<String>,
    pub requirements_json: String,
    pub prompts_json: Option<String>,
    pub public_key: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = agents)]
pub struct NewAgent<'a> {
    pub name: &'a str,
    pub description: &'a str,
    pub source: Option<&'a str>,
    pub runtime: &'a str,
    pub executable: &'a str,
    pub image_digest: Option<&'a str>,
    pub object_storage: Option<&'a str>,
    pub vector_storage: Option<&'a str>,
    pub requirements_json: &'a str,
    pub prompts_json: Option<&'a str>,
    pub public_key: Option<&'a str>,
}

// ============================================================================
// models (infra read model)
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = models)]
pub struct ModelRow {
    pub name: String,
    pub model_type: String,
    pub provider: String,
    pub model_path: String,
    pub digest: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = models)]
pub struct NewModel<'a> {
    pub name: &'a str,
    pub model_type: &'a str,
    pub provider: &'a str,
    pub model_path: &'a str,
    pub digest: &'a str,
}

// ============================================================================
// agent_states (infra lifecycle)
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = agent_states)]
pub struct AgentStateRow {
    pub id: i32,
    pub agent_name: String,
    pub state: String,
    pub updated_at: String,
    pub error: Option<String>,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = agent_states)]
pub struct NewAgentState<'a> {
    pub agent_name: &'a str,
    pub state: &'a str,
    pub updated_at: &'a str,
    pub error: Option<&'a str>,
}

// ============================================================================
// deploy_agent_nodes (infra plane)
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = deploy_agent_nodes)]
pub struct DeployAgentNodeRow {
    pub dag_hash: String,
    pub agent_name: String,
    pub manifest_json: String,
    pub message_id: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = deploy_agent_nodes)]
pub struct NewDeployAgentNode<'a> {
    pub dag_hash: &'a str,
    pub agent_name: &'a str,
    pub manifest_json: &'a str,
    pub message_id: &'a str,
}

// ============================================================================
// delete_agent_nodes (infra plane)
// ============================================================================

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = delete_agent_nodes)]
pub struct DeleteAgentNodeRow {
    pub dag_hash: String,
    pub agent_name: String,
    pub message_id: String,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = delete_agent_nodes)]
pub struct NewDeleteAgentNode<'a> {
    pub dag_hash: &'a str,
    pub agent_name: &'a str,
    pub message_id: &'a str,
}
