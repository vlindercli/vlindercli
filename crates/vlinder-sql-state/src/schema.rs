//! Diesel table definitions — the single source of truth for the SQL schema.
//!
//! Each `table!` macro generates a module with typed column accessors.
//! Diesel uses these at compile time to verify queries are well-formed:
//! wrong column names, type mismatches, and missing fields are all caught
//! before the code runs.

diesel::table! {
    dag_nodes (hash) {
        hash -> Text,
        parent_hash -> Text,
        message_type -> Text,
        sender -> Text,
        receiver -> Text,
        session_id -> Text,
        submission_id -> Text,
        payload -> Binary,
        diagnostics -> Binary,
        stderr -> Binary,
        created_at -> Text,
        state -> Nullable<Text>,
        protocol_version -> Text,
        checkpoint -> Nullable<Text>,
        operation -> Nullable<Text>,
        message_blob -> Nullable<Text>,
        branch_id -> BigInt,
        snapshot -> Text,
    }
}

diesel::table! {
    invoke_nodes (dag_hash) {
        dag_hash -> Text,
        harness -> Text,
        runtime -> Text,
        agent -> Text,
        message_id -> Text,
        state -> Nullable<Text>,
        diagnostics -> Binary,
        payload -> Binary,
    }
}

diesel::table! {
    complete_nodes (dag_hash) {
        dag_hash -> Text,
        agent -> Text,
        harness -> Text,
        message_id -> Text,
        state -> Nullable<Text>,
        diagnostics -> Binary,
        payload -> Binary,
    }
}

diesel::table! {
    request_nodes (dag_hash) {
        dag_hash -> Text,
        agent -> Text,
        service -> Text,
        operation -> Text,
        sequence -> Integer,
        message_id -> Text,
        state -> Nullable<Text>,
        diagnostics -> Binary,
        payload -> Binary,
        checkpoint -> Nullable<Text>,
    }
}

diesel::table! {
    response_nodes (dag_hash) {
        dag_hash -> Text,
        agent -> Text,
        service -> Text,
        operation -> Text,
        sequence -> Integer,
        message_id -> Text,
        correlation_id -> Text,
        state -> Nullable<Text>,
        diagnostics -> Binary,
        payload -> Binary,
        status_code -> Integer,
        checkpoint -> Nullable<Text>,
    }
}

diesel::table! {
    fork_nodes (dag_hash) {
        dag_hash -> Text,
        agent -> Text,
        branch_name -> Text,
        fork_point -> Text,
        message_id -> Text,
    }
}

diesel::table! {
    promote_nodes (dag_hash) {
        dag_hash -> Text,
        agent -> Text,
        message_id -> Text,
        branch_id -> Nullable<BigInt>,
    }
}

diesel::table! {
    branches (id) {
        id -> BigInt,
        name -> Text,
        session_id -> Text,
        fork_point -> Nullable<Text>,
        head -> Nullable<Text>,
        created_at -> Text,
        broken_at -> Nullable<Text>,
    }
}

diesel::table! {
    sessions (id) {
        id -> Text,
        name -> Text,
        agent_name -> Text,
        default_branch -> BigInt,
        created_at -> Text,
    }
}

// Foreign key relationships — lets Diesel verify joins at compile time.
diesel::joinable!(invoke_nodes -> dag_nodes (dag_hash));
diesel::joinable!(complete_nodes -> dag_nodes (dag_hash));
diesel::joinable!(request_nodes -> dag_nodes (dag_hash));
diesel::joinable!(response_nodes -> dag_nodes (dag_hash));
diesel::joinable!(fork_nodes -> dag_nodes (dag_hash));
diesel::joinable!(promote_nodes -> dag_nodes (dag_hash));

diesel::allow_tables_to_appear_in_same_query!(
    dag_nodes,
    invoke_nodes,
    complete_nodes,
    request_nodes,
    response_nodes,
    fork_nodes,
    promote_nodes,
    branches,
    sessions,
);
