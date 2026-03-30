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
        session_id -> Nullable<Text>,
        submission_id -> Nullable<Text>,
        branch_id -> Nullable<BigInt>,
        created_at -> Text,
        protocol_version -> Text,
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

diesel::table! {
    agents (name) {
        name -> Text,
        description -> Text,
        source -> Nullable<Text>,
        runtime -> Text,
        executable -> Text,
        image_digest -> Nullable<Text>,
        object_storage -> Nullable<Text>,
        vector_storage -> Nullable<Text>,
        requirements_json -> Text,
        prompts_json -> Nullable<Text>,
        public_key -> Nullable<Text>,
    }
}

diesel::table! {
    deploy_agent_nodes (dag_hash) {
        dag_hash -> Text,
        agent_name -> Text,
        manifest_json -> Text,
        message_id -> Text,
    }
}

diesel::table! {
    delete_agent_nodes (dag_hash) {
        dag_hash -> Text,
        agent_name -> Text,
        message_id -> Text,
    }
}

diesel::table! {
    agent_states (id) {
        id -> Integer,
        agent_name -> Text,
        state -> Text,
        updated_at -> Text,
        error -> Nullable<Text>,
    }
}

diesel::table! {
    models (name) {
        name -> Text,
        model_type -> Text,
        provider -> Text,
        model_path -> Text,
        digest -> Text,
    }
}

// Foreign key relationships — lets Diesel verify joins at compile time.
diesel::joinable!(invoke_nodes -> dag_nodes (dag_hash));
diesel::joinable!(complete_nodes -> dag_nodes (dag_hash));
diesel::joinable!(request_nodes -> dag_nodes (dag_hash));
diesel::joinable!(response_nodes -> dag_nodes (dag_hash));
diesel::joinable!(fork_nodes -> dag_nodes (dag_hash));
diesel::joinable!(promote_nodes -> dag_nodes (dag_hash));
diesel::joinable!(deploy_agent_nodes -> dag_nodes (dag_hash));
diesel::joinable!(delete_agent_nodes -> dag_nodes (dag_hash));
diesel::joinable!(agent_states -> agents (agent_name));

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
    agents,
    agent_states,
    deploy_agent_nodes,
    delete_agent_nodes,
    models,
);
