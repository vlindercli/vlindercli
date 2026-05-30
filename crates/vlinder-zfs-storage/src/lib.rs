//! HTTP-client `WorkspaceStore` impl that talks to `vlinder-zfs-server`
//! (which lives in the `vlinder-dev-machine` repo and runs as root inside
//! the bootc image).
//!
//! Per ADR 133, the trait is at vlinder's domain level; this crate is one of
//! its substrate impls. The zfs-server binary is the privileged component
//! that actually touches `/dev/zfs`; this client just forwards typed requests
//! over HTTP.

use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use vlinder_core::domain::{
    AgentName, BranchId, DagNodeId, SessionId, WorkspaceError, WorkspaceStore,
};

/// Configuration for the HTTP-client `ZfsWorkspaceStore`.
#[derive(Debug, Clone)]
pub struct ZfsConfig {
    /// URL of the zfs-server (e.g. `http://localhost:8783`).
    pub url: String,
    /// Per-request timeout. Defaults to 30s.
    pub timeout: Duration,
}

impl ZfsConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            timeout: Duration::from_secs(30),
        }
    }
}

/// `WorkspaceStore` impl backed by HTTP calls to `vlinder-zfs-server`.
pub struct ZfsWorkspaceStore {
    base_url: String,
    client: reqwest::Client,
}

impl ZfsWorkspaceStore {
    pub fn new(config: ZfsConfig) -> Result<Self, WorkspaceError> {
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|e| WorkspaceError::OperationFailed(format!("reqwest build: {e}")))?;
        Ok(Self {
            base_url: config.url.trim_end_matches('/').to_string(),
            client,
        })
    }

    async fn post_json<Req, Resp>(&self, path: &str, body: &Req) -> Result<Resp, WorkspaceError>
    where
        Req: Serialize,
        Resp: for<'de> Deserialize<'de>,
    {
        let url = format!("{}{}", self.base_url, path);
        let resp = self
            .client
            .post(&url)
            .json(body)
            .send()
            .await
            .map_err(|e| WorkspaceError::OperationFailed(format!("POST {url}: {e}")))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkspaceError::OperationFailed(format!(
                "POST {url} returned {status}: {body}"
            )));
        }

        resp.json::<Resp>()
            .await
            .map_err(|e| WorkspaceError::OperationFailed(format!("decoding {url} response: {e}")))
    }
}

// ---- Wire types (must match vlinder-zfs-server's src/types.rs) ----

#[derive(Debug, Serialize)]
struct EnsureWorkspaceRequest<'a> {
    agent: &'a str,
    session: String,
    branch: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_state: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct EnsureWorkspaceResponse {
    mount_path: String,
}

#[derive(Debug, Serialize)]
struct SnapshotRequest<'a> {
    agent: &'a str,
    session: String,
    branch: i64,
    dag_node_id: String,
}

#[derive(Debug, Deserialize)]
struct SnapshotResponse {
    state_pointer: String,
}

#[derive(Debug, Serialize)]
struct DestroyWorkspaceRequest<'a> {
    agent: &'a str,
    session: String,
    branch: i64,
}

#[derive(Debug, Deserialize)]
struct DestroyWorkspaceResponse {}

// ---- WorkspaceStore impl ----

#[async_trait]
impl WorkspaceStore for ZfsWorkspaceStore {
    async fn ensure_workspace(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
        parent_state: Option<&str>,
    ) -> Result<PathBuf, WorkspaceError> {
        let req = EnsureWorkspaceRequest {
            agent: agent.as_str(),
            session: session.to_string(),
            branch: branch.as_i64(),
            parent_state,
        };
        let resp: EnsureWorkspaceResponse = self.post_json("/v1/ensure_workspace", &req).await?;
        Ok(PathBuf::from(resp.mount_path))
    }

    async fn snapshot(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
        dag_node_id: &DagNodeId,
    ) -> Result<String, WorkspaceError> {
        let req = SnapshotRequest {
            agent: agent.as_str(),
            session: session.to_string(),
            branch: branch.as_i64(),
            dag_node_id: dag_node_id.to_string(),
        };
        let resp: SnapshotResponse = self.post_json("/v1/snapshot", &req).await?;
        Ok(resp.state_pointer)
    }

    async fn destroy_workspace(
        &self,
        agent: &AgentName,
        session: &SessionId,
        branch: &BranchId,
    ) -> Result<(), WorkspaceError> {
        let req = DestroyWorkspaceRequest {
            agent: agent.as_str(),
            session: session.to_string(),
            branch: branch.as_i64(),
        };
        let _: DestroyWorkspaceResponse = self.post_json("/v1/destroy_workspace", &req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vlinder_core::domain::{AgentName, BranchId, DagNodeId, SessionId};
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn test_ids() -> (AgentName, SessionId, BranchId, DagNodeId) {
        (
            AgentName::new("todoapp"),
            SessionId::new(),
            BranchId::from(1),
            DagNodeId::root(),
        )
    }

    async fn make_store(server: &MockServer) -> ZfsWorkspaceStore {
        ZfsWorkspaceStore::new(ZfsConfig::new(server.uri())).unwrap()
    }

    #[tokio::test]
    async fn ensure_workspace_posts_and_returns_mount_path() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/ensure_workspace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "mount_path": "/tank/vlinder/agents/todoapp/sessions/abc/1"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let store = make_store(&server).await;
        let (agent, session, branch, _) = test_ids();
        let path = store
            .ensure_workspace(&agent, &session, &branch, None)
            .await
            .unwrap();
        assert_eq!(
            path,
            PathBuf::from("/tank/vlinder/agents/todoapp/sessions/abc/1")
        );
    }

    #[tokio::test]
    async fn ensure_workspace_forwards_parent_state() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/ensure_workspace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "mount_path": "/x"
            })))
            .mount(&server)
            .await;

        let store = make_store(&server).await;
        let (agent, session, branch, _) = test_ids();
        let path = store
            .ensure_workspace(&agent, &session, &branch, Some("tank/foo@sha256-abc"))
            .await
            .unwrap();
        assert_eq!(path, PathBuf::from("/x"));
    }

    #[tokio::test]
    async fn snapshot_returns_state_pointer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/snapshot"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "state_pointer": "tank/vlinder/agents/todoapp/sessions/abc/1@sha256-deadbeef"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let store = make_store(&server).await;
        let (agent, session, branch, dag) = test_ids();
        let sp = store
            .snapshot(&agent, &session, &branch, &dag)
            .await
            .unwrap();
        assert_eq!(
            sp,
            "tank/vlinder/agents/todoapp/sessions/abc/1@sha256-deadbeef"
        );
    }

    #[tokio::test]
    async fn destroy_workspace_calls_endpoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/destroy_workspace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let store = make_store(&server).await;
        let (agent, session, branch, _) = test_ids();
        store
            .destroy_workspace(&agent, &session, &branch)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn server_error_propagates_as_operation_failed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/ensure_workspace"))
            .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
                "error": "pool not imported"
            })))
            .mount(&server)
            .await;

        let store = make_store(&server).await;
        let (agent, session, branch, _) = test_ids();
        let err = store
            .ensure_workspace(&agent, &session, &branch, None)
            .await
            .unwrap_err();
        match err {
            WorkspaceError::OperationFailed(msg) => {
                assert!(msg.contains("500"));
                assert!(msg.contains("pool not imported"));
            }
            other => panic!("expected OperationFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn trailing_slash_on_url_is_trimmed() {
        let server = MockServer::start().await;
        let url_with_slash = format!("{}/", server.uri());
        let store = ZfsWorkspaceStore::new(ZfsConfig::new(url_with_slash)).unwrap();

        Mock::given(method("POST"))
            .and(path("/v1/ensure_workspace"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "mount_path": "/x"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (agent, session, branch, _) = test_ids();
        store
            .ensure_workspace(&agent, &session, &branch, None)
            .await
            .unwrap();
    }
}
