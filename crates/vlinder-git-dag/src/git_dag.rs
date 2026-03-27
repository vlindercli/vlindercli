//! `GitDagWorker` — writes typed messages as git commits (ADR 064, 069, 070, 078, 114).
//!
//! Each message becomes a commit on `main`. The commit tree accumulates under
//! `<agent>/<session>/` — each commit contains all previous message directories
//! plus the new one. Each session has a `timelines/` folder with index files
//! tracking active message paths (ADR 114).
//!
//! Each message directory stores one file per field (ADR 078). Scalar fields
//! are plain-text blobs (just the value — the filename is the key). Binary
//! fields (payload, stderr) are raw blobs. Diagnostics are TOML via serde.
//! Git's content-addressing deduplicates identical blobs across messages.
//!
//! ```text
//! tree
//! ├── support-agent/
//! │   └── ses-abc123/
//! │       ├── timelines/
//! │       │   ├── main            # "001-cli-invoke\n002-..."
//! │       │   └── ACTIVE          # "main"
//! │       ├── 001-cli-invoke/
//! │       │   ├── type            # "invoke"
//! │       │   ├── session_id      # "ses-abc123"
//! │       │   ├── payload         # raw bytes
//! │       │   ├── hash            # canonical domain hash
//! │       │   └── diagnostics.toml
//! │       └── 002-support-agent-request/
//! │           ├── type            # "request"
//! │           ├── service         # "infer"
//! │           └── ...
//! ├── agent.toml
//! ├── platform.toml
//! └── models/
//! ```
//!
//! Directory names are `{NNN}-{sender}-{type}`. The sequence number is
//! per-session, giving natural `ls` ordering.
//!
//! All commits go on `main`. No orphan chains, no `refs/sessions/` refs.
//! Standard git commands (log, checkout, diff) work as expected.
//!
//! Uses git2 (libgit2) for all git operations — no subprocess spawning, no
//! file lock contention between processes.

use std::path::Path;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use git2::{FileMode, Oid, Repository, RepositoryInitOptions, Signature, TreeBuilder};

use vlinder_core::domain::workers::dag::build_dag_node;
use vlinder_core::domain::{
    hash_dag_node, DagNodeId, DagWorker, DataMessageKind, DataRoutingKey, InvokeMessage,
    MessageType, ObservableMessage, Registry, Snapshot,
};

/// DAG worker that writes commits to a git repository.
///
/// All commits go on `main`. Session state (message count, canonical hash)
/// is read from the tree structure on each message. No in-memory maps
/// survive across messages or restarts.
pub struct GitDagWorker {
    repo: Repository,
    registry_host: String,
    /// Registry access for looking up agent/model state at commit time.
    registry: Option<Arc<dyn Registry>>,
}

impl GitDagWorker {
    /// Open (or create) a git repo for DAG commits.
    pub fn open(
        repo_path: &Path,
        registry_host: &str,
        registry: Option<Arc<dyn Registry>>,
    ) -> Result<Self, String> {
        let repo = if repo_path.join(".git").exists() {
            Repository::open(repo_path)
        } else {
            std::fs::create_dir_all(repo_path)
                .map_err(|e| format!("failed to create repo directory: {e}"))?;
            Repository::init_opts(repo_path, RepositoryInitOptions::new().initial_head("main"))
        }
        .map_err(|e| format!("git repo open/init failed: {e}"))?;

        // Create an empty initial commit on main if the repo is fresh.
        // This gives `git checkout main` a clean working tree to return to.
        if repo.head().is_err() {
            let empty_tree = repo
                .treebuilder(None)
                .and_then(|tb| tb.write())
                .and_then(|oid| repo.find_tree(oid))
                .map_err(|e| format!("empty tree failed: {e}"))?;
            let sig = Signature::now("vlinder", "vlinder@localhost")
                .map_err(|e| format!("signature failed: {e}"))?;
            repo.commit(
                Some("refs/heads/main"),
                &sig,
                &sig,
                "Initialize conversations repository",
                &empty_tree,
                &[],
            )
            .map_err(|e| format!("initial commit failed: {e}"))?;
        }

        Ok(Self {
            repo,
            registry_host: registry_host.to_string(),
            registry,
        })
    }

    /// Get the HEAD commit (whatever branch HEAD points to).
    fn head_commit(&self) -> Option<Oid> {
        self.repo
            .head()
            .ok()
            .and_then(|r| r.peel_to_commit().ok())
            .map(|c| c.id())
    }

    /// Navigate into a named subtree entry.
    fn get_subtree<'a>(&'a self, tree: &git2::Tree, name: &str) -> Option<git2::Tree<'a>> {
        let entry = tree.get_name(name)?;
        if entry.kind() != Some(git2::ObjectType::Tree) {
            return None;
        }
        self.repo.find_tree(entry.id()).ok()
    }

    /// Count message directories in a session subtree.
    /// Message dirs match the pattern `NNN-sender-type`.
    #[allow(clippy::unused_self)]
    fn session_message_count(&self, session_tree: &git2::Tree) -> usize {
        session_tree
            .iter()
            .filter(|entry| {
                entry.kind() == Some(git2::ObjectType::Tree)
                    && entry.name().is_some_and(|n| {
                        n.len() >= 4
                            && n.as_bytes()[..3].iter().all(u8::is_ascii_digit)
                            && n.as_bytes()[3] == b'-'
                    })
            })
            .count()
    }

    /// Read the canonical hash from the last message in a session subtree.
    fn session_canonical_hash_from_tree(
        &self,
        root_tree: &git2::Tree,
        agent_name: &str,
        session_id: &str,
    ) -> String {
        let Some(agent_tree) = self.get_subtree(root_tree, agent_name) else {
            return String::new();
        };
        let Some(session_tree) = self.get_subtree(&agent_tree, session_id) else {
            return String::new();
        };

        let mut msg_dirs: Vec<String> = session_tree
            .iter()
            .filter(|e| e.kind() == Some(git2::ObjectType::Tree))
            .filter_map(|e| e.name().map(std::string::ToString::to_string))
            .filter(|n| {
                n.len() >= 4
                    && n.as_bytes()[..3].iter().all(u8::is_ascii_digit)
                    && n.as_bytes()[3] == b'-'
            })
            .collect();
        msg_dirs.sort();

        if let Some(last_dir) = msg_dirs.last() {
            let path = format!("{agent_name}/{session_id}/{last_dir}/hash");
            if let Ok(entry) = root_tree.get_path(std::path::Path::new(&path)) {
                if let Ok(blob) = self.repo.find_blob(entry.id()) {
                    return String::from_utf8_lossy(blob.content()).to_string();
                }
            }
        }

        String::new()
    }

    /// Build a subtree for a single message — one file per field (ADR 078).
    /// Returns (tree OID, canonical hash) so the caller can update the chain.
    fn build_message_subtree(
        &self,
        msg: &ObservableMessage,
        created_at: DateTime<Utc>,
        canonical_parent: &DagNodeId,
    ) -> Result<(Oid, String), String> {
        let mut tb = self
            .repo
            .treebuilder(None)
            .map_err(|e| format!("treebuilder failed: {e}"))?;

        let created_at_str = created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

        // Common fields present on every message type
        self.insert_field(&mut tb, "session_id", msg.session().as_str())?;
        self.insert_field(&mut tb, "submission_id", msg.submission().as_str())?;
        self.insert_field(&mut tb, "protocol_version", msg.protocol_version())?;
        self.insert_field(&mut tb, "created_at", &created_at_str)?;

        // Payload — raw bytes, every message has one
        let payload_oid = self.write_blob(msg.payload())?;
        tb.insert("payload", payload_oid, FileMode::Blob.into())
            .map_err(|e| format!("insert payload failed: {e}"))?;

        // Type-specific fields + diagnostics
        match msg {
            ObservableMessage::Repair(m) => {
                self.insert_field(&mut tb, "type", "repair")?;
                self.insert_field(&mut tb, "agent_id", m.agent_name.as_str())?;
                self.insert_field(&mut tb, "harness", m.harness.as_str())?;
                self.insert_field(&mut tb, "service", m.service.service_type().as_str())?;
                self.insert_field(&mut tb, "backend", m.service.backend_str())?;
                self.insert_field(&mut tb, "operation", m.operation.as_str())?;
                self.insert_field(&mut tb, "sequence", &m.sequence.as_u32().to_string())?;
                self.insert_field(&mut tb, "checkpoint", &m.checkpoint)?;
                self.insert_field(&mut tb, "dag_parent", m.dag_parent.as_str())?;
                if let Some(ref state) = m.state {
                    self.insert_field(&mut tb, "state", state)?;
                }
            }
            ObservableMessage::Fork(m) => {
                self.insert_field(&mut tb, "type", "fork")?;
                self.insert_field(&mut tb, "branch_name", &m.branch_name)?;
                self.insert_field(&mut tb, "fork_point", m.fork_point.as_str())?;
            }
            ObservableMessage::Promote(_) => {
                self.insert_field(&mut tb, "type", "promote")?;
            }
        }

        // Compute canonical hash and store it in the subtree
        // TODO(ADR-116): look up parent snapshot from store once git-dag tracks state
        let dag_node = build_dag_node(msg, canonical_parent, &Snapshot::empty());
        self.insert_field(&mut tb, "hash", dag_node.id.as_str())?;

        let tree_oid = tb
            .write()
            .map_err(|e| format!("write message subtree failed: {e}"))?;
        Ok((tree_oid, dag_node.id.to_string()))
    }

    /// Build the accumulated tree: nested under `<agent>/<session>/` with timeline
    /// indexes (ADR 114). Returns (tree OID, canonical hash) for the new message.
    ///
    /// Retained for reference — v1 `on_observable_message` now calls
    /// `build_message_subtree` + `nest_and_commit` instead.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines, dead_code)]
    fn build_accumulated_tree(
        &self,
        msg: &ObservableMessage,
        created_at: DateTime<Utc>,
        from: &str,
        _to: &str,
        msg_type: &str,
        parent_commit: Option<Oid>,
        canonical_parent: &DagNodeId,
    ) -> Result<(Oid, String), String> {
        let agent_name = message_agent_name(msg);
        let session_id = msg.session().as_str().to_string();

        // Get parent tree from main HEAD
        let parent_tree = parent_commit
            .and_then(|oid| self.repo.find_commit(oid).ok())
            .and_then(|c| c.tree().ok());

        // Navigate existing subtrees
        let existing_agent_tree = parent_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, &agent_name));
        let existing_session_tree = existing_agent_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, &session_id));
        let existing_timelines_tree = existing_session_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, "timelines"));

        // Sequence number = count of existing message dirs + 1
        let seq = existing_session_tree
            .as_ref()
            .map_or(0, |t| self.session_message_count(t))
            + 1;

        // Build new message subtree
        let (msg_tree_oid, canonical_hash) =
            self.build_message_subtree(msg, created_at, canonical_parent)?;
        let msg_dir = format!("{seq:03}-{from}-{msg_type}");

        // Build session subtree: existing messages + new one + timelines
        let mut session_tb = self
            .repo
            .treebuilder(existing_session_tree.as_ref())
            .map_err(|e| format!("session treebuilder failed: {e}"))?;
        let _ = session_tb.remove("timelines");
        session_tb
            .insert(&msg_dir, msg_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert message dir failed: {e}"))?;

        // Build timelines subtree
        let mut timelines_tb = self
            .repo
            .treebuilder(existing_timelines_tree.as_ref())
            .map_err(|e| format!("timelines treebuilder failed: {e}"))?;

        // Determine which timeline to append to
        let active_timeline = match msg {
            ObservableMessage::Fork(m) => m.branch_name.as_str(),
            _ => "main",
        };

        // Append new message dir to the active timeline index file
        let timeline_content = if let Some(ref tl_tree) = existing_timelines_tree {
            if let Some(entry) = tl_tree.get_name(active_timeline) {
                if let Ok(blob) = self.repo.find_blob(entry.id()) {
                    let existing = String::from_utf8_lossy(blob.content()).to_string();
                    format!("{existing}\n{msg_dir}")
                } else {
                    msg_dir.clone()
                }
            } else {
                msg_dir.clone()
            }
        } else {
            msg_dir.clone()
        };

        let timeline_oid = self.write_blob(timeline_content.as_bytes())?;
        timelines_tb
            .insert(active_timeline, timeline_oid, FileMode::Blob.into())
            .map_err(|e| format!("insert timeline '{active_timeline}' failed: {e}"))?;

        // For fork: also ensure the main timeline index is preserved (it may not
        // have been touched) and set ACTIVE to the new branch
        let active_value = active_timeline;
        let active_oid = self.write_blob(active_value.as_bytes())?;
        timelines_tb
            .insert("ACTIVE", active_oid, FileMode::Blob.into())
            .map_err(|e| format!("insert ACTIVE failed: {e}"))?;

        let timelines_tree_oid = timelines_tb
            .write()
            .map_err(|e| format!("write timelines tree failed: {e}"))?;
        session_tb
            .insert("timelines", timelines_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert timelines dir failed: {e}"))?;

        let session_tree_oid = session_tb
            .write()
            .map_err(|e| format!("write session tree failed: {e}"))?;

        // Build agent subtree
        let mut agent_tb = self
            .repo
            .treebuilder(existing_agent_tree.as_ref())
            .map_err(|e| format!("agent treebuilder failed: {e}"))?;
        agent_tb
            .insert(&session_id, session_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert session dir failed: {e}"))?;
        let agent_tree_oid = agent_tb
            .write()
            .map_err(|e| format!("write agent tree failed: {e}"))?;

        // Build root tree
        let mut root_tb = self
            .repo
            .treebuilder(parent_tree.as_ref())
            .map_err(|e| format!("root treebuilder failed: {e}"))?;
        root_tb
            .insert(&agent_name, agent_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert agent dir failed: {e}"))?;

        // Add top-level metadata from registry
        let _ = root_tb.remove("agent.toml");
        let _ = root_tb.remove("platform.toml");
        let _ = root_tb.remove("models");

        if let Some(ref registry) = self.registry {
            if let Some(agent) = registry.get_agent_by_name(&agent_name) {
                if let Ok(agent_toml) = toml::to_string_pretty(&agent) {
                    if let Ok(oid) = self.write_blob(agent_toml.as_bytes()) {
                        let _ = root_tb.insert("agent.toml", oid, FileMode::Blob.into());
                    }
                }

                if !agent.requirements.models.is_empty() {
                    if let Ok(models_oid) =
                        self.build_models_subtree(registry, &agent.requirements.models)
                    {
                        let _ = root_tb.insert("models", models_oid, FileMode::Tree.into());
                    }
                }
            }

            let platform_toml = format!(
                "version = \"{}\"\ncommit = \"{}\"\nregistry_host = \"{}\"\n",
                env!("CARGO_PKG_VERSION"),
                env!("VLINDER_GIT_SHA"),
                self.registry_host,
            );
            if let Ok(oid) = self.write_blob(platform_toml.as_bytes()) {
                let _ = root_tb.insert("platform.toml", oid, FileMode::Blob.into());
            }
        }

        let tree_oid = root_tb
            .write()
            .map_err(|e| format!("write root tree failed: {e}"))?;
        Ok((tree_oid, canonical_hash))
    }

    /// Build a models/ subtree with one TOML file per model.
    fn build_models_subtree(
        &self,
        registry: &Arc<dyn Registry>,
        models: &std::collections::HashMap<String, String>,
    ) -> Result<Oid, String> {
        let mut tb = self
            .repo
            .treebuilder(None)
            .map_err(|e| format!("treebuilder failed: {e}"))?;

        let mut has_entries = false;
        for (alias, model_name) in models {
            if let Some(model) = registry.get_model(model_name) {
                if let Ok(model_toml) = toml::to_string_pretty(&model) {
                    let oid = self.write_blob(model_toml.as_bytes())?;
                    let filename = format!("{}.toml", alias.replace('/', "-"));
                    tb.insert(&filename, oid, FileMode::Blob.into())
                        .map_err(|e| format!("insert model failed: {e}"))?;
                    has_entries = true;
                }
            }
        }

        if !has_entries {
            return Err("no models to write".to_string());
        }

        tb.write()
            .map_err(|e| format!("write models subtree failed: {e}"))
    }

    /// Write a blob to the git object store.
    fn write_blob(&self, data: &[u8]) -> Result<Oid, String> {
        self.repo
            .blob(data)
            .map_err(|e| format!("blob write failed: {e}"))
    }

    /// Write a scalar string field as a blob and insert into a tree builder.
    fn insert_field(&self, tb: &mut TreeBuilder, name: &str, value: &str) -> Result<(), String> {
        let oid = self.write_blob(value.as_bytes())?;
        tb.insert(name, oid, FileMode::Blob.into())
            .map_err(|e| format!("insert field '{name}' failed: {e}"))?;
        Ok(())
    }

    /// Serialize diagnostics to TOML and insert as a blob entry.
    fn insert_diagnostics_toml<T: serde::Serialize>(
        &self,
        tb: &mut TreeBuilder,
        diagnostics: &T,
    ) -> Result<(), String> {
        let toml_str = toml::to_string_pretty(diagnostics)
            .map_err(|e| format!("diagnostics TOML serialize failed: {e}"))?;
        let oid = self.write_blob(toml_str.as_bytes())?;
        tb.insert("diagnostics.toml", oid, FileMode::Blob.into())
            .map_err(|e| format!("insert diagnostics.toml failed: {e}"))?;
        Ok(())
    }

    /// Shared logic: nest the per-message subtree under `agent/session/`, build
    /// timeline indexes, add registry metadata, create the commit, and handle
    /// fork/promote branch operations.
    ///
    /// Both `on_observable_message` (v1) and `on_observable_message_v2` call this
    /// after building their message-specific subtree.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn nest_and_commit(
        &self,
        msg_tree_oid: Oid,
        agent_name: &str,
        session_id: &str,
        from: &str,
        to: &str,
        msg_type: &str,
        active_timeline: &str,
        parent_commit_oid: Option<Oid>,
        created_at: DateTime<Utc>,
        submission: &str,
        state: Option<&str>,
        checkpoint: Option<&str>,
        protocol_version: &str,
        fork_branch: Option<&str>,
    ) -> Result<(), String> {
        // Get parent tree from HEAD
        let parent_tree = parent_commit_oid
            .and_then(|oid| self.repo.find_commit(oid).ok())
            .and_then(|c| c.tree().ok());

        // Navigate existing subtrees
        let existing_agent_tree = parent_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, agent_name));
        let existing_session_tree = existing_agent_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, session_id));
        let existing_timelines_tree = existing_session_tree
            .as_ref()
            .and_then(|t| self.get_subtree(t, "timelines"));

        // Sequence number = count of existing message dirs + 1
        let seq = existing_session_tree
            .as_ref()
            .map_or(0, |t| self.session_message_count(t))
            + 1;

        let msg_dir = format!("{seq:03}-{from}-{msg_type}");

        // Build session subtree: existing messages + new one + timelines
        let mut session_tb = self
            .repo
            .treebuilder(existing_session_tree.as_ref())
            .map_err(|e| format!("session treebuilder failed: {e}"))?;
        let _ = session_tb.remove("timelines");
        session_tb
            .insert(&msg_dir, msg_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert message dir failed: {e}"))?;

        // Build timelines subtree
        let mut timelines_tb = self
            .repo
            .treebuilder(existing_timelines_tree.as_ref())
            .map_err(|e| format!("timelines treebuilder failed: {e}"))?;

        // Append new message dir to the active timeline index file
        let timeline_content = if let Some(ref tl_tree) = existing_timelines_tree {
            if let Some(entry) = tl_tree.get_name(active_timeline) {
                if let Ok(blob) = self.repo.find_blob(entry.id()) {
                    let existing = String::from_utf8_lossy(blob.content()).to_string();
                    format!("{existing}\n{msg_dir}")
                } else {
                    msg_dir.clone()
                }
            } else {
                msg_dir.clone()
            }
        } else {
            msg_dir.clone()
        };

        let timeline_oid = self.write_blob(timeline_content.as_bytes())?;
        timelines_tb
            .insert(active_timeline, timeline_oid, FileMode::Blob.into())
            .map_err(|e| format!("insert timeline '{active_timeline}' failed: {e}"))?;

        let active_oid = self.write_blob(active_timeline.as_bytes())?;
        timelines_tb
            .insert("ACTIVE", active_oid, FileMode::Blob.into())
            .map_err(|e| format!("insert ACTIVE failed: {e}"))?;

        let timelines_tree_oid = timelines_tb
            .write()
            .map_err(|e| format!("write timelines tree failed: {e}"))?;
        session_tb
            .insert("timelines", timelines_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert timelines dir failed: {e}"))?;

        let session_tree_oid = session_tb
            .write()
            .map_err(|e| format!("write session tree failed: {e}"))?;

        // Build agent subtree
        let mut agent_tb = self
            .repo
            .treebuilder(existing_agent_tree.as_ref())
            .map_err(|e| format!("agent treebuilder failed: {e}"))?;
        agent_tb
            .insert(session_id, session_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert session dir failed: {e}"))?;
        let agent_tree_oid = agent_tb
            .write()
            .map_err(|e| format!("write agent tree failed: {e}"))?;

        // Build root tree
        let mut root_tb = self
            .repo
            .treebuilder(parent_tree.as_ref())
            .map_err(|e| format!("root treebuilder failed: {e}"))?;
        root_tb
            .insert(agent_name, agent_tree_oid, FileMode::Tree.into())
            .map_err(|e| format!("insert agent dir failed: {e}"))?;

        // Add top-level metadata from registry
        let _ = root_tb.remove("agent.toml");
        let _ = root_tb.remove("platform.toml");
        let _ = root_tb.remove("models");

        if let Some(ref registry) = self.registry {
            if let Some(agent) = registry.get_agent_by_name(agent_name) {
                if let Ok(agent_toml) = toml::to_string_pretty(&agent) {
                    if let Ok(oid) = self.write_blob(agent_toml.as_bytes()) {
                        let _ = root_tb.insert("agent.toml", oid, FileMode::Blob.into());
                    }
                }

                if !agent.requirements.models.is_empty() {
                    if let Ok(models_oid) =
                        self.build_models_subtree(registry, &agent.requirements.models)
                    {
                        let _ = root_tb.insert("models", models_oid, FileMode::Tree.into());
                    }
                }
            }

            let platform_toml = format!(
                "version = \"{}\"\ncommit = \"{}\"\nregistry_host = \"{}\"\n",
                env!("CARGO_PKG_VERSION"),
                env!("VLINDER_GIT_SHA"),
                self.registry_host,
            );
            if let Ok(oid) = self.write_blob(platform_toml.as_bytes()) {
                let _ = root_tb.insert("platform.toml", oid, FileMode::Blob.into());
            }
        }

        let tree_oid = root_tb
            .write()
            .map_err(|e| format!("write root tree failed: {e}"))?;
        let tree = self
            .repo
            .find_tree(tree_oid)
            .map_err(|e| format!("find tree failed: {e}"))?;

        // Build commit message with trailers
        let mut message = format!(
            "{msg_type}: {from} \u{2192} {to}\n\nSession: {session_id}\nSubmission: {submission}",
        );
        if let Some(st) = state {
            message.push_str("\nState: ");
            message.push_str(st);
        }
        if let Some(cp) = checkpoint {
            message.push_str("\nCheckpoint: ");
            message.push_str(cp);
        }
        if !protocol_version.is_empty() {
            message.push_str("\nProtocol-Version: ");
            message.push_str(protocol_version);
        }

        // Author = message sender, committer = platform
        let author_email = format!("{}@{}", from, self.registry_host);
        let timestamp = git2::Time::new(created_at.timestamp(), 0);
        let author = Signature::new(from, &author_email, &timestamp)
            .map_err(|e| format!("author signature failed: {e}"))?;
        let committer = Signature::new("vlinder", "vlinder@localhost", &timestamp)
            .map_err(|e| format!("committer signature failed: {e}"))?;

        // Parent: main HEAD (all commits on main)
        let parent_commit = parent_commit_oid.and_then(|oid| self.repo.find_commit(oid).ok());
        let parents: Vec<&git2::Commit> = parent_commit.iter().collect();

        tracing::debug!(
            msg_type,
            from,
            to,
            session = session_id,
            parent = ?parent_commit_oid,
            "Committing message",
        );

        // Commit to HEAD (advances whatever branch HEAD points to)
        let commit_oid = self
            .repo
            .commit(Some("HEAD"), &author, &committer, &message, &tree, &parents)
            .map_err(|e| format!("commit failed: {e}"))?;

        // Fork-specific: create a git branch at this commit
        if let Some(branch_name) = fork_branch {
            let commit = self
                .repo
                .find_commit(commit_oid)
                .map_err(|e| format!("find fork commit failed: {e}"))?;
            self.repo
                .branch(branch_name, &commit, false)
                .map_err(|e| format!("create branch '{branch_name}' failed: {e}"))?;
            tracing::info!(
                branch = %branch_name,
                commit = %commit_oid,
                "Created git branch for fork"
            );
        }

        // Promote-specific: rename git branches
        if msg_type == "promote" {
            let commit = self
                .repo
                .find_commit(commit_oid)
                .map_err(|e| format!("find promote commit failed: {e}"))?;

            let sealed_name = format!("broken-{}", chrono::Utc::now().format("%Y%m%d-%H%M%S"));
            if let Ok(mut old_main) = self.repo.find_branch("main", git2::BranchType::Local) {
                old_main
                    .rename(&sealed_name, false)
                    .map_err(|e| format!("rename main to '{sealed_name}' failed: {e}"))?;
            }

            self.repo
                .branch("main", &commit, true)
                .map_err(|e| format!("create promoted main branch failed: {e}"))?;

            tracing::info!(
                commit = %commit_oid,
                sealed_name = %sealed_name,
                "Promoted branch to main in git"
            );
        }

        // Sync working tree so `ls` shows the folder structure
        self.repo
            .checkout_head(Some(git2::build::CheckoutBuilder::new().force()))
            .map_err(|e| format!("checkout HEAD failed: {e}"))?;

        tracing::debug!(commit = %commit_oid, session = session_id, "Commit succeeded");

        Ok(())
    }
}

impl DagWorker for GitDagWorker {
    fn on_observable_message(&mut self, msg: &ObservableMessage, created_at: DateTime<Utc>) {
        let result = (|| -> Result<(), String> {
            let session_id = msg.session().as_str().to_string();
            let agent_name = message_agent_name(msg);
            let (from, to, msg_type) = message_routing(msg);

            // 1. Resolve canonical parent from HEAD (stateless — no in-memory maps)
            let parent_commit_oid = self.head_commit();
            let canonical_parent = match parent_commit_oid {
                Some(oid) => {
                    let commit = self
                        .repo
                        .find_commit(oid)
                        .map_err(|e| format!("find commit failed: {e}"))?;
                    let tree = commit
                        .tree()
                        .map_err(|e| format!("tree lookup failed: {e}"))?;
                    DagNodeId::from(self.session_canonical_hash_from_tree(
                        &tree,
                        &agent_name,
                        &session_id,
                    ))
                }
                None => DagNodeId::root(),
            };

            // 2. Build per-message subtree
            let (msg_tree_oid, _canonical_hash) =
                self.build_message_subtree(msg, created_at, &canonical_parent)?;

            // 3. Determine fork/promote and timeline values
            let active_timeline = match msg {
                ObservableMessage::Fork(m) => m.branch_name.as_str(),
                _ => "main",
            };
            let fork_branch = match msg {
                ObservableMessage::Fork(m) => Some(m.branch_name.as_str()),
                _ => None,
            };

            // 4. Nest under agent/session, commit, handle fork/promote
            self.nest_and_commit(
                msg_tree_oid,
                &agent_name,
                &session_id,
                &from,
                &to,
                msg_type,
                active_timeline,
                parent_commit_oid,
                created_at,
                msg.submission().as_str(),
                message_state(msg),
                message_checkpoint(msg),
                msg.protocol_version(),
                fork_branch,
            )
        })();

        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to write git commit");
        }
    }

    #[allow(clippy::too_many_lines)]
    fn on_invoke(
        &mut self,
        key: &DataRoutingKey,
        invoke: &InvokeMessage,
        created_at: DateTime<Utc>,
    ) {
        let DataMessageKind::Invoke {
            harness,
            runtime,
            agent,
        } = &key.kind
        else {
            tracing::error!("on_invoke called with non-Invoke key");
            return;
        };

        let result = (|| -> Result<(), String> {
            let session_id = key.session.as_str();
            let agent_name = agent.as_str();
            let from = harness.as_str();
            let to = agent_name;
            let msg_type = "invoke";

            // Resolve canonical parent from HEAD
            let parent_commit_oid = self.head_commit();
            let canonical_parent = match parent_commit_oid {
                Some(oid) => {
                    let commit = self
                        .repo
                        .find_commit(oid)
                        .map_err(|e| format!("find commit failed: {e}"))?;
                    let tree = commit
                        .tree()
                        .map_err(|e| format!("tree lookup failed: {e}"))?;
                    DagNodeId::from(
                        self.session_canonical_hash_from_tree(&tree, agent_name, session_id),
                    )
                }
                None => DagNodeId::root(),
            };

            // Build message subtree inline
            let mut tb = self
                .repo
                .treebuilder(None)
                .map_err(|e| format!("treebuilder failed: {e}"))?;

            let created_at_str = created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            self.insert_field(&mut tb, "session_id", session_id)?;
            self.insert_field(&mut tb, "submission_id", key.submission.as_str())?;
            self.insert_field(&mut tb, "protocol_version", "v1")?;
            self.insert_field(&mut tb, "created_at", &created_at_str)?;

            let payload_oid = self.write_blob(&invoke.payload)?;
            tb.insert("payload", payload_oid, FileMode::Blob.into())
                .map_err(|e| format!("insert payload failed: {e}"))?;

            self.insert_field(&mut tb, "type", "invoke")?;
            self.insert_field(&mut tb, "harness", from)?;
            self.insert_field(&mut tb, "runtime", runtime.as_str())?;
            self.insert_field(&mut tb, "agent_id", agent_name)?;
            if let Some(ref state) = invoke.state {
                self.insert_field(&mut tb, "state", state)?;
            }
            self.insert_diagnostics_toml(&mut tb, &invoke.diagnostics)?;

            // Compute canonical hash
            let diagnostics_json = serde_json::to_vec(&invoke.diagnostics).unwrap_or_default();
            let canonical_hash = hash_dag_node(
                &invoke.payload,
                &canonical_parent,
                &MessageType::Invoke,
                &diagnostics_json,
                &key.session,
            );
            self.insert_field(&mut tb, "hash", canonical_hash.as_str())?;

            let msg_tree_oid = tb
                .write()
                .map_err(|e| format!("write message subtree failed: {e}"))?;

            // Nest under agent/session, commit
            self.nest_and_commit(
                msg_tree_oid,
                agent_name,
                session_id,
                from,
                to,
                msg_type,
                "main",
                parent_commit_oid,
                created_at,
                key.submission.as_str(),
                invoke.state.as_deref(),
                None, // invoke has no checkpoint
                "v1",
                None, // invoke is not a fork
            )
        })();

        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to write git commit for invoke");
        }
    }

    fn on_complete(
        &mut self,
        key: &DataRoutingKey,
        complete: &vlinder_core::domain::CompleteMessage,
        created_at: DateTime<Utc>,
    ) {
        let DataMessageKind::Complete { agent, harness } = &key.kind else {
            tracing::error!("on_complete called with non-Complete key");
            return;
        };

        let result = (|| -> Result<(), String> {
            let session_id = key.session.as_str();
            let agent_name = agent.as_str();
            let from = agent_name;
            let to = harness.as_str();
            let msg_type = "complete";

            let parent_commit_oid = self.head_commit();
            let canonical_parent = match parent_commit_oid {
                Some(oid) => {
                    let commit = self
                        .repo
                        .find_commit(oid)
                        .map_err(|e| format!("find commit failed: {e}"))?;
                    let tree = commit
                        .tree()
                        .map_err(|e| format!("tree lookup failed: {e}"))?;
                    DagNodeId::from(
                        self.session_canonical_hash_from_tree(&tree, agent_name, session_id),
                    )
                }
                None => DagNodeId::root(),
            };

            let mut tb = self
                .repo
                .treebuilder(None)
                .map_err(|e| format!("treebuilder failed: {e}"))?;

            let created_at_str = created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            self.insert_field(&mut tb, "session_id", session_id)?;
            self.insert_field(&mut tb, "submission_id", key.submission.as_str())?;
            self.insert_field(&mut tb, "protocol_version", "v1")?;
            self.insert_field(&mut tb, "created_at", &created_at_str)?;

            let payload_oid = self.write_blob(&complete.payload)?;
            tb.insert("payload", payload_oid, FileMode::Blob.into())
                .map_err(|e| format!("insert payload failed: {e}"))?;

            self.insert_field(&mut tb, "type", "complete")?;
            self.insert_field(&mut tb, "harness", to)?;
            self.insert_field(&mut tb, "agent_id", agent_name)?;
            if let Some(ref state) = complete.state {
                self.insert_field(&mut tb, "state", state)?;
            }
            self.insert_diagnostics_toml(&mut tb, &complete.diagnostics)?;

            let diagnostics_json = serde_json::to_vec(&complete.diagnostics).unwrap_or_default();
            let canonical_hash = hash_dag_node(
                &complete.payload,
                &canonical_parent,
                &MessageType::Complete,
                &diagnostics_json,
                &key.session,
            );
            self.insert_field(&mut tb, "hash", canonical_hash.as_str())?;

            let msg_tree_oid = tb
                .write()
                .map_err(|e| format!("write message subtree failed: {e}"))?;

            self.nest_and_commit(
                msg_tree_oid,
                agent_name,
                session_id,
                from,
                to,
                msg_type,
                "main",
                parent_commit_oid,
                created_at,
                key.submission.as_str(),
                complete.state.as_deref(),
                None,
                "v1",
                None,
            )
        })();

        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to write git commit for complete");
        }
    }

    #[allow(clippy::too_many_lines)]
    fn on_request(
        &mut self,
        key: &DataRoutingKey,
        request: &vlinder_core::domain::RequestMessage,
        created_at: DateTime<Utc>,
    ) {
        let DataMessageKind::Request {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("on_request called with non-Request key");
            return;
        };

        let result = (|| -> Result<(), String> {
            let session_id = key.session.as_str();
            let agent_name = agent.as_str();
            let from = agent_name;
            let to = &format!("{}.{}", service.service_type(), service.backend_str());
            let msg_type = "request";

            let parent_commit_oid = self.head_commit();
            let canonical_parent = match parent_commit_oid {
                Some(oid) => {
                    let commit = self
                        .repo
                        .find_commit(oid)
                        .map_err(|e| format!("find commit failed: {e}"))?;
                    let tree = commit
                        .tree()
                        .map_err(|e| format!("tree lookup failed: {e}"))?;
                    DagNodeId::from(
                        self.session_canonical_hash_from_tree(&tree, agent_name, session_id),
                    )
                }
                None => DagNodeId::root(),
            };

            let mut tb = self
                .repo
                .treebuilder(None)
                .map_err(|e| format!("treebuilder failed: {e}"))?;

            let created_at_str = created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            self.insert_field(&mut tb, "session_id", session_id)?;
            self.insert_field(&mut tb, "submission_id", key.submission.as_str())?;
            self.insert_field(&mut tb, "protocol_version", "v1")?;
            self.insert_field(&mut tb, "created_at", &created_at_str)?;

            let payload_oid = self.write_blob(&request.payload)?;
            tb.insert("payload", payload_oid, FileMode::Blob.into())
                .map_err(|e| format!("insert payload failed: {e}"))?;

            self.insert_field(&mut tb, "type", "request")?;
            self.insert_field(&mut tb, "agent_id", agent_name)?;
            self.insert_field(&mut tb, "service", service.service_type().as_str())?;
            self.insert_field(&mut tb, "backend", service.backend_str())?;
            self.insert_field(&mut tb, "operation", operation.as_str())?;
            self.insert_field(&mut tb, "sequence", &sequence.as_u32().to_string())?;
            if let Some(ref state) = request.state {
                self.insert_field(&mut tb, "state", state)?;
            }
            if let Some(ref checkpoint) = request.checkpoint {
                self.insert_field(&mut tb, "checkpoint", checkpoint)?;
            }
            self.insert_diagnostics_toml(&mut tb, &request.diagnostics)?;

            let diagnostics_json = serde_json::to_vec(&request.diagnostics).unwrap_or_default();
            let canonical_hash = hash_dag_node(
                &request.payload,
                &canonical_parent,
                &MessageType::Request,
                &diagnostics_json,
                &key.session,
            );
            self.insert_field(&mut tb, "hash", canonical_hash.as_str())?;

            let msg_tree_oid = tb
                .write()
                .map_err(|e| format!("write message subtree failed: {e}"))?;

            self.nest_and_commit(
                msg_tree_oid,
                agent_name,
                session_id,
                from,
                to,
                msg_type,
                "main",
                parent_commit_oid,
                created_at,
                key.submission.as_str(),
                request.state.as_deref(),
                request.checkpoint.as_deref(),
                "v1",
                None,
            )
        })();

        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to write git commit for request");
        }
    }

    #[allow(clippy::too_many_lines)]
    fn on_response(
        &mut self,
        key: &DataRoutingKey,
        response: &vlinder_core::domain::ResponseMessage,
        created_at: DateTime<Utc>,
    ) {
        let DataMessageKind::Response {
            agent,
            service,
            operation,
            sequence,
        } = &key.kind
        else {
            tracing::error!("on_response called with non-Response key");
            return;
        };

        let result = (|| -> Result<(), String> {
            let session_id = key.session.as_str();
            let agent_name = agent.as_str();
            let from = &format!("{}.{}", service.service_type(), service.backend_str());
            let to = agent_name;
            let msg_type = "response";

            let parent_commit_oid = self.head_commit();
            let canonical_parent = match parent_commit_oid {
                Some(oid) => {
                    let commit = self
                        .repo
                        .find_commit(oid)
                        .map_err(|e| format!("find commit failed: {e}"))?;
                    let tree = commit
                        .tree()
                        .map_err(|e| format!("tree lookup failed: {e}"))?;
                    DagNodeId::from(
                        self.session_canonical_hash_from_tree(&tree, agent_name, session_id),
                    )
                }
                None => DagNodeId::root(),
            };

            let mut tb = self
                .repo
                .treebuilder(None)
                .map_err(|e| format!("treebuilder failed: {e}"))?;

            let created_at_str = created_at.to_rfc3339_opts(chrono::SecondsFormat::Millis, true);

            self.insert_field(&mut tb, "session_id", session_id)?;
            self.insert_field(&mut tb, "submission_id", key.submission.as_str())?;
            self.insert_field(&mut tb, "protocol_version", "v1")?;
            self.insert_field(&mut tb, "created_at", &created_at_str)?;

            let payload_oid = self.write_blob(&response.payload)?;
            tb.insert("payload", payload_oid, FileMode::Blob.into())
                .map_err(|e| format!("insert payload failed: {e}"))?;

            self.insert_field(&mut tb, "type", "response")?;
            self.insert_field(&mut tb, "agent_id", agent_name)?;
            self.insert_field(&mut tb, "service", service.service_type().as_str())?;
            self.insert_field(&mut tb, "backend", service.backend_str())?;
            self.insert_field(&mut tb, "operation", operation.as_str())?;
            self.insert_field(&mut tb, "sequence", &sequence.as_u32().to_string())?;
            self.insert_field(&mut tb, "correlation_id", response.correlation_id.as_str())?;
            if let Some(ref state) = response.state {
                self.insert_field(&mut tb, "state", state)?;
            }
            if let Some(ref checkpoint) = response.checkpoint {
                self.insert_field(&mut tb, "checkpoint", checkpoint)?;
            }
            self.insert_diagnostics_toml(&mut tb, &response.diagnostics)?;

            let diagnostics_json = serde_json::to_vec(&response.diagnostics).unwrap_or_default();
            let canonical_hash = hash_dag_node(
                &response.payload,
                &canonical_parent,
                &MessageType::Response,
                &diagnostics_json,
                &key.session,
            );
            self.insert_field(&mut tb, "hash", canonical_hash.as_str())?;

            let msg_tree_oid = tb
                .write()
                .map_err(|e| format!("write message subtree failed: {e}"))?;

            self.nest_and_commit(
                msg_tree_oid,
                agent_name,
                session_id,
                from,
                to,
                msg_type,
                "main",
                parent_commit_oid,
                created_at,
                key.submission.as_str(),
                response.state.as_deref(),
                response.checkpoint.as_deref(),
                "v1",
                None,
            )
        })();

        if let Err(e) = result {
            tracing::error!(error = %e, "Failed to write git commit for response");
        }
    }
}

/// Extract (from, to, `type_str`) from an `ObservableMessage` for commit metadata.
fn message_routing(msg: &ObservableMessage) -> (String, String, &'static str) {
    match msg {
        ObservableMessage::Repair(m) => (
            m.harness.as_str().to_string(),
            m.agent_name.to_string(),
            "repair",
        ),
        ObservableMessage::Fork(m) => ("platform".to_string(), m.branch_name.clone(), "fork"),
        ObservableMessage::Promote(m) => {
            ("platform".to_string(), m.agent_name.to_string(), "promote")
        }
    }
}

/// Extract the agent name for registry lookup.
fn message_agent_name(msg: &ObservableMessage) -> String {
    match msg {
        ObservableMessage::Repair(m) => m.agent_name.to_string(),
        ObservableMessage::Fork(m) => m.agent_name.to_string(),
        ObservableMessage::Promote(m) => m.agent_name.to_string(),
    }
}

/// Extract state from the message if present.
fn message_state(msg: &ObservableMessage) -> Option<&str> {
    match msg {
        ObservableMessage::Repair(m) => m.state.as_deref(),
        ObservableMessage::Fork(_) | ObservableMessage::Promote(_) => None,
    }
}

/// Extract checkpoint handler name from the message (ADR 111).
fn message_checkpoint(msg: &ObservableMessage) -> Option<&str> {
    match msg {
        ObservableMessage::Repair(m) => Some(m.checkpoint.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use vlinder_core::domain::{
        Agent, AgentName, BranchId, CompleteMessage, ContainerId, DagNodeId, DataMessageKind,
        DataRoutingKey, HarnessType, InMemoryRegistry, InMemorySecretStore, InvokeDiagnostics,
        InvokeMessage, MessageId, RuntimeDiagnostics, RuntimeInfo, RuntimeType, SecretStore,
        SessionId, SubmissionId,
    };

    fn test_agent_id() -> AgentName {
        AgentName::new("support-agent")
    }

    fn test_complete_key(session: &str) -> DataRoutingKey {
        DataRoutingKey {
            session: SessionId::try_from(session.to_string()).unwrap(),
            branch: BranchId::from(1),
            submission: SubmissionId::from("sub-1".to_string()),
            kind: DataMessageKind::Complete {
                agent: test_agent_id(),
                harness: HarnessType::Cli,
            },
        }
    }

    fn test_complete_msg(payload: &[u8]) -> CompleteMessage {
        CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(0),
            payload: payload.to_vec(),
        }
    }

    fn send_complete(worker: &mut GitDagWorker, payload: &[u8], epoch_secs: i64) {
        let key = test_complete_key(SESSION);
        let msg = test_complete_msg(payload);
        let ts = DateTime::from_timestamp(epoch_secs, 0).unwrap();
        worker.on_complete(&key, &msg, ts);
    }

    fn test_worker() -> (GitDagWorker, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let worker = GitDagWorker::open(tmp.path(), "registry.local:9000", None).unwrap();
        (worker, tmp)
    }

    fn test_secret_store() -> Arc<dyn SecretStore> {
        Arc::new(InMemorySecretStore::new())
    }

    fn test_worker_with_registry() -> (GitDagWorker, tempfile::TempDir, Arc<InMemoryRegistry>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let registry = Arc::new(InMemoryRegistry::new(test_secret_store()));
        registry.register_runtime(RuntimeType::Container);

        let agent = Agent::from_toml(
            r#"
            name = "support-agent"
            description = "Support"
            runtime = "container"
            executable = "localhost/support-agent:latest"
            [requirements]

        "#,
        )
        .unwrap();
        registry.register_agent(agent).unwrap();

        let worker = GitDagWorker::open(
            tmp.path(),
            "registry.local:9000",
            Some(Arc::clone(&registry) as Arc<dyn Registry>),
        )
        .unwrap();

        (worker, tmp, registry)
    }

    /// Agent/session path prefix for the default test session.
    const AGENT: &str = "support-agent";
    const SESSION: &str = "d4761d76-dee4-4ebf-9df4-43b52efa4f78";
    const SESSION2: &str = "e2660cff-33d6-4428-acca-2d297dcc1cad";

    /// Run a git command against the test repo.
    fn git(repo_path: &Path, args: &[&str]) -> Result<String, String> {
        let output = Command::new("git")
            .args(args)
            .current_dir(repo_path)
            .output()
            .map_err(|e| format!("git {} failed: {}", args.join(" "), e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("git {} failed: {}", args.join(" "), stderr));
        }

        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Show a file from the session subtree on main.
    fn show_session_file(repo_path: &Path, msg_dir: &str, field: &str) -> Result<String, String> {
        let path = format!("main:{AGENT}/{SESSION}/{msg_dir}/{field}");
        git(repo_path, &["show", &path])
    }

    // --- Basic commit tests ---

    #[test]
    fn open_creates_git_repo() {
        let (_worker, tmp) = test_worker();
        assert!(tmp.path().join(".git").exists());
    }

    #[test]
    fn open_is_idempotent() {
        let tmp = tempfile::TempDir::new().unwrap();
        GitDagWorker::open(tmp.path(), "host", None).unwrap();
        GitDagWorker::open(tmp.path(), "host", None).unwrap();
        assert!(tmp.path().join(".git").exists());
    }

    #[test]
    fn commit_advances_main() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"hello", 1000);

        // main should have 2 commits: initial + complete
        let count = git(tmp.path(), &["rev-list", "--count", "main"]).unwrap();
        assert_eq!(count, "2");
    }

    #[test]
    fn commit_message_first_line() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"payload", 1000);

        let subject = git(tmp.path(), &["log", "-1", "--format=%s", "main"]).unwrap();
        assert_eq!(subject, "complete: support-agent \u{2192} cli");
    }

    #[test]
    fn commit_message_trailers() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"payload", 1000);

        let body = git(tmp.path(), &["log", "-1", "--format=%b", "main"]).unwrap();
        assert!(
            body.contains(&format!("Session: {SESSION}")),
            "body: {body}"
        );
        assert!(body.contains("Submission: sub-1"), "body: {body}");
    }

    #[test]
    fn complete_trailers_readable_by_timeline() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"question", 1000);

        let key = test_complete_key(SESSION);
        let complete = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: Some("state-abc123".to_string()),
            diagnostics: RuntimeDiagnostics::placeholder(100),
            payload: b"answer".to_vec(),
        };
        let ts2 = DateTime::from_timestamp(1001, 0).unwrap();
        worker.on_complete(&key, &complete, ts2);

        let session = git(
            tmp.path(),
            &[
                "log",
                "-1",
                "--format=%(trailers:key=Session,valueonly)",
                "main",
            ],
        )
        .unwrap();
        let state = git(
            tmp.path(),
            &[
                "log",
                "-1",
                "--format=%(trailers:key=State,valueonly)",
                "main",
            ],
        )
        .unwrap();

        assert_eq!(session.trim(), SESSION);
        assert_eq!(state.trim(), "state-abc123");
    }

    #[test]
    fn author_is_message_sender() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"data", 1000);

        let author = git(tmp.path(), &["log", "-1", "--format=%an <%ae>", "main"]).unwrap();
        assert_eq!(author, "support-agent <support-agent@registry.local:9000>");
    }

    #[test]
    fn committer_is_platform() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"data", 1000);

        let committer = git(tmp.path(), &["log", "-1", "--format=%cn <%ce>", "main"]).unwrap();
        assert_eq!(committer, "vlinder <vlinder@localhost>");
    }

    #[test]
    fn author_date_matches_node() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"data", 1_700_000_000);

        let date = git(tmp.path(), &["log", "-1", "--format=%at", "main"]).unwrap();
        assert_eq!(date, "1700000000");
    }

    // --- Per-field storage tests (ADR 078) ---

    #[test]
    fn invoke_directory_has_per_field_files() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"my-payload", 1000);

        let dir = "001-support-agent-complete";
        let show = |field: &str| show_session_file(tmp.path(), dir, field);

        assert_eq!(show("type").unwrap(), "complete");
        assert_eq!(show("session_id").unwrap(), SESSION);
        assert_eq!(show("submission_id").unwrap(), "sub-1");
        assert_eq!(show("harness").unwrap(), "cli");
        assert!(show("agent_id").unwrap().contains("support-agent"));
        assert_eq!(show("payload").unwrap(), "my-payload");
        assert_eq!(show("created_at").unwrap(), "1970-01-01T00:16:40.000Z");
        assert!(!show("protocol_version").unwrap().is_empty());
        let diag = show("diagnostics.toml").unwrap();
        assert!(diag.contains("duration_ms"), "diag: {diag}");
    }

    #[test]
    fn complete_directory_has_harness_and_diagnostics() {
        let (mut worker, tmp) = test_worker();
        let key = test_complete_key(SESSION);
        let complete = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics {
                stderr: b"WARN: something".to_vec(),
                runtime: RuntimeInfo::Container {
                    engine_version: "5.3.1".to_string(),
                    image_ref: None,
                    image_digest: None,
                    container_id: ContainerId::new("abc123"),
                },
                duration_ms: 2300,
                health: None,
            },
            payload: b"done".to_vec(),
        };
        let ts = DateTime::from_timestamp(1003, 0).unwrap();
        worker.on_complete(&key, &complete, ts);

        let dir = "001-support-agent-complete";
        let show = |field: &str| show_session_file(tmp.path(), dir, field);

        assert_eq!(show("type").unwrap(), "complete");
        assert_eq!(show("harness").unwrap(), "cli");
        let diag = show("diagnostics.toml").unwrap();
        assert!(diag.contains("duration_ms"), "diag: {diag}");
        // stderr is #[serde(skip)] on RuntimeDiagnostics, so it never
        // appears in the TOML serialization.
        assert!(
            !diag.contains("stderr"),
            "stderr should not be in diagnostics TOML: {diag}"
        );
    }

    #[test]
    fn state_file_present_when_state_set() {
        let (mut worker, tmp) = test_worker();
        let key = test_complete_key(SESSION);
        let complete = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: Some("abc123state".to_string()),
            diagnostics: RuntimeDiagnostics::placeholder(0),
            payload: b"hello".to_vec(),
        };
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        worker.on_complete(&key, &complete, ts);

        let state = show_session_file(tmp.path(), "001-support-agent-complete", "state").unwrap();
        assert_eq!(state, "abc123state");
    }

    #[test]
    fn state_file_absent_when_no_state() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"hello", 1000);

        let result = show_session_file(tmp.path(), "001-support-agent-complete", "state");
        assert!(result.is_err(), "should not have state file when None");
    }

    #[test]
    fn stderr_file_absent_when_empty() {
        let (mut worker, tmp) = test_worker();
        let key = test_complete_key(SESSION);
        let msg = CompleteMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: RuntimeDiagnostics::placeholder(100),
            payload: b"done".to_vec(),
        };
        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        worker.on_complete(&key, &msg, ts);

        let result = show_session_file(tmp.path(), "001-support-agent-complete", "stderr");
        assert!(result.is_err(), "should not have stderr when empty");
    }

    // --- Accumulation and chaining tests ---

    #[test]
    fn first_message_parents_initial_commit() {
        let (mut worker, tmp) = test_worker();
        let initial = git(tmp.path(), &["rev-parse", "main"]).unwrap();

        send_complete(&mut worker, b"first", 1000);

        let parent = git(tmp.path(), &["log", "-1", "--format=%P", "main"]).unwrap();
        assert_eq!(
            parent, initial,
            "first message should parent initial commit"
        );
    }

    // --- Rich tree tests (ADR 070) ---

    #[test]
    fn commit_tree_contains_agent_toml_when_registry_available() {
        let (mut worker, tmp, _registry) = test_worker_with_registry();
        send_complete(&mut worker, b"hello", 1000);

        let content = git(tmp.path(), &["show", "main:agent.toml"]).unwrap();
        assert!(content.contains("support-agent"), "agent.toml: {content}");
    }

    #[test]
    fn commit_tree_contains_platform_toml() {
        let (mut worker, tmp, _registry) = test_worker_with_registry();
        send_complete(&mut worker, b"hello", 1000);

        let content = git(tmp.path(), &["show", "main:platform.toml"]).unwrap();
        assert!(content.contains("version"), "platform.toml: {content}");
        assert!(
            content.contains("registry_host"),
            "platform.toml: {content}"
        );
    }

    #[test]
    fn main_branch_has_empty_initial_commit() {
        let (_worker, tmp) = test_worker();

        let count = git(tmp.path(), &["rev-list", "--count", "main"]).unwrap();
        assert_eq!(count, "1");

        let ls = git(tmp.path(), &["ls-tree", "--name-only", "main"]).unwrap();
        assert_eq!(ls, "", "main should have an empty tree");
    }

    // --- Session folder isolation tests (ADR 114) ---

    fn send_complete_for_session(
        worker: &mut GitDagWorker,
        payload: &[u8],
        epoch_secs: i64,
        session: &str,
    ) {
        let key = test_complete_key(session);
        let msg = test_complete_msg(payload);
        let ts = DateTime::from_timestamp(epoch_secs, 0).unwrap();
        worker.on_complete(&key, &msg, ts);
    }

    #[test]
    fn sessions_share_main_branch() {
        let (mut worker, tmp) = test_worker();

        send_complete_for_session(&mut worker, b"sess1", 1000, SESSION);
        send_complete_for_session(&mut worker, b"sess2", 1001, SESSION2);

        // Both commits on main, 3 total (initial + 2 messages)
        let count = git(tmp.path(), &["rev-list", "--count", "main"]).unwrap();
        assert_eq!(count, "3");

        // No session refs exist
        let refs = git(tmp.path(), &["for-each-ref", "refs/sessions/"]);
        assert!(
            refs.is_err() || refs.unwrap().is_empty(),
            "should not have session refs"
        );
    }

    #[test]
    fn sessions_isolated_by_folder() {
        let (mut worker, tmp) = test_worker();

        send_complete_for_session(&mut worker, b"sess1-msg", 1000, SESSION);
        send_complete_for_session(&mut worker, b"sess2-msg", 1001, SESSION2);

        // Each session has its own folder under the agent
        let ls = git(
            tmp.path(),
            &["ls-tree", "--name-only", &format!("main:{AGENT}")],
        )
        .unwrap();
        assert!(ls.contains(SESSION), "ls: {ls}");
        assert!(ls.contains(SESSION2), "ls: {ls}");

        // Each session folder has its own message
        let ls1 = git(
            tmp.path(),
            &["ls-tree", "--name-only", &format!("main:{AGENT}/{SESSION}")],
        )
        .unwrap();
        assert!(ls1.contains("001-support-agent-complete"), "ls1: {ls1}");

        let ls2 = git(
            tmp.path(),
            &[
                "ls-tree",
                "--name-only",
                &format!("main:{AGENT}/{SESSION2}"),
            ],
        )
        .unwrap();
        assert!(ls2.contains("001-support-agent-complete"), "ls2: {ls2}");
    }

    // --- Timeline index tests (ADR 114) ---

    #[test]
    fn active_file_points_to_main() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"q", 1000);

        let active = git(
            tmp.path(),
            &["show", &format!("main:{AGENT}/{SESSION}/timelines/ACTIVE")],
        )
        .unwrap();
        assert_eq!(active, "main");
    }

    #[test]
    fn working_tree_has_folder_structure() {
        let (mut worker, tmp) = test_worker();
        send_complete(&mut worker, b"browsable", 1000);

        // Working tree should have the agent/session/message folder structure
        let msg_dir = tmp
            .path()
            .join(AGENT)
            .join(SESSION)
            .join("001-support-agent-complete");
        assert!(msg_dir.exists(), "message dir should exist in working tree");

        let payload = std::fs::read_to_string(msg_dir.join("payload")).unwrap();
        assert_eq!(payload, "browsable");

        let active = std::fs::read_to_string(
            tmp.path()
                .join(AGENT)
                .join(SESSION)
                .join("timelines")
                .join("ACTIVE"),
        )
        .unwrap();
        assert_eq!(active, "main");
    }

    // --- Canonical hash tests ---

    #[test]
    fn message_subtree_contains_canonical_hash() {
        let (mut worker, tmp) = test_worker();
        let key = test_complete_key(SESSION);
        let msg = test_complete_msg(b"my-payload");

        let expected_hash = vlinder_core::domain::hash_dag_node(
            &msg.payload,
            &DagNodeId::root(),
            &vlinder_core::domain::MessageType::Complete,
            &serde_json::to_vec(&msg.diagnostics).unwrap_or_default(),
            &key.session,
        );

        let ts = DateTime::from_timestamp(1000, 0).unwrap();
        worker.on_complete(&key, &msg, ts);

        let hash = show_session_file(tmp.path(), "001-support-agent-complete", "hash").unwrap();
        assert_eq!(
            hash,
            expected_hash.to_string(),
            "hash file should contain canonical hash"
        );
    }

    // --- Checkpoint tests (ADR 111) ---

    // ========================================================================
    // Fork message tests
    // ========================================================================

    fn test_fork(
        agent_name: &str,
        branch_name: &str,
        fork_point: &str,
        epoch_secs: i64,
    ) -> (ObservableMessage, DateTime<Utc>) {
        use vlinder_core::domain::ForkMessage;
        let msg = ForkMessage::new(
            BranchId::from(1),
            SubmissionId::from("sub-fork".to_string()),
            SessionId::try_from(SESSION.to_string()).unwrap(),
            AgentName::new(agent_name),
            branch_name.to_string(),
            DagNodeId::from(fork_point.to_string()),
        );
        let created_at = DateTime::from_timestamp(epoch_secs, 0).unwrap();
        (ObservableMessage::Fork(msg), created_at)
    }

    #[test]
    fn fork_creates_git_branch() {
        let (mut worker, tmp) = test_worker();

        // Send a complete so there's a commit on main
        send_complete(&mut worker, b"hello", 1000);

        // Send a fork message
        let (fork, ft) = test_fork("support-agent", "repair-branch", "fake-hash", 1001);
        worker.on_observable_message(&fork, ft);

        // Verify the branch exists
        let branches = git(tmp.path(), &["branch", "--list"]).unwrap();
        assert!(
            branches.contains("repair-branch"),
            "expected 'repair-branch' in branches, got: {branches}"
        );
    }

    #[test]
    fn fork_branch_points_to_fork_commit() {
        let (mut worker, tmp) = test_worker();

        send_complete(&mut worker, b"hello", 1000);

        let (fork, ft) = test_fork("support-agent", "my-fork", "fake-hash", 1001);
        worker.on_observable_message(&fork, ft);

        // The fork branch should point to the same commit as main HEAD
        // (the fork commit was the last commit on main)
        let main_head = git(tmp.path(), &["rev-parse", "main"]).unwrap();
        let fork_head = git(tmp.path(), &["rev-parse", "my-fork"]).unwrap();
        assert_eq!(main_head, fork_head);
    }

    #[test]
    fn fork_creates_timeline_index_file() {
        let (mut worker, tmp) = test_worker();

        send_complete(&mut worker, b"hello", 1000);

        let (fork, ft) = test_fork("support-agent", "repair-branch", "fake-hash", 1001);
        worker.on_observable_message(&fork, ft);

        // The timelines/ dir should have both 'main' and 'repair-branch' index files
        let session_path = tmp
            .path()
            .join(format!("support-agent/{SESSION}/timelines"));
        assert!(
            session_path.join("main").exists(),
            "main timeline index should exist"
        );
        assert!(
            session_path.join("repair-branch").exists(),
            "repair-branch timeline index should exist"
        );

        // ACTIVE should point to the fork branch
        let active = std::fs::read_to_string(session_path.join("ACTIVE")).unwrap();
        assert_eq!(active, "repair-branch");
    }

    // --- Invoke tests ---

    fn test_data_invoke(
        payload: &[u8],
        epoch_secs: i64,
    ) -> (DataRoutingKey, InvokeMessage, DateTime<Utc>) {
        let key = DataRoutingKey {
            session: SessionId::try_from(SESSION.to_string()).unwrap(),
            branch: BranchId::from(1),
            submission: SubmissionId::from("sub-1".to_string()),
            kind: DataMessageKind::Invoke {
                harness: HarnessType::Cli,
                runtime: RuntimeType::Container,
                agent: test_agent_id(),
            },
        };
        let msg = InvokeMessage {
            id: MessageId::new(),
            dag_id: DagNodeId::root(),
            state: None,
            diagnostics: InvokeDiagnostics {
                harness_version: "0.1.0".to_string(),
            },
            dag_parent: DagNodeId::root(),
            payload: payload.to_vec(),
        };
        let created_at = DateTime::from_timestamp(epoch_secs, 0).unwrap();
        (key, msg, created_at)
    }

    #[test]
    fn data_invoke_creates_commit() {
        let (mut worker, tmp) = test_worker();
        let (key, msg, ts) = test_data_invoke(b"hello", 1000);

        worker.on_invoke(&key, &msg, ts);

        let count = git(tmp.path(), &["rev-list", "--count", "main"]).unwrap();
        assert_eq!(count, "2"); // initial + invoke
    }

    #[test]
    fn data_invoke_commit_message_has_trailers() {
        let (mut worker, tmp) = test_worker();
        let (key, msg, ts) = test_data_invoke(b"question", 1000);

        worker.on_invoke(&key, &msg, ts);

        let log = git(tmp.path(), &["log", "-1", "--format=%B", "main"]).unwrap();
        assert!(log.contains("invoke: cli"), "should have invoke type line");
        assert!(log.contains("Session:"), "should have Session trailer");
        assert!(
            log.contains("Submission:"),
            "should have Submission trailer"
        );
        assert!(
            log.contains("Protocol-Version: v1"),
            "should have protocol version"
        );
    }

    #[test]
    fn data_invoke_directory_has_per_field_files() {
        let (mut worker, tmp) = test_worker();
        let (key, msg, ts) = test_data_invoke(b"payload data", 1000);

        worker.on_invoke(&key, &msg, ts);

        let show = |field: &str| -> String {
            let path = format!("main:{AGENT}/{SESSION}/001-cli-invoke/{field}");
            git(tmp.path(), &["show", &path]).unwrap()
        };

        assert_eq!(show("type"), "invoke");
        assert_eq!(show("harness"), "cli");
        assert_eq!(show("runtime"), "container");
        assert_eq!(show("agent_id"), "support-agent");
        assert_eq!(show("payload"), "payload data");
        assert_eq!(show("protocol_version"), "v1");
        assert!(!show("hash").is_empty());
    }

    #[test]
    fn data_invoke_chains_with_complete() {
        let (mut worker, tmp) = test_worker();

        // Invoke
        let (key, invoke, t1) = test_data_invoke(b"question", 1000);
        worker.on_invoke(&key, &invoke, t1);

        // Complete
        let complete_key = test_complete_key(SESSION);
        let complete_msg = test_complete_msg(b"answer");
        let t2 = DateTime::from_timestamp(1001, 0).unwrap();
        worker.on_complete(&complete_key, &complete_msg, t2);

        let count = git(tmp.path(), &["rev-list", "--count", "main"]).unwrap();
        assert_eq!(count, "3"); // initial + invoke + complete

        // Complete should parent on invoke
        let parents = git(tmp.path(), &["log", "--format=%P", "-1", "main"]).unwrap();
        assert!(!parents.is_empty(), "complete should have a parent");
    }
}
