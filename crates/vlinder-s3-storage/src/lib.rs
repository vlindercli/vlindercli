//! S3‑backed multifile storage for Lambda agents.

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{operation::RequestId, Client};
use std::collections::HashMap;
use std::path::{Component, Path};
use walkdir::WalkDir;

/// S3 storage configuration derived from a `ResourceId`.
#[derive(Debug)]
pub struct S3Config {
    pub bucket: String,
    pub prefix: String,
}

impl S3Config {
    /// Parse an `<s3://bucket/prefix>` URI.
    pub fn from_resource_id(resource_id: &vlinder_core::domain::ResourceId) -> Result<Self> {
        let uri = resource_id.as_str();
        if !uri.starts_with("s3://") {
            anyhow::bail!("expected s3:// URI, got {uri}");
        }
        let without_scheme = &uri["s3://".len()..];
        let (bucket, prefix) = match without_scheme.split_once('/') {
            Some((b, p)) => (b.to_string(), p.to_string()),
            None => (without_scheme.to_string(), String::new()),
        };
        Ok(Self { bucket, prefix })
    }

    /// Build the S3 key for a manifest.
    fn manifest_key(&self, session_id: &str) -> String {
        if self.prefix.is_empty() {
            format!("agents/{session_id}/manifest.json")
        } else {
            format!("{}/agents/{session_id}/manifest.json", self.prefix)
        }
    }

    /// Build the S3 key for a file within a session.
    fn file_key(&self, session_id: &str, relative_path: &str) -> String {
        if self.prefix.is_empty() {
            format!("agents/{session_id}/files/{relative_path}")
        } else {
            format!("{}/agents/{session_id}/files/{relative_path}", self.prefix)
        }
    }
}

/// Manifest v1: maps relative file paths to S3 object version IDs.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Manifest {
    version: u8,
    /// base S3 prefix for files (for forward compatibility)
    storage_root: String,
    files: HashMap<String, FileEntry>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct FileEntry {
    /// S3 key relative to `storage_root` (initially same as map key).
    storage_key: String,
    version_id: String,
}

/// Download a specific version of an S3 object to a local path.
async fn download_version(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: &str,
    local_path: &Path,
) -> Result<()> {
    tracing::debug!(
        bucket = %bucket,
        key = %key,
        version_id = %version_id,
        local_path = %local_path.display(),
        "Downloading S3 object version"
    );
    let parent = local_path.parent().context("file has no parent")?;
    tokio::fs::create_dir_all(parent).await?;
    let mut file = tokio::fs::File::create(local_path).await?;
    let resp = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .version_id(version_id)
        .send()
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %bucket,
                key = %key,
                version_id = %version_id,
                "S3 get_object failed"
            );
            e
        })?;
    let mut stream = resp.body.into_async_read();
    tokio::io::copy(&mut stream, &mut file).await.map_err(|e| {
        tracing::error!(
            error = %e,
            bucket = %bucket,
            key = %key,
            "Failed to write file"
        );
        e
    })?;
    tracing::debug!("Successfully downloaded file");
    Ok(())
}

/// Upload a local file to S3, returning its version ID.
async fn upload_file(
    client: &Client,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> Result<String> {
    tracing::debug!(
        bucket = %bucket,
        key = %key,
        local_path = %local_path.display(),
        "Uploading file to S3"
    );
    let body = aws_sdk_s3::primitives::ByteStream::from_path(local_path)
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %bucket,
                key = %key,
                local_path = %local_path.display(),
                "Failed to read file for upload"
            );
            e
        })?;
    let resp = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %bucket,
                key = %key,
                request_id = e.request_id().unwrap_or(""),
                "S3 put_object failed"
            );
            e
        })?;
    let version_id = resp
        .version_id()
        .context("S3 versioning not enabled on bucket")
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %bucket,
                key = %key,
                "S3 versioning not enabled on bucket"
            );
            e
        })?
        .to_string();
    tracing::debug!(
        bucket = %bucket,
        key = %key,
        version_id = %version_id,
        "File uploaded successfully"
    );
    Ok(version_id)
}

/// Checkout: if `parent_state` is a manifest version ID, download manifest and files.
#[allow(clippy::too_many_lines)]
pub async fn checkout(
    client: &Client,
    config: &S3Config,
    session_id: &str,
    parent_state: Option<&str>,
    local_root: &Path,
) -> Result<()> {
    // Clear any existing files in the session directory (start clean).
    let _ = tokio::fs::remove_dir_all(local_root).await;
    tokio::fs::create_dir_all(local_root).await?;

    tracing::info!(
        bucket = %config.bucket,
        prefix = %config.prefix,
        session_id,
        parent_state = parent_state.unwrap_or(""),
        local_root = %local_root.display(),
        "Starting S3 checkout"
    );

    let Some(version_id) = parent_state else {
        // No parent state → empty workspace.
        tracing::debug!("No parent state, starting with empty workspace");
        return Ok(());
    };
    if version_id.is_empty() {
        tracing::debug!("Empty parent state, starting with empty workspace");
        return Ok(());
    }

    // Download manifest at the given version.
    let manifest_key = config.manifest_key(session_id);
    tracing::debug!(
        manifest_key = %manifest_key,
        version_id = %version_id,
        "Downloading manifest"
    );
    let manifest_bytes = client
        .get_object()
        .bucket(&config.bucket)
        .key(&manifest_key)
        .version_id(version_id)
        .send()
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %config.bucket,
                key = %manifest_key,
                version_id = %version_id,
                "Failed to download manifest"
            );
            e
        })?;
    let manifest_bytes = manifest_bytes
        .body
        .collect()
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                "Failed to collect manifest body"
            );
            e
        })?
        .to_vec();
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|e| {
        tracing::error!(
            error = %e,
            manifest_size = manifest_bytes.len(),
            "Failed to parse manifest JSON"
        );
        e
    })?;
    tracing::info!(
        manifest_version = manifest.version,
        file_count = manifest.files.len(),
        "Manifest downloaded successfully"
    );

    // Download each referenced file.
    for (rel_path, entry) in manifest.files {
        // Defensively reject paths that would escape the workspace (parent‑dir) or are absolute (root).
        if Path::new(&rel_path)
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::RootDir))
        {
            tracing::warn!(
                "skipping manifest entry with dangerous path component: {}",
                rel_path
            );
            continue;
        }
        let key = config.file_key(session_id, &entry.storage_key);
        let local_path = local_root.join(&rel_path);
        tracing::debug!(
            rel_path = %rel_path,
            key = %key,
            version_id = %entry.version_id,
            "Downloading file"
        );
        if let Err(e) =
            download_version(client, &config.bucket, &key, &entry.version_id, &local_path).await
        {
            tracing::error!(
                error = %e,
                bucket = %config.bucket,
                key = %key,
                version_id = %entry.version_id,
                "File download failed"
            );
            return Err(e);
        }
    }
    tracing::info!("Checkout completed successfully");
    Ok(())
}

/// Commit: upload all files under `local_root`, create a new manifest, return its version ID.
#[allow(clippy::too_many_lines)]
pub async fn commit(
    client: &Client,
    config: &S3Config,
    session_id: &str,
    local_root: &Path,
) -> Result<String> {
    tracing::info!(
        bucket = %config.bucket,
        prefix = %config.prefix,
        session_id,
        local_root = %local_root.display(),
        "Starting S3 commit"
    );

    let mut files = HashMap::new();
    let mut file_entries = Vec::new();

    for entry in WalkDir::new(local_root) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let rel_path = entry
            .path()
            .strip_prefix(local_root)
            .expect("walkdir should yield subpaths");
        let rel_path_str = rel_path.to_string_lossy().replace('\\', "/");
        file_entries.push((rel_path_str, entry.path().to_path_buf()));
    }

    tracing::info!(file_count = file_entries.len(), "Found files to upload");

    for (rel_path_str, local_path) in file_entries {
        let key = config.file_key(session_id, &rel_path_str);
        tracing::debug!(
            rel_path = %rel_path_str,
            key = %key,
            "Uploading file"
        );
        let version_id = upload_file(client, &config.bucket, &key, &local_path).await?;
        files.insert(
            rel_path_str.clone(),
            FileEntry {
                storage_key: rel_path_str.clone(),
                version_id,
            },
        );
    }

    let manifest = Manifest {
        version: 1,
        storage_root: format!("{}/agents/{}/files", config.prefix, session_id),
        files,
    };
    let manifest_key = config.manifest_key(session_id);
    tracing::debug!(
        manifest_key = %manifest_key,
        "Creating manifest"
    );
    let manifest_bytes = serde_json::to_vec(&manifest).map_err(|e| {
        tracing::error!(
            error = %e,
            "Failed to serialize manifest"
        );
        e
    })?;
    let resp = client
        .put_object()
        .bucket(&config.bucket)
        .key(&manifest_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(manifest_bytes))
        .send()
        .await
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %config.bucket,
                key = %manifest_key,
                request_id = e.request_id().unwrap_or(""),
                "Failed to upload manifest"
            );
            e
        })?;
    let version_id = resp
        .version_id()
        .context("S3 versioning not enabled on bucket")
        .map_err(|e| {
            tracing::error!(
                error = %e,
                bucket = %config.bucket,
                key = %manifest_key,
                "S3 versioning not enabled on bucket"
            );
            e
        })?
        .to_string();
    tracing::info!(
        bucket = %config.bucket,
        prefix = %config.prefix,
        session_id,
        version_id = %version_id,
        "S3 commit completed successfully"
    );
    Ok(version_id)
}

/// Build an S3 client using default AWS credentials (Lambda execution role).
pub async fn create_client() -> Result<Client> {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let region = config.region().map_or("unknown", |r| r.as_ref());
    tracing::debug!(
        region = %region,
        "Creating S3 client"
    );
    Ok(Client::new(&config))
}
