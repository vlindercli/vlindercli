//! S3‑backed multifile storage for Lambda agents.

use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{operation::RequestId, Client};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::path::{Component, Path};
use walkdir::WalkDir;

/// Walk `std::error::Error::source()` and join every layer with `": "`.
///
/// AWS SDK errors (`SdkError::DispatchFailure`, etc.) have almost nothing in
/// their top-level `Display` — the interesting bits live in the source chain.
/// Use this whenever we want the *real* reason a call failed to surface in
/// logs and in the error message the adapter propagates back.
fn format_error_chain<E: StdError + ?Sized>(err: &E) -> String {
    let mut parts = vec![err.to_string()];
    let mut cur: Option<&dyn StdError> = err.source();
    while let Some(e) = cur {
        parts.push(e.to_string());
        cur = e.source();
    }
    parts.join(": ")
}

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
    tracing::info!(
        bucket = %bucket,
        key = %key,
        version_id = %version_id,
        local_path = %local_path.display(),
        "[s3.diag] get_object: start"
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
            let chain = format_error_chain(&e);
            tracing::error!(
                error_chain = %chain,
                error_debug = ?e,
                bucket = %bucket,
                key = %key,
                version_id = %version_id,
                request_id = e.request_id().unwrap_or(""),
                "[s3.diag] get_object FAILED — full error chain above"
            );
            anyhow::anyhow!("get_object failed (bucket={bucket}, key={key}): {chain}")
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
    let file_size = tokio::fs::metadata(local_path).await.map_or(0, |m| m.len());
    tracing::info!(
        bucket = %bucket,
        key = %key,
        local_path = %local_path.display(),
        file_size = file_size,
        "[s3.diag] put_object: start (read file into ByteStream)"
    );
    let body = aws_sdk_s3::primitives::ByteStream::from_path(local_path)
        .await
        .map_err(|e| {
            let chain = format_error_chain(&e);
            tracing::error!(
                error_chain = %chain,
                error_debug = ?e,
                bucket = %bucket,
                key = %key,
                local_path = %local_path.display(),
                "[s3.diag] ByteStream::from_path FAILED — local file problem before any network call"
            );
            anyhow::anyhow!("ByteStream::from_path failed ({}): {chain}", local_path.display())
        })?;
    tracing::info!(
        bucket = %bucket,
        key = %key,
        "[s3.diag] put_object: sending request"
    );
    let resp = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .map_err(|e| {
            let chain = format_error_chain(&e);
            // Classify the error. DispatchFailure = never left the client
            // (DNS/TCP/TLS/credential‑IO). Service = S3 returned an HTTP
            // error (AccessDenied, NoSuchBucket, PermanentRedirect).
            // Timeout = request sent but took too long.
            let kind = match &e {
                aws_sdk_s3::error::SdkError::DispatchFailure(_) => "DispatchFailure",
                aws_sdk_s3::error::SdkError::TimeoutError(_) => "TimeoutError",
                aws_sdk_s3::error::SdkError::ResponseError(_) => "ResponseError",
                aws_sdk_s3::error::SdkError::ServiceError(_) => "ServiceError",
                aws_sdk_s3::error::SdkError::ConstructionFailure(_) => "ConstructionFailure",
                _ => "Other",
            };
            tracing::error!(
                error_chain = %chain,
                error_debug = ?e,
                sdk_error_kind = kind,
                bucket = %bucket,
                key = %key,
                request_id = e.request_id().unwrap_or(""),
                "[s3.diag] put_object FAILED — see error_chain for underlying cause"
            );
            anyhow::anyhow!("put_object failed (bucket={bucket}, key={key}, kind={kind}): {chain}")
        })?;
    let version_id = resp
        .version_id()
        .context(format!(
            "put_object returned no VersionId — S3 versioning not enabled on bucket '{bucket}'"
        ))?
        .to_string();
    tracing::info!(
        bucket = %bucket,
        key = %key,
        version_id = %version_id,
        "[s3.diag] put_object: success"
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
    tracing::info!(
        manifest_key = %manifest_key,
        version_id = %version_id,
        "[s3.diag] manifest get_object: start"
    );
    let manifest_bytes = client
        .get_object()
        .bucket(&config.bucket)
        .key(&manifest_key)
        .version_id(version_id)
        .send()
        .await
        .map_err(|e| {
            let chain = format_error_chain(&e);
            let kind = match &e {
                aws_sdk_s3::error::SdkError::DispatchFailure(_) => "DispatchFailure",
                aws_sdk_s3::error::SdkError::TimeoutError(_) => "TimeoutError",
                aws_sdk_s3::error::SdkError::ResponseError(_) => "ResponseError",
                aws_sdk_s3::error::SdkError::ServiceError(_) => "ServiceError",
                aws_sdk_s3::error::SdkError::ConstructionFailure(_) => "ConstructionFailure",
                _ => "Other",
            };
            tracing::error!(
                error_chain = %chain,
                error_debug = ?e,
                sdk_error_kind = kind,
                bucket = %config.bucket,
                key = %manifest_key,
                version_id = %version_id,
                request_id = e.request_id().unwrap_or(""),
                "[s3.diag] manifest get_object FAILED"
            );
            anyhow::anyhow!(
                "manifest get_object failed (bucket={}, key={manifest_key}, kind={kind}): {chain}",
                config.bucket
            )
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
    tracing::info!(
        manifest_key = %manifest_key,
        manifest_size = manifest_bytes.len(),
        "[s3.diag] manifest put_object: sending"
    );
    let resp = client
        .put_object()
        .bucket(&config.bucket)
        .key(&manifest_key)
        .body(aws_sdk_s3::primitives::ByteStream::from(manifest_bytes))
        .send()
        .await
        .map_err(|e| {
            let chain = format_error_chain(&e);
            let kind = match &e {
                aws_sdk_s3::error::SdkError::DispatchFailure(_) => "DispatchFailure",
                aws_sdk_s3::error::SdkError::TimeoutError(_) => "TimeoutError",
                aws_sdk_s3::error::SdkError::ResponseError(_) => "ResponseError",
                aws_sdk_s3::error::SdkError::ServiceError(_) => "ServiceError",
                aws_sdk_s3::error::SdkError::ConstructionFailure(_) => "ConstructionFailure",
                _ => "Other",
            };
            tracing::error!(
                error_chain = %chain,
                error_debug = ?e,
                sdk_error_kind = kind,
                bucket = %config.bucket,
                key = %manifest_key,
                request_id = e.request_id().unwrap_or(""),
                "[s3.diag] manifest put_object FAILED — check VPC egress + S3 endpoint + bucket region"
            );
            anyhow::anyhow!(
                "manifest put_object failed (bucket={}, key={manifest_key}, kind={kind}): {chain}",
                config.bucket
            )
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
    let env_region = std::env::var("AWS_REGION").unwrap_or_else(|_| "<unset>".to_string());
    let env_default_region =
        std::env::var("AWS_DEFAULT_REGION").unwrap_or_else(|_| "<unset>".to_string());
    let has_access_key = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
    let has_session_token = std::env::var("AWS_SESSION_TOKEN").is_ok();
    tracing::info!(
        env_aws_region = %env_region,
        env_aws_default_region = %env_default_region,
        has_aws_access_key_id = has_access_key,
        has_aws_session_token = has_session_token,
        "[s3.diag] create_client: loading default AWS config"
    );
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let region = config
        .region()
        .map_or("<none>".to_string(), |r| r.as_ref().to_string());
    tracing::info!(
        resolved_region = %region,
        "[s3.diag] create_client: S3 client ready (resolved region)"
    );
    Ok(Client::new(&config))
}
