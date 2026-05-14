//! Manifest lifecycle operations: glob, snapshot, diff, merge, write,
//! upload, and download.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::attachments::errors::JobAttachmentsError;
use serde::Serialize;

use crate::attachments::api::read_manifests;
use crate::attachments::asset_manifests::{AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion, decode_manifest, hash_data};
use crate::attachments::diff::{FileStatus, fast_diff, hash_diff};
use crate::attachments::download::{
    download_manifest_from_s3, get_output_manifests_by_asset_root, merge_asset_manifests,
};
use crate::attachments::models::JobAttachmentS3Settings;
use crate::attachments::upload::S3UploadContext;

// --- Types ---

/// Which manifest types to download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssetType {
    Input,
    Output,
    All,
}

/// Glob include/exclude configuration.
#[derive(Debug, Clone)]
pub struct GlobConfig {
    pub include: Vec<String>,
    pub exclude: Vec<String>,
}

impl Default for GlobConfig {
    fn default() -> Self {
        Self {
            include: vec!["**/*".into()],
            exclude: vec![],
        }
    }
}

/// Result of a manifest snapshot operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestSnapshot {
    pub root: PathBuf,
    pub manifest: PathBuf,
}

/// Result of a manifest diff operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestDiffResult {
    pub new: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

/// Result of a manifest merge operation.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestMergeResult {
    pub manifest_root: PathBuf,
    pub local_manifest_path: PathBuf,
}

/// One entry in a manifest download response.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestDownloadEntry {
    pub manifest_root: PathBuf,
    pub local_manifest_path: PathBuf,
}

/// Response from `manifest_download`.
#[derive(Debug, Clone, Serialize)]
pub struct ManifestDownloadResponse {
    pub downloaded: Vec<ManifestDownloadEntry>,
}

// --- Functions ---

/// Resolve glob configuration from CLI arguments.
///
/// If include/exclude are non-empty, they take precedence. Otherwise
/// parse `include_exclude_config` as a file path or JSON string. Falls
/// back to default (all files).
pub fn resolve_glob_config(
    include: &[String],
    exclude: &[String],
    include_exclude_config: Option<&str>,
) -> Result<GlobConfig, JobAttachmentsError> {
    if !include.is_empty() || !exclude.is_empty() {
        return Ok(GlobConfig {
            include: if include.is_empty() {
                vec!["**/*".into()]
            } else {
                include.to_vec()
            },
            exclude: exclude.to_vec(),
        });
    }

    if let Some(config_input) = include_exclude_config {
        let json_str = match std::fs::read_to_string(config_input) {
            Ok(contents) => contents,
            Err(_) => config_input.to_owned(),
        };

        let parsed: serde_json::Value = serde_json::from_str(&json_str).map_err(|_| {
            JobAttachmentsError::AssetSync(format!(
                "Glob input {config_input} cannot be deserialized as JSON"
            ))
        })?;

        let include = parsed
            .get("include")
            .and_then(|v| v.as_array())
            .map_or_else(
                || vec!["**/*".into()],
                |arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                },
            );

        let exclude = parsed
            .get("exclude")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        return Ok(GlobConfig { include, exclude });
    }

    Ok(GlobConfig::default())
}

/// Return absolute normalized paths of all files matching the glob config under root.
pub fn glob_files(root: &Path, config: &GlobConfig) -> Result<Vec<String>, JobAttachmentsError> {
    let base = std::path::absolute(root)
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Invalid root path: {e}")))?;

    let mut matched: std::collections::HashSet<String> = std::collections::HashSet::new();

    for pattern in &config.include {
        let full_pattern = base.join(pattern).to_string_lossy().into_owned();
        for entry in glob::glob(&full_pattern)
            .map_err(|e| JobAttachmentsError::AssetSync(format!("Invalid glob pattern: {e}")))?
        {
            if let Ok(path) = entry
                && path.is_file()
            {
                // Use absolute instead of canonicalize to avoid /private symlink resolution on macOS
                let normalized = std::path::absolute(&path)
                    .unwrap_or(path)
                    .to_string_lossy()
                    .into_owned();
                matched.insert(normalized);
            }
        }
    }

    for pattern in &config.exclude {
        let full_pattern = base.join(pattern).to_string_lossy().into_owned();
        if let Ok(entries) = glob::glob(&full_pattern) {
            for entry in entries.flatten() {
                let normalized = std::path::absolute(&entry)
                    .unwrap_or(entry)
                    .to_string_lossy()
                    .into_owned();
                matched.remove(&normalized);
            }
        }
    }

    Ok(matched.into_iter().collect())
}

/// Write a manifest to disk. Returns the written file path.
pub fn write_manifest(
    root: &Path,
    manifest: &AssetManifest,
    destination: &Path,
    name: Option<&str>,
) -> Result<PathBuf, JobAttachmentsError> {
    let root_hash = hash_data(root.to_string_lossy().as_bytes());
    let timestamp = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S").to_string();

    let manifest_name = if let Some(n) = name {
        n.to_owned()
    } else {
        let derived = root.to_string_lossy().replace(['/', '\\', ':'], "_");
        derived.strip_prefix('_').unwrap_or(&derived).to_owned()
    };

    let filename = format!("{manifest_name}-{root_hash}-{timestamp}.manifest");
    let dest_path = destination.join(&filename);

    if let Some(parent) = dest_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to create directory: {e}"))
        })?;
    }

    std::fs::write(&dest_path, manifest.encode())
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Failed to write manifest: {e}")))?;

    Ok(dest_path)
}

/// Create a manifest snapshot of files in a directory.
pub fn manifest_snapshot(
    root: &Path,
    destination: &Path,
    name: Option<&str>,
    config: &GlobConfig,
    diff: Option<&str>,
    force_rehash: bool,
) -> Result<Option<ManifestSnapshot>, JobAttachmentsError> {
    let root_str = root.to_string_lossy();
    let current_files = glob_files(root, config)?;
    if current_files.is_empty() && diff.is_none() {
        return Ok(None);
    }

    let output_manifest = if let Some(diff_path) = diff {
        let diff_contents = std::fs::read_to_string(diff_path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read diff manifest: {e}"))
        })?;
        let diff_manifest = decode_manifest(&diff_contents)?;

        let changed_paths: Vec<String> = if force_rehash {
            // Hash all files, compare manifests
            let current_manifest = hash_files_to_manifest(&root_str, &current_files)?;
            match current_manifest {
                None => return Ok(None),
                Some(ref cm) => {
                    let diffs = hash_diff(&diff_manifest, cm);
                    diffs
                        .into_iter()
                        .filter(|(s, _)| *s == FileStatus::New || *s == FileStatus::Modified)
                        .map(|(_, p)| root.join(&p.path).to_string_lossy().into_owned())
                        .collect()
                }
            }
        } else {
            let diffs = fast_diff(&root_str, &current_files, &diff_manifest);
            diffs
                .into_iter()
                .filter(|(_, s)| *s != FileStatus::Deleted)
                .map(|(p, _)| root.join(&p).to_string_lossy().into_owned())
                .collect()
        };

        if changed_paths.is_empty() {
            return Ok(None);
        }

        // Hash only the changed files
        hash_files_to_manifest(&root_str, &changed_paths)?
    } else {
        // Full snapshot
        hash_files_to_manifest(&root_str, &current_files)?
    };

    match output_manifest {
        None => Ok(None),
        Some(manifest) => {
            let path = write_manifest(root, &manifest, destination, name)?;
            Ok(Some(ManifestSnapshot {
                root: root.to_path_buf(),
                manifest: path,
            }))
        }
    }
}

/// Compute file differences between a manifest and a directory.
pub fn manifest_diff(
    manifest_path: &str,
    root: &Path,
    config: &GlobConfig,
    force_rehash: bool,
) -> Result<ManifestDiffResult, JobAttachmentsError> {
    let contents = std::fs::read_to_string(manifest_path)
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Failed to read manifest: {e}")))?;
    let reference = decode_manifest(&contents)?;
    let root_str = root.to_string_lossy();
    let current_files = glob_files(root, config)?;

    let mut result = ManifestDiffResult {
        new: Vec::new(),
        modified: Vec::new(),
        deleted: Vec::new(),
    };

    if force_rehash {
        if let Some(current) = hash_files_to_manifest(&root_str, &current_files)? {
            for (status, path) in hash_diff(&reference, &current) {
                match status {
                    FileStatus::New => result.new.push(path.path),
                    FileStatus::Modified => result.modified.push(path.path),
                    FileStatus::Deleted => result.deleted.push(path.path),
                    FileStatus::Unchanged => {}
                }
            }
        }
    } else {
        for (path, status) in fast_diff(&root_str, &current_files, &reference) {
            match status {
                FileStatus::New => result.new.push(path),
                FileStatus::Modified => result.modified.push(path),
                FileStatus::Deleted => result.deleted.push(path),
                FileStatus::Unchanged => {}
            }
        }
    }

    Ok(result)
}

/// Merge multiple manifest files into one.
pub fn manifest_merge(
    root: &Path,
    manifest_files: &[String],
    destination: &Path,
    name: Option<&str>,
) -> Result<Option<ManifestMergeResult>, JobAttachmentsError> {
    let manifest_map = read_manifests(manifest_files)?;
    let manifests: Vec<AssetManifest> = manifest_map.into_values().collect();

    let merged = merge_asset_manifests(&manifests)?;

    match merged {
        None => Ok(None),
        Some(manifest) => {
            let path = write_manifest(root, &manifest, destination, name)?;
            Ok(Some(ManifestMergeResult {
                manifest_root: root.to_path_buf(),
                local_manifest_path: path,
            }))
        }
    }
}

// --- Internal helpers ---

/// Hash files and return an `AssetManifest` using openjd's hash engine.
fn hash_files_to_manifest(
    root: &str,
    files: &[String],
) -> Result<Option<AssetManifest>, JobAttachmentsError> {
    use openjd_snapshots::{AbsManifest, CollectOptions, HashOptions, collect_abs_snapshot, hash_abs_manifest};
    use std::path::PathBuf;

    if files.is_empty() {
        return Ok(None);
    }

    let file_paths: Vec<PathBuf> = files.iter().map(PathBuf::from).collect();
    let abs_snapshot = collect_abs_snapshot(
        &[] as &[PathBuf],
        &file_paths,
        CollectOptions::default(),
    )
    .map_err(|e| JobAttachmentsError::AssetSync(format!("Failed to collect snapshot: {e}")))?;

    let hash_result = hash_abs_manifest(
        &AbsManifest::Snapshot(abs_snapshot),
        HashOptions::default(),
    )
    .map_err(|e| JobAttachmentsError::AssetSync(format!("Failed to hash files: {e}")))?;

    let AbsManifest::Snapshot(hashed) = &hash_result.manifest else { unreachable!() };

    let root_prefix = root.to_owned();
    let paths: Vec<ManifestPath> = hashed
        .files
        .iter()
        .filter(|f| !f.deleted && f.symlink_target.is_none())
        .map(|f| {
            let rel = f.path.strip_prefix(&root_prefix)
                .or_else(|| f.path.strip_prefix("/"))
                .unwrap_or(&f.path)
                .trim_start_matches('/');
            ManifestPath {
                path: rel.to_owned(),
                hash: f.hash.clone().unwrap_or_default(),
                size: f.size.unwrap_or(0),
                mtime: f.mtime.unwrap_or(0) as i64,
            }
        })
        .collect();

    let total_size: u64 = paths.iter().map(|p| p.size).sum();
    Ok(Some(AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )?))
}

/// Upload a manifest file to S3 CAS.
pub async fn manifest_upload(
    manifest_file: &str,
    s3_bucket_name: &str,
    s3_cas_prefix: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    s3_key_prefix: Option<&str>,
) -> Result<(), JobAttachmentsError> {
    let file_path = Path::new(manifest_file);
    let filename = file_path.file_name().unwrap_or_default().to_string_lossy();

    let manifest_s3_key = match s3_key_prefix {
        Some(prefix) => format!("{s3_cas_prefix}/Manifests/{prefix}/{filename}"),
        None => format!("{s3_cas_prefix}/Manifests/{filename}"),
    };

    let contents = std::fs::read(manifest_file).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to read manifest file: {e}"))
    })?;

    let mut metadata = HashMap::new();
    metadata.insert(
        "file-system-location-name".to_owned(),
        manifest_file.to_owned(),
    );

    let ctx = S3UploadContext::new(s3_client.clone(), account_id.to_owned())?;
    ctx.upload_bytes_to_s3(&contents, s3_bucket_name, &manifest_s3_key, Some(metadata))
        .await
}

/// Download and merge manifests for a job from S3, write to disk.
///
/// `job_attachments` is the `attachments` field from the `GetJob` API
/// response (or empty map if the job has no attachments). The CLI layer
/// is responsible for calling `GetJob` and passing this in.
#[allow(clippy::implicit_hasher, reason = "only used with default HashMap")]
#[allow(
    clippy::too_many_arguments,
    reason = "S3 + Deadline context params needed for manifest resolution"
)]
pub async fn manifest_download(
    download_dir: &Path,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    s3_settings: &JobAttachmentS3Settings,
    job_attachments: &HashMap<String, serde_json::Value>,
    step_id: Option<&str>,
    asset_type: AssetType,
) -> Result<ManifestDownloadResponse, JobAttachmentsError> {
    let download_input = matches!(asset_type, AssetType::Input | AssetType::All);
    let download_output = matches!(asset_type, AssetType::Output | AssetType::All);

    let s3_prefix = format!("{}/Manifests", s3_settings.root_prefix);

    let mut manifests_by_root: HashMap<String, Vec<AssetManifest>> = HashMap::new();

    // Download input manifests
    if download_input
        && let Some(manifest_list) = job_attachments.get("manifests").and_then(|v| v.as_array())
    {
        for entry in manifest_list {
            let input_path = entry
                .get("inputManifestPath")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let root_path = entry.get("rootPath").and_then(|v| v.as_str()).unwrap_or("");

            if input_path.is_empty() {
                continue;
            }

            let manifest_key = format!("{s3_prefix}/{input_path}");
            let (_, _last_modified, manifest) = download_manifest_from_s3(
                s3_client,
                &s3_settings.s3_bucket_name,
                &manifest_key,
                account_id,
            )
            .await?;

            manifests_by_root
                .entry(root_path.to_owned())
                .or_default()
                .push(manifest);
        }
    }

    // Step-step dependencies (if step_id provided)
    // Deferred: requires Deadline API client for ListStepDependencies.
    // Will be wired in batch 9e-3 when the CLI has access to the
    // Deadline client.

    // Download output manifests
    if download_output {
        let output_by_root = get_output_manifests_by_asset_root(
            s3_settings,
            farm_id,
            queue_id,
            job_id,
            step_id,
            None, // task_id
            None, // session_action_id
            s3_client,
            account_id,
        )
        .await
        .unwrap_or_default();

        for (root, manifests) in output_by_root {
            manifests_by_root.entry(root).or_default().extend(manifests);
        }
    }

    // Merge per root and write to disk
    let mut downloaded = Vec::new();

    for (root, manifests) in &manifests_by_root {
        let merged = merge_asset_manifests(manifests)?;
        if let Some(manifest) = merged {
            let root_hash = hash_data(root.as_bytes());
            let timestamp = chrono::Local::now().format("%Y-%m-%dT%H-%M-%S").to_string();

            // Name derivation: replace / with _, strip leading _
            // (differs from write_manifest which also replaces \ and :)
            let mut manifest_name = root.replace('/', "_");
            if manifest_name.starts_with('_') {
                manifest_name = manifest_name[1..].to_string();
            }
            let filename = format!("{manifest_name}-{root_hash}-{timestamp}.manifest");
            let local_path = download_dir.join(&filename);

            std::fs::write(&local_path, manifest.encode()).map_err(|e| {
                JobAttachmentsError::AssetSync(format!("Failed to write manifest: {e}"))
            })?;

            downloaded.push(ManifestDownloadEntry {
                manifest_root: PathBuf::from(root),
                local_manifest_path: local_path,
            });
        }
    }

    Ok(ManifestDownloadResponse { downloaded })
}
