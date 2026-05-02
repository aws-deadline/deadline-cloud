//! Download engine for job attachments.
//!
//! Downloads files from S3 content-addressed storage (CAS) by hash,
//! with conflict resolution, manifest merging, and output manifest
//! retrieval grouped by asset root.

use std::collections::HashMap;
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};
use crate::errors::JobAttachmentsError;

use crate::asset_manifests::{decode_manifest, AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion};
use crate::models::{FileConflictResolution, JobAttachmentS3Settings};
use crate::progress_tracker::{
    DownloadSummaryStatistics, ProgressReportMetadata, ProgressStatus, ProgressTracker,
};

/// Shared state for `CreateCopy` collision tracking across concurrent downloads.
/// Maps local file path string → highest copy number used.
pub type CollisionState = Arc<Mutex<HashMap<String, i32>>>;

// ---------------------------------------------------------------------------
// Path traversal validation
// ---------------------------------------------------------------------------

/// Validate that all manifest paths resolve within the given root directory.
/// Rejects path traversal attacks (e.g. `../../etc/passwd`).
fn ensure_paths_within_directory(
    root_path: &str,
    paths: &[ManifestPath],
) -> Result<(), JobAttachmentsError> {
    let root = Path::new(root_path);
    if !root.is_absolute() {
        return Err(JobAttachmentsError::PathOutsideDirectory(
            format!("The provided root path is not an absolute path: {root_path}"),
        ));
    }
    let normalized_root = normalize_path(root);

    for p in paths {
        let joined = root.join(&p.path);
        let normalized = normalize_path(&joined);
        if !normalized.starts_with(&normalized_root) {
            return Err(JobAttachmentsError::PathOutsideDirectory(
                format!("The provided path is not under the root directory: {}", p.path),
            ));
        }
    }
    Ok(())
}

/// Lexically normalize a path (resolve `.` and `..` without filesystem access).
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::ParentDir => { components.pop(); }
            std::path::Component::CurDir => {}
            other => components.push(other),
        }
    }
    components.iter().collect()
}

// ---------------------------------------------------------------------------
// Helper: S3 error handling (matches upload patterns exactly)
// ---------------------------------------------------------------------------

fn s3_download_error(
    status_code: u16,
    raw: &str,
    s3_bucket: &str,
    s3_key: &str,
    local_path: &Path,
) -> JobAttachmentsError {
    match status_code {
        403 => {
            let guidance = if raw.contains("kms:") {
                "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket \
                 encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or \
                 User has the 'kms:Decrypt' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
            } else {
                "Forbidden or Access denied. Please check your AWS credentials, and ensure that \
                 your AWS IAM Role or User has the 's3:GetObject' permission for this bucket. "
            };
            JobAttachmentsError::S3Client {
                action: "downloading file".into(),
                status_code: 403,
                bucket: s3_bucket.into(),
                key: s3_key.into(),
                message: Some(format!(
                    "{guidance} {raw} (Failed to download the file to {})",
                    local_path.display()
                )),
            }
        }
        404 => JobAttachmentsError::S3Client {
            action: "downloading file".into(),
            status_code: 404,
            bucket: s3_bucket.into(),
            key: s3_key.into(),
            message: Some(format!(
                "Not found. Please check your bucket name and object key, \
                 and ensure that they exist in the AWS account. {raw} \
                 (Failed to download the file to {})",
                local_path.display()
            )),
        },
        408 => JobAttachmentsError::S3Client {
            action: "downloading file".into(),
            status_code: 408,
            bucket: s3_bucket.into(),
            key: s3_key.into(),
            message: Some(format!(
                "Request timeout. Please consider retrying later, or ensure \
                 your network connection is stable. {raw}"
            )),
        },
        500 => JobAttachmentsError::S3Client {
            action: "downloading file".into(),
            status_code: 500,
            bucket: s3_bucket.into(),
            key: s3_key.into(),
            message: Some(format!(
                "Internal server error. It might be an issue on AWS's side; \
                 please consider retrying later or contacting AWS support. {raw}"
            )),
        },
        503 => JobAttachmentsError::S3Client {
            action: "downloading file".into(),
            status_code: 503,
            bucket: s3_bucket.into(),
            key: s3_key.into(),
            message: Some(format!(
                "Service unavailable. AWS S3 might be down or experiencing \
                 high traffic. Please consider retrying after some time. {raw}"
            )),
        },
        _ => JobAttachmentsError::S3BotoCore {
            action: "downloading file".into(),
            details: raw.to_owned(),
        },
    }
}

// ---------------------------------------------------------------------------
// Helper: S3 GetObject with error handling
// ---------------------------------------------------------------------------

/// Downloads from S3 and streams directly to a file. Avoids buffering
/// the entire file in memory.
async fn s3_stream_to_file(
    s3_client: &S3Client,
    s3_bucket: &str,
    s3_key: &str,
    account_id: &str,
    local_path: &Path,
) -> Result<(), JobAttachmentsError> {
    let result = s3_client
        .get_object()
        .bucket(s3_bucket)
        .key(s3_key)
        .expected_bucket_owner(account_id)
        .send()
        .await;

    match result {
        Ok(output) => {
            let mut file = tokio::fs::File::create(local_path).await.map_err(|e| {
                JobAttachmentsError::AssetSync(format!(
                    "Failed to create {}: {e}", local_path.display()
                ))
            })?;
            let mut body = output.body.into_async_read();
            tokio::io::copy(&mut body, &mut file).await.map_err(|e| {
                JobAttachmentsError::AssetSync(format!(
                    "Failed to write {}: {e}", local_path.display()
                ))
            })?;
            Ok(())
        }
        Err(sdk_err) => {
            let status = sdk_err
                .raw_response()
                .map_or(0, |r| r.status().as_u16());
            let service_err = sdk_err.into_service_error();
            let raw = format!("{service_err}");
            use aws_sdk_s3::error::ProvideErrorMetadata;
            let msg = service_err.message().unwrap_or_default();
            let full_text = format!("{raw} {msg}");
            Err(s3_download_error(status, &full_text, s3_bucket, s3_key, local_path))
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: CreateCopy collision resolution
// ---------------------------------------------------------------------------

/// Generate a unique file path by appending " (N)" before the extension.
/// Uses atomic file creation to handle concurrent downloads.
fn get_new_copy_file_path(
    local_file_path: &Path,
    collision_state: &CollisionState,
) -> PathBuf {
    let mut state = collision_state.lock().expect("collision mutex poisoned");
    let key = local_file_path.to_string_lossy().to_string();
    let num = state.entry(key).or_insert(0);

    let stem = local_file_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy();
    let ext = local_file_path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let parent = local_file_path.parent().unwrap_or(Path::new("."));

    loop {
        *num += 1;
        let candidate = parent.join(format!("{stem} ({num}){ext}"));
        // Atomic check: try to exclusively create the file
        match OpenOptions::new().write(true).create_new(true).open(&candidate) {
            Ok(_) => return candidate,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(_) => return candidate, // best effort on other errors
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: extract asset root from S3 object metadata
// ---------------------------------------------------------------------------

fn get_asset_root_from_metadata(metadata: &HashMap<String, String>) -> Option<String> {
    if let Some(json_root) = metadata.get("asset-root-json") {
        serde_json::from_str(json_root).ok()
    } else {
        metadata.get("asset-root").cloned()
    }
}

// ---------------------------------------------------------------------------
// Helper: build output manifest S3 prefix
// ---------------------------------------------------------------------------

fn get_output_manifest_prefix(
    s3_settings: &JobAttachmentS3Settings,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: Option<&str>,
    task_id: Option<&str>,
) -> Result<String, JobAttachmentsError> {
    let prefix = if let Some(tid) = task_id {
        let sid = step_id.ok_or_else(|| {
            JobAttachmentsError::AssetSync(
                "Task ID specified, but no Step ID. Job, Step, and Task ID are \
                 required to retrieve task outputs."
                    .into(),
            )
        })?;
        s3_settings.full_task_output_prefix(farm_id, queue_id, job_id, sid, tid)?
    } else if let Some(sid) = step_id {
        s3_settings.full_step_output_prefix(farm_id, queue_id, job_id, sid)?
    } else {
        s3_settings.full_job_output_prefix(farm_id, queue_id, job_id)?
    };
    Ok(format!("{prefix}/"))
}

// =========================================================================
// Public API
// =========================================================================

/// Merge multiple manifests into one. Later manifests' paths win on conflict.
///
/// - Empty list → `None`
/// - Single manifest → clone as-is
/// - Multiple → collect paths keyed by path string; later entries overwrite
///   earlier ones. Recalculate `total_size`.
/// - Different hash algorithms → `Err(JobAttachmentsError::AssetSync)`.
pub fn merge_asset_manifests(
    manifests: &[AssetManifest],
) -> Result<Option<AssetManifest>, JobAttachmentsError> {
    if manifests.is_empty() {
        return Ok(None);
    }
    if manifests.len() == 1 {
        return Ok(Some(manifests[0].clone()));
    }

    let hash_alg = manifests[0].hash_alg;
    let mut merged: HashMap<String, ManifestPath> = HashMap::new();

    for manifest in manifests {
        if manifest.hash_alg != hash_alg {
            return Err(JobAttachmentsError::AssetSync(format!(
                "Merging manifests with different hash algorithms is not supported: \
                 {} vs {}",
                hash_alg.as_str(),
                manifest.hash_alg.as_str(),
            )));
        }
        for path in &manifest.paths {
            merged.insert(path.path.clone(), path.clone());
        }
    }

    let paths: Vec<ManifestPath> = merged.into_values().collect();
    let total_size: i64 = paths.iter().map(|p| p.size).sum();

    Ok(Some(AssetManifest::new(
        hash_alg,
        ManifestVersion::V2023_03_03,
        total_size,
        paths,
    )?))
}

/// Download a single file from S3 CAS to a local path.
///
/// Returns `(file_bytes, Option<local_path>)`. `None` path means the file
/// was skipped (conflict resolution = Skip and file exists locally).
pub async fn download_file(
    file: &ManifestPath,
    hash_algorithm: HashAlgorithm,
    local_download_dir: &str,
    s3_client: &S3Client,
    s3_bucket: &str,
    cas_prefix: Option<&str>,
    account_id: &str,
    progress_tracker: Option<&ProgressTracker>,
    file_conflict_resolution: FileConflictResolution,
    collision_state: &CollisionState,
) -> Result<(i64, Option<PathBuf>), JobAttachmentsError> {
    let file_bytes = file.size;

    // Build local path
    let mut local_file_path = PathBuf::from(local_download_dir).join(&file.path);

    // Build S3 key
    let s3_key = match cas_prefix {
        Some(prefix) => format!("{}/{}.{}", prefix, file.hash, hash_algorithm.as_str()),
        None => format!("{}.{}", file.hash, hash_algorithm.as_str()),
    };

    // Conflict resolution if file exists locally
    if local_file_path.is_file() {
        match file_conflict_resolution {
            FileConflictResolution::Skip => {
                if let Some(tracker) = progress_tracker {
                    tracker.increase_skipped(1, file_bytes as u64);
                    tracker.report_progress();
                }
                return Ok((file_bytes, None));
            }
            FileConflictResolution::Overwrite => {} // proceed
            FileConflictResolution::CreateCopy => {
                local_file_path = get_new_copy_file_path(&local_file_path, collision_state);
            }
        }
    }

    // Create parent directories
    if let Some(parent) = local_file_path.parent() {
        fs::create_dir_all(parent).map_err(|e| {
            JobAttachmentsError::AssetSync(format!(
                "Failed to create directory {}: {e}",
                parent.display()
            ))
        })?;
    }

    // Download from S3 — stream directly to file, retry on 404 without suffix
    let stream_result = s3_stream_to_file(
        s3_client, s3_bucket, &s3_key, account_id, &local_file_path,
    )
    .await;

    match stream_result {
        Ok(()) => {}
        Err(JobAttachmentsError::S3Client { status_code: 404, .. }) => {
            // Retry without algorithm suffix (backward compatibility)
            let fallback_key = match cas_prefix {
                Some(prefix) => format!("{}/{}", prefix, file.hash),
                None => file.hash.clone(),
            };
            s3_stream_to_file(
                s3_client, s3_bucket, &fallback_key, account_id, &local_file_path,
            )
            .await?;
        }
        Err(e) => return Err(e),
    }

    // Set mtime from manifest (microseconds → seconds)
    let mtime_secs = file.mtime as f64 / 1_000_000.0;
    let ft = filetime::FileTime::from_unix_time(
        mtime_secs as i64,
        ((mtime_secs.fract()) * 1_000_000_000.0) as u32,
    );
    let _ = filetime::set_file_mtime(&local_file_path, ft);

    // Report progress
    if let Some(tracker) = progress_tracker {
        tracker.increase_processed(1, file_bytes as u64);
        tracker.report_progress();
    }

    Ok((file_bytes, Some(local_file_path)))
}

/// Download all files from manifests grouped by local root directory.
///
/// Returns download summary statistics with per-root file counts.
pub async fn download_files_from_manifests(
    s3_bucket: &str,
    manifests_by_root: &HashMap<String, AssetManifest>,
    cas_prefix: Option<&str>,
    s3_client: &S3Client,
    account_id: &str,
    on_downloading_files: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    conflict_resolution: FileConflictResolution,
) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
    // Compute totals
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    for manifest in manifests_by_root.values() {
        total_files += manifest.paths.len() as u64;
        total_bytes += manifest.total_size as u64;
    }

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::DownloadInProgress,
        total_files,
        total_bytes,
        on_downloading_files,
    );

    let start_time = std::time::Instant::now();
    let collision_state: CollisionState = Arc::new(Mutex::new(HashMap::new()));
    let mut downloaded_files_by_root: HashMap<String, Vec<String>> = HashMap::new();

    // Validate all paths before downloading
    for (local_root, manifest) in manifests_by_root {
        ensure_paths_within_directory(local_root, &manifest.paths)?;
    }

    // Compute download worker count from config
    let num_workers = crate::s3::compute_download_workers(
        crate::s3::get_s3_max_pool_connections(None).unwrap_or(50),
    );

    // Download files in parallel across all manifests
    use futures::stream::{self, StreamExt, TryStreamExt};

    for (local_root, manifest) in manifests_by_root {
        let results: Vec<(i64, Option<PathBuf>)> = stream::iter(
            manifest.paths.iter().map(|file| {
                let collision = &collision_state;
                let tracker = &progress_tracker;
                async move {
                    let result = download_file(
                        file,
                        manifest.hash_alg,
                        local_root,
                        s3_client,
                        s3_bucket,
                        cas_prefix,
                        account_id,
                        Some(tracker),
                        conflict_resolution,
                        collision,
                    )
                    .await?;

                    // Check cancellation after each file
                    if !tracker.report_progress() {
                        let processed = tracker.processed_files();
                        return Err(JobAttachmentsError::Cancelled {
                            message: format!(
                                "Download cancelled. (Downloaded {} file{} before cancellation.)",
                                processed,
                                if processed == 1 { "" } else { "s" }
                            ),
                        });
                    }

                    Ok(result)
                }
            })
        )
        .buffer_unordered(num_workers)
        .try_collect()
        .await?;

        let mut downloaded = Vec::new();
        for (_file_bytes, local_path) in results {
            if let Some(path) = local_path {
                downloaded.push(
                    path.canonicalize()
                        .unwrap_or(path.clone())
                        .to_string_lossy()
                        .to_string(),
                );
            }
        }

        downloaded_files_by_root
            .entry(local_root.clone())
            .or_default()
            .extend(downloaded);
    }

    // Final progress report
    progress_tracker.report_progress();

    let elapsed = start_time.elapsed().as_secs_f64();
    progress_tracker.set_total_time(elapsed);

    let stats = progress_tracker.get_summary_statistics();
    let mut file_counts: std::collections::BTreeMap<String, usize> =
        std::collections::BTreeMap::new();
    let mut all_downloaded: Vec<String> = Vec::new();

    for (root, files) in &downloaded_files_by_root {
        file_counts.insert(root.clone(), files.len());
        all_downloaded.extend(files.iter().cloned());
    }
    all_downloaded.sort();

    Ok(DownloadSummaryStatistics {
        stats,
        file_counts_by_root_directory: file_counts,
        downloaded_files: all_downloaded,
    })
}

/// List and download output manifests from S3, grouped by asset root.
///
/// Manifests are merged chronologically per asset root (oldest first,
/// so newer files overwrite older ones).
pub async fn get_output_manifests_by_asset_root(
    s3_settings: &JobAttachmentS3Settings,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: Option<&str>,
    task_id: Option<&str>,
    session_action_id: Option<&str>,
    s3_client: &S3Client,
    account_id: &str,
) -> Result<HashMap<String, Vec<AssetManifest>>, JobAttachmentsError> {
    // Handle session_action_id case
    if let Some(sa_id) = session_action_id {
        if step_id.is_none() || task_id.is_none() {
            return Err(JobAttachmentsError::AssetSync(
                "Session Action ID specified, but missing Step ID or Task ID. \
                 Job, Step, and Task ID are required to retrieve session action outputs."
                    .into(),
            ));
        }
        return get_manifests_by_session_action_id(
            s3_settings, farm_id, queue_id, job_id,
            step_id.expect("infallible"), task_id.expect("infallible"), sa_id,
            s3_client, account_id,
        )
        .await;
    }

    let manifest_prefix = get_output_manifest_prefix(
        s3_settings, farm_id, queue_id, job_id, step_id, task_id,
    )?;

    // List S3 objects under the prefix
    let manifest_keys = match list_manifest_keys_from_s3(
        s3_client, &s3_settings.s3_bucket_name, &manifest_prefix, account_id,
    )
    .await
    {
        Ok(keys) => keys,
        Err(_) => return Ok(HashMap::new()),
    };

    if manifest_keys.is_empty() {
        return Ok(HashMap::new());
    }

    // Select latest per task
    let selected_keys = select_latest_manifests_per_task(&manifest_keys);

    // Download each manifest and group by asset root with timestamps
    let mut by_root: HashMap<String, Vec<(DateTime<Utc>, AssetManifest)>> = HashMap::new();

    for key in &selected_keys {
        let (asset_root, last_modified, manifest) = download_manifest_from_s3(
            s3_client,
            &s3_settings.s3_bucket_name,
            key,
            account_id,
        )
        .await?;

        let root = asset_root.ok_or_else(|| {
            JobAttachmentsError::MissingAssetRoot(format!(
                "Failed to get asset root from metadata of output manifest: {key}"
            ))
        })?;

        by_root.entry(root).or_default().push((last_modified, manifest));
    }

    // Sort each asset root's manifests by LastModified (oldest first, newer wins)
    // then merge them
    let mut outputs: HashMap<String, Vec<AssetManifest>> = HashMap::new();
    for (root, mut manifest_list) in by_root {
        manifest_list.sort_by_key(|(ts, _)| *ts);
        let manifests: Vec<AssetManifest> = manifest_list.into_iter().map(|(_, m)| m).collect();
        if let Some(merged) = merge_asset_manifests(&manifests)? {
            outputs.insert(root, vec![merged]);
        }
    }

    Ok(outputs)
}

// ---------------------------------------------------------------------------
// Internal: S3 listing and manifest download helpers
// ---------------------------------------------------------------------------

/// List all S3 object keys under a prefix (paginated). Public wrapper.
pub async fn list_output_manifest_keys(
    s3_client: &S3Client,
    s3_bucket: &str,
    prefix: &str,
    account_id: &str,
) -> Result<Vec<String>, JobAttachmentsError> {
    list_manifest_keys_from_s3(s3_client, s3_bucket, prefix, account_id).await
}

/// List all S3 object keys under a prefix (paginated).
async fn list_manifest_keys_from_s3(
    s3_client: &S3Client,
    s3_bucket: &str,
    prefix: &str,
    account_id: &str,
) -> Result<Vec<String>, JobAttachmentsError> {
    let mut all_keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut req = s3_client
            .list_objects_v2()
            .bucket(s3_bucket)
            .prefix(prefix)
            .expected_bucket_owner(account_id);

        if let Some(token) = continuation_token.take() {
            req = req.continuation_token(token);
        }

        let resp = req.send().await.map_err(|sdk_err| {
            let status = sdk_err
                .raw_response()
                .map_or(0, |r| r.status().as_u16());
            let service_err = sdk_err.into_service_error();
            let raw = format!("{service_err}");
            match status {
                403 => JobAttachmentsError::S3Client {
                    action: "listing bucket contents".into(),
                    status_code: 403,
                    bucket: s3_bucket.into(),
                    key: prefix.into(),
                    message: Some(format!(
                        "Forbidden or Access denied. Please check your AWS credentials, \
                         and ensure that your AWS IAM Role or User has the 's3:ListBucket' \
                         permission for this bucket. {raw}"
                    )),
                },
                _ => JobAttachmentsError::S3BotoCore {
                    action: "listing bucket contents".into(),
                    details: raw,
                },
            }
        })?;

        let contents = resp.contents();
        if contents.is_empty() {
            if all_keys.is_empty() {
                return Err(JobAttachmentsError::AssetSync(format!(
                    "Unable to find asset manifest in s3://{s3_bucket}/{prefix}"
                )));
            }
            break;
        }
        for obj in contents {
            if let Some(key) = obj.key() {
                all_keys.push(key.to_owned());
            }
        }
        if resp.is_truncated() == Some(true) {
            continuation_token = resp.next_continuation_token().map(ToOwned::to_owned);
        } else {
            break;
        }
    }

    Ok(all_keys)
}

/// Select latest manifest per task from a list of S3 keys.
///
/// For task-based paths (containing "task-"), selects only the latest
/// session action subfolder per task (alphabetical sort of
/// `timestamp_sessionaction_id`). For chunked steps (no task ID),
/// all manifests are included.
fn select_latest_manifests_per_task(keys: &[String]) -> Vec<String> {
    let step_pattern = regex::Regex::new(r"step-.*/.*/.*output.*").unwrap_or_else(|_| {
        // Fallback: accept all keys if regex fails
        regex::Regex::new(r".*").expect("valid regex")
    });

    let mut direct_keys = Vec::new();
    let mut task_prefixes: HashMap<String, Vec<String>> = HashMap::new();

    for key in keys {
        if !step_pattern.is_match(key) {
            continue;
        }
        if key.contains("task-") {
            let parts: Vec<&str> = key.split('/').collect();
            let mut task_folder = None;
            for (i, part) in parts.iter().enumerate() {
                if part.contains("task-") {
                    task_folder = Some(parts[..=i].join("/"));
                    break;
                }
            }
            if let Some(folder) = task_folder {
                task_prefixes.entry(folder).or_default().push(key.clone());
            }
        } else {
            direct_keys.push(key.clone());
        }
    }

    // For each task folder, select the latest subfolder (alphabetically last)
    for (task_folder, files) in &task_prefixes {
        let folder_depth = task_folder.split('/').count();
        let mut subfolders: Vec<&str> = files
            .iter()
            .filter_map(|f| f.split('/').nth(folder_depth))
            .collect();
        subfolders.sort_unstable();
        subfolders.dedup();

        if let Some(latest) = subfolders.last() {
            let prefix = format!("{task_folder}/{latest}/");
            for f in files {
                if f.contains(&prefix) {
                    direct_keys.push(f.clone());
                }
            }
        }
    }

    direct_keys
}

/// Download a manifest from S3 and extract its asset root from metadata.
pub async fn download_manifest_from_s3(
    s3_client: &S3Client,
    s3_bucket: &str,
    manifest_key: &str,
    account_id: &str,
) -> Result<(Option<String>, DateTime<Utc>, AssetManifest), JobAttachmentsError> {
    let result = s3_client
        .get_object()
        .bucket(s3_bucket)
        .key(manifest_key)
        .expected_bucket_owner(account_id)
        .send()
        .await
        .map_err(|sdk_err| {
            let status = sdk_err
                .raw_response()
                .map_or(0, |r| r.status().as_u16());
            let service_err = sdk_err.into_service_error();
            let raw = format!("{service_err}");
            JobAttachmentsError::S3Client {
                action: "downloading binary file".into(),
                status_code: status,
                bucket: s3_bucket.into(),
                key: manifest_key.into(),
                message: Some(raw),
            }
        })?;

    // Extract LastModified from S3 response
    let last_modified = result
        .last_modified()
        .and_then(|dt| {
            let epoch_secs = dt.secs();
            let nanos = dt.subsec_nanos();
            DateTime::from_timestamp(epoch_secs, nanos)
        })
        .unwrap_or_else(Utc::now);

    // Extract asset root from metadata
    let metadata: HashMap<String, String> = result
        .metadata()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let asset_root = get_asset_root_from_metadata(&metadata);

    // Read and decode manifest
    let body_bytes = result
        .body
        .collect()
        .await
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Failed to read manifest body: {e}")))?
        .into_bytes();
    let contents = String::from_utf8(body_bytes.to_vec()).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Manifest is not valid UTF-8: {e}"))
    })?;
    let manifest = decode_manifest(&contents)?;

    Ok((asset_root, last_modified, manifest))
}

/// Get manifests for a specific session action ID by searching S3 paths.
async fn get_manifests_by_session_action_id(
    s3_settings: &JobAttachmentS3Settings,
    farm_id: &str,
    queue_id: &str,
    job_id: &str,
    step_id: &str,
    task_id: &str,
    session_action_id: &str,
    s3_client: &S3Client,
    account_id: &str,
) -> Result<HashMap<String, Vec<AssetManifest>>, JobAttachmentsError> {
    let mut outputs: HashMap<String, Vec<AssetManifest>> = HashMap::new();

    // Try task-specific prefix first
    let task_prefix = get_output_manifest_prefix(
        s3_settings, farm_id, queue_id, job_id, Some(step_id), Some(task_id),
    )?;

    let sa_pattern = regex::Regex::new(&format!(r".*{}.*output.*", regex::escape(session_action_id)))
        .unwrap_or_else(|_| regex::Regex::new(r"$^").expect("valid regex"));

    let mut manifest_keys: Vec<String> = Vec::new();

    if let Ok(all_keys) = list_manifest_keys_from_s3(
        s3_client, &s3_settings.s3_bucket_name, &task_prefix, account_id,
    )
    .await
    {
        manifest_keys = all_keys
            .into_iter()
            .filter(|k| sa_pattern.is_match(k))
            .collect();
    }

    // Fall back to step level if no task-level manifests found
    if manifest_keys.is_empty() {
        let step_prefix = get_output_manifest_prefix(
            s3_settings, farm_id, queue_id, job_id, Some(step_id), None,
        )?;
        if let Ok(all_keys) = list_manifest_keys_from_s3(
            s3_client, &s3_settings.s3_bucket_name, &step_prefix, account_id,
        )
        .await
        {
            manifest_keys = all_keys
                .into_iter()
                .filter(|k| sa_pattern.is_match(k))
                .collect();
        }
    }

    // Download all found manifests
    for key in &manifest_keys {
        let (asset_root, _last_modified, manifest) = download_manifest_from_s3(
            s3_client,
            &s3_settings.s3_bucket_name,
            key,
            account_id,
        )
        .await?;

        let root = asset_root.ok_or_else(|| {
            JobAttachmentsError::MissingAssetRoot(format!(
                "Failed to get asset root from metadata of output manifest: {key}"
            ))
        })?;

        outputs.entry(root).or_default().push(manifest);
    }

    Ok(outputs)
}

// =========================================================================
// OutputDownloader — orchestrates job output download
// =========================================================================

/// Handler for downloading output files from a job, with optional step/task
/// granularity. Wraps `get_output_manifests_by_asset_root` and
/// `download_files_from_manifests`.
pub struct OutputDownloader {
    s3_settings: JobAttachmentS3Settings,
    outputs_by_root: HashMap<String, Vec<AssetManifest>>,
    s3_client: S3Client,
    account_id: String,
}

impl OutputDownloader {
    /// Create a new downloader by fetching output manifests from S3.
    pub async fn new(
        s3_settings: JobAttachmentS3Settings,
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: Option<&str>,
        task_id: Option<&str>,
        session_action_id: Option<&str>,
        s3_client: S3Client,
        account_id: String,
    ) -> Result<Self, JobAttachmentsError> {
        let outputs_by_root = get_output_manifests_by_asset_root(
            &s3_settings, farm_id, queue_id, job_id,
            step_id, task_id, session_action_id,
            &s3_client, &account_id,
        ).await?;
        Ok(Self { s3_settings, outputs_by_root, s3_client, account_id })
    }

    /// Get output file paths grouped by asset root.
    pub fn get_output_paths_by_root(&self) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        for (root, manifests) in &self.outputs_by_root {
            let paths: Vec<String> = manifests
                .iter()
                .flat_map(|m| m.paths.iter().map(|p| p.path.clone()))
                .collect();
            if !paths.is_empty() {
                result.insert(root.clone(), paths);
            }
        }
        result
    }

    /// Change the root path for a set of output files.
    pub fn set_root_path(&mut self, original_root: &str, new_root: &str) {
        if original_root == new_root {
            return;
        }
        if let Some(manifests) = self.outputs_by_root.remove(original_root) {
            self.outputs_by_root
                .entry(new_root.to_owned())
                .or_default()
                .extend(manifests);
        }
    }

    /// Download all output files to their respective root directories.
    pub async fn download_job_output(
        &self,
        file_conflict_resolution: FileConflictResolution,
        on_downloading_files: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    ) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
        // Flatten manifests: merge per-root into single manifest per root
        let mut manifests_by_root = HashMap::new();
        for (root, manifest_list) in &self.outputs_by_root {
            if let Some(merged) = merge_asset_manifests(manifest_list)? {
                manifests_by_root.insert(root.clone(), merged);
            }
        }

        let cas_prefix = self.s3_settings.full_cas_prefix()?;
        download_files_from_manifests(
            &self.s3_settings.s3_bucket_name,
            &manifests_by_root,
            Some(&cas_prefix),
            &self.s3_client,
            &self.account_id,
            on_downloading_files,
            file_conflict_resolution,
        ).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_asset_root_from_metadata_prefers_json() {
        let mut meta = HashMap::new();
        meta.insert("asset-root".into(), "raw-root".into());
        meta.insert("asset-root-json".into(), r#""/tmp/日本語""#.into());
        let root = get_asset_root_from_metadata(&meta);
        assert_eq!(root, Some("/tmp/日本語".into()));
    }

    #[test]
    fn get_asset_root_from_metadata_falls_back_to_plain() {
        let mut meta = HashMap::new();
        meta.insert("asset-root".into(), "/tmp/root".into());
        let root = get_asset_root_from_metadata(&meta);
        assert_eq!(root, Some("/tmp/root".into()));
    }

    #[test]
    fn get_asset_root_from_metadata_missing_returns_none() {
        let meta = HashMap::new();
        let root = get_asset_root_from_metadata(&meta);
        assert!(root.is_none());
    }

    #[test]
    fn select_latest_manifests_per_task_picks_last_subfolder() {
        let keys = vec![
            "rp/Manifests/f/q/j/step-1/task-1/2024-01-01T00:00:00Z_sa-1/output.manifest".into(),
            "rp/Manifests/f/q/j/step-1/task-1/2024-01-02T00:00:00Z_sa-2/output.manifest".into(),
        ];
        let selected = select_latest_manifests_per_task(&keys);
        assert_eq!(selected.len(), 1);
        assert!(selected[0].contains("sa-2"));
    }

    #[test]
    fn output_manifest_prefix_job_level() {
        let s = JobAttachmentS3Settings::from_root_path("b/rp").unwrap();
        let prefix = get_output_manifest_prefix(&s, "f", "q", "j", None, None).unwrap();
        assert_eq!(prefix, "rp/Manifests/f/q/j/");
    }

    #[test]
    fn output_manifest_prefix_task_without_step_errors() {
        let s = JobAttachmentS3Settings::from_root_path("b/rp").unwrap();
        let err = get_output_manifest_prefix(&s, "f", "q", "j", None, Some("t")).unwrap_err();
        assert!(err.to_string().contains("Step ID"));
    }
}
