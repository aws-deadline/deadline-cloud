//! Download engine for job attachments.
//!
//! Downloads files from S3 content-addressed storage (CAS) by hash,
//! with conflict resolution, manifest merging, and output manifest
//! retrieval grouped by asset root.

use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;

use crate::attachments::errors::JobAttachmentsError;
use aws_sdk_s3::Client as S3Client;
use chrono::{DateTime, Utc};

use crate::attachments::models::{Attachments, FileConflictResolution, JobAttachmentS3Settings};
use crate::attachments::progress_tracker::{
    DownloadSummaryStatistics, ProgressStatus, ProgressTracker,
};
use openjd_snapshots::{FileEntry, Snapshot, WHOLE_FILE_CHUNK_SIZE, decode_v2023};

// Shared state for `CreateCopy` collision tracking across concurrent downloads.
// Maps local file path string → highest copy number used.
// ---------------------------------------------------------------------------
// Path traversal validation
// ---------------------------------------------------------------------------

/// Validate that all manifest paths resolve within the given root directory.
/// Rejects path traversal attacks (e.g. `../../etc/passwd`).
fn ensure_paths_within_directory(
    root_path: &str,
    paths: &[FileEntry],
) -> Result<(), JobAttachmentsError> {
    let root = Path::new(root_path);
    if !root.is_absolute() {
        return Err(JobAttachmentsError::PathOutsideDirectory(format!(
            "The provided root path is not an absolute path: {root_path}"
        )));
    }
    let normalized_root = crate::util::normalize_path(root);

    for p in paths {
        let joined = root.join(&p.path);
        let normalized = crate::util::normalize_path(&joined);
        if !normalized.starts_with(&normalized_root) {
            return Err(JobAttachmentsError::PathOutsideDirectory(format!(
                "The provided path is not under the root directory: {}",
                p.path
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: S3 error handling (matches upload patterns exactly)
// ---------------------------------------------------------------------------

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
// Include-path filtering
// =========================================================================

/// Recompute manifests from initial state by applying root mappings then filter groups.
///
/// Shared logic between `OutputDownloader` and `InputDownloader`.
/// Root mappings are applied first (merging manifests if two roots map to the same target),
/// then each filter group is applied sequentially (AND between groups, OR within each group).
pub(crate) fn rebuild_manifests(
    initial: &HashMap<String, Vec<Snapshot>>,
    root_mappings: &HashMap<String, String>,
    filter_groups: &[Vec<String>],
) -> HashMap<String, Vec<Snapshot>> {
    let mut rebuilt: HashMap<String, Vec<Snapshot>> = HashMap::new();
    for (root, manifests) in initial {
        let mapped_root = root_mappings.get(root).unwrap_or(root).clone();
        rebuilt
            .entry(mapped_root)
            .or_default()
            .extend(manifests.clone());
    }
    for filter_group in filter_groups {
        rebuilt = filter_manifests(&rebuilt, filter_group);
    }
    rebuilt
}

/// Normalize path filter patterns: `\` → `/`, strip `./`, collapse `//`.
pub fn normalize_filters(patterns: &[String]) -> Vec<String> {
    patterns
        .iter()
        .filter_map(|f| {
            let mut f = f.replace('\\', "/");
            if f.starts_with("./") {
                f = f[2..].to_string();
            }
            while f.contains("//") {
                f = f.replace("//", "/");
            }
            if f.is_empty() { None } else { Some(f) }
        })
        .collect()
}

/// Check if a file path matches any of the given filters using fnmatch-style matching.
///
/// - `*` matches everything including `/` (like Python's fnmatch)
/// - A filter ending with `/` matches all files under that directory
/// - A relative filter (not starting with `/`, `*`, or drive letter) is auto-prepended with `*/`
pub fn matches_any_filter(file_path: &str, filters: &[String]) -> bool {
    let filter_set = FilterSet::new(filters);
    filter_set.matches(file_path)
}

/// Filter manifests by root, keeping only paths that match any filter.
pub fn filter_manifests<S: std::hash::BuildHasher + Clone>(
    manifests_by_root: &HashMap<String, Vec<Snapshot>, S>,
    filters: &[String],
) -> HashMap<String, Vec<Snapshot>, S> {
    let filter_set = FilterSet::new(filters);
    let mut filtered = HashMap::with_capacity_and_hasher(
        manifests_by_root.len(),
        manifests_by_root.hasher().clone(),
    );
    for (root, manifest_list) in manifests_by_root {
        let mut filtered_manifests = Vec::new();
        for manifest in manifest_list {
            let matching: Vec<FileEntry> = manifest
                .files
                .iter()
                .filter(|p| filter_set.matches(&full_path(root, &p.path)))
                .cloned()
                .collect();
            if !matching.is_empty() {
                let total_size = matching.iter().map(|p| p.size.unwrap_or(0)).sum();
                let mut m = Snapshot::new(manifest.hash_alg, WHOLE_FILE_CHUNK_SIZE);
                m.files = matching;
                m.total_size = total_size;
                filtered_manifests.push(m);
            }
        }
        if !filtered_manifests.is_empty() {
            filtered.insert(root.clone(), filtered_manifests);
        }
    }
    filtered
}

/// Join root and relative path with forward slashes for consistent matching.
fn full_path(root: &str, relative: &str) -> String {
    let root = root.replace('\\', "/");
    if root.ends_with('/') {
        format!("{root}{relative}")
    } else {
        format!("{root}/{relative}")
    }
}

/// Compile an fnmatch pattern into a regex.
fn compile_fnmatch(pattern: &str) -> Option<regex::Regex> {
    let mut regex = String::with_capacity(pattern.len() * 2 + 2);
    regex.push('^');
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '[' => {
                regex.push('[');
                // fnmatch: [!seq] means negation → regex [^seq]
                if chars.peek() == Some(&'!') {
                    chars.next();
                    regex.push('^');
                }
                // Copy until closing ]
                let mut found_close = false;
                while let Some(&next) = chars.peek() {
                    chars.next();
                    if next == ']' {
                        regex.push(']');
                        found_close = true;
                        break;
                    }
                    regex.push(next);
                }
                if !found_close {
                    regex.push(']');
                }
            }
            '.' | '+' | '^' | '$' | '(' | ')' | '{' | '}' | '|' | '\\' => {
                regex.push('\\');
                regex.push(c);
            }
            _ => regex.push(c),
        }
    }
    regex.push('$');
    regex::Regex::new(&regex).ok()
}

/// Pre-compiled set of filter patterns for efficient repeated matching.
struct FilterSet {
    patterns: Vec<(String, regex::Regex)>,
}

impl FilterSet {
    fn new(filters: &[String]) -> Self {
        fn is_absolute(p: &str) -> bool {
            p.starts_with('/') || p.starts_with('*') || (p.len() >= 2 && p.as_bytes()[1] == b':')
        }

        let patterns = filters
            .iter()
            .filter_map(|f| {
                let pattern = if f.ends_with('/') {
                    if is_absolute(f) {
                        format!("{f}*")
                    } else {
                        format!("*/{f}*")
                    }
                } else if is_absolute(f) {
                    f.clone()
                } else {
                    format!("*/{f}")
                };
                compile_fnmatch(&pattern).map(|re| (pattern, re))
            })
            .collect();
        Self { patterns }
    }

    fn matches(&self, file_path: &str) -> bool {
        self.patterns.iter().any(|(_, re)| re.is_match(file_path))
    }
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
    manifests: &[Snapshot],
) -> Result<Option<Snapshot>, JobAttachmentsError> {
    if manifests.is_empty() {
        return Ok(None);
    }
    if manifests.len() == 1 {
        return Ok(Some(manifests[0].clone()));
    }

    let hash_alg = manifests[0].hash_alg;
    let mut merged: HashMap<String, FileEntry> = HashMap::new();

    for manifest in manifests {
        if manifest.hash_alg != hash_alg {
            return Err(JobAttachmentsError::AssetSync(format!(
                "Merging manifests with different hash algorithms is not supported: \
                 {} vs {}",
                hash_alg, manifest.hash_alg,
            )));
        }
        for file in &manifest.files {
            merged.insert(file.path.clone(), file.clone());
        }
    }

    let files: Vec<FileEntry> = merged.into_values().collect();
    let total_size: u64 = files.iter().map(|f| f.size.unwrap_or(0)).sum();

    let mut snap = Snapshot::new(hash_alg, WHOLE_FILE_CHUNK_SIZE);
    snap.files = files;
    snap.total_size = total_size;
    Ok(Some(snap))
}

/// Download all files from manifests grouped by local root directory.
///
/// Uses openjd-snapshots' download engine for parallel downloads with
/// conflict resolution and mtime restoration.
#[allow(clippy::implicit_hasher, reason = "only used with default HashMap")]
pub async fn download_files_from_manifests(
    s3_bucket: &str,
    manifests_by_root: &HashMap<String, Snapshot>,
    cas_prefix: Option<&str>,
    s3_client: &S3Client,
    account_id: &str,
    on_downloading_files: Option<Box<dyn Fn(u64, u64) -> bool + Send>>,
    conflict_resolution: FileConflictResolution,
) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
    use openjd_snapshots::{
        AbsManifest, AsyncDataCache, DownloadOptions as OpenjdDownloadOptions,
        FileConflictResolution as OpenjdConflict, FileEntry, S3DataCache, download_abs_manifest,
    };
    use std::sync::Arc;

    // Compute totals
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    for manifest in manifests_by_root.values() {
        total_files += manifest.files.len() as u64;
        total_bytes += manifest.total_size;
    }

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::DownloadInProgress,
        total_files,
        total_bytes,
        on_downloading_files,
    );

    // Check cancellation before starting
    if !progress_tracker.report_progress() {
        return Err(JobAttachmentsError::Cancelled {
            message: "Download cancelled.".into(),
        });
    }

    let start_time = std::time::Instant::now();

    // Validate all paths before downloading
    for (local_root, manifest) in manifests_by_root {
        ensure_paths_within_directory(local_root, &manifest.files)?;
    }

    // Build S3DataCache
    let prefix = cas_prefix.unwrap_or_default().to_owned();
    let s3_cache = S3DataCache::new(s3_bucket.to_owned(), prefix, s3_client.clone())
        .with_expected_bucket_owner(Some(account_id.to_owned()));
    let data_cache: Arc<dyn AsyncDataCache> = Arc::new(s3_cache);

    // Map our conflict resolution to openjd's
    let openjd_conflict = match conflict_resolution {
        FileConflictResolution::Skip => OpenjdConflict::Skip,
        FileConflictResolution::Overwrite => OpenjdConflict::Overwrite,
        FileConflictResolution::CreateCopy => OpenjdConflict::CreateCopy,
    };

    let mut downloaded_files_by_root: HashMap<String, Vec<String>> = HashMap::new();

    for (local_root, manifest) in manifests_by_root {
        // Build AbsSnapshot with absolute paths (root + relative)
        let mut abs_snapshot = openjd_snapshots::Manifest::new(
            openjd_snapshots::HashAlgorithm::Xxh128,
            WHOLE_FILE_CHUNK_SIZE,
        );
        for p in &manifest.files {
            let abs_path = if local_root.ends_with('/') {
                format!("{}{}", local_root, p.path)
            } else {
                format!("{}/{}", local_root, p.path)
            };
            let mut entry = FileEntry::file(&abs_path, p.size.unwrap_or(0), p.mtime.unwrap_or(0));
            entry.hash.clone_from(&p.hash);
            abs_snapshot.files.push(entry);
        }
        abs_snapshot.total_size = manifest.total_size;

        let result = download_abs_manifest(
            &AbsManifest::Snapshot(abs_snapshot),
            data_cache.clone(),
            OpenjdDownloadOptions {
                file_conflict_resolution: openjd_conflict,
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            // TODO: openjd's download engine returns generic S3 errors.
            // We previously provided KMS-specific guidance on 403 errors
            // (e.g., "ensure kms:Decrypt permission"). Consider wrapping
            // SnapshotError::S3 to detect KMS errors and add guidance.
            JobAttachmentsError::AssetSync(format!("Download failed: {e}"))
        })?;

        // Collect downloaded file paths
        let stats = &result.statistics;
        let downloaded: Vec<String> = match &result.manifest {
            AbsManifest::Snapshot(s) => s
                .files
                .iter()
                .filter(|f| !f.deleted && f.symlink_target.is_none())
                .map(|f| f.path.clone())
                .collect(),
            AbsManifest::Diff(_) => vec![],
        };

        downloaded_files_by_root
            .entry(local_root.clone())
            .or_default()
            .extend(downloaded);

        progress_tracker.increase_processed(stats.downloaded_files as u64, stats.downloaded_bytes);
        progress_tracker.increase_skipped(stats.skipped_files as u64, stats.skipped_bytes);
    }

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
) -> Result<HashMap<String, Vec<Snapshot>>, JobAttachmentsError> {
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
            s3_settings,
            farm_id,
            queue_id,
            job_id,
            step_id.expect("checked above"),
            task_id.expect("checked above"),
            sa_id,
            s3_client,
            account_id,
        )
        .await;
    }

    let manifest_prefix =
        get_output_manifest_prefix(s3_settings, farm_id, queue_id, job_id, step_id, task_id)?;

    // List S3 objects under the prefix
    let Ok(manifest_keys) = list_manifest_keys_from_s3(
        s3_client,
        &s3_settings.s3_bucket_name,
        &manifest_prefix,
        account_id,
    )
    .await
    else {
        return Ok(HashMap::new());
    };

    if manifest_keys.is_empty() {
        return Ok(HashMap::new());
    }

    // Select latest per task
    let selected_keys = select_latest_manifests_per_task(&manifest_keys);

    // Download each manifest and group by asset root with timestamps
    let mut by_root: HashMap<String, Vec<(DateTime<Utc>, Snapshot)>> = HashMap::new();

    for key in &selected_keys {
        let (asset_root, last_modified, manifest) =
            download_manifest_from_s3(s3_client, &s3_settings.s3_bucket_name, key, account_id)
                .await?;

        let root = asset_root.ok_or_else(|| {
            JobAttachmentsError::MissingAssetRoot(format!(
                "Failed to get asset root from metadata of output manifest: {key}"
            ))
        })?;

        by_root
            .entry(root)
            .or_default()
            .push((last_modified, manifest));
    }

    // Sort each asset root's manifests by LastModified (oldest first, newer wins)
    // then merge them
    let mut outputs: HashMap<String, Vec<Snapshot>> = HashMap::new();
    for (root, mut manifest_list) in by_root {
        manifest_list.sort_by_key(|(ts, _)| *ts);
        let manifests: Vec<Snapshot> = manifest_list.into_iter().map(|(_, m)| m).collect();
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
            let status = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
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

/// Compiled regex for matching step output manifest keys.
static STEP_OUTPUT_PATTERN: LazyLock<regex::Regex> =
    LazyLock::new(|| regex::Regex::new(r"step-.*/.*/.*output.*").expect("valid regex"));

/// Select latest manifest per task from a list of S3 keys.
///
/// For task-based paths (containing "task-"), selects only the latest
/// session action subfolder per task (alphabetical sort of
/// `timestamp_sessionaction_id`). For chunked steps (no task ID),
/// all manifests are included.
fn select_latest_manifests_per_task(keys: &[String]) -> Vec<String> {
    let step_pattern = &*STEP_OUTPUT_PATTERN;

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
) -> Result<(Option<String>, DateTime<Utc>, Snapshot), JobAttachmentsError> {
    let result = s3_client
        .get_object()
        .bucket(s3_bucket)
        .key(manifest_key)
        .expected_bucket_owner(account_id)
        .send()
        .await
        .map_err(|sdk_err| {
            let status = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
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
    let contents = String::from_utf8(body_bytes.to_vec())
        .map_err(|e| JobAttachmentsError::AssetSync(format!("Manifest is not valid UTF-8: {e}")))?;
    let manifest =
        decode_v2023(&contents).map_err(|e| JobAttachmentsError::ManifestDecode(e.to_string()))?;

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
) -> Result<HashMap<String, Vec<Snapshot>>, JobAttachmentsError> {
    let mut outputs: HashMap<String, Vec<Snapshot>> = HashMap::new();

    // Try task-specific prefix first
    let task_prefix = get_output_manifest_prefix(
        s3_settings,
        farm_id,
        queue_id,
        job_id,
        Some(step_id),
        Some(task_id),
    )?;

    let sa_pattern = regex::Regex::new(&format!(
        r".*{}.*output.*",
        regex::escape(session_action_id)
    ))
    .unwrap_or_else(|_| regex::Regex::new(r"$^").expect("valid regex"));

    let mut manifest_keys: Vec<String> = Vec::new();

    if let Ok(all_keys) = list_manifest_keys_from_s3(
        s3_client,
        &s3_settings.s3_bucket_name,
        &task_prefix,
        account_id,
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
            s3_settings,
            farm_id,
            queue_id,
            job_id,
            Some(step_id),
            None,
        )?;
        if let Ok(all_keys) = list_manifest_keys_from_s3(
            s3_client,
            &s3_settings.s3_bucket_name,
            &step_prefix,
            account_id,
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
        let (asset_root, _last_modified, manifest) =
            download_manifest_from_s3(s3_client, &s3_settings.s3_bucket_name, key, account_id)
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
    initial_outputs_by_root: HashMap<String, Vec<Snapshot>>,
    include_filter_groups: Vec<Vec<String>>,
    root_mappings: HashMap<String, String>,
    outputs_by_root: HashMap<String, Vec<Snapshot>>,
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
        include_filters: Option<&[String]>,
    ) -> Result<Self, JobAttachmentsError> {
        let initial_outputs_by_root = get_output_manifests_by_asset_root(
            &s3_settings,
            farm_id,
            queue_id,
            job_id,
            step_id,
            task_id,
            session_action_id,
            &s3_client,
            &account_id,
        )
        .await?;
        let mut include_filter_groups = Vec::new();
        if let Some(filters) = include_filters
            && !filters.is_empty()
        {
            include_filter_groups.push(filters.to_vec());
        }
        let mut dl = Self {
            s3_settings,
            initial_outputs_by_root,
            include_filter_groups,
            root_mappings: HashMap::new(),
            outputs_by_root: HashMap::new(),
            s3_client,
            account_id,
        };
        dl.rebuild();
        Ok(dl)
    }

    /// Recompute `outputs_by_root` from initial state applying root mappings then filters.
    fn rebuild(&mut self) {
        self.outputs_by_root = rebuild_manifests(
            &self.initial_outputs_by_root,
            &self.root_mappings,
            &self.include_filter_groups,
        );
    }

    /// Get output file paths grouped by asset root.
    pub fn get_output_paths_by_root(&self) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        for (root, manifests) in &self.outputs_by_root {
            let paths: Vec<String> = manifests
                .iter()
                .flat_map(|m| m.files.iter().map(|p| p.path.clone()))
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
        // Find the initial root that maps to original_root
        let initial_root = self
            .initial_outputs_by_root
            .keys()
            .find(|k| self.root_mappings.get(*k).unwrap_or(k).as_str() == original_root)
            .cloned();
        if let Some(init_root) = initial_root {
            self.root_mappings.insert(init_root, new_root.to_owned());
            self.rebuild();
        }
    }

    /// Apply glob-style include filters against the current paths.
    pub fn apply_include_filters(&mut self, filters: &[String]) {
        if !filters.is_empty() {
            self.include_filter_groups.push(filters.to_vec());
            self.rebuild();
        }
    }

    /// Download all output files to their respective root directories.
    pub async fn download_job_output(
        &self,
        file_conflict_resolution: FileConflictResolution,
        on_downloading_files: Option<Box<dyn Fn(u64, u64) -> bool + Send>>,
    ) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
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
        )
        .await
    }
}

// =========================================================================
// InputDownloader — orchestrates job input download
// =========================================================================

/// Fetch input manifests from S3 based on the job's attachments metadata.
/// Returns manifests grouped by asset root path.
pub async fn get_input_manifests_by_asset_root(
    s3_settings: &JobAttachmentS3Settings,
    attachments: &Attachments,
    s3_client: &S3Client,
    account_id: &str,
) -> Result<HashMap<String, Vec<Snapshot>>, JobAttachmentsError> {
    let mut inputs: HashMap<String, Vec<Snapshot>> = HashMap::new();

    for manifest_props in &attachments.manifests {
        if let Some(ref input_path) = manifest_props.input_manifest_path {
            if input_path.is_empty() {
                continue;
            }
            let key = s3_settings.add_root_and_manifest_folder_prefix(input_path)?;
            let (_, _last_modified, manifest) =
                download_manifest_from_s3(s3_client, &s3_settings.s3_bucket_name, &key, account_id)
                    .await?;
            inputs
                .entry(manifest_props.root_path.clone())
                .or_default()
                .push(manifest);
        }
    }

    Ok(inputs)
}

/// Handler for downloading input files from a job, with optional include filtering.
/// Inputs are job-level only (no step/task scoping).
pub struct InputDownloader {
    s3_settings: JobAttachmentS3Settings,
    initial_inputs_by_root: HashMap<String, Vec<Snapshot>>,
    include_filter_groups: Vec<Vec<String>>,
    root_mappings: HashMap<String, String>,
    inputs_by_root: HashMap<String, Vec<Snapshot>>,
    s3_client: S3Client,
    account_id: String,
}

impl InputDownloader {
    /// Create a new downloader by fetching input manifests from S3.
    pub async fn new(
        s3_settings: JobAttachmentS3Settings,
        attachments: &Attachments,
        s3_client: S3Client,
        account_id: String,
        include_filters: Option<&[String]>,
    ) -> Result<Self, JobAttachmentsError> {
        let initial_inputs_by_root =
            get_input_manifests_by_asset_root(&s3_settings, attachments, &s3_client, &account_id)
                .await?;
        let mut include_filter_groups = Vec::new();
        if let Some(filters) = include_filters
            && !filters.is_empty()
        {
            include_filter_groups.push(filters.to_vec());
        }
        let mut dl = Self {
            s3_settings,
            initial_inputs_by_root,
            include_filter_groups,
            root_mappings: HashMap::new(),
            inputs_by_root: HashMap::new(),
            s3_client,
            account_id,
        };
        dl.rebuild();
        Ok(dl)
    }

    fn rebuild(&mut self) {
        self.inputs_by_root = rebuild_manifests(
            &self.initial_inputs_by_root,
            &self.root_mappings,
            &self.include_filter_groups,
        );
    }

    /// Get input file paths grouped by asset root.
    pub fn get_paths_by_root(&self) -> HashMap<String, Vec<String>> {
        let mut result = HashMap::new();
        for (root, manifests) in &self.inputs_by_root {
            let paths: Vec<String> = manifests
                .iter()
                .flat_map(|m| m.files.iter().map(|p| p.path.clone()))
                .collect();
            if !paths.is_empty() {
                result.insert(root.clone(), paths);
            }
        }
        result
    }

    /// Change the root path for a set of input files.
    pub fn set_root_path(&mut self, original_root: &str, new_root: &str) {
        if original_root == new_root {
            return;
        }
        let initial_root = self
            .initial_inputs_by_root
            .keys()
            .find(|k| self.root_mappings.get(*k).unwrap_or(k).as_str() == original_root)
            .cloned();
        if let Some(init_root) = initial_root {
            self.root_mappings.insert(init_root, new_root.to_owned());
            self.rebuild();
        }
    }

    /// Apply glob-style include filters against the current paths.
    pub fn apply_include_filters(&mut self, filters: &[String]) {
        if !filters.is_empty() {
            self.include_filter_groups.push(filters.to_vec());
            self.rebuild();
        }
    }

    /// Download all input files to their respective root directories.
    pub async fn download(
        &self,
        file_conflict_resolution: FileConflictResolution,
        on_downloading_files: Option<Box<dyn Fn(u64, u64) -> bool + Send>>,
    ) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
        let mut manifests_by_root = HashMap::new();
        for (root, manifest_list) in &self.inputs_by_root {
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
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openjd_snapshots::HashAlgorithm;

    // --- Helper to build test manifests ---

    fn make_manifest(paths: &[&str]) -> Snapshot {
        let files: Vec<FileEntry> = paths
            .iter()
            .map(|p| {
                let mut e = FileEntry::file(*p, 100, 1_700_000_000);
                e.hash = Some("aaa111bbb222ccc333ddd444eee55566".into());
                e
            })
            .collect();
        let total_size = files.iter().map(|f| f.size.unwrap_or(0)).sum();
        let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
        snap.files = files;
        snap.total_size = total_size;
        snap
    }

    // --- rebuild_manifests L1 tests ---

    #[test]
    fn rebuild_manifests_no_mappings_no_filters_returns_initial() {
        let mut initial = HashMap::new();
        initial.insert(
            "/root".to_string(),
            vec![make_manifest(&["a.txt", "b.txt"])],
        );

        let result = rebuild_manifests(&initial, &HashMap::new(), &[]);

        assert_eq!(result.len(), 1);
        let paths: Vec<&str> = result["/root"]
            .iter()
            .flat_map(|m| m.files.iter().map(|p| p.path.as_str()))
            .collect();
        assert!(paths.contains(&"a.txt"));
        assert!(paths.contains(&"b.txt"));
    }

    #[test]
    fn rebuild_manifests_applies_root_mapping() {
        let mut initial = HashMap::new();
        initial.insert("/old".to_string(), vec![make_manifest(&["file.txt"])]);

        let mut mappings = HashMap::new();
        mappings.insert("/old".to_string(), "/new".to_string());

        let result = rebuild_manifests(&initial, &mappings, &[]);

        assert!(!result.contains_key("/old"));
        assert!(result.contains_key("/new"));
        assert_eq!(result["/new"][0].files[0].path, "file.txt");
    }

    #[test]
    fn rebuild_manifests_root_collision_merges_manifests() {
        let mut initial = HashMap::new();
        initial.insert("/a".to_string(), vec![make_manifest(&["one.txt"])]);
        initial.insert("/b".to_string(), vec![make_manifest(&["two.txt"])]);

        // Map both to the same root
        let mut mappings = HashMap::new();
        mappings.insert("/a".to_string(), "/merged".to_string());
        mappings.insert("/b".to_string(), "/merged".to_string());

        let result = rebuild_manifests(&initial, &mappings, &[]);

        assert_eq!(result.len(), 1);
        let all_paths: Vec<&str> = result["/merged"]
            .iter()
            .flat_map(|m| m.files.iter().map(|p| p.path.as_str()))
            .collect();
        assert!(all_paths.contains(&"one.txt"));
        assert!(all_paths.contains(&"two.txt"));
    }

    #[test]
    fn rebuild_manifests_applies_single_filter_group() {
        let mut initial = HashMap::new();
        initial.insert(
            "/root".to_string(),
            vec![make_manifest(&["render.exr", "log.txt"])],
        );

        let filters = vec![vec!["*.exr".to_string()]];

        let result = rebuild_manifests(&initial, &HashMap::new(), &filters);

        let paths: Vec<&str> = result["/root"]
            .iter()
            .flat_map(|m| m.files.iter().map(|p| p.path.as_str()))
            .collect();
        assert_eq!(paths, vec!["render.exr"]);
    }

    #[test]
    fn rebuild_manifests_multiple_filter_groups_applied_sequentially() {
        let mut initial = HashMap::new();
        initial.insert(
            "/root".to_string(),
            vec![make_manifest(&["a.exr", "b.exr", "c.txt"])],
        );

        // First filter: keep only .exr files (removes c.txt)
        // Second filter: keep only files starting with "a" (removes b.exr)
        let filters = vec![vec!["*.exr".to_string()], vec!["a*".to_string()]];

        let result = rebuild_manifests(&initial, &HashMap::new(), &filters);

        let paths: Vec<&str> = result
            .get("/root")
            .map(|ms| {
                ms.iter()
                    .flat_map(|m| m.files.iter().map(|p| p.path.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        assert_eq!(paths, vec!["a.exr"]);
    }

    #[test]
    fn rebuild_manifests_filter_removes_all_returns_empty() {
        let mut initial = HashMap::new();
        initial.insert("/root".to_string(), vec![make_manifest(&["file.txt"])]);

        let filters = vec![vec!["*.nonexistent".to_string()]];

        let result = rebuild_manifests(&initial, &HashMap::new(), &filters);

        assert!(result.is_empty());
    }

    #[test]
    fn rebuild_manifests_mapping_then_filter_uses_new_root_in_path() {
        let mut initial = HashMap::new();
        initial.insert(
            "/old".to_string(),
            vec![make_manifest(&["sub/file.exr", "sub/file.txt"])],
        );

        let mut mappings = HashMap::new();
        mappings.insert("/old".to_string(), "/new".to_string());

        // Filter matches against full path: /new/sub/file.exr
        let filters = vec![vec!["*.exr".to_string()]];

        let result = rebuild_manifests(&initial, &mappings, &filters);

        assert!(result.contains_key("/new"));
        let paths: Vec<&str> = result["/new"]
            .iter()
            .flat_map(|m| m.files.iter().map(|p| p.path.as_str()))
            .collect();
        assert_eq!(paths, vec!["sub/file.exr"]);
    }

    // --- Existing tests ---

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
