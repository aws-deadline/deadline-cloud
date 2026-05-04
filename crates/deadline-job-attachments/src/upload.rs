use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::errors::JobAttachmentsError;
use crate::models::PathFormat;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use deadline_config::ini::IniConfig;

use crate::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion, hash_data, hash_file,
};
use crate::caches::{
    HashCache, HashCacheEntry, S3CheckCache, S3CheckCacheEntry, format_mtime_for_cache,
};
use crate::models::{
    AssetRootGroup, AssetRootManifest, AssetUploadGroup, Attachments, FileSystemLocationType,
    JobAttachmentS3Settings, ManifestProperties, StorageProfile, join_s3_paths,
};
use crate::progress_tracker::{
    ProgressReportMetadata, ProgressStatus, ProgressTracker, SummaryStatistics,
};
use crate::s3::compute_upload_config;

fn is_relative_to(path: &Path, base: &str) -> bool {
    let base_path = Path::new(base);
    path.starts_with(base_path)
}

/// Normalizes a path to absolute without following symlinks, filtering empty
/// strings and paths relative to SHARED locations. Returns None if filtered.
fn resolve_and_filter(p: &str, shared_locations: &[&str]) -> Option<PathBuf> {
    if p.is_empty() {
        return None;
    }
    let abs_path = normalize_absolute(p);
    if shared_locations
        .iter()
        .any(|s| is_relative_to(&abs_path, s))
    {
        return None;
    }
    Some(abs_path)
}

fn normalize_absolute(p: &str) -> PathBuf {
    use std::path::Component;
    let path = Path::new(p);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    // Normalize: resolve `.` and `..` without following symlinks
    let mut components = Vec::new();
    for c in abs.components() {
        match c {
            Component::ParentDir => {
                components.pop();
            }
            Component::CurDir => {}
            _ => components.push(c),
        }
    }
    components.iter().collect()
}

pub fn prepare_paths_for_upload(
    input_paths: &[String],
    output_paths: &[String],
    referenced_paths: &[String],
    storage_profile: Option<&StorageProfile>,
    require_paths_exist: bool,
) -> Result<AssetUploadGroup, JobAttachmentsError> {
    let mut local_locations: Vec<(&str, &str)> = Vec::new(); // (path, name)
    let mut shared_locations: Vec<&str> = Vec::new();

    if let Some(profile) = storage_profile {
        for loc in &profile.file_system_locations {
            match loc.location_type {
                FileSystemLocationType::Local => {
                    local_locations.push((&loc.path, &loc.name));
                }
                FileSystemLocationType::Shared => {
                    shared_locations.push(&loc.path);
                }
            }
        }
    }

    let mut groupings: Vec<(String, AssetRootGroup)> = Vec::new(); // (key, group)
    let mut missing_inputs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut misconfigured_dirs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut extra_referenced: Vec<String> = Vec::new();

    // Process input paths
    for p in input_paths {
        if p.is_empty() {
            continue;
        }
        let abs_path = normalize_absolute(p);

        if !abs_path.exists() {
            if require_paths_exist {
                missing_inputs.insert(abs_path);
            } else {
                log::warn!(
                    "Input path '{}' resolving to '{}' does not exist. Adding to referenced paths.",
                    p,
                    abs_path.display()
                );
                extra_referenced.push(p.clone());
            }
            continue;
        }
        if abs_path.is_dir() {
            misconfigured_dirs.insert(abs_path);
            continue;
        }
        if shared_locations
            .iter()
            .any(|s| is_relative_to(&abs_path, s))
        {
            continue;
        }
        let key = find_group_key(&abs_path, &local_locations, &mut groupings);
        let group = get_group_mut(&key, &mut groupings);
        group.inputs.insert(abs_path);
    }

    if !missing_inputs.is_empty() || !misconfigured_dirs.is_empty() {
        use std::fmt::Write;
        let mut msg = "Job submission contains missing input files or directories specified as files. All inputs must exist and be classified properly.".to_owned();
        if !missing_inputs.is_empty() {
            let list: Vec<String> = missing_inputs
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            let _ = write!(msg, "\nMissing input files:\n\t{}", list.join("\n\t"));
        }
        if !misconfigured_dirs.is_empty() {
            let list: Vec<String> = misconfigured_dirs
                .iter()
                .map(|p| p.display().to_string())
                .collect();
            let _ = write!(
                msg,
                "\nDirectories classified as files:\n\t{}",
                list.join("\n\t")
            );
        }
        return Err(JobAttachmentsError::MisconfiguredInputs(msg));
    }

    // Process output paths
    for p in output_paths {
        if let Some(abs_path) = resolve_and_filter(p, &shared_locations) {
            let key = find_group_key(&abs_path, &local_locations, &mut groupings);
            get_group_mut(&key, &mut groupings).outputs.insert(abs_path);
        }
    }

    // Process referenced paths (including extras from missing inputs)
    let all_referenced = referenced_paths.iter().chain(extra_referenced.iter());
    for p in all_referenced {
        if let Some(abs_path) = resolve_and_filter(p, &shared_locations) {
            let key = find_group_key(&abs_path, &local_locations, &mut groupings);
            get_group_mut(&key, &mut groupings)
                .references
                .insert(abs_path);
        }
    }

    // Compute root_path for each group as common_path of all paths
    for (_, group) in &mut groupings {
        let all_paths: Vec<&PathBuf> = group
            .inputs
            .iter()
            .chain(group.outputs.iter())
            .chain(group.references.iter())
            .collect();
        if all_paths.is_empty() {
            continue;
        }
        let common = common_path(&all_paths);
        let root = if common.is_file() {
            common.parent().unwrap_or(&common).to_path_buf()
        } else {
            common
        };
        group.root_path = root.to_string_lossy().into_owned();
    }

    // Sort groups by (root_path, file_system_location_name)
    groupings.sort_by(|a, b| {
        (&a.1.root_path, &a.1.file_system_location_name)
            .cmp(&(&b.1.root_path, &b.1.file_system_location_name))
    });

    let asset_groups: Vec<AssetRootGroup> = groupings.into_iter().map(|(_, g)| g).collect();

    // Compute totals
    let mut total_input_files: u64 = 0;
    let mut total_input_bytes: u64 = 0;
    for group in &asset_groups {
        for input in &group.inputs {
            total_input_files += 1;
            total_input_bytes += std::fs::metadata(input).map(|m| m.len()).unwrap_or(0);
        }
    }

    Ok(AssetUploadGroup {
        asset_groups,
        total_input_files,
        total_input_bytes,
    })
}

fn find_group_key(
    abs_path: &Path,
    local_locations: &[(&str, &str)],
    groupings: &mut Vec<(String, AssetRootGroup)>,
) -> String {
    // Find most specific LOCAL location match
    let mut best_match: Option<(&str, &str)> = None;
    for &(loc_path, loc_name) in local_locations {
        if is_relative_to(abs_path, loc_path)
            && (best_match.is_none()
                || loc_path.len() > best_match.expect("checked is_none above").0.len())
        {
            best_match = Some((loc_path, loc_name));
        }
    }

    if let Some((loc_path, loc_name)) = best_match {
        // Ensure group exists for this local location
        if !groupings.iter().any(|(k, _)| k == loc_path) {
            groupings.push((
                loc_path.to_owned(),
                AssetRootGroup {
                    file_system_location_name: Some(loc_name.to_owned()),
                    root_path: String::new(),
                    inputs: BTreeSet::new(),
                    outputs: BTreeSet::new(),
                    references: BTreeSet::new(),
                },
            ));
        }
        loc_path.to_owned()
    } else {
        // Use top-level directory component as key
        let top = top_directory(abs_path);
        if !groupings.iter().any(|(k, _)| k.eq_ignore_ascii_case(&top)) {
            groupings.push((
                top.clone(),
                AssetRootGroup {
                    file_system_location_name: None,
                    root_path: String::new(),
                    inputs: BTreeSet::new(),
                    outputs: BTreeSet::new(),
                    references: BTreeSet::new(),
                },
            ));
        }
        top
    }
}

fn get_group_mut<'a>(
    key: &str,
    groupings: &'a mut [(String, AssetRootGroup)],
) -> &'a mut AssetRootGroup {
    let idx = groupings
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case(key))
        .expect("group must exist");
    &mut groupings[idx].1
}

fn top_directory(path: &Path) -> String {
    path.components()
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn common_path(paths: &[&PathBuf]) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    }
    let first = paths[0];
    let mut prefix: Vec<_> = first.components().collect();
    for p in &paths[1..] {
        let comps: Vec<_> = p.components().collect();
        let len = prefix.len().min(comps.len());
        let mut common_len = 0;
        for i in 0..len {
            if prefix[i] == comps[i] {
                common_len = i + 1;
            } else {
                break;
            }
        }
        prefix.truncate(common_len);
    }
    prefix.iter().collect()
}

/// Hashes a file using the cache. Returns the hash and sets `was_cached` if the
/// cached entry was reused without re-hashing.
fn hash_with_cache(
    cache: &HashCache,
    full_path: &str,
    file_path: &Path,
    hash_alg: HashAlgorithm,
    mtime_str: &str,
    was_cached: &mut bool,
) -> Result<String, JobAttachmentsError> {
    if let Some(entry) = cache.get_entry(full_path, hash_alg, 0, -1)
        && entry.last_modified_time == mtime_str
    {
        *was_cached = true;
        return Ok(entry.file_hash);
    }
    let h = hash_file(file_path, hash_alg)?;
    cache.put_entry(&HashCacheEntry {
        file_path: full_path.to_owned(),
        hash_algorithm: hash_alg,
        file_hash: h.clone(),
        last_modified_time: mtime_str.to_owned(),
        range_start: 0,
        range_end: -1,
    });
    Ok(h)
}

pub fn hash_assets_and_create_manifest(
    asset_groups: &[AssetRootGroup],
    total_input_files: u64,
    total_input_bytes: u64,
    hash_cache_dir: Option<&str>,
    on_preparing_to_submit: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(SummaryStatistics, Vec<AssetRootManifest>), JobAttachmentsError> {
    let start = std::time::Instant::now();

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::PreparingInProgress,
        total_input_files,
        total_input_bytes,
        on_preparing_to_submit,
    );

    let cache_dir = hash_cache_dir
        .map(ToOwned::to_owned)
        .or_else(crate::caches::default_cache_dir);

    let mut asset_root_manifests = Vec::new();

    for group in asset_groups {
        let asset_manifest = if group.inputs.is_empty() {
            None
        } else {
            let cache = cache_dir.as_deref().map(HashCache::new).transpose()?;

            let mut paths = Vec::new();
            let sorted_inputs: Vec<_> = group.inputs.iter().cloned().collect();

            for input_path in &sorted_inputs {
                // Check cancellation
                if !progress_tracker.continue_reporting() {
                    return Err(JobAttachmentsError::Cancelled {
                        message: "File hashing cancelled.".into(),
                    });
                }

                let full_path = input_path.to_string_lossy().into_owned();
                let meta = std::fs::metadata(input_path).map_err(|e| {
                    JobAttachmentsError::AssetSync(format!(
                        "Failed to stat {}: {e}",
                        input_path.display()
                    ))
                })?;
                let file_size = meta.len();

                #[cfg(unix)]
                let (mtime_secs, mtime_nsec) = {
                    use std::os::unix::fs::MetadataExt;
                    (meta.mtime(), meta.mtime_nsec())
                };
                #[cfg(not(unix))]
                let (mtime_secs, mtime_nsec) = {
                    let dur = meta
                        .modified()
                        .unwrap_or(std::time::UNIX_EPOCH)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    (dur.as_secs() as i64, dur.subsec_nanos() as i64)
                };

                let mtime_ns = mtime_secs * 1_000_000_000 + mtime_nsec;
                let mtime_us = mtime_ns / 1000; // truncate to microseconds
                let mtime_str = format_mtime_for_cache(mtime_secs, mtime_nsec);

                let hash_alg = HashAlgorithm::Xxh128;
                let mut was_cached = false;

                let file_hash = match cache {
                    Some(ref cache) => hash_with_cache(
                        cache,
                        &full_path,
                        input_path,
                        hash_alg,
                        &mtime_str,
                        &mut was_cached,
                    )?,
                    None => hash_file(input_path, hash_alg)?,
                };

                // Relative POSIX path
                let root = Path::new(&group.root_path);
                let rel_path = input_path
                    .strip_prefix(root)
                    .unwrap_or(input_path)
                    .to_string_lossy()
                    .replace('\\', "/");

                paths.push(ManifestPath {
                    path: rel_path,
                    hash: file_hash,
                    size: file_size,
                    mtime: mtime_us,
                });

                if was_cached {
                    progress_tracker.increase_skipped(1, file_size);
                } else {
                    progress_tracker.increase_processed(1, file_size);
                }
                if !progress_tracker.report_progress() {
                    return Err(JobAttachmentsError::Cancelled {
                        message: "File hashing cancelled.".into(),
                    });
                }
            }

            let total_size: u64 = paths.iter().map(|p| p.size).sum();
            Some(AssetManifest::new(
                HashAlgorithm::Xxh128,
                ManifestVersion::V2023_03_03,
                total_size,
                paths,
            )?)
        };

        asset_root_manifests.push(AssetRootManifest {
            file_system_location_name: group.file_system_location_name.clone(),
            root_path: group.root_path.clone(),
            asset_manifest,
            outputs: group.outputs.iter().cloned().collect(),
        });
    }

    let elapsed = start.elapsed().as_secs_f64();
    progress_tracker.set_total_time(elapsed);

    Ok((
        progress_tracker.get_summary_statistics(),
        asset_root_manifests,
    ))
}

// =====================================================================
// S3 upload context and orchestration (batch 9b)
// =====================================================================

/// Build an S3 upload error with HTTP status-specific guidance.
/// Shared by `upload_file_to_s3` and `upload_bytes_to_s3`.
fn s3_upload_error(
    status_code: u16,
    raw: &str,
    action: &str,
    bucket: &str,
    key: &str,
) -> JobAttachmentsError {
    match status_code {
        403 => {
            let guidance = if raw.contains("kms:") {
                "Forbidden or Access denied. Please check your AWS credentials and Job Attachments S3 bucket \
                 encryption settings. If a customer-managed KMS key is set, confirm that your AWS IAM Role or \
                 User has the 'kms:GenerateDataKey' and 'kms:DescribeKey' permissions for the key used to encrypt the bucket."
            } else {
                "Forbidden or Access denied. Please check your AWS credentials, and ensure that \
                 your AWS IAM Role or User has the 's3:PutObject' permission for this bucket. "
            };
            JobAttachmentsError::S3Client {
                action: action.into(),
                status_code: 403,
                bucket: bucket.into(),
                key: key.into(),
                message: Some(format!("{guidance} {raw}")),
            }
        }
        404 => JobAttachmentsError::S3Client {
            action: action.into(),
            status_code: 404,
            bucket: bucket.into(),
            key: key.into(),
            message: Some(format!(
                "Not found. Please check your bucket name and object key, \
                 and ensure that they exist in the AWS account. {raw}"
            )),
        },
        408 => JobAttachmentsError::S3Client {
            action: action.into(),
            status_code: 408,
            bucket: bucket.into(),
            key: key.into(),
            message: Some(format!(
                "Request timeout. Please consider retrying later, or ensure \
                 your network connection is stable. {raw}"
            )),
        },
        500 => JobAttachmentsError::S3Client {
            action: action.into(),
            status_code: 500,
            bucket: bucket.into(),
            key: key.into(),
            message: Some(format!(
                "Internal server error. It might be an issue on AWS's side; \
                 please consider retrying later or contacting AWS support. {raw}"
            )),
        },
        503 => JobAttachmentsError::S3Client {
            action: action.into(),
            status_code: 503,
            bucket: bucket.into(),
            key: key.into(),
            message: Some(format!(
                "Service unavailable. AWS S3 might be down or experiencing \
                 high traffic. Please consider retrying after some time. {raw}"
            )),
        },
        _ => JobAttachmentsError::S3BotoCore {
            action: action.into(),
            details: raw.to_owned(),
        },
    }
}

/// Context for performing S3 uploads: holds a configured S3 client,
/// the caller's account ID (for `ExpectedBucketOwner`), and computed
/// config values (file size threshold, worker count).
pub struct S3UploadContext {
    s3_client: aws_sdk_s3::Client,
    account_id: String,
    small_file_threshold: usize,
    num_upload_workers: usize,
}

impl S3UploadContext {
    /// Build from a pre-configured S3 client, account ID, and optional config.
    /// Reads `small_file_threshold_multiplier` and `s3_max_pool_connections`
    /// from config to compute thresholds. Falls back to defaults if config is None.
    pub fn new(
        s3_client: aws_sdk_s3::Client,
        account_id: String,
        config: Option<&IniConfig>,
    ) -> Result<Self, JobAttachmentsError> {
        let (small_file_threshold, num_upload_workers) = compute_upload_config(config)?;
        Ok(Self {
            s3_client,
            account_id,
            small_file_threshold,
            num_upload_workers,
        })
    }

    /// Check whether an object already exists in S3 via `HeadObject`.
    pub async fn file_already_uploaded(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<bool, JobAttachmentsError> {
        match self
            .s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(sdk_err) => {
                // Extract HTTP status from the raw response if available
                let status = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());

                if status == 404 {
                    return Ok(false);
                }
                if status == 403 {
                    return Err(JobAttachmentsError::S3Client {
                        action: "checking if object exists".into(),
                        status_code: 403,
                        bucket: bucket.into(),
                        key: key.into(),
                        message: Some(format!(
                            "Access denied. Ensure that the bucket is in the account {}, \
                             and your AWS IAM Role or User has the 's3:ListBucket' permission for this bucket.",
                            self.account_id
                        )),
                    });
                }
                // Check if it's a "not found" via the service error type
                let service_err = sdk_err.into_service_error();
                if service_err.is_not_found() {
                    return Ok(false);
                }
                Err(JobAttachmentsError::S3BotoCore {
                    action: "checking for the existence of an object in the S3 bucket".into(),
                    details: format!("{service_err}"),
                })
            }
        }
    }

    /// Upload a single file to S3. Silently skips directories, non-existent
    /// files, and symlinks. Files larger than `small_file_threshold` use
    /// multipart upload; smaller files use single `PutObject`.
    pub async fn upload_file_to_s3(
        &self,
        local_path: &Path,
        s3_bucket: &str,
        s3_upload_key: &str,
        progress_tracker: Option<&ProgressTracker>,
    ) -> Result<(), JobAttachmentsError> {
        // Skip non-existent
        if !local_path.exists() {
            return Ok(());
        }
        // Skip directories
        if local_path.is_dir() {
            return Ok(());
        }
        // Reject symlinks
        match std::fs::symlink_metadata(local_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                log::warn!("Skipping symlink: {}", local_path.display());
                return Ok(());
            }
            Err(e) => {
                log::warn!("Failed to stat {}. Skipping: {e}", local_path.display());
                return Ok(());
            }
            Ok(meta) => {
                if meta.len() as usize > self.small_file_threshold {
                    return self
                        .multipart_upload_file(
                            local_path,
                            s3_bucket,
                            s3_upload_key,
                            progress_tracker,
                        )
                        .await;
                }
            }
        }

        let body = ByteStream::from_path(local_path).await.map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read {}: {e}", local_path.display()))
        })?;

        let result = self
            .s3_client
            .put_object()
            .bucket(s3_bucket)
            .key(s3_upload_key)
            .expected_bucket_owner(&self.account_id)
            .body(body)
            .send()
            .await;

        match result {
            Ok(_) => {
                if let Some(tracker) = progress_tracker {
                    tracker.increase_processed(1, 0);
                }
                Ok(())
            }
            Err(sdk_err) => {
                let status_code = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
                let service_err = sdk_err.into_service_error();
                let raw = format!("{service_err}");
                let msg = service_err.message().unwrap_or_default();
                let full_text = format!("{raw} {msg}");

                Err(s3_upload_error(
                    status_code,
                    &full_text,
                    "uploading file",
                    s3_bucket,
                    s3_upload_key,
                ))
            }
        }
    }

    /// Upload a large file using S3 multipart upload.
    /// Chunks the file into `S3_MULTIPART_UPLOAD_CHUNK_SIZE` parts,
    /// uploads parts concurrently, then completes. Aborts on error or cancellation.
    async fn multipart_upload_file(
        &self,
        local_path: &Path,
        s3_bucket: &str,
        s3_upload_key: &str,
        progress_tracker: Option<&ProgressTracker>,
    ) -> Result<(), JobAttachmentsError> {
        use crate::s3::S3_MULTIPART_UPLOAD_CHUNK_SIZE;
        use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
        use futures::stream::{self, StreamExt, TryStreamExt};

        // Initiate multipart upload
        let create_resp = self
            .s3_client
            .create_multipart_upload()
            .bucket(s3_bucket)
            .key(s3_upload_key)
            .expected_bucket_owner(&self.account_id)
            .send()
            .await
            .map_err(|sdk_err| {
                let status_code = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
                let service_err = sdk_err.into_service_error();
                let msg = service_err.message().unwrap_or_default();
                s3_upload_error(
                    status_code,
                    &format!("{service_err} {msg}"),
                    "initiating multipart upload",
                    s3_bucket,
                    s3_upload_key,
                )
            })?;

        let upload_id = create_resp.upload_id().unwrap_or_default().to_owned();

        // Read file and split into chunks
        let file_bytes = std::fs::read(local_path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read {}: {e}", local_path.display()))
        })?;

        let chunks: Vec<(usize, &[u8])> = file_bytes
            .chunks(S3_MULTIPART_UPLOAD_CHUNK_SIZE)
            .enumerate()
            .collect();

        // Upload parts concurrently using buffer_unordered
        let part_results: Result<Vec<CompletedPart>, JobAttachmentsError> =
            stream::iter(chunks.into_iter().map(|(idx, chunk)| {
                let part_number = (idx + 1) as i32;
                let body = ByteStream::from(chunk.to_vec());
                let client = &self.s3_client;
                let account = &self.account_id;
                let uid = &upload_id;
                async move {
                    let resp = client
                        .upload_part()
                        .bucket(s3_bucket)
                        .key(s3_upload_key)
                        .upload_id(uid)
                        .part_number(part_number)
                        .expected_bucket_owner(account)
                        .body(body)
                        .send()
                        .await
                        .map_err(|sdk_err| {
                            let status_code =
                                sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
                            let service_err = sdk_err.into_service_error();
                            let msg = service_err.message().unwrap_or_default();
                            s3_upload_error(
                                status_code,
                                &format!("{service_err} {msg}"),
                                "uploading part",
                                s3_bucket,
                                s3_upload_key,
                            )
                        })?;
                    Ok(CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(resp.e_tag().unwrap_or_default())
                        .build())
                }
            }))
            .buffer_unordered(crate::s3::S3_UPLOAD_MAX_CONCURRENCY)
            .try_collect()
            .await;

        match part_results {
            Ok(mut parts) => {
                // Parts must be sorted by part number for CompleteMultipartUpload
                parts.sort_by_key(CompletedPart::part_number);

                self.s3_client
                    .complete_multipart_upload()
                    .bucket(s3_bucket)
                    .key(s3_upload_key)
                    .upload_id(&upload_id)
                    .expected_bucket_owner(&self.account_id)
                    .multipart_upload(
                        CompletedMultipartUpload::builder()
                            .set_parts(Some(parts))
                            .build(),
                    )
                    .send()
                    .await
                    .map_err(|sdk_err| {
                        let status_code = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
                        let service_err = sdk_err.into_service_error();
                        let msg = service_err.message().unwrap_or_default();
                        s3_upload_error(
                            status_code,
                            &format!("{service_err} {msg}"),
                            "completing multipart upload",
                            s3_bucket,
                            s3_upload_key,
                        )
                    })?;

                if let Some(tracker) = progress_tracker {
                    tracker.increase_processed(1, 0);
                }
                Ok(())
            }
            Err(e) => {
                // Abort the multipart upload on failure
                let _ = self
                    .s3_client
                    .abort_multipart_upload()
                    .bucket(s3_bucket)
                    .key(s3_upload_key)
                    .upload_id(&upload_id)
                    .expected_bucket_owner(&self.account_id)
                    .send()
                    .await;
                Err(e)
            }
        }
    }

    /// Upload raw bytes to S3 (used for manifest files). Includes `ExpectedBucketOwner`.
    pub async fn upload_bytes_to_s3(
        &self,
        bytes: &[u8],
        bucket: &str,
        key: &str,
        metadata: Option<std::collections::HashMap<String, String>>,
    ) -> Result<(), JobAttachmentsError> {
        let body = ByteStream::from(bytes.to_vec());
        let mut req = self
            .s3_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .expected_bucket_owner(&self.account_id)
            .body(body);

        if let Some(meta) = metadata {
            for (k, v) in meta {
                req = req.metadata(k, v);
            }
        }

        req.send().await.map_err(|sdk_err| {
            let status_code = sdk_err.raw_response().map_or(0, |r| r.status().as_u16());
            let service_err = sdk_err.into_service_error();
            let raw = format!("{service_err}");
            let msg = service_err.message().unwrap_or_default();
            let full_text = format!("{raw} {msg}");
            s3_upload_error(
                status_code,
                &full_text,
                "uploading binary file",
                bucket,
                key,
            )
        })?;
        Ok(())
    }

    /// Verify S3 check cache integrity by sampling up to 30 cached entries
    /// and confirming they exist in S3. Returns false if any are missing.
    pub async fn verify_hash_cache_integrity(
        &self,
        s3_check_cache_dir: Option<&str>,
        manifest: &AssetManifest,
        s3_cas_prefix: &str,
        s3_bucket: &str,
    ) -> bool {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let cache_dir = s3_check_cache_dir
            .map(ToOwned::to_owned)
            .or_else(crate::caches::default_cache_dir);
        let Some(Ok(cache)) = cache_dir.as_deref().map(S3CheckCache::new) else {
            return true; // No cache → nothing to verify
        };

        // Build S3 keys for all manifest files, shuffle, sample up to 30
        let mut s3_keys: Vec<String> = manifest
            .paths
            .iter()
            .map(|f| format!("{}/{}.xxh128", s3_cas_prefix, f.hash))
            .collect();

        // Deterministic-ish shuffle using hash of first key + time
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for i in (1..s3_keys.len()).rev() {
            let mut h = DefaultHasher::new();
            seed.hash(&mut h);
            i.hash(&mut h);
            let j = (h.finish() as usize) % (i + 1);
            s3_keys.swap(i, j);
        }

        let mut sampled = Vec::new();
        for key in &s3_keys {
            let cache_key = format!("{s3_bucket}/{key}");
            if cache.get_entry(&cache_key).is_some() {
                sampled.push(key.clone());
                if sampled.len() >= 30 {
                    break;
                }
            }
        }

        for key in &sampled {
            match self.file_already_uploaded(s3_bucket, key).await {
                Ok(true) => {}
                _ => return false,
            }
        }
        true
    }

    /// Reset the S3 check cache by removing the database file.
    pub fn reset_s3_check_cache(&self, s3_check_cache_dir: Option<&str>) {
        let cache_dir = s3_check_cache_dir
            .map(ToOwned::to_owned)
            .or_else(crate::caches::default_cache_dir);
        if let Some(dir) = cache_dir {
            let db_path = Path::new(&dir).join("s3_check_cache.db");
            if db_path.exists() {
                log::debug!("Deleting s3_check_cache.db due to integrity mismatch");
                let _ = std::fs::remove_file(db_path);
            }
        }
    }

    /// Upload all files from a manifest to S3 CAS. Small files in parallel,
    /// large files serially. Uses S3 check cache to skip already-uploaded files.
    pub async fn upload_input_files(
        &self,
        manifest: &AssetManifest,
        s3_bucket: &str,
        source_root: &Path,
        s3_cas_prefix: &str,
        progress_tracker: Option<&ProgressTracker>,
        s3_check_cache_dir: Option<&str>,
        force_s3_check: Option<bool>,
    ) -> Result<(), JobAttachmentsError> {
        use futures::stream::{self, StreamExt, TryStreamExt};

        let cache_dir = s3_check_cache_dir
            .map(ToOwned::to_owned)
            .or_else(crate::caches::default_cache_dir);
        let cache = cache_dir.as_deref().map(S3CheckCache::new).transpose()?;

        let force = force_s3_check.unwrap_or(false);

        // Separate files into small and large queues (matching Python's
        // _separate_files_by_size). Small files upload in parallel; large
        // files upload serially with internal multipart parallelism.
        let mut small_files = Vec::new();
        let mut large_files = Vec::new();
        for file in &manifest.paths {
            if (file.size as usize) > self.small_file_threshold {
                large_files.push(file);
            } else {
                small_files.push(file);
            }
        }

        // Upload small files in parallel
        stream::iter(small_files.into_iter().map(|file| {
            let cache = &cache;
            async move {
                self.upload_one_file(
                    file,
                    s3_bucket,
                    source_root,
                    s3_cas_prefix,
                    progress_tracker,
                    cache,
                    force,
                )
                .await
            }
        }))
        .buffer_unordered(self.num_upload_workers)
        .try_collect::<Vec<()>>()
        .await?;

        // Upload large files serially
        for file in large_files {
            self.upload_one_file(
                file,
                s3_bucket,
                source_root,
                s3_cas_prefix,
                progress_tracker,
                &cache,
                force,
            )
            .await?;
        }

        // Final progress report + cancellation check
        if let Some(tracker) = progress_tracker {
            tracker.report_progress();
            if !tracker.continue_reporting() {
                return Err(JobAttachmentsError::Cancelled {
                    message: "File upload cancelled.".into(),
                });
            }
        }

        Ok(())
    }

    /// Upload a single file: check cache, check S3, upload if needed, update cache.
    // Changing &Option<T> → Option<&T> would require restructuring callers
    // that hold the Option in a variable and pass a reference to it.
    #[allow(
        clippy::ref_option,
        reason = "callers pass &Option from local bindings"
    )]
    async fn upload_one_file(
        &self,
        file: &ManifestPath,
        s3_bucket: &str,
        source_root: &Path,
        s3_cas_prefix: &str,
        progress_tracker: Option<&ProgressTracker>,
        cache: &Option<S3CheckCache>,
        force: bool,
    ) -> Result<(), JobAttachmentsError> {
        let local_path = source_root.join(&file.path);
        let s3_key = format!("{}/{}.{}", s3_cas_prefix, file.hash, "xxh128");
        let cache_key = format!("{s3_bucket}/{s3_key}");

        // Check cache unless force
        if !force
            && let Some(c) = cache
            && c.get_entry(&cache_key).is_some()
        {
            if let Some(tracker) = progress_tracker {
                tracker.increase_skipped(1, file.size);
            }
            return Ok(());
        }

        // HeadObject check
        if self.file_already_uploaded(s3_bucket, &s3_key).await? {
            if let Some(c) = cache {
                c.put_entry(&S3CheckCacheEntry {
                    s3_key: cache_key,
                    last_seen_time: current_timestamp(),
                });
            }
            if let Some(tracker) = progress_tracker {
                tracker.increase_skipped(1, file.size);
            }
            return Ok(());
        }

        // Upload
        self.upload_file_to_s3(&local_path, s3_bucket, &s3_key, progress_tracker)
            .await?;

        // Update cache
        if let Some(c) = cache {
            c.put_entry(&S3CheckCacheEntry {
                s3_key: cache_key,
                last_seen_time: current_timestamp(),
            });
        }

        Ok(())
    }
}

fn current_timestamp() -> String {
    format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64()
    )
}

/// Orchestrate uploading all manifests to S3. Builds `ManifestProperties`
/// and Attachments from the results.
pub async fn upload_assets(
    farm_id: &str,
    queue_id: &str,
    job_attachment_settings: &JobAttachmentS3Settings,
    manifests: &[AssetRootManifest],
    ctx: &S3UploadContext,
    on_uploading_assets: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    s3_check_cache_dir: Option<&str>,
    force_s3_check: Option<bool>,
) -> Result<(SummaryStatistics, Attachments), JobAttachmentsError> {
    if farm_id.is_empty() || queue_id.is_empty() {
        return Err(JobAttachmentsError::AssetSync(
            "upload_assets: Farm or Fleet ID is missing.".into(),
        ));
    }

    // Compute totals from manifests that have files
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    for m in manifests {
        if let Some(ref am) = m.asset_manifest {
            total_files += am.paths.len() as u64;
            total_bytes += am.paths.iter().map(|p| p.size).sum::<u64>();
        }
    }

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::UploadInProgress,
        total_files,
        total_bytes,
        on_uploading_assets,
    );

    let start = std::time::Instant::now();
    let mut manifest_properties_list = Vec::new();

    for arm in manifests {
        let output_rel_paths: Vec<String> = arm
            .outputs
            .iter()
            .filter_map(|p| {
                p.strip_prefix(&arm.root_path)
                    .ok()
                    .map(|r| r.to_string_lossy().into_owned())
            })
            .collect();

        let mut props = ManifestProperties {
            root_path: arm.root_path.clone(),
            root_path_format: PathFormat::host(),
            file_system_location_name: arm.file_system_location_name.clone(),
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: if output_rel_paths.is_empty() {
                None
            } else {
                Some(output_rel_paths)
            },
        };

        if let Some(ref manifest) = arm.asset_manifest {
            let partial_prefix = job_attachment_settings.partial_manifest_prefix(farm_id, queue_id);
            let cas_prefix = job_attachment_settings.full_cas_prefix()?;

            // Upload manifest bytes first (Python order: manifest, then files)
            let hash_alg = HashAlgorithm::Xxh128;
            let manifest_bytes = manifest.encode().into_bytes();
            let manifest_name_prefix = hash_data(arm.root_path.as_bytes(), hash_alg);
            let manifest_name = format!("{manifest_name_prefix}_input");
            let partial_key = join_s3_paths(&[&partial_prefix, &manifest_name]);
            let full_key =
                job_attachment_settings.add_root_and_manifest_folder_prefix(&partial_key)?;

            ctx.upload_bytes_to_s3(
                &manifest_bytes,
                &job_attachment_settings.s3_bucket_name,
                &full_key,
                None,
            )
            .await?;

            // Verify S3 check cache integrity before uploading files.
            // Skip when force_s3_check is True — we'll HEAD every file anyway.
            if force_s3_check != Some(true)
                && !ctx
                    .verify_hash_cache_integrity(
                        s3_check_cache_dir,
                        manifest,
                        &cas_prefix,
                        &job_attachment_settings.s3_bucket_name,
                    )
                    .await
            {
                ctx.reset_s3_check_cache(s3_check_cache_dir);
            }

            // Upload input files
            ctx.upload_input_files(
                manifest,
                &job_attachment_settings.s3_bucket_name,
                Path::new(&arm.root_path),
                &cas_prefix,
                Some(&progress_tracker),
                s3_check_cache_dir,
                force_s3_check,
            )
            .await?;

            props.input_manifest_path = Some(partial_key);
            props.input_manifest_hash = Some(hash_data(&manifest_bytes, hash_alg));
        }

        manifest_properties_list.push(props);
    }

    progress_tracker.set_total_time(start.elapsed().as_secs_f64());

    Ok((
        progress_tracker.get_summary_statistics(),
        Attachments {
            manifests: manifest_properties_list,
            ..Default::default()
        },
    ))
}

/// Snapshot assets to a local directory instead of S3.
/// Copies files to `snapshot_dir/Data/` and manifests to `snapshot_dir/Manifests/`.
pub fn snapshot_assets(
    farm_id: &str,
    queue_id: &str,
    job_attachment_settings: &JobAttachmentS3Settings,
    snapshot_dir: &Path,
    manifests: &[AssetRootManifest],
    on_snapshotting_assets: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(SummaryStatistics, Attachments), JobAttachmentsError> {
    if farm_id.is_empty() || queue_id.is_empty() {
        return Err(JobAttachmentsError::AssetSync(
            "snapshot_assets: Farm or Fleet ID is missing.".into(),
        ));
    }

    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    for m in manifests {
        if let Some(ref am) = m.asset_manifest {
            total_files += am.paths.len() as u64;
            total_bytes += am.paths.iter().map(|p| p.size).sum::<u64>();
        }
    }

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::SnapshotInProgress,
        total_files,
        total_bytes,
        on_snapshotting_assets,
    );

    let start = std::time::Instant::now();
    let data_dir = snapshot_dir.join("Data");
    std::fs::create_dir_all(&data_dir).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to create snapshot Data dir: {e}"))
    })?;

    let mut manifest_properties_list = Vec::new();

    for arm in manifests {
        let output_rel_paths: Vec<String> = arm
            .outputs
            .iter()
            .filter_map(|p| {
                p.strip_prefix(&arm.root_path)
                    .ok()
                    .map(|r| r.to_string_lossy().into_owned())
            })
            .collect();

        let mut props = ManifestProperties {
            root_path: arm.root_path.clone(),
            root_path_format: PathFormat::host(),
            file_system_location_name: arm.file_system_location_name.clone(),
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: if output_rel_paths.is_empty() {
                None
            } else {
                Some(output_rel_paths)
            },
        };

        if let Some(ref manifest) = arm.asset_manifest {
            let partial_prefix = job_attachment_settings.partial_manifest_prefix(farm_id, queue_id);

            // Copy files to Data/
            for file in &manifest.paths {
                let src = Path::new(&arm.root_path).join(&file.path);
                let dest_name = format!("{}.xxh128", file.hash);
                let dest = data_dir.join(&dest_name);
                std::fs::copy(&src, &dest).map_err(|e| {
                    JobAttachmentsError::AssetSync(format!(
                        "Failed to copy {} to {}: {e}",
                        src.display(),
                        dest.display()
                    ))
                })?;

                progress_tracker.track_progress(file.size, true);
                if !progress_tracker.continue_reporting() {
                    return Err(JobAttachmentsError::Cancelled {
                        message: "File snapshot cancelled.".into(),
                    });
                }
            }

            // Write manifest
            let hash_alg = HashAlgorithm::Xxh128;
            let manifest_bytes = manifest.encode().into_bytes();
            let manifest_name_prefix = hash_data(arm.root_path.as_bytes(), hash_alg);
            let manifest_name = format!("{manifest_name_prefix}_input");
            let partial_key = join_s3_paths(&[&partial_prefix, &manifest_name]);

            let manifest_path = snapshot_dir.join("Manifests").join(&partial_key);
            if let Some(parent) = manifest_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    JobAttachmentsError::AssetSync(format!("Failed to create manifest dir: {e}"))
                })?;
            }
            std::fs::write(&manifest_path, &manifest_bytes).map_err(|e| {
                JobAttachmentsError::AssetSync(format!("Failed to write manifest: {e}"))
            })?;

            props.input_manifest_path = Some(partial_key);
            props.input_manifest_hash = Some(hash_data(&manifest_bytes, hash_alg));
        }

        manifest_properties_list.push(props);
    }

    progress_tracker.set_total_time(start.elapsed().as_secs_f64());

    Ok((
        progress_tracker.get_summary_statistics(),
        Attachments {
            manifests: manifest_properties_list,
            ..Default::default()
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AssetRootGroup, FileSystemLocation, FileSystemLocationType, StorageProfile,
    };
    use crate::progress_tracker::ProgressReportMetadata;
    use std::fs;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tempfile::TempDir;

    fn create_test_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
        path
    }

    // === Happy path — input files, output dirs, referenced paths ===

    #[test]
    fn prepare_paths_groups_inputs_outputs_references() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"hello");
        let f2 = create_test_file(&dir, "b.txt", b"world");
        let out_dir = dir.path().join("output");
        fs::create_dir_all(&out_dir).unwrap();

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            &[out_dir.to_string_lossy().into()],
            &[f1.to_string_lossy().into()],
            None,
            false,
        )
        .unwrap();

        assert!(result.total_input_files > 0);
        assert!(result.total_input_bytes > 0);
        assert!(!result.asset_groups.is_empty());
    }

    // === Input path relative to SHARED location is excluded ===

    #[test]
    fn prepare_paths_shared_location_input_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();
        let f1 = create_test_file(&dir, "shared/a.txt", b"data");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into()],
            &[],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert_eq!(result.total_input_files, 0);
    }

    // === Input path relative to LOCAL location grouped under that root ===

    #[test]
    fn prepare_paths_local_location_groups_under_root() {
        let dir = TempDir::new().unwrap();
        let local_dir = dir.path().join("local");
        fs::create_dir_all(&local_dir).unwrap();
        let f1 = create_test_file(&dir, "local/a.txt", b"data");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "LocalLoc".into(),
                path: local_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Local,
            }],
        };

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into()],
            &[],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert_eq!(result.asset_groups.len(), 1);
        assert_eq!(
            result.asset_groups[0].file_system_location_name.as_deref(),
            Some("LocalLoc")
        );
    }

    // === Multiple input files share common parent ===

    #[test]
    fn prepare_paths_common_parent_grouped_together() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "sub/a.txt", b"aaa");
        let f2 = create_test_file(&dir, "sub/b.txt", b"bbb");

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            &[],
            &[],
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.asset_groups.len(), 1);
        let root = &result.asset_groups[0].root_path;
        assert!(
            root.contains("sub"),
            "root should contain 'sub' directory, got: {root}"
        );
    }

    // === Non-existent input with require_paths_exist=true errors ===

    #[test]
    fn prepare_paths_missing_input_require_exist_errors() {
        let result =
            prepare_paths_for_upload(&["/nonexistent/file.txt".into()], &[], &[], None, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Missing input") || err.contains("missing") || err.contains("not exist"),
            "expected missing file error, got: {err}"
        );
    }

    // === Non-existent input with require_paths_exist=false moves to referenced ===

    #[test]
    fn prepare_paths_missing_input_no_require_moves_to_referenced() {
        let dir = TempDir::new().unwrap();
        let existing = create_test_file(&dir, "exists.txt", b"data");
        let missing = dir.path().join("missing.txt");

        let result = prepare_paths_for_upload(
            &[
                existing.to_string_lossy().into(),
                missing.to_string_lossy().into(),
            ],
            &[],
            &[],
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.total_input_files, 1);
    }

    // === Directory classified as input file errors ===

    #[test]
    fn prepare_paths_directory_as_input_file_errors() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("subdir");
        fs::create_dir_all(&sub).unwrap();

        let result =
            prepare_paths_for_upload(&[sub.to_string_lossy().into()], &[], &[], None, true);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("irectories classified as files") || err.contains("irectory"),
            "expected directory error, got: {err}"
        );
    }

    // === Empty inputs/outputs/references returns empty group ===

    #[test]
    fn prepare_paths_all_empty_returns_empty() {
        let result = prepare_paths_for_upload(&[], &[], &[], None, false).unwrap();
        assert!(result.asset_groups.is_empty());
        assert_eq!(result.total_input_files, 0);
        assert_eq!(result.total_input_bytes, 0);
    }

    // === Empty strings in input paths are filtered out ===

    #[test]
    fn prepare_paths_empty_strings_filtered() {
        let result =
            prepare_paths_for_upload(&[String::new(), String::new()], &[], &[], None, false)
                .unwrap();
        assert!(result.asset_groups.is_empty());
        assert_eq!(result.total_input_files, 0);
    }

    // === Output path relative to SHARED location excluded ===

    #[test]
    fn prepare_paths_shared_location_output_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[],
            &[shared_dir.to_string_lossy().into()],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert!(result.asset_groups.is_empty());
    }

    // === Referenced path relative to SHARED location excluded ===

    #[test]
    fn prepare_paths_shared_location_reference_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();
        let ref_path = dir.path().join("shared/ref.txt");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[],
            &[],
            &[ref_path.to_string_lossy().into()],
            Some(&profile),
            false,
        )
        .unwrap();

        assert!(result.asset_groups.is_empty());
    }

    // === hash_assets_and_create_manifest single group with inputs ===

    #[test]
    fn hash_assets_creates_manifest_for_single_group() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"hello");
        let f2 = create_test_file(&dir, "b.txt", b"world");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1, f2].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (stats, manifests) = hash_assets_and_create_manifest(
            &[group],
            2,
            10,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_some());
        assert_eq!(stats.processed_files + stats.skipped_files, 2);
    }

    // === Group with outputs but no inputs has None manifest ===

    #[test]
    fn hash_assets_output_only_group_has_no_manifest() {
        let dir = TempDir::new().unwrap();
        let out_dir = dir.path().join("output");
        fs::create_dir_all(&out_dir).unwrap();

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: Default::default(),
            outputs: [out_dir].into_iter().collect(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_stats, manifests) = hash_assets_and_create_manifest(
            &[group],
            0,
            0,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_none());
    }

    // === Multiple groups produce multiple manifests ===

    #[test]
    fn hash_assets_multiple_groups_multiple_manifests() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let f1 = create_test_file(&dir1, "a.txt", b"aaa");
        let f2 = create_test_file(&dir2, "b.txt", b"bbb");

        let groups = vec![
            AssetRootGroup {
                root_path: dir1.path().to_string_lossy().into(),
                file_system_location_name: None,
                inputs: [f1].into_iter().collect(),
                outputs: Default::default(),
                references: Default::default(),
            },
            AssetRootGroup {
                root_path: dir2.path().to_string_lossy().into(),
                file_system_location_name: None,
                inputs: [f2].into_iter().collect(),
                outputs: Default::default(),
                references: Default::default(),
            },
        ];

        let cache_dir = TempDir::new().unwrap();
        let (_stats, manifests) = hash_assets_and_create_manifest(
            &groups,
            2,
            6,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 2);
        assert!(manifests[0].asset_manifest.is_some());
        assert!(manifests[1].asset_manifest.is_some());
    }

    // === New file (not in cache) is hashed and cached ===

    #[test]
    fn hash_assets_new_file_hashed_and_cached() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "new.txt", b"new content");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (stats, _) = hash_assets_and_create_manifest(
            &[group],
            1,
            11,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(stats.processed_files, 1);
        assert_eq!(stats.skipped_files, 0);
    }

    // === Cached unmodified file uses cached hash (skipped) ===

    #[test]
    fn hash_assets_cached_unmodified_file_skipped() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "cached.txt", b"cached content");

        let cache_dir = TempDir::new().unwrap();

        // First pass: hash the file (populates cache)
        let group1 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1.clone()].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats1, _) = hash_assets_and_create_manifest(
            &[group1],
            1,
            14,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats1.processed_files, 1);

        // Second pass: same file, same mtime — should be skipped
        let group2 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats2, _) = hash_assets_and_create_manifest(
            &[group2],
            1,
            14,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats2.skipped_files, 1);
        assert_eq!(stats2.processed_files, 0);
    }

    // === Cached file with different mtime is re-hashed ===

    #[test]
    fn hash_assets_modified_file_rehashed() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "modify.txt", b"original");

        let cache_dir = TempDir::new().unwrap();

        // First pass
        let group1 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1.clone()].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        hash_assets_and_create_manifest(
            &[group1],
            1,
            8,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        // Modify the file (changes mtime)
        std::thread::sleep(std::time::Duration::from_millis(50));
        fs::write(&f1, b"modified content").unwrap();

        // Second pass: mtime changed, should be re-hashed (processed, not skipped)
        let group2 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats2, _) = hash_assets_and_create_manifest(
            &[group2],
            1,
            16,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats2.processed_files, 1);
        assert_eq!(stats2.skipped_files, 0);
    }

    // === Callback returns false cancels with error ===

    #[test]
    fn hash_assets_callback_cancel_returns_error() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"data");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let cancel_callback = |_: ProgressReportMetadata| -> bool { false };

        let result = hash_assets_and_create_manifest(
            &[group],
            1,
            4,
            Some(cache_dir.path().to_str().unwrap()),
            Some(Box::new(cancel_callback)),
        );
        assert!(result.is_err());
    }

    // === Progress tracker reports progress for each file ===

    #[test]
    fn hash_assets_reports_progress() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"aaa");
        let f2 = create_test_file(&dir, "b.txt", b"bbb");

        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            true
        };

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1, f2].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        hash_assets_and_create_manifest(
            &[group],
            2,
            6,
            Some(cache_dir.path().to_str().unwrap()),
            Some(Box::new(callback)),
        )
        .unwrap();

        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    // === Manifest paths are POSIX-style relative paths ===

    #[test]
    fn hash_assets_manifest_paths_are_posix_relative() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "sub/file.txt", b"content");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            1,
            7,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        let manifest = manifests[0].asset_manifest.as_ref().unwrap();
        let path = &manifest.paths[0].path;
        assert!(
            path.contains('/') || !path.contains('\\'),
            "path should use forward slashes: {path}"
        );
        assert!(!path.starts_with('/'), "path should be relative: {path}");
    }

    // === File mtime stored as microseconds (integer) ===

    #[test]
    fn hash_assets_mtime_is_microseconds_integer() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"data");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            1,
            4,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        let manifest = manifests[0].asset_manifest.as_ref().unwrap();
        let mtime = manifest.paths[0].mtime;
        assert!(
            mtime > 1_000_000_000_000,
            "mtime should be in microseconds since epoch, got: {mtime}"
        );
    }

    // === Empty file list returns None manifest ===

    #[test]
    fn hash_assets_empty_inputs_returns_none_manifest() {
        let dir = TempDir::new().unwrap();

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: Default::default(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            0,
            0,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_none());
    }
}
