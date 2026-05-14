use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::errors::JobAttachmentsError;
use crate::models::PathFormat;
use aws_sdk_s3::primitives::ByteStream;

use crate::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion, hash_data,
};
use crate::caches::{HashCache, S3CheckCache};
use crate::models::{
    AssetRootGroup, AssetRootManifest, AssetUploadGroup, Attachments, FileSystemLocationType,
    JobAttachmentS3Settings, ManifestProperties, StorageProfile, join_s3_paths,
};
use crate::progress_tracker::{
    ProgressFn, ProgressStatus, ProgressTracker, SummaryStatistics,
};

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
}

impl S3UploadContext {
    /// Build from a pre-configured S3 client and account ID.
    pub fn new(
        s3_client: aws_sdk_s3::Client,
        account_id: String,
    ) -> Result<Self, JobAttachmentsError> {
        Ok(Self {
            s3_client,
            account_id,
        })
    }

    /// Returns a reference to the underlying S3 client.
    pub fn s3_client(&self) -> &aws_sdk_s3::Client {
        &self.s3_client
    }

    /// Returns the account ID used for `ExpectedBucketOwner`.
    pub fn account_id(&self) -> &str {
        &self.account_id
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
            use aws_sdk_s3::error::ProvideErrorMetadata;
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
}
/// Orchestrate hashing and uploading asset files to S3. Builds `ManifestProperties`
/// and Attachments from the results. Uses openjd's pipelined hash+upload engine.
pub async fn upload_assets(
    farm_id: &str,
    queue_id: &str,
    job_attachment_settings: &JobAttachmentS3Settings,
    asset_groups: &[AssetRootGroup],
    ctx: &S3UploadContext,
    on_uploading_assets: Option<ProgressFn>,
    s3_check_cache_dir: Option<&str>,
    force_s3_check: Option<bool>,
) -> Result<(SummaryStatistics, Attachments), JobAttachmentsError> {
    use openjd_snapshots::{
        AbsManifest, AsyncDataCache, HashUploadOptions, S3DataCache,
        collect_abs_snapshot, hash_upload_abs_manifest, CollectOptions,
    };
    use std::sync::Arc;

    if farm_id.is_empty() || queue_id.is_empty() {
        return Err(JobAttachmentsError::AssetSync(
            "upload_assets: Farm or Fleet ID is missing.".into(),
        ));
    }

    // Compute totals
    let mut total_files: u64 = 0;
    let mut total_bytes: u64 = 0;
    for group in asset_groups {
        for input in &group.inputs {
            total_files += 1;
            total_bytes += std::fs::metadata(input).map(|m| m.len()).unwrap_or(0);
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

    let cas_prefix = job_attachment_settings.full_cas_prefix()?;
    let partial_prefix = job_attachment_settings.partial_manifest_prefix(farm_id, queue_id);

    for group in asset_groups {
        let output_rel_paths: Vec<String> = group
            .outputs
            .iter()
            .filter_map(|p| {
                p.strip_prefix(&group.root_path)
                    .ok()
                    .map(|r| r.to_string_lossy().into_owned())
            })
            .collect();

        let mut props = ManifestProperties {
            root_path: group.root_path.clone(),
            root_path_format: PathFormat::host(),
            file_system_location_name: group.file_system_location_name.clone(),
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: if output_rel_paths.is_empty() {
                None
            } else {
                Some(output_rel_paths)
            },
        };

        if !group.inputs.is_empty() {
            // Check cancellation before starting
            if !progress_tracker.report_progress() {
                return Err(JobAttachmentsError::Cancelled {
                    message: "File upload cancelled.".into(),
                });
            }

            // Collect unhashed AbsSnapshot from files on disk
            let file_paths: Vec<PathBuf> = group.inputs.iter().cloned().collect();
            let abs_snapshot = collect_abs_snapshot(
                &[] as &[PathBuf],
                &file_paths,
                CollectOptions::default(),
            )
            .map_err(|e| {
                JobAttachmentsError::AssetSync(format!("Failed to collect snapshot: {e}"))
            })?;

            // Build S3DataCache
            let s3_cache = S3DataCache::new(
                job_attachment_settings.s3_bucket_name.clone(),
                cas_prefix.clone(),
                ctx.s3_client().clone(),
            )
            .with_expected_bucket_owner(Some(ctx.account_id().to_owned()));

            // Attach S3CheckCache if available
            let check_cache_dir = s3_check_cache_dir
                .map(ToOwned::to_owned)
                .or_else(crate::caches::default_cache_dir);
            let s3_cache = if let Some(ref dir) = check_cache_dir {
                if let Ok(check_cache) = S3CheckCache::new(dir) {
                    s3_cache.with_s3_check_cache(Some(Arc::new(check_cache)))
                } else {
                    s3_cache
                }
            } else {
                s3_cache
            };
            let s3_cache = if force_s3_check == Some(true) {
                s3_cache.with_force_s3_check(true)
            } else {
                s3_cache
            };

            // HashCache for skipping unchanged files
            let hash_cache_dir = s3_check_cache_dir
                .map(ToOwned::to_owned)
                .or_else(crate::caches::default_cache_dir);
            let hash_cache = hash_cache_dir
                .as_deref()
                .and_then(|d| HashCache::new(d).ok())
                .map(Arc::new);

            let data_cache: Arc<dyn AsyncDataCache> = Arc::new(s3_cache);

            // Hash + upload in one pipelined pass
            let upload_result = hash_upload_abs_manifest(
                &AbsManifest::Snapshot(abs_snapshot),
                data_cache,
                HashUploadOptions {
                    hash_cache,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| {
                JobAttachmentsError::AssetSync(format!("Upload failed: {e}"))
            })?;

            // Build AssetManifest from the hashed result for manifest JSON encoding
            let AbsManifest::Snapshot(hashed_snapshot) = &upload_result.manifest else { unreachable!("input was Snapshot") };
            let source_root = Path::new(&group.root_path);
            let root_str = source_root.to_string_lossy();
            let paths: Vec<ManifestPath> = hashed_snapshot
                .files
                .iter()
                .filter(|f| !f.deleted && f.symlink_target.is_none())
                .map(|f| {
                    // Convert absolute path back to relative
                    let rel = f.path.strip_prefix(&*root_str)
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
            let manifest = AssetManifest::new(
                HashAlgorithm::Xxh128,
                ManifestVersion::V2023_03_03,
                total_size,
                paths,
            )?;

            // Encode and upload manifest JSON
            let manifest_bytes = manifest.encode().into_bytes();
            let manifest_name_prefix = hash_data(group.root_path.as_bytes());
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

            props.input_manifest_path = Some(partial_key);
            props.input_manifest_hash = Some(hash_data(&manifest_bytes));

            // Update progress tracker
            let stats = &upload_result.statistics;
            progress_tracker.increase_processed(
                stats.hashed_files as u64,
                stats.hashed_bytes,
            );
            progress_tracker.increase_skipped(
                stats.skipped_files as u64,
                stats.skipped_bytes,
            );
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
    on_snapshotting_assets: Option<ProgressFn>,
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
            
            let manifest_bytes = manifest.encode().into_bytes();
            let manifest_name_prefix = hash_data(arm.root_path.as_bytes());
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
            props.input_manifest_hash = Some(hash_data(&manifest_bytes));
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
}
