//! Public API for downloading and uploading job attachments.
//!
//! Stateless orchestration functions that compose the lower-level
//! upload and download engines. Corresponds to Python's
//! `deadline.job_attachments.api.attachment` and `api._utils`.

use std::collections::HashMap;
use std::path::Path;

use deadline_config::ini::IniConfig;
use crate::errors::JobAttachmentsError;

use crate::asset_manifests::{decode_manifest, hash_data, AssetManifest, HashAlgorithm};
use crate::download::download_files_from_manifests;
use crate::models::{
    FileConflictResolution, JobAttachmentS3Settings, PathMappingRule, UploadManifestInfo,
};
use crate::progress_tracker::{DownloadSummaryStatistics, ProgressReportMetadata};
use crate::upload::S3UploadContext;

/// Read and decode manifest files from disk.
///
/// Returns a `HashMap` keyed by base filename. Validates all paths
/// exist upfront; collects invalid ones into a single error.
pub fn read_manifests(manifest_paths: &[String]) -> Result<HashMap<String, AssetManifest>, JobAttachmentsError> {
    if manifest_paths.is_empty() {
        return Ok(HashMap::new());
    }

    let invalid: Vec<&String> = manifest_paths
        .iter()
        .filter(|p| !Path::new(p).is_file())
        .collect();

    if !invalid.is_empty() {
        let list: Vec<&str> = invalid.iter().map(|s| s.as_str()).collect();
        return Err(JobAttachmentsError::AssetSync(format!(
            "Specified manifests {list:?} are not valid."
        )));
    }

    let mut result = HashMap::new();
    for path in manifest_paths {
        let filename = Path::new(path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let contents = std::fs::read_to_string(path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read manifest {path}: {e}"))
        })?;
        let manifest = decode_manifest(&contents)?;
        result.insert(filename, manifest);
    }

    Ok(result)
}

/// Build path mapping rules from a JSON file and/or root directories.
///
/// Both can be provided (concatenated). Neither → empty list.
pub fn process_path_mapping(
    path_mapping_rules: Option<&str>,
    root_dirs: &[String],
) -> Result<Vec<PathMappingRule>, JobAttachmentsError> {
    let mut rules = Vec::new();

    if let Some(rules_path) = path_mapping_rules {
        if !Path::new(rules_path).is_file() {
            return Err(JobAttachmentsError::AssetSync(format!(
                "Specified path mapping file {rules_path} is not valid."
            )));
        }
        let contents = std::fs::read_to_string(rules_path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read path mapping file: {e}"))
        })?;
        let data: serde_json::Value = serde_json::from_str(&contents).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to parse path mapping JSON: {e}"))
        })?;

        let list = if let Some(nested) = data.get("path_mapping_rules") {
            nested
        } else {
            &data
        };

        let arr = list.as_array().ok_or_else(|| {
            JobAttachmentsError::AssetSync(
                "Path mapping rules have to be a list of dict.".into(),
            )
        })?;

        for item in arr {
            let source_path_format = item
                .get("source_path_format")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let source_path = item
                .get("source_path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let destination_path = item
                .get("destination_path")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            rules.push(PathMappingRule {
                source_path_format,
                source_path,
                destination_path,
            });
        }
    }

    // Validate root_dirs
    let invalid_dirs: Vec<&String> = root_dirs
        .iter()
        .filter(|d| !Path::new(d).is_dir())
        .collect();
    if !invalid_dirs.is_empty() {
        let list: Vec<&str> = invalid_dirs.iter().map(|s| s.as_str()).collect();
        return Err(JobAttachmentsError::AssetSync(format!(
            "Specified root dir {list:?} are not valid."
        )));
    }

    for dir in root_dirs {
        rules.push(PathMappingRule {
            source_path_format: String::new(),
            source_path: dir.clone(),
            destination_path: dir.clone(),
        });
    }

    Ok(rules)
}

/// Download job attachments based on manifest files and optional path mapping.
pub async fn attachment_download(
    manifests: &[String],
    s3_root_uri: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    path_mapping_rules: Option<&str>,
    on_progress: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    conflict_resolution: FileConflictResolution,
) -> Result<DownloadSummaryStatistics, JobAttachmentsError> {
    let file_name_manifest_dict = read_manifests(manifests)?;

    let rule_list = process_path_mapping(path_mapping_rules, &[])?;

    let s3_settings = JobAttachmentS3Settings::from_s3_root_uri(s3_root_uri)?;
    let cas_prefix = s3_settings.full_cas_prefix()?;

    let mut manifests_by_root: HashMap<String, AssetManifest> = HashMap::new();

    for (file_name, manifest) in &file_name_manifest_dict {
        let destination = rule_list
            .iter()
            .find(|rule| {
                let hashed = rule.get_hashed_source_path(manifest.hash_alg);
                file_name.contains(&hashed)
            })
            .map(|rule| rule.destination_path.clone())
            .unwrap_or_else(|| {
                let cwd = std::env::current_dir()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned();
                format!("{cwd}/{file_name}")
            });

        if manifests_by_root.contains_key(&destination) {
            return Err(JobAttachmentsError::AssetSync(format!(
                "{destination} is already in use, one destination path maps to one manifest file only."
            )));
        }

        manifests_by_root.insert(destination, manifest.clone());
    }

    download_files_from_manifests(
        &s3_settings.s3_bucket_name,
        &manifests_by_root,
        Some(&cas_prefix),
        s3_client,
        account_id,
        on_progress,
        conflict_resolution,
    )
    .await
}

/// Upload job attachments based on manifest files and path mapping rules or root directories.
pub async fn attachment_upload(
    manifests: &[String],
    s3_root_uri: &str,
    s3_client: &aws_sdk_s3::Client,
    account_id: &str,
    root_dirs: &[String],
    path_mapping_rules: Option<&str>,
    upload_manifest_path: Option<&str>,
    _on_progress: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    config: Option<&IniConfig>,
) -> Result<Vec<UploadManifestInfo>, JobAttachmentsError> {
    let file_name_manifest_dict = read_manifests(manifests)?;

    // Validate exactly one of path_mapping_rules / root_dirs
    let has_rules = path_mapping_rules.is_some();
    let has_dirs = !root_dirs.is_empty();
    if has_rules == has_dirs {
        return Err(JobAttachmentsError::AssetSync(
            "One of path mapping rule and root dir must exist, and not both.".into(),
        ));
    }

    let rule_list = process_path_mapping(path_mapping_rules, root_dirs)?;

    let s3_settings = JobAttachmentS3Settings::from_s3_root_uri(s3_root_uri)?;
    let cas_prefix = s3_settings.full_cas_prefix()?;

    let ctx = S3UploadContext::new(s3_client.clone(), account_id.to_string(), config)?;

    let mut result = Vec::new();

    // Iterate in original manifest order
    for manifest_path in manifests {
        let file_name = Path::new(manifest_path)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        let manifest = &file_name_manifest_dict[&file_name];

        let rule = rule_list
            .iter()
            .find(|r| {
                let hashed = r.get_hashed_source_path(manifest.hash_alg);
                file_name.contains(&hashed)
            })
            .ok_or_else(|| {
                JobAttachmentsError::AssetSync(format!(
                    "No valid root defined for given manifest {file_name}, please check input root dirs and path mapping rule."
                ))
            })?;

        // Build S3 metadata
        let mut metadata = HashMap::new();
        // Try ASCII first, fall back to JSON-encoded
        if rule.source_path.is_ascii() {
            metadata.insert("asset-root".to_string(), rule.source_path.clone());
        } else {
            let json_encoded =
                serde_json::to_string(&rule.source_path).unwrap_or_default();
            metadata.insert("asset-root-json".to_string(), json_encoded.clone());
            metadata.insert("asset-root".to_string(), json_encoded);
        }
        if !rule.source_path_format.is_empty() {
            metadata.insert(
                "file-system-location-name".to_string(),
                rule.source_path_format.clone(),
            );
        }

        // Upload files from manifest — use destination_path as the local root
        // (source_path is the original machine's path; destination_path is where
        // files are on this machine after path mapping)
        ctx.upload_input_files(
            manifest,
            &s3_settings.s3_bucket_name,
            Path::new(&rule.destination_path),
            &cas_prefix,
            None, // progress tracker — TODO: wire up on_progress
            None,
            None,
        )
        .await?;

        // Upload manifest file itself if upload_manifest_path provided
        let manifest_bytes = manifest.encode().into_bytes();
        let hash_alg = HashAlgorithm::Xxh128;
        let manifest_hash = hash_data(&manifest_bytes, hash_alg);

        let partial_key = if let Some(prefix) = upload_manifest_path {
            let key = format!("{prefix}/{file_name}");
            let full_key =
                s3_settings.add_root_and_manifest_folder_prefix(&key)?;
            ctx.upload_bytes_to_s3(
                &manifest_bytes,
                &s3_settings.s3_bucket_name,
                &full_key,
                Some(metadata),
            )
            .await?;
            key
        } else {
            file_name.clone()
        };

        result.push(UploadManifestInfo {
            output_manifest_path: partial_key,
            output_manifest_hash: manifest_hash,
            source_path: Some(rule.source_path.clone()),
        });
    }

    Ok(result)
}
