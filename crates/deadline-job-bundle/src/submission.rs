use deadline_api::errors::DeadlineError;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

#[derive(Debug, Clone, Default)]
pub struct AssetReferences {
    pub input_filenames: BTreeSet<String>,
    pub input_directories: BTreeSet<String>,
    pub output_directories: BTreeSet<String>,
    pub referenced_paths: BTreeSet<String>,
}

impl AssetReferences {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_non_empty(&self) -> bool {
        !self.input_filenames.is_empty()
            || !self.input_directories.is_empty()
            || !self.output_directories.is_empty()
            || !self.referenced_paths.is_empty()
    }

    #[must_use]
    pub fn union(&self, other: &AssetReferences) -> AssetReferences {
        AssetReferences {
            input_filenames: &self.input_filenames | &other.input_filenames,
            input_directories: &self.input_directories | &other.input_directories,
            output_directories: &self.output_directories | &other.output_directories,
            referenced_paths: &self.referenced_paths | &other.referenced_paths,
        }
    }

    pub fn from_dict(obj: Option<&serde_json::Value>) -> Self {
        let Some(obj) = obj else { return Self::new() };
        let ar = &obj["assetReferences"];
        let extract = |parent: &serde_json::Value, key: &str| -> BTreeSet<String> {
            parent[key]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str()).map(normalize_path).collect())
                .unwrap_or_default()
        };
        let inputs = &ar["inputs"];
        AssetReferences {
            input_filenames: extract(inputs, "filenames"),
            input_directories: extract(inputs, "directories"),
            output_directories: extract(&ar["outputs"], "directories"),
            referenced_paths: extract(ar, "referencedPaths"),
        }
    }

    #[must_use]
    pub fn to_dict(&self) -> serde_json::Value {
        serde_json::json!({
            "assetReferences": {
                "inputs": {
                    "directories": self.input_directories.iter().collect::<Vec<_>>(),
                    "filenames": self.input_filenames.iter().collect::<Vec<_>>(),
                },
                "outputs": {
                    "directories": self.output_directories.iter().collect::<Vec<_>>(),
                },
                "referencedPaths": self.referenced_paths.iter().collect::<Vec<_>>(),
            }
        })
    }
}

/// Normalize a path: resolve `.` and `..` components without touching the filesystem.
pub fn normalize_path(s: &str) -> String {
    let mut parts: Vec<std::path::Component> = Vec::new();
    for c in Path::new(s).components() {
        match c {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                if matches!(parts.last(), Some(std::path::Component::Normal(_))) {
                    parts.pop();
                } else {
                    parts.push(c);
                }
            }
            _ => parts.push(c),
        }
    }
    if parts.is_empty() {
        ".".into()
    } else {
        parts.iter().collect::<PathBuf>().to_string_lossy().into_owned()
    }
}

const DEFAULT_APP_NAME: &str = "deadline";
const DEFAULT_SUPPORTED_APP_PARAMETER_NAMES: &[&str] = &[
    "targetTaskRunStatus",
    "priority",
    "maxFailedTasksCount",
    "maxRetriesPerTask",
    "maxWorkerCount",
];

#[allow(clippy::type_complexity)]
pub fn split_parameter_args(
    parameters: &[serde_json::Value],
    job_bundle_dir: &str,
    app_name: Option<&str>,
    supported_app_parameter_names: Option<&[&str]>,
) -> Result<
    (
        serde_json::Map<String, serde_json::Value>,
        serde_json::Map<String, serde_json::Value>,
    ),
    DeadlineError,
> {
    let app_name = app_name.unwrap_or(DEFAULT_APP_NAME);
    let supported = supported_app_parameter_names.unwrap_or(DEFAULT_SUPPORTED_APP_PARAMETER_NAMES);
    let prefix = format!("{app_name}:");

    let mut app_parameters = serde_json::Map::new();
    let mut job_parameters = serde_json::Map::new();

    for param in parameters {
        let Some(value) = param.get("value") else { continue };
        let Some(name) = param["name"].as_str() else { continue };

        if let Some(app_param) = name.strip_prefix(&prefix) {
            if supported.contains(&app_param) {
                app_parameters.insert(app_param.into(), value.clone());
            } else {
                return Err(op_err(format!(
                    "Unrecognized parameter named '{name}' from job bundle:\n{job_bundle_dir}"
                )));
            }
        } else if name.contains(':') {
            // Other app prefix — silently drop
        } else {
            let ptype = param.get("type").and_then(|t| t.as_str()).unwrap_or("STRING").to_lowercase();
            let val_str = match value {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            job_parameters.insert(name.into(), serde_json::json!({ ptype: val_str }));
        }
    }
    Ok((app_parameters, job_parameters))
}

static FRAME_RANGE_RE: LazyLock<regex::Regex> = LazyLock::new(|| {
    regex::Regex::new(r"^(?P<start>-?\d+)(-(?P<stop>-?\d+)(:(?P<step>-?\d+))?)?$").unwrap()
});

pub fn parse_frame_range(frame_string: &str) -> Result<Vec<i64>, DeadlineError> {
    let caps = FRAME_RANGE_RE
        .captures(frame_string)
        .ok_or_else(|| op_err("Framelist not valid".into()))?;

    let start: i64 = caps["start"].parse().unwrap();
    let stop: i64 = caps.name("stop").map(|m| m.as_str().parse::<i64>().unwrap()).unwrap_or(start);
    let step: i64 = caps.name("step").map(|m| m.as_str().parse::<i64>().unwrap())
        .unwrap_or(if start <= stop { 1 } else { -1 });

    if step == 0 {
        return Err(op_err("Frame step cannot be zero".into()));
    }

    let mut frames = Vec::new();
    let mut cur = start;
    if step > 0 {
        while cur <= stop { frames.push(cur); cur += step; }
    } else {
        while cur >= stop { frames.push(cur); cur += step; }
    }
    Ok(frames)
}

/// Shorthand for the most common error variant.
fn op_err(msg: String) -> DeadlineError {
    DeadlineError::OperationError(msg)
}

// ---------------------------------------------------------------------------
// Job submission orchestration
// ---------------------------------------------------------------------------

use crate::loader::{
    deadline_yaml_dump, parse_yaml_or_json_content, read_yaml_or_json, read_yaml_or_json_object,
    validate_directory_symlink_containment,
};
use crate::parameters::{apply_job_parameters, merge_queue_job_parameters, read_job_bundle_parameters};

use deadline_api::{api, queue_parameters, session};
use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use deadline_job_attachments::models::{
    FileSystemLocationType, JobAttachmentS3Settings, StorageProfile,
};
use deadline_job_attachments::progress_tracker::ProgressReportMetadata;
use deadline_job_attachments::upload;

use serde_json::{json, Value};

/// Parameters for job submission.
pub struct SubmitJobParams<'a> {
    pub job_bundle_dir: String,
    pub job_parameters: Vec<Value>,
    pub name: Option<String>,
    pub priority: Option<i32>,
    pub max_failed_tasks_count: Option<i32>,
    pub max_retries_per_task: Option<i32>,
    pub max_worker_count: Option<i32>,
    pub target_task_run_status: Option<String>,
    pub job_attachments_file_system: Option<String>,
    pub require_paths_exist: bool,
    pub submitter_name: Option<String>,
    pub known_asset_paths: Vec<String>,
    pub auto_accept: bool,
    pub force_s3_check: Option<bool>,
    pub config: Option<&'a IniConfig>,
    pub print_callback: Box<dyn Fn(&str) + Send + 'a>,
    pub hashing_progress_callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    pub upload_progress_callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    pub continue_callback: Option<Box<dyn Fn() -> bool + Send>>,
}

fn get_setting(name: &str, config: Option<&IniConfig>) -> String {
    match config {
        Some(c) => config_file::get_setting_with_config(name, c).unwrap_or_default(),
        None => config_file::get_setting(name).unwrap_or_default(),
    }
}

/// Submit a job bundle to Deadline Cloud. Returns the job ID on success.
pub async fn create_job_from_job_bundle(params: SubmitJobParams<'_>) -> Result<Option<String>, DeadlineError> {
    let print = &params.print_callback;
    let submitter_name = params.submitter_name.as_deref().unwrap_or("Custom");

    session::set_submitter_info(submitter_name, None);

    // 1. Validate symlink containment
    validate_directory_symlink_containment(&params.job_bundle_dir)?;

    // 2. Load template
    let (mut file_contents, file_type) = read_yaml_or_json(&params.job_bundle_dir, "template", true)?;

    if let Some(ref name) = params.name {
        let mut template_obj = parse_yaml_or_json_content(&file_contents, &file_type, &params.job_bundle_dir, "template")?;
        template_obj.as_object_mut()
            .ok_or_else(|| op_err("Template is not a JSON object".into()))?
            .insert("name".into(), json!(name));
        file_contents = if file_type == "YAML" {
            deadline_yaml_dump(&template_obj)
        } else {
            serde_json::to_string(&template_obj).map_err(|e| op_err(format!("Failed to serialize template: {e}")))?
        };
    }

    // 3. Get queue info
    let farm_id = get_setting("defaults.farm_id", params.config);
    let queue_id = get_setting("defaults.queue_id", params.config);

    let queue = api::get_queue(&farm_id, &queue_id, params.config, None).await?;
    let queue_display_name = queue.get("displayName").and_then(|v| v.as_str()).unwrap_or("Unknown");
    print(&format!("Submitting to Queue: {queue_display_name}\n"));

    // 4. Get storage profile (conditional)
    let storage_profile_id = get_setting("settings.storage_profile_id", params.config);
    let storage_profile = if !storage_profile_id.is_empty() {
        let sp_json = api::get_storage_profile_for_queue(
            &farm_id, &queue_id, &storage_profile_id, params.config, None,
        ).await?;
        StorageProfile::from_json(&sp_json)
    } else {
        None
    };

    // 5. Load and merge parameters
    let job_bundle_parameters = read_job_bundle_parameters(&params.job_bundle_dir)?;

    let asset_references_obj = read_yaml_or_json_object(&params.job_bundle_dir, "asset_references", false)?;
    let mut asset_references = AssetReferences::from_dict(asset_references_obj.as_ref());

    let queue_parameter_definitions = queue_parameters::get_queue_parameter_definitions(
        &farm_id, &queue_id, params.config, None,
    ).await?;

    let mut parameters = merge_queue_job_parameters(
        &job_bundle_parameters,
        &queue_parameter_definitions,
        Some(&queue_id),
    )?;

    apply_job_parameters(
        &params.job_parameters,
        &params.job_bundle_dir,
        &mut parameters,
        &mut asset_references,
    )?;

    // 6. Split parameters
    let (app_parameters, job_parameters) = split_parameter_args(&parameters, &params.job_bundle_dir, None, None)?;

    let ja_file_system = params.job_attachments_file_system
        .unwrap_or_else(|| get_setting("defaults.job_attachments_file_system", params.config));

    let force_s3_check = params.force_s3_check.unwrap_or_else(|| {
        config_file::str2bool(&get_setting("settings.force_s3_check", params.config)).unwrap_or(false)
    });

    // 8. Build CreateJob args
    let mut create_job_args = serde_json::Map::new();
    create_job_args.insert("farmId".into(), json!(farm_id));
    create_job_args.insert("queueId".into(), json!(queue_id));
    create_job_args.insert("template".into(), json!(file_contents));
    create_job_args.insert("templateType".into(), json!(file_type));
    create_job_args.insert("priority".into(), json!(50));

    if !storage_profile_id.is_empty() {
        create_job_args.insert("storageProfileId".into(), json!(storage_profile_id));
    }

    // 7. Handle attachments
    let has_attachment_settings = queue.get("jobAttachmentSettings")
        .and_then(|s| s.get("s3BucketName"))
        .and_then(|b| b.as_str())
        .is_some_and(|b| !b.is_empty());

    if asset_references.is_non_empty() && has_attachment_settings {
        expand_input_directories(&mut asset_references, params.require_paths_exist)?;

        let mut known_paths = params.known_asset_paths.clone();
        known_paths.push(std::fs::canonicalize(&params.job_bundle_dir)
            .unwrap_or_else(|_| Path::new(&params.job_bundle_dir).to_path_buf())
            .to_string_lossy().into_owned());

        if let Some(ref sp) = storage_profile {
            for loc in &sp.file_system_locations {
                if loc.location_type == FileSystemLocationType::Local {
                    known_paths.push(loc.path.clone());
                }
            }
        }

        let configured_known = get_setting("settings.known_asset_paths", params.config);
        if !configured_known.is_empty() {
            known_paths.extend(configured_known.split(std::path::MAIN_SEPARATOR).map(String::from));
        }

        let known_param_names: std::collections::HashSet<String> = params.job_parameters.iter()
            .filter_map(|p| p.get("name")?.as_str().map(String::from))
            .collect();
        for param in &parameters {
            let is_known_path_param = param.get("type").and_then(|v| v.as_str()) == Some("PATH")
                && known_param_names.contains(param["name"].as_str().unwrap_or(""));
            if let (true, Some(val)) = (is_known_path_param, param.get("value").and_then(|v| v.as_str())) {
                if val.is_empty() { continue; }
                if param.get("objectType").and_then(|v| v.as_str()) == Some("FILE") {
                    if let Some(parent) = Path::new(val).parent() {
                        known_paths.push(parent.to_string_lossy().into_owned());
                    }
                } else {
                    known_paths.push(val.to_string());
                }
            }
        }

        let known_paths = filter_redundant_known_paths(&known_paths);

        // Warn about files outside known paths
        if !asset_references.input_filenames.is_empty() {
            let outside: Vec<&String> = asset_references.input_filenames.iter()
                .filter(|f| !known_paths.iter().any(|kp| {
                    let kp_sep = if kp.ends_with(std::path::MAIN_SEPARATOR) {
                        kp.clone()
                    } else {
                        format!("{kp}{}", std::path::MAIN_SEPARATOR)
                    };
                    f.starts_with(&kp_sep) || *f == kp
                }))
                .collect();
            if !outside.is_empty() {
                print(&format!(
                    "Warning: {} file(s) found outside of known asset paths:",
                    outside.len()
                ));
                for f in outside.iter().take(10) {
                    print(&format!("  {f}"));
                }
                if outside.len() > 10 {
                    print(&format!("  ... and {} more", outside.len() - 10));
                }
                if !params.auto_accept {
                    let should_continue = params.continue_callback.as_ref().map_or(true, |cb| cb());
                    if !should_continue {
                        return Err(op_err("Submission canceled by user.".into()));
                    }
                }
            }
        }

        let queue_sdk_config = session::get_queue_user_config(
            Some(&farm_id), Some(&queue_id),
            Some(queue_display_name.to_string()),
            false, params.config,
        ).await?;

        let s3_client = deadline_job_attachments::s3::build_s3_client(&queue_sdk_config, params.config);
        let account_id = deadline_job_attachments::s3::get_account_id(&queue_sdk_config).await
            .map_err(|e| op_err(format!("Failed to get account ID: {e}")))?;

        let upload_group = upload::prepare_paths_for_upload(
            &asset_references.input_filenames.iter().cloned().collect::<Vec<_>>(),
            &asset_references.output_directories.iter().cloned().collect::<Vec<_>>(),
            &asset_references.referenced_paths.iter().cloned().collect::<Vec<_>>(),
            storage_profile.as_ref(),
            params.require_paths_exist,
        ).map_err(|e| op_err(e.to_string()))?;

        if !upload_group.asset_groups.is_empty() {
            let cache_dir = config_file::get_cache_directory();
            let cache_dir_str = cache_dir.to_str();

            let (hashing_summary, manifests) = upload::hash_assets_and_create_manifest(
                &upload_group.asset_groups,
                upload_group.total_input_files,
                upload_group.total_input_bytes,
                cache_dir_str,
                params.hashing_progress_callback,
            ).map_err(|e| op_err(e.to_string()))?;

            if hashing_summary.processed_files > 0 {
                print("Hashing Summary:");
                for line in hashing_summary.to_string().lines() {
                    print(&format!("    {line}"));
                }
            }

            let ja_settings = queue.get("jobAttachmentSettings").unwrap();
            let s3_settings = JobAttachmentS3Settings::from_root_path(&format!(
                "{}/{}",
                ja_settings["s3BucketName"].as_str().unwrap_or(""),
                ja_settings["rootPrefix"].as_str().unwrap_or(""),
            )).map_err(|e| op_err(e.to_string()))?;

            let upload_ctx = upload::S3UploadContext::new(s3_client, account_id, params.config)
                .map_err(|e| op_err(e.to_string()))?;

            let (upload_summary, attachments) = upload::upload_assets(
                &farm_id, &queue_id, &s3_settings,
                &manifests, &upload_ctx,
                params.upload_progress_callback,
                cache_dir_str,
                Some(force_s3_check),
            ).await.map_err(|e| op_err(e.to_string()))?;

            if upload_summary.processed_files > 0 {
                print("Upload Summary:");
                for line in upload_summary.to_string().lines() {
                    print(&format!("    {line}"));
                }
            }

            let mut att_json = attachments.to_json();
            att_json.as_object_mut().unwrap().insert(
                "fileSystem".into(),
                json!(if ja_file_system == "VIRTUAL" { "VIRTUAL" } else { "COPIED" }),
            );
            create_job_args.insert("attachments".into(), att_json);
        }
    } else {
        // No files to process — call callbacks once at 100% to close progress bars
        if let Some(ref cb) = params.hashing_progress_callback {
            cb(ProgressReportMetadata {
                status: deadline_job_attachments::progress_tracker::ProgressStatus::PreparingInProgress,
                progress: 100.0,
                transfer_rate: 0.0,
                progress_message: "No files to hash".into(),
                processed_files: 0,
            });
        }
        if let Some(ref cb) = params.upload_progress_callback {
            cb(ProgressReportMetadata {
                status: deadline_job_attachments::progress_tracker::ProgressStatus::UploadInProgress,
                progress: 100.0,
                transfer_rate: 0.0,
                progress_message: "No files to upload".into(),
                processed_files: 0,
            });
        }
    }

    for (k, v) in &app_parameters {
        create_job_args.insert(k.clone(), v.clone());
    }
    if !job_parameters.is_empty() {
        create_job_args.insert("parameters".into(), Value::Object(job_parameters));
    }
    if let Some(p) = params.priority {
        create_job_args.insert("priority".into(), json!(p));
    }
    if let Some(v) = params.max_failed_tasks_count {
        create_job_args.insert("maxFailedTasksCount".into(), json!(v));
    }
    if let Some(v) = params.max_retries_per_task {
        create_job_args.insert("maxRetriesPerTask".into(), json!(v));
    }
    if let Some(v) = params.max_worker_count {
        create_job_args.insert("maxWorkerCount".into(), json!(v));
    }
    if let Some(ref v) = params.target_task_run_status {
        create_job_args.insert("targetTaskRunStatus".into(), json!(v));
    }

    // 9. Call CreateJob
    let response = api::create_job(&create_job_args, params.config, None).await?;

    let job_id = response.get("jobId").and_then(|v| v.as_str())
        .ok_or_else(|| op_err("CreateJob response was empty, or did not contain a Job ID.".into()))?
        .to_string();

    // 10. Poll for completion
    print("Waiting for Job to be created...");

    let continue_cb = params.continue_callback.unwrap_or_else(|| Box::new(|| true));

    let (success, status_message) = api::wait_for_create_job_to_complete(
        &farm_id, &queue_id, &job_id, params.config, &*continue_cb,
    ).await?;

    if !success {
        return Err(op_err(format!("Job {job_id} creation failed: {status_message}")));
    }

    print(&format!("Submitted job bundle:\n   {}", params.job_bundle_dir));
    print(&format!("{status_message}\n{job_id}"));

    Ok(Some(job_id))
}

/// Walk input directories recursively, adding files to input_filenames.
fn expand_input_directories(
    asset_references: &mut AssetReferences,
    require_paths_exist: bool,
) -> Result<(), DeadlineError> {
    let dirs: Vec<String> = asset_references.input_directories.iter().cloned().collect();
    asset_references.input_directories.clear();
    let mut missing = Vec::new();

    for directory in &dirs {
        let dir_path = Path::new(directory);
        if !dir_path.is_dir() {
            if require_paths_exist {
                missing.push(directory.clone());
            } else {
                log::warn!("Input path '{directory}' does not exist. Adding to referenced paths.");
                asset_references.referenced_paths.insert(directory.clone());
            }
            continue;
        }
        let mut is_empty = true;
        for entry in walkdir(dir_path)? {
            is_empty = false;
            asset_references.input_filenames.insert(entry);
        }
        if is_empty {
            log::info!("Input directory '{directory}' is empty. Adding to referenced paths.");
            asset_references.referenced_paths.insert(directory.clone());
        }
    }

    if !missing.is_empty() {
        let list = missing.join("\n\t");
        return Err(op_err(format!(
            "Job submission contains misconfigured input directories and cannot be submitted. \
             All input directories must exist.\n\
             Non-existent directories:\n\t{list}"
        )));
    }
    Ok(())
}

fn walkdir(dir: &Path) -> Result<Vec<String>, DeadlineError> {
    let mut files = Vec::new();
    let entries = std::fs::read_dir(dir)
        .map_err(|e| op_err(format!("Failed to read directory {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| op_err(format!("Failed to read entry: {e}")))?;
        let path = entry.path();
        if path.is_dir() {
            files.extend(walkdir(&path)?);
        } else {
            files.push(path.to_string_lossy().into_owned());
        }
    }
    Ok(files)
}

/// Filter out paths that have another path as a prefix.
pub fn filter_redundant_known_paths(paths: &[String]) -> Vec<String> {
    let mut sorted: Vec<&String> = paths.iter().collect();
    sorted.sort_by_key(|p| p.len());
    let mut filtered: Vec<&String> = Vec::new();

    for path in sorted {
        let dominated = filtered.iter().any(|existing| {
            let existing_with_sep = if existing.ends_with(std::path::MAIN_SEPARATOR) {
                existing.to_string()
            } else {
                format!("{}{}", existing, std::path::MAIN_SEPARATOR)
            };
            path.starts_with(&existing_with_sep) || *path == **existing
        });
        if !dominated {
            filtered.push(path);
        }
    }
    filtered.into_iter().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- filter_redundant_known_paths ---

    #[test]
    fn filter_redundant_empty_list_returns_empty() {
        assert!(filter_redundant_known_paths(&[]).is_empty());
    }

    #[test]
    fn filter_redundant_single_path_returned_as_is() {
        let paths = vec!["/mnt/prod".to_string()];
        assert_eq!(filter_redundant_known_paths(&paths), vec!["/mnt/prod"]);
    }

    #[test]
    fn filter_redundant_child_path_removed() {
        let paths = vec![
            "/mnt/prod".to_string(),
            "/mnt/prod/project".to_string(),
        ];
        assert_eq!(filter_redundant_known_paths(&paths), vec!["/mnt/prod"]);
    }

    #[test]
    fn filter_redundant_unrelated_paths_kept() {
        let paths = vec![
            "/mnt/prod".to_string(),
            "/home/user".to_string(),
        ];
        let result = filter_redundant_known_paths(&paths);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&"/mnt/prod".to_string()));
        assert!(result.contains(&"/home/user".to_string()));
    }

    #[test]
    fn filter_redundant_duplicate_path_removed() {
        let paths = vec![
            "/mnt/prod".to_string(),
            "/mnt/prod".to_string(),
        ];
        assert_eq!(filter_redundant_known_paths(&paths), vec!["/mnt/prod"]);
    }

    #[test]
    fn filter_redundant_deep_nesting_only_keeps_root() {
        let paths = vec![
            "/a/b/c/d".to_string(),
            "/a/b".to_string(),
            "/a/b/c".to_string(),
        ];
        assert_eq!(filter_redundant_known_paths(&paths), vec!["/a/b"]);
    }

    #[test]
    fn filter_redundant_similar_prefix_not_confused() {
        // /mnt/production should NOT be filtered by /mnt/prod
        let paths = vec![
            "/mnt/prod".to_string(),
            "/mnt/production".to_string(),
        ];
        let result = filter_redundant_known_paths(&paths);
        assert_eq!(result.len(), 2);
    }

    // --- expand_input_directories ---

    #[test]
    fn expand_input_directories_empty_dir_becomes_referenced() {
        let dir = tempfile::TempDir::new().unwrap();
        let empty_dir = dir.path().join("empty");
        std::fs::create_dir_all(&empty_dir).unwrap();

        let mut refs = AssetReferences::new();
        refs.input_directories.insert(empty_dir.to_string_lossy().into_owned());

        expand_input_directories(&mut refs, false).unwrap();

        assert!(refs.input_filenames.is_empty());
        assert!(refs.input_directories.is_empty());
        assert!(refs.referenced_paths.contains(&empty_dir.to_string_lossy().into_owned()));
    }

    #[test]
    fn expand_input_directories_with_files_adds_to_filenames() {
        let dir = tempfile::TempDir::new().unwrap();
        let input_dir = dir.path().join("inputs");
        std::fs::create_dir_all(&input_dir).unwrap();
        std::fs::write(input_dir.join("a.txt"), "hello").unwrap();
        std::fs::write(input_dir.join("b.txt"), "world").unwrap();

        let mut refs = AssetReferences::new();
        refs.input_directories.insert(input_dir.to_string_lossy().into_owned());

        expand_input_directories(&mut refs, false).unwrap();

        assert_eq!(refs.input_filenames.len(), 2);
        assert!(refs.input_directories.is_empty());
    }

    #[test]
    fn expand_input_directories_missing_dir_require_paths_errors() {
        let mut refs = AssetReferences::new();
        refs.input_directories.insert("/nonexistent/dir".to_string());

        let result = expand_input_directories(&mut refs, true);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Non-existent directories"));
    }

    #[test]
    fn expand_input_directories_missing_dir_no_require_becomes_referenced() {
        let mut refs = AssetReferences::new();
        refs.input_directories.insert("/nonexistent/dir".to_string());

        expand_input_directories(&mut refs, false).unwrap();

        assert!(refs.input_filenames.is_empty());
        assert!(refs.referenced_paths.contains("/nonexistent/dir"));
    }

    #[test]
    fn expand_input_directories_nested_dirs_walks_recursively() {
        let dir = tempfile::TempDir::new().unwrap();
        let input_dir = dir.path().join("root");
        let sub_dir = input_dir.join("sub");
        std::fs::create_dir_all(&sub_dir).unwrap();
        std::fs::write(input_dir.join("top.txt"), "top").unwrap();
        std::fs::write(sub_dir.join("nested.txt"), "nested").unwrap();

        let mut refs = AssetReferences::new();
        refs.input_directories.insert(input_dir.to_string_lossy().into_owned());

        expand_input_directories(&mut refs, false).unwrap();

        assert_eq!(refs.input_filenames.len(), 2);
    }
}
