use crate::hooks::{self, HookManager, HookMetadata};
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

use deadline_api::{api, client, queue_parameters, session};
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
    pub debug_snapshot_dir: Option<String>,
    pub config: Option<&'a IniConfig>,
    pub print_callback: Box<dyn Fn(&str) + Send + 'a>,
    pub hashing_progress_callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    pub upload_progress_callback: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
    pub continue_callback: Option<Box<dyn Fn() -> bool + Send>>,
    pub interactive_confirmation_callback: Option<Box<dyn Fn(&str, bool) -> bool + Send>>,
    pub telemetry: Option<&'a deadline_api::telemetry::TelemetryClient>,
}

fn get_setting(name: &str, config: Option<&IniConfig>) -> String {
    match config {
        Some(c) => config_file::get_setting(name, c).unwrap_or_default(),
        None => config_file::get_setting_from_disk(name).unwrap_or_default(),
    }
}

/// Submit a job bundle to Deadline Cloud. Returns the job ID on success.
pub async fn create_job_from_job_bundle(params: SubmitJobParams<'_>) -> Result<Option<String>, DeadlineError> {
    let print = &params.print_callback;
    let submitter_name = params.submitter_name.as_deref().unwrap_or("Custom");

    session::set_submitter_info(submitter_name, None).await;

    // 1. Validate symlink containment
    validate_directory_symlink_containment(&params.job_bundle_dir)?;

    // 1b. Load hooks from bundle and/or environment
    let allow_bundle_hooks = config_file::str2bool(
        &get_setting("settings.allow_bundle_hooks", params.config),
    ).unwrap_or(false);
    let allow_env_hooks = config_file::str2bool(
        &get_setting("settings.allow_environment_hooks", params.config),
    ).unwrap_or(false);
    let env_hooks_dir = std::env::var("DEADLINE_HOOKS_DIR").ok();

    let mut merged_hooks: Option<hooks::HookConfiguration> = None;

    // Check environment hooks
    if let Some(ref ehd) = env_hooks_dir {
        if allow_env_hooks {
            if Path::new(ehd).is_dir() {
                let mut env_mgr = HookManager::new(ehd, Box::new(|_| {}));
                if let Some(eh) = env_mgr.load_hooks()? {
                    merged_hooks = Some(eh.clone());
                }
            } else {
                print(&format!("Warning: DEADLINE_HOOKS_DIR '{ehd}' is not a valid directory"));
            }
        } else {
            print("Warning: DEADLINE_HOOKS_DIR is set but environment hooks are disabled.\nEnable with: deadline config set settings.allow_environment_hooks true");
        }
    }

    // Check bundle hooks
    let mut bundle_mgr = HookManager::new(&params.job_bundle_dir, Box::new(|_| {}));
    let bundle_hooks = bundle_mgr.load_hooks()?;
    if let Some(bh) = bundle_hooks {
        if !bh.pre_submission.is_empty() || !bh.post_submission.is_empty() {
            if allow_bundle_hooks {
                match merged_hooks.as_mut() {
                    Some(mh) => {
                        mh.pre_submission.extend(bh.pre_submission.clone());
                        mh.post_submission.extend(bh.post_submission.clone());
                    }
                    None => merged_hooks = Some(bh.clone()),
                }
            } else {
                print("Note: Job bundle contains hooks.yaml but bundle hooks are disabled.\nEnable with: deadline config set settings.allow_bundle_hooks true");
            }
        }
    }

    // Show confirmation and build the hook manager we'll actually use
    let mut hook_manager = HookManager::new(&params.job_bundle_dir,
        Box::new(|s| { eprintln!("{s}"); }));
    hook_manager.hooks = merged_hooks.clone();

    if let Some(ref mh) = merged_hooks {
        if !mh.pre_submission.is_empty() || !mh.post_submission.is_empty() {
            if !params.auto_accept {
                let msg = hooks::generate_hooks_confirmation_message(mh, &params.job_bundle_dir);
                match &params.interactive_confirmation_callback {
                    None => {
                        print(&msg);
                        print("Job submission canceled (hooks present but user confirmation not available).");
                        return Err(op_err("Job submission canceled.".into()));
                    }
                    Some(cb) => {
                        if !cb(&format!("{msg}Do you want to run these hooks?"), true) {
                            return Err(op_err("Job submission canceled (user declined hooks).".into()));
                        }
                    }
                }
            }
        }
    }

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

    let queue = session::deadline_client(params.config).await
        .get_queue().farm_id(&farm_id).queue_id(&queue_id)
        .send().await
        .map_err(client::deadline_error)?;
    let queue_display_name = queue.display_name();
    print(&format!("Submitting to Queue: {queue_display_name}\n"));

    // 4. Get storage profile (conditional)
    let storage_profile_id = get_setting("settings.storage_profile_id", params.config);
    let storage_profile = if !storage_profile_id.is_empty() {
        let sp_output = deadline_api::session::deadline_client(params.config).await
            .get_storage_profile_for_queue()
            .farm_id(&farm_id).queue_id(&queue_id).storage_profile_id(&storage_profile_id)
            .send().await
            .map_err(deadline_api::client::deadline_error)?;
        let sp_json = storage_profile_output_to_value(&sp_output);
        StorageProfile::from_json(&sp_json)
    } else {
        None
    };

    // 5. Load and merge parameters
    let job_bundle_parameters = read_job_bundle_parameters(&params.job_bundle_dir)?;

    let asset_references_obj = read_yaml_or_json_object(&params.job_bundle_dir, "asset_references", false)?;
    let mut asset_references = AssetReferences::from_dict(asset_references_obj.as_ref());

    let queue_parameter_definitions = queue_parameters::get_queue_parameter_definitions(
        &farm_id, &queue_id, params.config,
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

    // 6b. Execute pre-submission hooks (before hashing/uploading)
    if hook_manager.hooks.as_ref().is_some_and(|h| !h.pre_submission.is_empty()) {
        let template_obj = parse_yaml_or_json_content(&file_contents, &file_type, &params.job_bundle_dir, "template")?;
        let mut hook_metadata = HookMetadata {
            job_name: template_obj.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            priority: params.priority.unwrap_or(50),
            farm_id: farm_id.clone(),
            queue_id: queue_id.clone(),
            job_bundle_dir: std::fs::canonicalize(&params.job_bundle_dir)
                .unwrap_or_else(|_| PathBuf::from(&params.job_bundle_dir))
                .to_string_lossy().into_owned(),
            parameters: parameters.iter()
                .filter_map(|p| Some((p.get("name")?.as_str()?.to_string(), p.get("value").cloned().unwrap_or(Value::Null))))
                .collect(),
            submitter_name: submitter_name.to_string(),
            asset_references: asset_references.to_dict(),
            submission_payload: serde_json::json!({}),
            storage_profile_id: if storage_profile_id.is_empty() { None } else { Some(storage_profile_id.clone()) },
            job_id: None,
        };
        let hook_result = hook_manager.execute_pre_submission_hooks(&mut hook_metadata, serde_json::json!({}))?;

        // Merge any asset references from hooks
        if let Some(refs) = hook_result.get("attachments").and_then(|a| a.get("assetReferences")) {
            if let Some(arr) = refs.get("inputFilenames").and_then(|v| v.as_array()) {
                for f in arr { if let Some(s) = f.as_str() { asset_references.input_filenames.insert(s.to_string()); } }
            }
            if let Some(arr) = refs.get("inputDirectories").and_then(|v| v.as_array()) {
                for d in arr { if let Some(s) = d.as_str() { asset_references.input_directories.insert(s.to_string()); } }
            }
            if let Some(arr) = refs.get("outputDirectories").and_then(|v| v.as_array()) {
                for d in arr { if let Some(s) = d.as_str() { asset_references.output_directories.insert(s.to_string()); } }
            }
            if let Some(arr) = refs.get("referencedPaths").and_then(|v| v.as_array()) {
                for p in arr { if let Some(s) = p.as_str() { asset_references.referenced_paths.insert(s.to_string()); } }
            }
        }
    }

    // 7. Handle attachments
    let has_attachment_settings = queue.job_attachment_settings()
        .is_some_and(|s| !s.s3_bucket_name().is_empty());

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
            let path_list_sep = if cfg!(windows) { ';' } else { ':' };
            known_paths.extend(configured_known.split(path_list_sep).map(String::from));
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
                if params.auto_accept {
                    print("Job submission canceled (settings.auto_accept enabled and there were unknown paths).");
                    return Err(op_err("Job submission canceled (settings.auto_accept enabled and there were unknown paths).".into()));
                } else {
                    let msg = format!(
                        "WARNING: {} file(s) found outside of known asset paths.\nDo you wish to proceed?",
                        outside.len()
                    );
                    let should_continue = params.interactive_confirmation_callback
                        .as_ref()
                        .map_or(true, |cb| cb(&msg, false));
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
            // Print upload summary (matches Python's _generate_message_for_asset_paths)
            print(&format!(
                "Job submission contains {} input file{} totaling {}. \
                 All input files will be uploaded to S3 if they are not already present in the job attachments bucket.\n",
                upload_group.total_input_files,
                if upload_group.total_input_files == 1 { "" } else { "s" },
                deadline_api::path_utils::human_readable_file_size(upload_group.total_input_bytes),
            ));

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

            // F5: Emit hashing summary telemetry
            if let Some(tc) = params.telemetry {
                let mut details = std::collections::HashMap::new();
                details.insert("total_files".into(), json!(hashing_summary.total_files));
                details.insert("total_bytes".into(), json!(hashing_summary.total_bytes));
                details.insert("processed_files".into(), json!(hashing_summary.processed_files));
                details.insert("processed_bytes".into(), json!(hashing_summary.processed_bytes));
                details.insert("skipped_files".into(), json!(hashing_summary.skipped_files));
                details.insert("skipped_bytes".into(), json!(hashing_summary.skipped_bytes));
                details.insert("total_time".into(), json!(hashing_summary.total_time));
                details.insert("transfer_rate".into(), json!(hashing_summary.transfer_rate));
                tc.record_event("com.amazon.rum.deadline.job_attachments.hashing_summary", details, false);
            }

            let ja_settings = queue.job_attachment_settings().unwrap();
            let s3_settings = JobAttachmentS3Settings::from_root_path(&format!(
                "{}/{}",
                ja_settings.s3_bucket_name(),
                ja_settings.root_prefix(),
            )).map_err(|e| op_err(e.to_string()))?;

            let upload_ctx = upload::S3UploadContext::new(s3_client, account_id, params.config)
                .map_err(|e| op_err(e.to_string()))?;

            let (upload_summary, attachments) = if let Some(ref snap_dir) = params.debug_snapshot_dir {
                // F8: Snapshot assets locally instead of uploading to S3
                upload::snapshot_assets(
                    &farm_id, &queue_id, &s3_settings,
                    std::path::Path::new(snap_dir),
                    &manifests,
                    params.upload_progress_callback,
                ).map_err(|e| op_err(e.to_string()))?
            } else {
                upload::upload_assets(
                    &farm_id, &queue_id, &s3_settings,
                    &manifests, &upload_ctx,
                    params.upload_progress_callback,
                    cache_dir_str,
                    Some(force_s3_check),
                ).await.map_err(|e| op_err(e.to_string()))?
            };

            if upload_summary.processed_files > 0 {
                print("Upload Summary:");
                for line in upload_summary.to_string().lines() {
                    print(&format!("    {line}"));
                }
            }

            // F5: Emit upload summary telemetry
            if let Some(tc) = params.telemetry {
                let mut details = std::collections::HashMap::new();
                details.insert("total_files".into(), json!(upload_summary.total_files));
                details.insert("total_bytes".into(), json!(upload_summary.total_bytes));
                details.insert("processed_files".into(), json!(upload_summary.processed_files));
                details.insert("processed_bytes".into(), json!(upload_summary.processed_bytes));
                details.insert("skipped_files".into(), json!(upload_summary.skipped_files));
                details.insert("skipped_bytes".into(), json!(upload_summary.skipped_bytes));
                details.insert("total_time".into(), json!(upload_summary.total_time));
                details.insert("transfer_rate".into(), json!(upload_summary.transfer_rate));
                tc.record_event("com.amazon.rum.deadline.job_attachments.upload_summary", details, false);
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

    // 9. Record submission telemetry and call CreateJob
    if let Some(tc) = params.telemetry {
        let mut details = std::collections::HashMap::new();
        details.insert("submitter_name".into(), Value::String(submitter_name.to_string()));
        tc.record_event("com.amazon.rum.deadline.submission", details, false);
    }

    // F8: If debug snapshot dir is set, save snapshot and return without calling CreateJob
    if let Some(ref snapshot_dir) = params.debug_snapshot_dir {
        // Pass full queue response and storage profile for complete snapshot
        let queue_json = {
            use deadline_api::responses::QueueResponse;
            let resp = QueueResponse::from(queue.clone());
            serde_json::to_value(&resp).unwrap_or_default()
        };
        let sp_json = storage_profile.as_ref().map(|sp| {
            serde_json::json!({
                "storageProfileId": sp.storage_profile_id,
                "displayName": sp.display_name,
                "osFamily": format!("{:?}", sp.os_family).to_uppercase(),
                "fileSystemLocations": sp.file_system_locations.iter().map(|loc| {
                    serde_json::json!({
                        "name": loc.name,
                        "path": loc.path,
                        "type": format!("{:?}", loc.location_type).to_uppercase(),
                    })
                }).collect::<Vec<_>>(),
            })
        });
        save_debug_snapshot(snapshot_dir, &create_job_args, &queue_json, sp_json.as_ref())?;
        return Ok(None);
    }

    let response = api::create_job(&create_job_args, params.config).await?;

    let job_id = response.job_id().to_string();

    // 10. Poll for completion
    print("Waiting for Job to be created...");

    let continue_cb = params.continue_callback.unwrap_or_else(|| Box::new(|| true));

    let (success, status_message) = api::wait_for_create_job_to_complete(
        &farm_id, &queue_id, &job_id, params.config, &*continue_cb,
    ).await?;

    // Record create_job telemetry
    if let Some(tc) = params.telemetry {
        let mut details = std::collections::HashMap::new();
        details.insert("is_success".into(), Value::Bool(success));
        tc.record_event("com.amazon.rum.deadline.create_job", details, false);
    }

    if !success {
        return Err(op_err(format!("Job {job_id} creation failed: {status_message}")));
    }

    print(&format!("Submitted job bundle:\n   {}", params.job_bundle_dir));
    print(&format!("{status_message}\n{job_id}"));

    // 11. Execute post-submission hooks
    if hook_manager.hooks.as_ref().is_some_and(|h| !h.post_submission.is_empty()) {
        let template_obj = parse_yaml_or_json_content(&file_contents, &file_type, &params.job_bundle_dir, "template")
            .unwrap_or(serde_json::json!({}));
        let post_metadata = HookMetadata {
            job_name: template_obj.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
            priority: params.priority.unwrap_or(50),
            farm_id: farm_id.clone(),
            queue_id: queue_id.clone(),
            job_bundle_dir: std::fs::canonicalize(&params.job_bundle_dir)
                .unwrap_or_else(|_| PathBuf::from(&params.job_bundle_dir))
                .to_string_lossy().into_owned(),
            parameters: std::collections::HashMap::new(),
            submitter_name: submitter_name.to_string(),
            asset_references: serde_json::json!({}),
            submission_payload: serde_json::json!({}),
            storage_profile_id: if storage_profile_id.is_empty() { None } else { Some(storage_profile_id.clone()) },
            job_id: Some(job_id.clone()),
        };
        hook_manager.execute_post_submission_hooks(&post_metadata);
    }

    Ok(Some(job_id))
}

/// Write a debug snapshot of the CreateJob payload and helper scripts.
fn save_debug_snapshot(
    snapshot_dir: &str,
    create_job_args: &serde_json::Map<String, Value>,
    queue_json: &Value,
    storage_profile_json: Option<&Value>,
) -> Result<(), DeadlineError> {
    use std::fs;
    use std::io::Write;

    fs::create_dir_all(snapshot_dir)
        .map_err(|e| op_err(format!("Failed to create snapshot dir: {e}")))?;

    // 1. create_job_args.json
    let args_json = serde_json::to_string_pretty(&Value::Object(create_job_args.clone()))
        .map_err(|e| op_err(format!("Failed to serialize create_job_args: {e}")))?;
    fs::write(
        std::path::Path::new(snapshot_dir).join("create_job_args.json"),
        &args_json,
    ).map_err(|e| op_err(format!("Failed to write create_job_args.json: {e}")))?;

    // 2. Per-parameter files + CLI args list
    let mut cli_args: Vec<(String, String)> = Vec::new();
    for (param_name, param_value) in create_job_args {
        let kebab = camel_to_kebab(param_name);
        match param_value {
            Value::Object(_) | Value::Array(_) => {
                let file_name = format!("{kebab}_param.json");
                let content = serde_json::to_string_pretty(param_value)
                    .unwrap_or_default();
                let _ = fs::write(
                    std::path::Path::new(snapshot_dir).join(&file_name),
                    &content,
                );
                cli_args.push((format!("--{kebab}"), format!("file://{file_name}")));
            }
            Value::String(s) if s.contains('\n') => {
                let file_name = format!("{kebab}_param.data");
                let _ = fs::write(
                    std::path::Path::new(snapshot_dir).join(&file_name),
                    s.as_bytes(),
                );
                cli_args.push((format!("--{kebab}"), format!("file://{file_name}")));
            }
            _ => {
                cli_args.push((format!("--{kebab}"), param_value.to_string().trim_matches('"').to_string()));
            }
        }
    }

    // Determine S3 path for attachment upload commands in scripts
    let s3_base = queue_json.get("jobAttachmentSettings").and_then(|ja| {
        let bucket = ja.get("s3BucketName")?.as_str()?;
        let prefix = ja.get("rootPrefix")?.as_str()?;
        Some(format!("s3://{bucket}/{prefix}"))
    });
    let has_attachments = create_job_args.contains_key("attachments");

    // 3. submit_job.sh
    let sh_path = std::path::Path::new(snapshot_dir).join("submit_job.sh");
    let mut sh = fs::File::create(&sh_path)
        .map_err(|e| op_err(format!("Failed to create submit_job.sh: {e}")))?;
    writeln!(sh, "#!/bin/sh").ok();
    writeln!(sh, "# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.").ok();
    writeln!(sh, "set -xeuo pipefail").ok();
    writeln!(sh, "cd \"$(dirname \"$0\")\"").ok();
    writeln!(sh).ok();
    if has_attachments {
        if let Some(ref base) = s3_base {
            write_s3_copy_commands(&mut sh, base, " \\\n")
                .map_err(|e| op_err(format!("Failed to write submit_job.sh: {e}")))?;
        }
    }
    write_create_job_commands(&mut sh, &cli_args, " \\\n", shell_quote)
        .map_err(|e| op_err(format!("Failed to write submit_job.sh: {e}")))?;

    // 4. submit_job.bat
    let bat_path = std::path::Path::new(snapshot_dir).join("submit_job.bat");
    let mut bat = fs::File::create(&bat_path)
        .map_err(|e| op_err(format!("Failed to create submit_job.bat: {e}")))?;
    writeln!(bat, "REM Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.").ok();
    writeln!(bat, "cd /d \"%~dp0\"").ok();
    writeln!(bat).ok();
    if has_attachments {
        if let Some(ref base) = s3_base {
            write_s3_copy_commands(&mut bat, base, " ^\r\n")
                .map_err(|e| op_err(format!("Failed to write submit_job.bat: {e}")))?;
        }
    }
    write_create_job_commands(&mut bat, &cli_args, " ^\r\n", bat_quote)
        .map_err(|e| op_err(format!("Failed to write submit_job.bat: {e}")))?;

    // 5. queue.json — full queue response
    let queue_str = serde_json::to_string_pretty(queue_json)
        .unwrap_or_else(|_| "{}".to_string());
    fs::write(
        std::path::Path::new(snapshot_dir).join("queue.json"),
        &queue_str,
    ).map_err(|e| op_err(format!("Failed to write queue.json: {e}")))?;

    // 6. storage_profile.json — when storage profile is configured
    if let Some(sp) = storage_profile_json {
        let sp_str = serde_json::to_string_pretty(sp)
            .unwrap_or_else(|_| "{}".to_string());
        fs::write(
            std::path::Path::new(snapshot_dir).join("storage_profile.json"),
            &sp_str,
        ).map_err(|e| op_err(format!("Failed to write storage_profile.json: {e}")))?;
    }

    Ok(())
}

fn write_s3_copy_commands(
    w: &mut impl std::io::Write,
    s3_base: &str,
    continuation: &str,
) -> std::io::Result<()> {
    for subdir in ["Data", "Manifests"] {
        write!(w, "aws s3 cp{continuation}", )?;
        write!(w, "    --recursive{continuation}")?;
        write!(w, "    ./{subdir}{continuation}")?;
        writeln!(w, "    {s3_base}/{subdir}")?;
        writeln!(w)?;
    }
    Ok(())
}

fn write_create_job_commands(
    w: &mut impl std::io::Write,
    cli_args: &[(String, String)],
    continuation: &str,
    quote_fn: fn(&str) -> String,
) -> std::io::Result<()> {
    write!(w, "aws deadline create-job")?;
    for (flag, val) in cli_args {
        write!(w, "{continuation}    {flag} {}", quote_fn(val))?;
    }
    writeln!(w)?;
    Ok(())
}

/// Unix shell quoting: wrap in single quotes, matching Python's shlex.join().
fn shell_quote(s: &str) -> String {
    if s.is_empty() {
        return "''".to_string();
    }
    // Wrap in single quotes, escaping any embedded single quotes
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Windows cmd.exe quoting: wrap in double quotes.
fn bat_quote(s: &str) -> String {
    if s.is_empty() {
        return "\"\"".to_string();
    }
    format!("\"{}\"", s.replace('"', "\\\""))
}

/// Convert camelCase to kebab-case.
fn camel_to_kebab(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            result.push('-');
        }
        result.push(c.to_ascii_lowercase());
    }
    result
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

    // known_asset_paths must split on path-list separator (: on Unix, ; on Windows)
    // not on directory separator (/ on Unix, \ on Windows)
    #[test]
    fn path_list_separator_splits_correctly() {
        let sep = if cfg!(windows) { ';' } else { ':' };
        let input = "/mnt/shared:/home/user";
        let paths: Vec<&str> = input.split(sep).collect();
        assert_eq!(paths, vec!["/mnt/shared", "/home/user"]);

        // Verify MAIN_SEPARATOR would destroy paths (the bug)
        let bad: Vec<&str> = input.split(std::path::MAIN_SEPARATOR).collect();
        assert!(bad.len() > 2, "MAIN_SEPARATOR splits paths incorrectly: {bad:?}");
    }
}

/// Convert GetStorageProfileForQueueOutput to a serde_json::Value matching the API JSON shape.
fn storage_profile_output_to_value(output: &aws_sdk_deadline::operation::get_storage_profile_for_queue::GetStorageProfileForQueueOutput) -> serde_json::Value {
    deadline_api::type_conversions::storage_profile_output_to_value(output)
}
