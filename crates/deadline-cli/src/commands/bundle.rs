use clap::Subcommand;
use deadline_config::config_file;
use deadline_job_bundle::{SubmissionHandler, SubmitJobParams, create_job_from_job_bundle};
use regex::Regex;
use std::sync::LazyLock;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

/// CLI handler: prints messages to stdout, confirms via stdout + sigint check.
struct CliSubmissionHandler;

impl SubmissionHandler for CliSubmissionHandler {
    fn on_message(&self, msg: &str) {
        println!("{msg}");
    }
    fn confirm(&self, msg: &str, _default: bool) -> bool {
        println!("{msg}");
        crate::common::should_continue()
    }
    fn should_continue(&self) -> bool {
        crate::common::should_continue()
    }
}

static OPENJD_IDENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").expect("valid regex"));

#[derive(Subcommand)]
pub(crate) enum BundleAction {
    /// Submit an Open Job Description job bundle to a Deadline Cloud queue
    Submit {
        /// Path to the job bundle directory
        job_bundle_dir: String,

        /// Parameter overrides in Name=Value format (repeatable)
        #[arg(short = 'p', long = "parameter", num_args = 1)]
        parameter: Vec<String>,

        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long)]
        queue_id: Option<String>,
        #[arg(long)]
        storage_profile_id: Option<String>,

        /// Override the job name from the template
        #[arg(long)]
        name: Option<String>,

        /// Job priority (default: 50)
        #[arg(long, default_value = "50")]
        priority: i32,

        /// Maximum failed tasks before the job fails
        #[arg(long)]
        max_failed_tasks_count: Option<i32>,

        /// Maximum retries per task
        #[arg(long)]
        max_retries_per_task: Option<i32>,

        /// Maximum worker count
        #[arg(long)]
        max_worker_count: Option<i32>,

        /// Initial task run status (READY or SUSPENDED)
        #[arg(long, value_parser = ["READY", "SUSPENDED"])]
        target_task_run_status: Option<String>,

        /// How workers access job attachments (COPIED or VIRTUAL)
        #[arg(long, value_parser = ["COPIED", "VIRTUAL"])]
        job_attachments_file_system: Option<String>,

        /// Skip confirmation prompts
        #[arg(long)]
        yes: bool,

        /// Error if any input paths are missing
        #[arg(long)]
        require_paths_exist: bool,

        /// Name of the submitting application
        #[arg(long)]
        submitter_name: Option<String>,

        /// Paths that should not generate warnings (repeatable)
        #[arg(long = "known-asset-path")]
        known_asset_path: Vec<String>,

        /// Force S3 existence verification
        #[arg(long = "force-s3-check", overrides_with = "no_force_s3_check")]
        force_s3_check: bool,

        /// Skip S3 existence verification
        #[arg(long = "no-force-s3-check", overrides_with = "force_s3_check")]
        no_force_s3_check: bool,

        /// EXPERIMENTAL — Save a debug snapshot instead of submitting.
        /// Generates a directory (or .zip) with `CreateJob` args and scripts.
        #[arg(long = "save-debug-snapshot")]
        save_debug_snapshot: Option<String>,
    },

    /// Open a GUI to submit an Open Job Description job bundle
    #[command(name = "gui-submit")]
    GuiSubmit {
        /// Path to the job bundle directory
        job_bundle_dir: Option<String>,

        /// Initial parameter values for the GUI (repeatable)
        #[arg(short = 'p', long = "parameter", num_args = 1)]
        parameter: Vec<String>,

        /// Open a folder browser to select a bundle
        #[arg(long)]
        browse: bool,

        /// Install GUI dependencies if not already installed
        #[arg(long)]
        install_gui: bool,

        /// [DEPRECATED] Use --submitter-info `submitter_name`=<name> instead
        #[arg(long)]
        submitter_name: Option<String>,

        /// Output format: verbose or json
        #[arg(long, default_value = "verbose", ignore_case = true, value_parser = ["verbose", "json"])]
        output: String,

        /// Paths that should not generate warnings (repeatable)
        #[arg(long = "known-asset-path")]
        known_asset_path: Vec<String>,

        /// Submitter and environment information (key=value, JSON, or file://)
        #[arg(long = "submitter-info", num_args = 1)]
        submitter_info: Vec<String>,

        /// Override the job name shown in the GUI
        #[arg(long)]
        name: Option<String>,
    },
}

fn parse_parameters(raw: &[String]) -> Result<Vec<serde_json::Value>, CliError> {
    let mut result = Vec::new();
    for param in raw {
        let Some((name, value)) = param.split_once('=') else {
            return Err(CliError::Operation(format!(
                "Parameters must be provided in the format \"ParamName=Value\". Invalid parameter: {param}"
            )));
        };
        if !OPENJD_IDENT_RE.is_match(name) {
            return Err(CliError::Operation(format!(
                "Parameter names must be alphanumeric Open Job Description identifiers. Invalid parameter name: {name}"
            )));
        }
        result.push(serde_json::json!({"name": name, "value": value}));
    }
    Ok(result)
}

pub(crate) fn run(action: BundleAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

#[allow(
    clippy::too_many_lines,
    reason = "bundle submit orchestrates validation, attachment upload, and job creation"
)]
async fn run_async(action: BundleAction) -> Result<(), CliError> {
    match action {
        BundleAction::Submit {
            job_bundle_dir,
            parameter,
            profile,
            farm_id,
            queue_id,
            storage_profile_id,
            name,
            priority,
            max_failed_tasks_count,
            max_retries_per_task,
            max_worker_count,
            target_task_run_status,
            job_attachments_file_system,
            yes,
            require_paths_exist,
            submitter_name,
            known_asset_path,
            force_s3_check,
            no_force_s3_check,
            save_debug_snapshot,
        } => {
            let job_parameters = parse_parameters(&parameter)?;

            // Apply CLI options to config
            let mut config =
                config_file::read_config().map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(),
                    farm_id: farm_id.clone(),
                    queue_id: queue_id.clone(),
                    job_id: None,
                    yes,
                    ..Default::default()
                },
                &["farm_id", "queue_id"],
            )?;

            // Handle storage_profile_id separately (not in shared CliOptions)
            if let Some(ref sp) = storage_profile_id {
                config_file::set_setting("settings.storage_profile_id", sp, &mut config)
                    .map_err(|e| CliError::Operation(e.to_string()))?;
            }

            // Resolve force_s3_check: explicit flags > config
            let resolved_force_s3_check = if force_s3_check {
                Some(true)
            } else if no_force_s3_check {
                Some(false)
            } else {
                None
            };

            let hash_progress = std::sync::Mutex::new(crate::common::ProgressBarManager::new(
                100,
                "Hashing Attachments",
            ));
            let upload_progress = std::sync::Mutex::new(crate::common::ProgressBarManager::new(
                100,
                "Uploading Attachments",
            ));

            let telemetry = deadline_api::telemetry::create_telemetry(&config);

            // F8: If snapshot path ends in .zip, use a temp dir then zip after
            let snapshot_tmpdir: Option<std::path::PathBuf> = if save_debug_snapshot
                .as_ref()
                .is_some_and(|p| {
                    std::path::Path::new(p.as_str())
                        .extension()
                        .is_some_and(|e| e.eq_ignore_ascii_case("zip"))
                }) {
                let tmp =
                    std::env::temp_dir().join(format!("deadline-snapshot-{}", std::process::id()));
                std::fs::create_dir_all(&tmp)
                    .map_err(|e| CliError::Operation(format!("Failed to create temp dir: {e}")))?;
                Some(tmp)
            } else {
                None
            };
            let effective_snapshot_dir = match (&save_debug_snapshot, &snapshot_tmpdir) {
                (Some(_), Some(tmp)) => Some(tmp.to_string_lossy().to_string()),
                (Some(p), None) => Some(p.clone()),
                _ => None,
            };

            let submit_params = SubmitJobParams {
                job_bundle_dir: job_bundle_dir.clone(),
                job_parameters,
                name,
                priority: Some(priority),
                max_failed_tasks_count: Some(max_failed_tasks_count.unwrap_or_else(|| {
                    config_file::get_setting("settings.max_failed_tasks_count", &config)
                        .unwrap_or_else(|_| "20".into())
                        .parse::<i32>()
                        .unwrap_or(20)
                })),
                max_retries_per_task: Some(max_retries_per_task.unwrap_or_else(|| {
                    config_file::get_setting("settings.max_retries_per_task", &config)
                        .unwrap_or_else(|_| "5".into())
                        .parse::<i32>()
                        .unwrap_or(5)
                })),
                max_worker_count,
                target_task_run_status,
                job_attachments_file_system,
                require_paths_exist,
                submitter_name: Some(submitter_name.unwrap_or_else(|| "CLI".into())),
                known_asset_paths: known_asset_path,
                auto_accept: yes
                    || config_file::str2bool(
                        &config_file::get_setting("settings.auto_accept", &config)
                            .unwrap_or_default(),
                    )
                    .unwrap_or(false),
                force_s3_check: resolved_force_s3_check,
                debug_snapshot_dir: effective_snapshot_dir,
                config: &config,
                handler: &CliSubmissionHandler,
                hashing_progress_callback: Some(Box::new(move |processed, total| {
                    let pct = if total > 0 { processed * 100 / total } else { 100 };
                    hash_progress
                        .lock()
                        .expect("lock poisoned")
                        .callback(pct);
                    true
                })),
                upload_progress_callback: Some(Box::new(move |processed, total| {
                    let pct = if total > 0 { processed * 100 / total } else { 100 };
                    upload_progress
                        .lock()
                        .expect("lock poisoned")
                        .callback(pct);
                    true
                })),
                telemetry: Some(&telemetry),
            };

            let job_id = match create_job_from_job_bundle(submit_params).await {
                Ok(id) => id,
                Err(e) => {
                    // F6: Emit error telemetry before flushing
                    let mut details = std::collections::HashMap::new();
                    details.insert("exception_scope".into(), serde_json::json!("on_submit"));
                    details.insert(
                        "exception_type".into(),
                        serde_json::json!("DeadlineOperationError"),
                    );
                    telemetry.record_event("com.amazon.rum.deadline.error", details, false);
                    drop(telemetry); // Flush telemetry before exit
                    let farm =
                        config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
                    let queue =
                        config_file::get_setting("defaults.queue_id", &config).unwrap_or_default();
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        "CreateJob",
                        Some(&farm),
                        Some(&queue),
                        None,
                        &config,
                    )
                    .await;
                    return Err(CliError::Operation(format!("{e}{suggestion}")));
                }
            };

            // Flush telemetry events before process exits
            drop(telemetry);

            if let Some(ref snap_path) = save_debug_snapshot {
                // F8: If zip mode, create the zip from the temp dir
                if let Some(ref tmp) = snapshot_tmpdir {
                    if let Some(parent) = std::path::Path::new(snap_path).parent() {
                        std::fs::create_dir_all(parent).ok();
                    }
                    create_zip_from_dir(tmp, snap_path)
                        .map_err(|e| CliError::Operation(format!("Failed to create zip: {e}")))?;
                    let _ = std::fs::remove_dir_all(tmp);
                }
                println!("Saved job debug snapshot:");
                println!("    {snap_path}");
            }

            // Update defaults.job_id only when no CLI overrides were provided
            if profile.is_none()
                && farm_id.is_none()
                && queue_id.is_none()
                && storage_profile_id.is_none()
                && let Some(ref id) = job_id
            {
                let _ = config_file::set_setting_to_disk("defaults.job_id", id);
            }

            Ok(())
        }
        BundleAction::GuiSubmit {
            job_bundle_dir,
            parameter,
            browse,
            install_gui,
            submitter_name,
            output,
            known_asset_path,
            submitter_info,
            name,
        } => {
            // Validate --submitter-info
            let submitter_info_json = if !submitter_info.is_empty() {
                Some(validate_submitter_info(
                    &submitter_info,
                    submitter_name.as_deref(),
                )?)
            } else if let Some(ref name) = submitter_name {
                eprintln!(
                    "DeprecationWarning: The option --submitter-name is deprecated. Use --submitter-info instead."
                );
                Some(serde_json::json!({"submitter_name": name}))
            } else {
                None
            };

            // Validate parameters
            let job_parameters = parse_parameters(&parameter)?;

            // Build params JSON for the Python entry point
            let params = serde_json::json!({
                "job_bundle_dir": job_bundle_dir,
                "browse": browse,
                "output": output.to_lowercase(),
                "known_asset_paths": known_asset_path,
                "submitter_info": submitter_info_json,
                "job_parameters": job_parameters,
                "name": name,
            });

            let python = super::gui::find_python()?;
            let stdout =
                super::gui::launch_gui(&python, "gui-submit", &params.to_string(), install_gui)?;

            if !stdout.trim().is_empty() {
                print!("{stdout}");
            }
            Ok(())
        }
    }
}

const SUBMITTER_INFO_FIELDS: &[&str] = &[
    "submitter_name",
    "submitter_package_name",
    "submitter_package_version",
    "host_application_name",
    "host_application_version",
    "additional_info",
];

/// Validate `--submitter-info` values and merge into a single JSON object.
fn validate_submitter_info(
    values: &[String],
    deprecated_name: Option<&str>,
) -> Result<serde_json::Value, CliError> {
    let mut merged = serde_json::Map::new();

    for val in values {
        let val = val.trim();
        if val.starts_with('{') {
            let obj: serde_json::Value = serde_json::from_str(val).map_err(|e| {
                CliError::Operation(format!("Invalid JSON in --submitter-info '{val}': {e}"))
            })?;
            let map = obj.as_object().ok_or_else(|| {
                CliError::Operation(format!(
                    "--submitter-info JSON must be an object, got: {val}"
                ))
            })?;
            for (k, v) in map {
                merged.insert(k.clone(), v.clone());
            }
        } else if let Some(path) = val.strip_prefix("file://") {
            let content = std::fs::read_to_string(path).map_err(|e| {
                CliError::Operation(format!("Cannot read --submitter-info file '{path}': {e}"))
            })?;
            let ext = std::path::Path::new(path)
                .extension()
                .and_then(|e| e.to_str());
            let obj: serde_json::Value = if ext
                .is_some_and(|e| e.eq_ignore_ascii_case("yaml") || e.eq_ignore_ascii_case("yml"))
            {
                serde_yaml::from_str(&content)
                    .map_err(|e| CliError::Operation(format!("Invalid YAML in '{path}': {e}")))?
            } else {
                serde_json::from_str(&content)
                    .map_err(|e| CliError::Operation(format!("Invalid JSON in '{path}': {e}")))?
            };
            let map = obj.as_object().ok_or_else(|| {
                CliError::Operation(format!("File '{path}' must contain a JSON object"))
            })?;
            for (k, v) in map {
                merged.insert(k.clone(), v.clone());
            }
        } else if let Some((key, value)) = val.split_once('=') {
            merged.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
        } else {
            return Err(CliError::Operation(format!(
                "--submitter-info '{val}' not formatted correctly. \
                 Use key=value, inline JSON, or file://path."
            )));
        }
    }

    // Apply deprecated --submitter-name (takes precedence)
    if let Some(name) = deprecated_name {
        eprintln!(
            "DeprecationWarning: The option --submitter-name is deprecated. Use --submitter-info instead."
        );
        merged.insert(
            "submitter_name".to_owned(),
            serde_json::Value::String(name.to_owned()),
        );
    }

    // Validate field names
    for key in merged.keys() {
        if !SUBMITTER_INFO_FIELDS.contains(&key.as_str()) {
            return Err(CliError::Operation(format!(
                "Unknown field '{key}' in --submitter-info. Valid fields are: {}",
                SUBMITTER_INFO_FIELDS.join(", ")
            )));
        }
    }

    // Require submitter_name
    if !merged.contains_key("submitter_name") {
        return Err(CliError::Operation(
            "submitter_name is required when using --submitter-info. \
             Example: --submitter-info submitter_name=MyApp"
                .into(),
        ));
    }

    Ok(serde_json::Value::Object(merged))
}

/// Create a zip file from a directory's contents.
fn create_zip_from_dir(src_dir: &std::path::Path, zip_path: &str) -> Result<(), String> {
    let status = std::process::Command::new("zip")
        .args(["-r", "-j", zip_path, "."])
        .current_dir(src_dir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map_err(|e| format!("Failed to run zip: {e}"))?;
    if !status.success() {
        return Err(format!("zip exited with status {status}"));
    }
    Ok(())
}
