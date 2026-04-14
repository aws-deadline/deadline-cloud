use clap::Subcommand;
use deadline_config::config_file;
use deadline_job_bundle::{create_job_from_job_bundle, SubmitJobParams};
use regex::Regex;
use std::sync::LazyLock;

use super::config::CliError;

static OPENJD_IDENT_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_][A-Za-z0-9_]*$").unwrap());

#[derive(Subcommand)]
pub enum BundleAction {
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
        #[arg(long)]
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

pub fn run(action: BundleAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

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
        } => {
            let job_parameters = parse_parameters(&parameter)?;

            // Apply CLI options to config
            let mut config = config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile: profile.clone(),
                    farm_id: farm_id.clone(),
                    queue_id: queue_id.clone(),
                    job_id: None,
                    yes,
                },
                &["farm_id", "queue_id"],
            )?;

            // Handle storage_profile_id separately (not in shared CliOptions)
            if let Some(ref sp) = storage_profile_id {
                config_file::set_setting_in_config("settings.storage_profile_id", sp, &mut config)
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

            let hash_progress = std::sync::Mutex::new(
                crate::common::ProgressBarManager::new(100, "Hashing Attachments"),
            );
            let upload_progress = std::sync::Mutex::new(
                crate::common::ProgressBarManager::new(100, "Uploading Attachments"),
            );

            let submit_params = SubmitJobParams {
                job_bundle_dir: job_bundle_dir.clone(),
                job_parameters,
                name,
                priority: Some(priority),
                max_failed_tasks_count,
                max_retries_per_task,
                max_worker_count,
                target_task_run_status,
                job_attachments_file_system,
                require_paths_exist,
                submitter_name: Some(submitter_name.unwrap_or_else(|| "CLI".into())),
                known_asset_paths: known_asset_path,
                auto_accept: yes || config_file::str2bool(
                    &config_file::get_setting_with_config("settings.auto_accept", &config)
                        .unwrap_or_default(),
                ).unwrap_or(false),
                force_s3_check: resolved_force_s3_check,
                config: Some(&config),
                print_callback: Box::new(|msg| println!("{msg}")),
                hashing_progress_callback: Some(Box::new(move |meta| {
                    hash_progress.lock().unwrap().callback(meta.progress as u64)
                })),
                upload_progress_callback: Some(Box::new(move |meta| {
                    upload_progress.lock().unwrap().callback(meta.progress as u64)
                })),
                continue_callback: None,
            };

            let job_id = create_job_from_job_bundle(submit_params).await
                .map_err(|e| CliError::Operation(e.to_string()))?;

            // Update defaults.job_id only when no CLI overrides were provided
            if profile.is_none()
                && farm_id.is_none()
                && queue_id.is_none()
                && storage_profile_id.is_none()
            {
                if let Some(ref id) = job_id {
                    let _ = config_file::set_setting("defaults.job_id", id);
                }
            }

            Ok(())
        }
    }
}
