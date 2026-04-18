use std::fs;
use std::path::{Path, PathBuf};

use chrono::{Duration, Local, Utc};
use clap::Subcommand;
use deadline_api::api;
use deadline_config::config_file;
use deadline_api::telemetry::create_telemetry;
use deadline_job_attachments::incremental_download::IncrementalDownloadState;
use deadline_job_attachments::incremental_download::IncrementalDownloadJob;
use deadline_job_attachments::models::FileConflictResolution;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

const DOWNLOAD_CHECKPOINT_FILE_NAME: &str = "download_checkpoint.json";
const DEFAULT_CHECKPOINT_DIR: &str = "~/.deadline/incremental_download";

#[derive(Subcommand)]
pub enum QueueAction {
    /// List available queues
    List {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
    },
    /// Get details of a specific queue
    Get {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
    },
    /// Export queue credentials for use with AWS CLI
    ExportCredentials {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        /// USER (default) or READ
        #[arg(long, default_value = "USER")]
        mode: String,
    },
    /// Get a storage profile for a queue
    GetStorageProfile {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] storage_profile_id: String,
    },
    /// List queue parameter definitions from queue environments
    Paramdefs {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
    },
    /// Download new job attachment output for all jobs in a queue
    #[command(name = "sync-output")]
    SyncOutput {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] storage_profile_id: Option<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
        /// Lookback minutes at bootstrap
        #[arg(long, default_value = "0")]
        bootstrap_lookback_minutes: f64,
        /// Directory for checkpoint files
        #[arg(long, default_value = DEFAULT_CHECKPOINT_DIR)]
        checkpoint_dir: String,
        /// Force re-bootstrap from lookback
        #[arg(long)]
        force_bootstrap: bool,
        /// Ignore storage profile configuration
        #[arg(long)]
        ignore_storage_profiles: bool,
        /// File conflict resolution: SKIP, OVERWRITE, CREATE_COPY
        #[arg(long, default_value = "OVERWRITE")]
        conflict_resolution: String,
        /// Perform a dry run without downloading
        #[arg(long)]
        dry_run: bool,
    },
}

pub fn run(action: QueueAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

fn setup(profile: Option<String>, farm_id: Option<String>, queue_id: Option<String>, required: &[&str]) -> Result<deadline_config::ini::IniConfig, CliError> {
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    crate::common::apply_cli_options_to_config(
        &mut config,
        &crate::common::CliOptions { profile, farm_id, queue_id, job_id: None, yes: false, ..Default::default() },
        required,
    )?;
    Ok(config)
}

// =========================================================================
// PID file lock
// =========================================================================

struct PidFileLock {
    path: PathBuf,
}

impl PidFileLock {
    fn acquire(path: &Path, operation_name: &str) -> Result<Self, CliError> {
        let pid = std::process::id();
        let tmp_path = path.with_extension(format!("{}~tmp", pid));

        // Write PID to temp file
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        fs::write(&tmp_path, pid.to_string())
            .map_err(|e| CliError::Operation(format!("Failed to create PID lock: {e}")))?;

        // Try atomic rename/link
        if Self::try_claim(&tmp_path, path) {
            return Ok(Self { path: path.to_path_buf() });
        }

        // Lock exists — check if holder is alive
        if let Ok(contents) = fs::read_to_string(path) {
            if let Ok(holder_pid) = contents.trim().parse::<u32>() {
                if Self::pid_exists(holder_pid) {
                    let _ = fs::remove_file(&tmp_path);
                    return Err(CliError::Operation(format!(
                        "Unable to perform {operation_name} as process with pid {holder_pid} already holds the lock {}",
                        path.display()
                    )));
                }
                // Stale lock — remove it
                let _ = fs::remove_file(path);
            } else {
                // Corrupt lock file
                let _ = fs::remove_file(path);
            }
        }

        // Retry after cleanup
        if Self::try_claim(&tmp_path, path) {
            return Ok(Self { path: path.to_path_buf() });
        }

        let _ = fs::remove_file(&tmp_path);
        Err(CliError::Operation(format!(
            "Unable to perform {operation_name} as another process already holds the lock {}",
            path.display()
        )))
    }

    fn try_claim(tmp: &Path, target: &Path) -> bool {
        // On POSIX, hard link is atomic and fails if target exists
        #[cfg(unix)]
        {
            if std::fs::hard_link(tmp, target).is_ok() {
                let _ = fs::remove_file(tmp);
                return true;
            }
            false
        }
        #[cfg(not(unix))]
        {
            // On Windows, rename fails if target exists
            fs::rename(tmp, target).is_ok()
        }
    }

    fn pid_exists(pid: u32) -> bool {
        #[cfg(unix)]
        {
            // Signal 0 checks if process exists without sending a signal
            unsafe { libc::kill(pid as i32, 0) == 0 }
        }
        #[cfg(not(unix))]
        {
            // Conservative: assume alive
            true
        }
    }
}

impl Drop for PidFileLock {
    fn drop(&mut self) {
        // Only remove if we still own it (our PID matches)
        if let Ok(contents) = fs::read_to_string(&self.path) {
            if contents.trim() == std::process::id().to_string() {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}

// =========================================================================
// Subcommand dispatch
// =========================================================================

async fn run_async(action: QueueAction) -> Result<(), CliError> {
    match action {
        QueueAction::List { profile, farm_id } => {
            let config = setup(profile, farm_id, None, &["farm_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            match api::list_queues(&farm, Some(&config), None).await {
                Ok(resp) => {
                    let empty = vec![];
                    let queues = resp["queues"].as_array().unwrap_or(&empty);
                    let structured: Vec<serde_json::Value> = queues
                        .iter()
                        .map(|q| serde_json::json!({"queueId": q["queueId"], "displayName": q["displayName"]}))
                        .collect();
                    println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), None, None, Some(&config),
                    ).await;
                    Err(CliError::Operation(format!(
                        "Failed to get Queues from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        QueueAction::Get { profile, farm_id, queue_id } => {
            let config = setup(profile, farm_id, queue_id, &["farm_id", "queue_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let queue = config_file::get_setting_with_config("defaults.queue_id", &config).unwrap_or_default();
            match api::get_queue(&farm, &queue, Some(&config), None).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    Err(CliError::Operation(format!(
                        "Failed to get Queue from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        QueueAction::ExportCredentials { profile, farm_id, queue_id, mode } => {
            let start = std::time::Instant::now();
            let config = setup(profile, farm_id, queue_id, &["farm_id", "queue_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let queue = config_file::get_setting_with_config("defaults.queue_id", &config).unwrap_or_default();

            let telemetry = create_telemetry(Some(&config));

            let result = match mode.to_uppercase().as_str() {
                "READ" => api::assume_queue_role_for_read(&farm, &queue, Some(&config), Some(&telemetry)).await,
                _ => api::assume_queue_role_for_user(&farm, &queue, Some(&config), Some(&telemetry)).await,
            };

            let duration_ms = start.elapsed().as_millis() as u64;
            let mut details = std::collections::HashMap::new();
            details.insert("mode".into(), serde_json::json!(mode.to_uppercase()));
            details.insert("queue_id".into(), serde_json::json!(queue));
            details.insert("duration_ms".into(), serde_json::json!(duration_ms));

            match result {
                Ok(resp) => {
                    let creds = &resp["credentials"];
                    let access_key = creds["accessKeyId"].as_str();
                    let secret_key = creds["secretAccessKey"].as_str();
                    let session_token = creds["sessionToken"].as_str();
                    let expiration = creds["expiration"].as_str();

                    if let (Some(ak), Some(sk), Some(st), Some(exp)) =
                        (access_key, secret_key, session_token, expiration)
                    {
                        details.insert("is_success".into(), serde_json::json!(true));
                        telemetry.record_event("com.amazon.rum.deadline.queue_export_credentials", details, false);

                        let output = serde_json::json!({
                            "Version": 1,
                            "AccessKeyId": ak,
                            "SecretAccessKey": sk,
                            "SessionToken": st,
                            "Expiration": exp.replacen(' ', "T", 1),
                        });
                        println!("{}", serde_json::to_string_pretty(&output).unwrap());
                        Ok(())
                    } else {
                        details.insert("is_success".into(), serde_json::json!(false));
                        details.insert("error_type".into(), serde_json::json!("MissingCredentials"));
                        telemetry.record_event("com.amazon.rum.deadline.queue_export_credentials", details, false);
                        Err(CliError::Operation(
                            "Failed to export credentials:\nResponse missing required credential fields".into()
                        ))
                    }
                }
                Err(e) => {
                    details.insert("is_success".into(), serde_json::json!(false));
                    details.insert("error_type".into(), serde_json::json!(e.to_string()));
                    telemetry.record_event("com.amazon.rum.deadline.queue_export_credentials", details, false);
                    Err(CliError::Operation(format!("Failed to export credentials:\n{e}")))
                }
            }
        }
        QueueAction::GetStorageProfile { profile, farm_id, queue_id, storage_profile_id } => {
            let config = setup(profile, farm_id, queue_id, &["farm_id", "queue_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let queue = config_file::get_setting_with_config("defaults.queue_id", &config).unwrap_or_default();
            let resp = api::get_storage_profile_for_queue(&farm, &queue, &storage_profile_id, Some(&config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to get storage profile:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
        QueueAction::Paramdefs { profile, farm_id, queue_id } => {
            let config = setup(profile, farm_id, queue_id, &["farm_id", "queue_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let queue = config_file::get_setting_with_config("defaults.queue_id", &config).unwrap_or_default();
            match deadline_api::queue_parameters::get_queue_parameter_definitions(
                &farm, &queue, Some(&config), None,
            ).await {
                Ok(params) => {
                    println!("{}", crate::common::cli_object_repr(&serde_json::json!(params)));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), Some(&farm), Some(&queue), None, Some(&config),
                    ).await;
                    Err(CliError::Operation(format!(
                        "Failed to get Queue Parameter Definitions from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        QueueAction::SyncOutput {
            profile, farm_id, queue_id, storage_profile_id,
            json, bootstrap_lookback_minutes, checkpoint_dir,
            force_bootstrap, ignore_storage_profiles,
            conflict_resolution, dry_run,
        } => {
            run_sync_output(
                profile, farm_id, queue_id, storage_profile_id,
                json, bootstrap_lookback_minutes, checkpoint_dir,
                force_bootstrap, ignore_storage_profiles,
                conflict_resolution, dry_run,
            ).await
        }
    }
}

// =========================================================================
// sync-output implementation
// =========================================================================

#[allow(clippy::too_many_arguments)]
async fn run_sync_output(
    profile: Option<String>,
    farm_id: Option<String>,
    queue_id: Option<String>,
    storage_profile_id: Option<String>,
    _json_output: bool,
    bootstrap_lookback_minutes: f64,
    checkpoint_dir: String,
    force_bootstrap: bool,
    ignore_storage_profiles: bool,
    conflict_resolution: String,
    dry_run: bool,
) -> Result<(), CliError> {
    // Validate mutual exclusion
    if ignore_storage_profiles && storage_profile_id.is_some() {
        return Err(CliError::ExitCode {
            code: 2,
            message: "Options '--storage-profile-id' and '--ignore-storage-profiles' cannot be provided together".into(),
        });
    }

    // Expand checkpoint dir and create it
    let checkpoint_dir = expand_tilde(&checkpoint_dir);
    fs::create_dir_all(&checkpoint_dir).map_err(|e| {
        CliError::Operation(format!(
            "Failed to create checkpoint directory {}: {e}", checkpoint_dir.display()
        ))
    })?;

    // Check writable
    if !is_writable(&checkpoint_dir) {
        return Err(CliError::Operation(format!(
            "Download progress checkpoint directory {} exists but is not writable, please provide write permissions",
            checkpoint_dir.display()
        )));
    }

    // Apply CLI options including --storage-profile-id override
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    let opts = crate::common::CliOptions {
        profile, farm_id, queue_id, job_id: None, yes: false, ..Default::default()
    };
    crate::common::apply_cli_options_to_config(&mut config, &opts, &["farm_id", "queue_id"])?;

    // Override storage profile if provided via CLI
    if let Some(ref sp_id) = storage_profile_id {
        let _ = config_file::set_setting_in_config("settings.storage_profile_id", sp_id, &mut config);
    }

    let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
    let queue_id_str = config_file::get_setting_with_config("defaults.queue_id", &config).unwrap_or_default();

    // Resolve storage profile
    let local_storage_profile_id: Option<String> = if ignore_storage_profiles {
        eprintln!("Ignoring all storage profiles.");
        None
    } else {
        let sp_id = config_file::get_setting_with_config("settings.storage_profile_id", &config)
            .unwrap_or_default();
        if sp_id.is_empty() {
            return Err(CliError::Operation(
                "The sync-output operation requires a storage profile defined in the deadline client configuration or \
                 provided with the --storage-profile-id option.\n\n\
                 Storage profiles are used to generate path mappings when a job was submitted from a machine with a different \
                 operating system or file system mount locations than the machine downloading outputs. See \
                 https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/modeling-your-shared-filesystem-locations-with-storage-profiles.html \
                 for more information.\n\n\
                 If you only submit and download jobs from the same operating system and mount locations, you can use the --ignore-storage-profiles option."
                .into()
            ));
        }
        // Validate the storage profile exists
        api::get_storage_profile_for_queue(&farm, &queue_id_str, &sp_id, Some(&config), None)
            .await
            .map_err(|e| CliError::Operation(format!(
                "Could not retrieve the storage profile {sp_id:?} from Deadline Cloud:\n{e}"
            )))?;
        Some(sp_id)
    };

    // Build checkpoint file path
    let sp_label = local_storage_profile_id.as_deref().unwrap_or("ignore-storage-profiles");
    let checkpoint_file_name = format!("{queue_id_str}_{sp_label}_{DOWNLOAD_CHECKPOINT_FILE_NAME}");
    let checkpoint_file_path = checkpoint_dir.join(&checkpoint_file_name);

    // Get queue and validate job attachment settings
    let queue = api::get_queue(&farm, &queue_id_str, Some(&config), None)
        .await
        .map_err(|e| CliError::Operation(format!("Failed to get queue:\n{e}")))?;

    if queue.get("jobAttachmentSettings").is_none() {
        let display_name = queue["displayName"].as_str().unwrap_or(&queue_id_str);
        return Err(CliError::Operation(format!(
            "Queue '{display_name}' does not have job attachments configured."
        )));
    }

    let display_name = queue["displayName"].as_str().unwrap_or(&queue_id_str);
    eprintln!("Started incremental download for queue: {display_name}");
    eprintln!("Checkpoint: {}", checkpoint_file_path.display());
    eprintln!();

    // PID lock
    let pid_lock_path = checkpoint_dir.join(format!("{checkpoint_file_name}.pid"));
    let _lock = PidFileLock::acquire(&pid_lock_path, "incremental output download")?;

    // Load or bootstrap checkpoint
    let checkpoint: IncrementalDownloadState = if force_bootstrap || !checkpoint_file_path.exists() {
        let lookback = Duration::milliseconds((bootstrap_lookback_minutes * 60_000.0) as i64);
        let bootstrap_timestamp = Utc::now() - lookback;

        if force_bootstrap {
            eprintln!("Bootstrap forced, lookback is {bootstrap_lookback_minutes} minutes");
        } else {
            eprintln!("Checkpoint not found, lookback is {bootstrap_lookback_minutes} minutes");
        }
        eprintln!("Initializing from: {}", bootstrap_timestamp.with_timezone(&Local).to_rfc3339());

        IncrementalDownloadState::new(
            local_storage_profile_id.clone(),
            bootstrap_timestamp,
            None, None, None,
        )
    } else {
        let loaded = IncrementalDownloadState::from_file(&checkpoint_file_path)
            .map_err(|e| CliError::Operation(format!("Failed to load checkpoint: {e}")))?;

        eprintln!("Checkpoint found");

        // Validate storage profile matches
        if local_storage_profile_id != loaded.local_storage_profile_id {
            if loaded.local_storage_profile_id.is_none() {
                return Err(CliError::Operation(
                    "The checkpoint was created with the --ignore-storage-profiles, you must use the same option to continue from it.".into()
                ));
            }
            if local_storage_profile_id.is_none() {
                return Err(CliError::Operation(
                    "The checkpoint was created without the --ignore-storage-profiles, you must leave out the option to continue from it.".into()
                ));
            }
            return Err(CliError::Operation(format!(
                "The checkpoint was created with local storage profile {}, but the configured storage profile is {}",
                loaded.local_storage_profile_id.as_deref().unwrap_or("none"),
                local_storage_profile_id.as_deref().unwrap_or("none"),
            )));
        }

        eprintln!("Continuing from: {}", loaded.downloads_completed_timestamp.with_timezone(&Local).to_rfc3339());
        loaded
    };

    eprintln!();

    // Parse conflict resolution
    let conflict = match conflict_resolution.to_uppercase().as_str() {
        "SKIP" => FileConflictResolution::Skip,
        "OVERWRITE" => FileConflictResolution::Overwrite,
        "CREATE_COPY" => FileConflictResolution::CreateCopy,
        other => return Err(CliError::Operation(format!("Unknown conflict resolution: {other}"))),
    };

    // Run the incremental output download orchestration
    let updated_checkpoint = incremental_output_download(
        &farm, &queue_id_str, &queue, &config,
        checkpoint, &local_storage_profile_id, conflict, dry_run,
    ).await?;

    if dry_run {
        eprintln!("This is a DRY RUN so the checkpoint was not saved");
    } else {
        updated_checkpoint.save_file(&checkpoint_file_path)
            .map_err(|e| CliError::Operation(format!("Failed to save checkpoint: {e}")))?;
        eprintln!("Checkpoint saved");
    }

    Ok(())
}

/// Core orchestration: find jobs with new output, download manifests and files.
async fn incremental_output_download(
    farm_id: &str,
    queue_id: &str,
    queue: &serde_json::Value,
    config: &deadline_config::ini::IniConfig,
    mut checkpoint: IncrementalDownloadState,
    local_storage_profile_id: &Option<String>,
    conflict: FileConflictResolution,
    dry_run: bool,
) -> Result<IncrementalDownloadState, CliError> {
    let now = Utc::now();
    let new_completed = std::cmp::max(
        checkpoint.downloads_started_timestamp,
        now - Duration::seconds(checkpoint.eventual_consistency_max_seconds),
    );

    eprintln!("Updating download state across time interval:");
    eprintln!("    From: {}", checkpoint.downloads_completed_timestamp.with_timezone(&Local).to_rfc3339());
    eprintln!("      To: {}", now.with_timezone(&Local).to_rfc3339());
    let update_length = now - checkpoint.downloads_completed_timestamp;
    let ec_delta = Duration::seconds(checkpoint.eventual_consistency_max_seconds);
    if update_length > ec_delta {
        eprintln!("  Length: {} + {} (eventual consistency allowance)",
            format_duration(update_length - ec_delta), format_duration(ec_delta));
    } else {
        eprintln!("  Length: {}", format_duration(update_length));
    }
    eprintln!();

    // Step 1: Get download candidate jobs via SearchJobs
    eprintln!("Retrieving updated data from Deadline Cloud...");
    let starting_ts = checkpoint.downloads_completed_timestamp;

    // Active jobs with at least one SUCCEEDED task
    let active_filter = serde_json::json!({
        "filters": [{
            "stringListFilter": {
                "name": "TASK_RUN_STATUS",
                "operator": "ANY_EQUALS",
                "values": ["READY", "ASSIGNED", "STARTING", "SCHEDULED", "RUNNING"]
            }
        }],
        "operator": "OR"
    });
    let active_resp = api::search_jobs_with_filters(
        farm_id, &[queue_id], 0, 100,
        Some(&active_filter), None, Some(config), None,
    ).await.map_err(|e| CliError::Operation(format!("Failed to search active jobs: {e}")))?;

    let mut download_candidates: std::collections::HashMap<String, serde_json::Value> =
        std::collections::HashMap::new();
    if let Some(jobs) = active_resp["jobs"].as_array() {
        for job in jobs {
            if let Some(counts) = job.get("taskRunStatusCounts") {
                if counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
                    if let Some(id) = job["jobId"].as_str() {
                        download_candidates.insert(id.to_string(), job.clone());
                    }
                }
            }
        }
    }

    // Recently ended jobs
    let ended_filter = serde_json::json!({
        "filters": [{
            "dateTimeFilter": {
                "name": "ENDED_AT",
                "dateTime": starting_ts.to_rfc3339(),
                "operator": "GREATER_THAN_EQUAL_TO"
            }
        }],
        "operator": "AND"
    });
    let ended_resp = api::search_jobs_with_filters(
        farm_id, &[queue_id], 0, 100,
        Some(&ended_filter), None, Some(config), None,
    ).await.map_err(|e| CliError::Operation(format!("Failed to search ended jobs: {e}")))?;

    if let Some(jobs) = ended_resp["jobs"].as_array() {
        for job in jobs {
            if let Some(counts) = job.get("taskRunStatusCounts") {
                if counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0) > 0 {
                    if let Some(id) = job["jobId"].as_str() {
                        download_candidates.insert(id.to_string(), job.clone());
                    }
                }
            }
        }
    }

    eprintln!("...retrieval completed");
    eprintln!();

    // Step 2: Categorize jobs
    let checkpoint_jobs_map: std::collections::HashMap<String, &IncrementalDownloadJob> =
        checkpoint.jobs.iter().map(|j| (j.job_id().to_string(), j)).collect();
    let checkpoint_job_ids: std::collections::HashSet<String> =
        checkpoint_jobs_map.keys().cloned().collect();
    let candidate_ids: std::collections::HashSet<String> =
        download_candidates.keys().cloned().collect();

    let mut new_job_ids: std::collections::HashSet<String> =
        candidate_ids.difference(&checkpoint_job_ids).cloned().collect();
    let mut updated_job_ids: std::collections::HashSet<String> =
        candidate_ids.intersection(&checkpoint_job_ids).cloned().collect();
    let finished_tracking_ids: std::collections::HashSet<String> =
        checkpoint_job_ids.difference(&candidate_ids).cloned().collect();
    let mut unchanged_job_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut completed_job_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut attachments_free_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut missing_storage_profile_ids: std::collections::HashSet<String> = std::collections::HashSet::new();

    eprintln!("Categorizing {} checkpoint jobs against {} download candidate jobs...",
        checkpoint.jobs.len(), download_candidates.len());

    // Copy attachments from checkpoint for updated jobs
    for job_id in updated_job_ids.clone() {
        if let Some(cp_job) = checkpoint_jobs_map.get(&job_id) {
            if cp_job.job.get("attachments").map_or(false, |a| a.is_null()) {
                attachments_free_ids.insert(job_id.clone());
                continue;
            }
            if let Some(dc_job) = download_candidates.get_mut(&job_id) {
                if let Some(att) = cp_job.job.get("attachments") {
                    dc_job["attachments"] = att.clone();
                }
                if let Some(sp) = cp_job.job.get("storageProfileId") {
                    dc_job["storageProfileId"] = sp.clone();
                }
            }
        }
    }
    updated_job_ids = updated_job_ids.difference(&attachments_free_ids).cloned().collect();

    // Detect unchanged jobs (same SUCCEEDED count and endedAt)
    for job_id in updated_job_ids.clone() {
        if let (Some(cp_job), Some(dc_job)) = (checkpoint_jobs_map.get(&job_id), download_candidates.get(&job_id)) {
            let cp_succeeded = cp_job.job.get("taskRunStatusCounts")
                .and_then(|c| c.get("SUCCEEDED")).and_then(|v| v.as_i64()).unwrap_or(0);
            let dc_succeeded = dc_job.get("taskRunStatusCounts")
                .and_then(|c| c.get("SUCCEEDED")).and_then(|v| v.as_i64()).unwrap_or(0);
            let cp_ended = cp_job.job.get("endedAt").and_then(|v| v.as_str());
            let dc_ended = dc_job.get("endedAt").and_then(|v| v.as_str());
            if cp_succeeded == dc_succeeded && cp_ended == dc_ended {
                let name = dc_job["name"].as_str().unwrap_or("unknown");
                eprintln!("UNCHANGED Job: {name} ({job_id})");
                unchanged_job_ids.insert(job_id);
            }
        }
    }
    updated_job_ids = updated_job_ids.difference(&unchanged_job_ids).cloned().collect();

    // Print updated jobs
    for job_id in &updated_job_ids {
        if let (Some(cp_job), Some(dc_job)) = (checkpoint_jobs_map.get(job_id), download_candidates.get(job_id)) {
            let name = cp_job.job["name"].as_str().unwrap_or("unknown");
            eprintln!("EXISTING Job: {name} ({job_id})");
            let cp_counts = &cp_job.job["taskRunStatusCounts"];
            let dc_counts = &dc_job["taskRunStatusCounts"];
            let cp_succeeded = cp_counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0);
            let cp_total: i64 = cp_counts.as_object().map(|m| m.values().filter_map(|v| v.as_i64()).sum()).unwrap_or(0);
            let dc_succeeded = dc_counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0);
            let dc_total: i64 = dc_counts.as_object().map(|m| m.values().filter_map(|v| v.as_i64()).sum()).unwrap_or(0);
            eprintln!("  Succeeded tasks (before): {cp_succeeded} / {cp_total}");
            eprintln!("  Succeeded tasks (now)   : {dc_succeeded} / {dc_total}");

            // Check if completed
            if dc_succeeded == dc_total && dc_job.get("endedAt").is_some() {
                completed_job_ids.insert(job_id.clone());
            }
        }
    }
    updated_job_ids = updated_job_ids.difference(&completed_job_ids).cloned().collect();

    // Print finished tracking jobs
    for job_id in &finished_tracking_ids {
        if let Some(cp_job) = checkpoint_jobs_map.get(job_id) {
            let name = cp_job.job.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
            if cp_job.job.as_object().map_or(true, |m| m.len() <= 1) {
                continue; // minimal placeholder, skip
            }
            eprintln!("FINISHED TRACKING Job: {name} ({job_id})");
            if cp_job.job.get("attachments").map_or(false, |a| a.is_null()) {
                eprintln!("  Job without job attachments is no longer active");
            } else {
                let counts = &cp_job.job["taskRunStatusCounts"];
                let succeeded = counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0);
                let total: i64 = counts.as_object().map(|m| m.values().filter_map(|v| v.as_i64()).sum()).unwrap_or(0);
                if succeeded == total {
                    eprintln!("   Job succeeded");
                } else {
                    eprintln!("   Job is not a download candidate anymore (likely suspended, canceled or failed)");
                }
            }
        }
    }

    // For new jobs, call GetJob to get attachments
    for job_id in new_job_ids.clone() {
        let job_detail = api::get_job(farm_id, queue_id, &job_id, Some(config), None)
            .await
            .map_err(|e| CliError::Operation(format!("Failed to get job {job_id}: {e}")))?;
        if let Some(dc_job) = download_candidates.get_mut(&job_id) {
            dc_job["attachments"] = job_detail.get("attachments").cloned().unwrap_or(serde_json::Value::Null);
            dc_job["storageProfileId"] = job_detail.get("storageProfileId").cloned().unwrap_or(serde_json::Value::Null);
        }

        let dc_job = &download_candidates[&job_id];
        let name = dc_job["name"].as_str().unwrap_or("unknown");
        let counts = &dc_job["taskRunStatusCounts"];
        let succeeded = counts.get("SUCCEEDED").and_then(|v| v.as_i64()).unwrap_or(0);
        let total: i64 = counts.as_object().map(|m| m.values().filter_map(|v| v.as_i64()).sum()).unwrap_or(0);

        if dc_job.get("attachments").map_or(true, |a| a.is_null()) {
            eprintln!("NEW Job: {name} ({job_id})");
            eprintln!("  Succeeded tasks: {succeeded} / {total}");
            eprintln!("  Job does not use job attachments.");
            attachments_free_ids.insert(job_id.clone());
        } else if dc_job.get("storageProfileId").map_or(false, |s| s.is_null())
            && local_storage_profile_id.is_some()
        {
            eprintln!("NEW Job: {name} ({job_id})");
            eprintln!("  WARNING: THE JOB OUTPUT WILL NOT BE DOWNLOADED, IT HAS NO STORAGE PROFILE.");
            missing_storage_profile_ids.insert(job_id.clone());
        } else {
            eprintln!("NEW Job: {name} ({job_id})");
            eprintln!("  Succeeded tasks: {succeeded} / {total}");

            // Check if already completed
            if succeeded == total && dc_job.get("endedAt").is_some() {
                completed_job_ids.insert(job_id.clone());
            }
        }
    }
    new_job_ids = new_job_ids.difference(&attachments_free_ids).cloned().collect();
    new_job_ids = new_job_ids.difference(&completed_job_ids).cloned().collect();
    new_job_ids = new_job_ids.difference(&missing_storage_profile_ids).cloned().collect();

    eprintln!("...categorization completed");
    eprintln!();

    // Step 2b: Storage profile path mapping rules
    if let Some(local_sp_id) = &local_storage_profile_id {
        // Collect unique storage profile IDs from jobs to process
        let mut sp_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        sp_ids.insert(local_sp_id.clone());
        for job_id in new_job_ids.iter().chain(updated_job_ids.iter()).chain(completed_job_ids.iter()) {
            if let Some(sp) = download_candidates.get(job_id)
                .and_then(|j| j.get("storageProfileId"))
                .and_then(|v| v.as_str())
            {
                sp_ids.insert(sp.to_string());
            }
        }

        // Fetch all storage profiles
        let mut storage_profiles: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();
        for sp_id in &sp_ids {
            let sp = api::get_storage_profile_for_queue(farm_id, queue_id, sp_id, Some(config), None)
                .await
                .map_err(|e| CliError::Operation(format!("Failed to get storage profile {sp_id}: {e}")))?;
            storage_profiles.insert(sp_id.clone(), sp);
        }

        // Print local profile info
        let local_sp = &storage_profiles[local_sp_id];
        let local_name = local_sp["displayName"].as_str().unwrap_or("unknown");
        eprintln!("Local storage profile is {local_name} ({local_sp_id})");
        let same_sp_count = download_candidates.values()
            .filter(|j| j.get("storageProfileId").and_then(|v| v.as_str()) == Some(local_sp_id))
            .count();
        eprintln!("  {same_sp_count} download candidate jobs have the same storage profile and will be downloaded to their original specified paths");

        // Print path mapping rules for each non-local storage profile
        for (sp_id, sp) in &storage_profiles {
            if sp_id == local_sp_id {
                continue;
            }
            let sp_name = sp["displayName"].as_str().unwrap_or("unknown");
            let sp_os = sp["osFamily"].as_str().unwrap_or("unknown");
            let local_os = local_sp["osFamily"].as_str().unwrap_or("unknown");
            let job_count = download_candidates.values()
                .filter(|j| j.get("storageProfileId").and_then(|v| v.as_str()) == Some(sp_id.as_str()))
                .count();

            eprintln!();
            eprintln!("Path mapping rules for {job_count} download candidate jobs with storage profile {sp_name} ({sp_id})");
            eprintln!("  job storage profile: {sp_name} ({sp_os})");
            eprintln!("  local storage profile: {local_name} ({local_os})");

            // Generate rules using Batch B's path mapping
            use deadline_job_attachments::models::StorageProfile;
            let source_sp = StorageProfile::from_json(sp);
            let dest_sp = StorageProfile::from_json(local_sp);
            if let (Some(src), Some(dst)) = (source_sp, dest_sp) {
                let rules = deadline_job_attachments::path_mapping::generate_path_mapping_rules(&src, &dst);
                if rules.is_empty() {
                    eprintln!("   No rules generated. Storage profiles {local_name} and {sp_name} share no file system location names.");
                } else {
                    for rule in &rules {
                        eprintln!("  - from: {}", rule.source_path);
                        eprintln!("    to:   {}", rule.destination_path);
                    }
                }
            }
        }
        eprintln!();
    }

    // Step 3: Get sessions and session actions for jobs with downloads
    let jobs_to_process: std::collections::HashSet<String> = new_job_ids.iter()
        .chain(updated_job_ids.iter())
        .chain(completed_job_ids.iter())
        .filter(|id| {
            download_candidates.get(*id)
                .and_then(|j| j.get("attachments"))
                .map(|a| !a.is_null())
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    eprintln!("Retrieving sessions for {} jobs...", jobs_to_process.len());

    // Collect session completed indexes from checkpoint
    let checkpoint_session_indexes: std::collections::HashMap<String, std::collections::HashMap<String, i64>> =
        checkpoint.jobs.iter()
            .map(|j| (j.job_id().to_string(), j.session_completed_indexes.clone()))
            .collect();

    let mut all_session_actions: Vec<serde_json::Value> = Vec::new();

    for job_id in &jobs_to_process {
        let sessions_resp = api::list_sessions(farm_id, queue_id, job_id, Some(config), None)
            .await
            .map_err(|e| CliError::Operation(format!("Failed to list sessions for {job_id}: {e}")))?;

        if let Some(sessions) = sessions_resp["sessions"].as_array() {
            for session in sessions {
                let session_id = session["sessionId"].as_str().unwrap_or("");
                let actions_resp = api::list_session_actions(
                    farm_id, queue_id, job_id, session_id, Some(config), None,
                ).await.map_err(|e| CliError::Operation(
                    format!("Failed to list session actions for {session_id}: {e}")
                ))?;

                if let Some(actions) = actions_resp["sessionActions"].as_array() {
                    for action in actions {
                        // Only include succeeded taskRun actions
                        let succeeded = action.get("status")
                            .and_then(|s| s.as_str()) == Some("SUCCEEDED");
                        let is_task_run = action.get("definition")
                            .and_then(|d| d.get("taskRun")).is_some();
                        if succeeded && is_task_run {
                            // Check if already downloaded (by session action index)
                            let sa_id = action["sessionActionId"].as_str().unwrap_or("");
                            let sa_index: i64 = sa_id.rsplit('-').next()
                                .and_then(|s| s.parse().ok()).unwrap_or(0);
                            let completed_index = checkpoint_session_indexes
                                .get(job_id)
                                .and_then(|m| m.get(session_id))
                                .copied();
                            if completed_index.map_or(true, |ci| sa_index > ci) {
                                all_session_actions.push(action.clone());
                            }
                        }
                    }
                }
            }
        }
    }

    // Step 4: Download output manifests and files
    let attachment_settings = &queue["jobAttachmentSettings"];
    let bucket = attachment_settings["s3BucketName"].as_str().unwrap_or("");
    let prefix = attachment_settings["rootPrefix"].as_str().unwrap_or("");

    let mut downloaded_manifests: Vec<(chrono::DateTime<Utc>, deadline_job_attachments::asset_manifests::AssetManifest)> = Vec::new();
    let mut downloaded_files_count: usize = 0;
    let mut downloaded_bytes: u64 = 0;

    if !jobs_to_process.is_empty() {
        let sdk_config = deadline_api::session::get_queue_scoped_config(
            farm_id, queue_id, Some(config),
        ).await.map_err(|e| CliError::Operation(format!("Failed to get S3 credentials:\n{e}")))?;

        let s3_client = deadline_job_attachments::s3::build_s3_client(&sdk_config, Some(config));
        let account_id = deadline_job_attachments::s3::get_account_id(&sdk_config)
            .await
            .map_err(|e| CliError::Operation(format!("Failed to get account ID:\n{e}")))?;

        // Download output manifests for each job
        for job_id in &jobs_to_process {
            let dc_job = match download_candidates.get(job_id) {
                Some(j) => j,
                None => continue,
            };

            let manifest_prefix = format!("{}/Manifests/{}/{}/{}/", prefix, farm_id, queue_id, job_id);
            let manifest_keys = deadline_job_attachments::download::list_output_manifest_keys(
                &s3_client, bucket, &manifest_prefix, &account_id,
            ).await.unwrap_or_default();

            for key in &manifest_keys {
                match deadline_job_attachments::download::download_manifest_from_s3(
                    &s3_client, bucket, key, &account_id,
                ).await {
                    Ok((Some(asset_root), mut manifest)) => {
                    let root_path_format = dc_job.get("attachments")
                        .and_then(|a| a["manifests"].as_array())
                        .and_then(|m| m.first())
                        .and_then(|m| m["rootPathFormat"].as_str());

                    let mut unmapped = Vec::new();
                    let _ = deadline_job_attachments::incremental_download::make_manifest_paths_absolute(
                        &asset_root, &mut manifest, None, root_path_format, &mut unmapped,
                    );
                    downloaded_manifests.push((Utc::now(), manifest));
                    }
                    Ok((None, _)) => {
                        log::warn!("Manifest {key} has no asset root metadata, skipping");
                    }
                    Err(e) => {
                        log::warn!("Failed to download manifest {key}: {e}");
                    }
                }
            }
        }

        // Merge and download
        let manifest_paths = deadline_job_attachments::incremental_download::merge_absolute_path_manifest_list(
            &mut downloaded_manifests,
        );

        let total_bytes: i64 = manifest_paths.iter().map(|p| p.size).sum();
        let total_files = manifest_paths.len();

        eprintln!("Summary of paths to download:");
        if manifest_paths.is_empty() {
            eprintln!("  (no files to download)");
        } else {
            eprintln!("  {} files, {}",
                total_files,
                deadline_job_attachments::progress_tracker::human_readable_file_size(total_bytes as u64));
        }
        eprintln!();

        if !dry_run && !manifest_paths.is_empty() {
            eprintln!("Downloading {} files from S3...", total_files);

            let s3_settings = deadline_job_attachments::models::JobAttachmentS3Settings {
                s3_bucket_name: bucket.to_string(),
                root_prefix: prefix.to_string(),
            };
            let cas_prefix = s3_settings.full_cas_prefix()
                .map_err(|e| CliError::Operation(format!("Failed to compute CAS prefix: {e}")))?;

            // Group manifest paths by parent directory for download
            let mut manifests_by_root: std::collections::HashMap<String, deadline_job_attachments::asset_manifests::AssetManifest> =
                std::collections::HashMap::new();
            for mp in &manifest_paths {
                let dir = std::path::Path::new(&mp.path)
                    .parent()
                    .map(|p| p.to_string_lossy().to_string())
                    .unwrap_or_else(|| "/".to_string());
                let root = if dir.is_empty() { "/".to_string() } else { dir };
                let entry = manifests_by_root.entry(root).or_insert_with(|| {
                    deadline_job_attachments::asset_manifests::AssetManifest::new(
                        deadline_job_attachments::asset_manifests::HashAlgorithm::Xxh128,
                        deadline_job_attachments::asset_manifests::ManifestVersion::V2023_03_03,
                        0, vec![],
                    ).unwrap()
                });
                let filename = std::path::Path::new(&mp.path)
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_else(|| mp.path.clone());
                entry.paths.push(deadline_job_attachments::asset_manifests::ManifestPath {
                    path: filename, hash: mp.hash.clone(), size: mp.size, mtime: mp.mtime,
                });
                entry.total_size += mp.size;
            }

            match deadline_job_attachments::download::download_files_from_manifests(
                bucket, &manifests_by_root, Some(&cas_prefix),
                &s3_client, &account_id, None, conflict,
            ).await {
                Ok(stats) => {
                    downloaded_files_count = stats.downloaded_files.len();
                    downloaded_bytes = stats.stats.total_bytes;
                }
                Err(e) => eprintln!("Warning: download error: {e}"),
            }
        } else if dry_run {
            eprintln!("Skipping downloads due to DRY RUN");
        }
    } else {
        eprintln!("Summary of paths to download:");
        eprintln!("  (no files to download)");
    }
    eprintln!();

    if dry_run {
        eprintln!("Summary of DRY RUN for incremental output download (no files were downloaded to the file system):");
    } else {
        eprintln!("Summary of incremental output download:");
    }
    eprintln!("  Downloaded session actions: {}", all_session_actions.len());
    eprintln!("  Downloaded files: {}", downloaded_files_count);
    eprintln!("  Downloaded bytes: {}",
        deadline_job_attachments::progress_tracker::human_readable_file_size(downloaded_bytes));
    eprintln!("  Jobs with downloads:");
    eprintln!("    completed: {}", completed_job_ids.len());
    eprintln!("    added: {}", new_job_ids.len());
    eprintln!("    updated: {}", updated_job_ids.len());
    eprintln!("  Jobs without downloads:");
    eprintln!("    not using job attachments: {}", attachments_free_ids.len());
    eprintln!("    missing storage profile: {}", missing_storage_profile_ids.len());
    eprintln!("    unchanged: {}", unchanged_job_ids.len());
    eprintln!("    inactive: {}", finished_tracking_ids.len());

    // Update checkpoint
    let mut updated_jobs: Vec<deadline_job_attachments::incremental_download::IncrementalDownloadJob> = Vec::new();
    for (job_id, job) in &download_candidates {
        updated_jobs.push(
            deadline_job_attachments::incremental_download::IncrementalDownloadJob::new(
                job.clone(), None, None,
            )
        );
    }

    checkpoint.downloads_completed_timestamp = new_completed;
    checkpoint.jobs = updated_jobs;

    Ok(checkpoint)
}

/// Format a chrono::Duration as H:MM:SS.ffffff matching Python's timedelta str()
fn format_duration(d: Duration) -> String {
    let total_secs = d.num_seconds();
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let micros = d.num_microseconds().unwrap_or(0) % 1_000_000;
    format!("{hours}:{mins:02}:{secs:02}.{micros:06}")
}

fn expand_tilde(path: &str) -> PathBuf {
    if path.starts_with("~/") || path == "~" {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(&path[2..]);
        }
        #[cfg(windows)]
        if let Ok(profile) = std::env::var("USERPROFILE") {
            return PathBuf::from(profile).join(&path[2..]);
        }
    }
    PathBuf::from(path)
}

fn is_writable(path: &Path) -> bool {
    // Try creating a temp file in the directory
    let test_file = path.join(".deadline_write_test");
    match fs::File::create(&test_file) {
        Ok(_) => {
            let _ = fs::remove_file(&test_file);
            true
        }
        Err(_) => false,
    }
}
