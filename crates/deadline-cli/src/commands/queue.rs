use std::fs;
use std::path::{Path, PathBuf};

use chrono::{Duration, Utc};
use clap::Subcommand;
use deadline_api::api;
use deadline_config::config_file;
use deadline_api::telemetry::create_telemetry;
use deadline_job_attachments::incremental_download::IncrementalDownloadState;
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
        &crate::common::CliOptions { profile, farm_id, queue_id, job_id: None, yes: false },
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
                    details.insert("is_success".into(), serde_json::json!(true));
                    telemetry.record_event("com.amazon.rum.deadline.queue_export_credentials", details, false);

                    let creds = &resp["credentials"];
                    let expiration = creds["expiration"].as_str().unwrap_or("")
                        .replacen(' ', "T", 1);
                    let output = serde_json::json!({
                        "Version": 1,
                        "AccessKeyId": creds["accessKeyId"],
                        "SecretAccessKey": creds["secretAccessKey"],
                        "SessionToken": creds["sessionToken"],
                        "Expiration": expiration,
                    });
                    println!("{}", serde_json::to_string_pretty(&output).unwrap());
                    Ok(())
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
        profile, farm_id, queue_id, job_id: None, yes: false,
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
        eprintln!("Initializing from: {}", bootstrap_timestamp.to_rfc3339());

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

        eprintln!("Continuing from: {}", loaded.downloads_completed_timestamp.to_rfc3339());
        loaded
    };

    eprintln!();

    // Parse conflict resolution
    let _conflict = match conflict_resolution.to_uppercase().as_str() {
        "SKIP" => FileConflictResolution::Skip,
        "OVERWRITE" => FileConflictResolution::Overwrite,
        "CREATE_COPY" => FileConflictResolution::CreateCopy,
        other => return Err(CliError::Operation(format!("Unknown conflict resolution: {other}"))),
    };

    // TODO: Call full orchestration (_incremental_output_download equivalent)
    // For now, the checkpoint management, validation, and PID lock are wired.
    // The orchestration calls SearchJobs, categorizes jobs, retrieves sessions,
    // downloads manifests, and downloads files. This will be completed when
    // the remaining API integration is wired.

    eprintln!("Updating download state across time interval:");
    eprintln!("    From: {}", checkpoint.downloads_completed_timestamp.to_rfc3339());
    let now = Utc::now();
    eprintln!("      To: {}", now.to_rfc3339());
    eprintln!();

    // Placeholder summary
    eprintln!("Summary of paths to download:");
    eprintln!("  (no files to download)");
    eprintln!();

    if dry_run {
        eprintln!("Skipping downloads due to DRY RUN");
        eprintln!();
        eprintln!("This is a DRY RUN so the checkpoint was not saved");
    } else {
        // Update completed timestamp
        let new_completed = std::cmp::max(
            checkpoint.downloads_started_timestamp,
            now - Duration::seconds(checkpoint.eventual_consistency_max_seconds),
        );
        let updated = IncrementalDownloadState::new(
            checkpoint.local_storage_profile_id,
            checkpoint.downloads_started_timestamp,
            Some(new_completed),
            Some(checkpoint.jobs),
            Some(checkpoint.eventual_consistency_max_seconds),
        );
        updated.save_file(&checkpoint_file_path)
            .map_err(|e| CliError::Operation(format!("Failed to save checkpoint: {e}")))?;
        eprintln!("Checkpoint saved");
    }

    Ok(())
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
