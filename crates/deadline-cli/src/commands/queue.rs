use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting, suggest_resources_on_client_error};

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
}

pub fn run(action: QueueAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: QueueAction) -> Result<(), CliError> {
    match action {
        QueueAction::List { profile, farm_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let resp = api::list_queues(&farm, config.as_ref()).await.map_err(|e| {
                CliError::Operation(format!("Failed to get Queues from Deadline:\n{e}"))
            })?;
            let empty = vec![];
            let queues = resp["queues"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = queues
                .iter()
                .map(|q| serde_json::json!({"queueId": q["queueId"], "displayName": q["displayName"]}))
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        QueueAction::Get { profile, farm_id, queue_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            match api::get_queue(&farm, &queue, config.as_ref()).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        Some(&farm),
                        Some(&queue),
                        None,
                        config.as_ref(),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Queue from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        QueueAction::ExportCredentials { profile, farm_id, queue_id, mode } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let resp = match mode.to_uppercase().as_str() {
                "READ" => api::assume_queue_role_for_read(&farm, &queue, config.as_ref()).await,
                _ => api::assume_queue_role_for_user(&farm, &queue, config.as_ref()).await,
            }
            .map_err(|e| CliError::Operation(format!("Failed to export credentials:\n{e}")))?;
            let creds = &resp["credentials"];
            // credential_process spec requires RFC 3339 timestamps (T separator).
            // ResponseBodyCapture converts datetimes to Python display format
            // (space separator), so convert back for machine-readable output.
            // See: https://docs.aws.amazon.com/sdkref/latest/guide/feature-process-credentials.html
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
        QueueAction::GetStorageProfile { profile, farm_id, queue_id, storage_profile_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let resp = api::get_storage_profile_for_queue(&farm, &queue, &storage_profile_id, config.as_ref())
                .await
                .map_err(|e| CliError::Operation(format!("Failed to get storage profile:\n{e}")))?;
            println!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
    }
}
