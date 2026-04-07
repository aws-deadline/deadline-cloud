use clap::Subcommand;
use deadline_client::api;
use deadline_config::config_file;
use deadline_common::telemetry::create_telemetry;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

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
    ).map_err(CliError::Operation)?;
    Ok(config)
}

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
            match deadline_client::queue_parameters::get_queue_parameter_definitions(
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
    }
}
