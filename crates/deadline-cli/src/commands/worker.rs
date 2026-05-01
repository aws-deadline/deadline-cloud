use clap::Subcommand;
use deadline_api::{api, responses::WorkerResponse};
use deadline_config::config_file;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

#[derive(Subcommand)]
pub enum WorkerAction {
    /// List workers in a fleet
    List {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long, required = true)] fleet_id: String,
        #[arg(long, default_value = "5")] page_size: i32,
        #[arg(long, default_value = "0")] item_offset: i32,
    },
    /// Get details of a specific worker
    Get {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long, required = true)] fleet_id: String,
        #[arg(long, required = true)] worker_id: String,
    },
}

pub fn run(action: WorkerAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

fn setup(profile: Option<String>, farm_id: Option<String>) -> Result<deadline_config::ini::IniConfig, CliError> {
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    crate::common::apply_cli_options_to_config(
        &mut config,
        &crate::common::CliOptions { profile, farm_id, queue_id: None, job_id: None, yes: false, ..Default::default() },
        &["farm_id"],
    )?;
    Ok(config)
}

async fn run_async(action: WorkerAction) -> Result<(), CliError> {
    match action {
        WorkerAction::List { profile, farm_id, fleet_id, page_size, item_offset } => {
            let config = setup(profile, farm_id)?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let resp = match api::search_workers(&farm, &[&fleet_id], item_offset, page_size, Some(&config)).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(&e.to_string(), "SearchWorkers", Some(&farm), None, Some(&fleet_id), Some(&config)).await;
                    return Err(CliError::Operation(format!("Failed to get Workers from Deadline:\n{e}{suggestion}")));
                }
            };
            let total = resp["totalResults"].as_i64().unwrap_or(0);
            let empty = vec![];
            let workers = resp["workers"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = workers
                .iter()
                .map(|w| serde_json::json!({"workerId": w["workerId"], "status": w["status"], "createdAt": w["createdAt"]}))
                .collect();
            println!("Displaying {} of {} workers starting at {}", structured.len(), total, item_offset);
            println!();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        WorkerAction::Get { profile, farm_id, fleet_id, worker_id } => {
            let config = setup(profile, farm_id)?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let resp = match api::get_worker(&farm, &fleet_id, &worker_id, Some(&config)).await {
                Ok(r) => r,
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(&e.to_string(), "GetWorker", Some(&farm), None, Some(&fleet_id), Some(&config)).await;
                    return Err(CliError::Operation(format!("Failed to get Worker from Deadline:\n{e}{suggestion}")));
                }
            };
            let worker_resp = WorkerResponse::from(resp);
            let val = serde_json::to_value(&worker_resp).map_err(|e| CliError::Operation(e.to_string()))?;
            println!("{}", crate::common::cli_object_repr(&val));
            Ok(())
        }
    }
}
