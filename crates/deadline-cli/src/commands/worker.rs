use clap::Subcommand;
use deadline_api::{client, responses::WorkerResponse, session};
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
            let dl = deadline_api::session::deadline_client(Some(&config)).await;
            let resp = match dl.search_workers()
                .farm_id(&farm)
                .fleet_ids(&fleet_id)
                .item_offset(item_offset)
                .page_size(page_size)
                .send()
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let err_str = deadline_api::api::format_sdk_error(&e);
                    let suggestion = suggest_resources_on_client_error(&err_str, "SearchWorkers", Some(&farm), None, Some(&fleet_id), Some(&config)).await;
                    return Err(CliError::Operation(format!("Failed to get Workers from Deadline:\n{err_str}{suggestion}")));
                }
            };
            let total = resp.total_results() as i64;
            let workers = resp.workers();
            let structured: Vec<serde_json::Value> = workers
                .iter()
                .map(|w| serde_json::json!({"workerId": w.worker_id().unwrap_or(""), "status": w.status().map(|s| s.as_str()).unwrap_or(""), "createdAt": w.created_at().map(|d| {
                    d.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTimeWithOffset).unwrap_or_default().replace('T', " ").replace('Z', "+00:00")
                }).unwrap_or_default()}))
                .collect();
            println!("Displaying {} of {} workers starting at {}", structured.len(), total, item_offset);
            println!();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        WorkerAction::Get { profile, farm_id, fleet_id, worker_id } => {
            let config = setup(profile, farm_id)?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let resp = match session::deadline_client(Some(&config)).await
                .get_worker().farm_id(&farm).fleet_id(&fleet_id).worker_id(&worker_id)
                .send().await {
                Ok(r) => r,
                Err(e) => {
                    let err_str = client::format_sdk_error(&e);
                    let suggestion = suggest_resources_on_client_error(&err_str, "GetWorker", Some(&farm), None, Some(&fleet_id), Some(&config)).await;
                    return Err(CliError::Operation(format!("Failed to get Worker from Deadline:\n{err_str}{suggestion}")));
                }
            };
            let worker_resp = WorkerResponse::from(resp);
            let val = serde_json::to_value(&worker_resp).map_err(|e| CliError::Operation(e.to_string()))?;
            println!("{}", crate::common::cli_object_repr(&val));
            Ok(())
        }
    }
}
