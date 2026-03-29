use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting};

#[derive(Subcommand)]
pub enum WorkerAction {
    /// List workers in a fleet
    List {
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long, required = true)]
        fleet_id: String,
        #[arg(long, default_value = "5")]
        page_size: i32,
        #[arg(long, default_value = "0")]
        item_offset: i32,
    },
    /// Get details of a specific worker
    Get {
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long, required = true)]
        fleet_id: String,
        #[arg(long, required = true)]
        worker_id: String,
    },
}

pub fn run(action: WorkerAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: WorkerAction) -> Result<(), CliError> {
    match action {
        WorkerAction::List {
            profile,
            farm_id,
            fleet_id,
            page_size,
            item_offset,
        } => {
            let config = apply_profile(profile)?;
            let farm =
                require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let resp =
                api::search_workers(&farm, &[&fleet_id], item_offset, page_size, config.as_ref())
                    .await
                    .map_err(|e| {
                        CliError::Operation(format!("Failed to get Workers from Deadline:\n{e}"))
                    })?;
            let total = resp["totalResults"].as_i64().unwrap_or(0);
            let empty = vec![];
            let workers = resp["workers"].as_array().unwrap_or(&empty);

            let structured: Vec<serde_json::Value> = workers
                .iter()
                .map(|w| {
                    serde_json::json!({
                        "workerId": w["workerId"],
                        "status": w["status"],
                        "createdAt": w["createdAt"],
                    })
                })
                .collect();

            println!(
                "Displaying {} of {} workers starting at {}",
                structured.len(),
                total,
                item_offset
            );
            println!();
            println!(
                "{}",
                crate::common::cli_object_repr(&serde_json::json!(structured))
            );
            Ok(())
        }
        WorkerAction::Get {
            profile,
            farm_id,
            fleet_id,
            worker_id,
        } => {
            let config = apply_profile(profile)?;
            let farm =
                require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let resp = api::get_worker(&farm, &fleet_id, &worker_id, config.as_ref())
                .await
                .map_err(|e| {
                    CliError::Operation(format!("Failed to get Worker from Deadline:\n{e}"))
                })?;
            println!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
    }
}
