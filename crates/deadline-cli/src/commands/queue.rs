use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting};

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
            let resp = api::list_queues(&farm, config.as_ref()).await
                .map_err(|e| CliError::Operation(format!("Failed to get Queues from Deadline:\n{e}")))?;
            let empty = vec![];
            let queues = resp["queues"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = queues.iter()
                .map(|q| serde_json::json!({"queueId": q["queueId"], "displayName": q["displayName"]}))
                .collect();
            print!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        QueueAction::Get { profile, farm_id, queue_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let resp = api::get_queue(&farm, &queue, config.as_ref()).await
                .map_err(|e| CliError::Operation(format!("Failed to get Queue from Deadline:\n{e}")))?;
            print!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
    }
}
