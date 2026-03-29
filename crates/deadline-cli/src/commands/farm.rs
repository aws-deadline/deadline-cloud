use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting};

#[derive(Subcommand)]
pub enum FarmAction {
    /// List available farms
    List {
        #[arg(long)] profile: Option<String>,
    },
    /// Get details of a specific farm
    Get {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
    },
}

pub fn run(action: FarmAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: FarmAction) -> Result<(), CliError> {
    match action {
        FarmAction::List { profile } => {
            let config = apply_profile(profile)?;
            let resp = api::list_farms(config.as_ref()).await
                .map_err(|e| CliError::Operation(format!("Failed to get Farms from Deadline:\n{e}")))?;
            let empty = vec![];
            let farms = resp["farms"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = farms.iter()
                .map(|f| serde_json::json!({"farmId": f["farmId"], "displayName": f["displayName"]}))
                .collect();
            print!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        FarmAction::Get { profile, farm_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let resp = api::get_farm(&farm, config.as_ref()).await
                .map_err(|e| CliError::Operation(format!("Failed to get Farm from Deadline:\n{e}")))?;
            print!("{}", crate::common::cli_object_repr(&resp));
            Ok(())
        }
    }
}
