use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting, suggest_resources_on_client_error};

#[derive(Subcommand)]
pub enum FleetAction {
    /// List available fleets
    List {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
    },
    /// Get details of a specific fleet
    Get {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] fleet_id: Option<String>,
    },
}

pub fn run(action: FleetAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: FleetAction) -> Result<(), CliError> {
    match action {
        FleetAction::List { profile, farm_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let resp = api::list_fleets(&farm, config.as_ref()).await.map_err(|e| {
                CliError::Operation(format!("Failed to get Fleets from Deadline:\n{e}"))
            })?;
            let empty = vec![];
            let fleets = resp["fleets"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = fleets
                .iter()
                .map(|f| serde_json::json!({"fleetId": f["fleetId"], "displayName": f["displayName"]}))
                .collect();
            print!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        FleetAction::Get { profile, farm_id, fleet_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let fleet = require_setting("fleet_id", fleet_id, "defaults.fleet_id", config.as_ref())?;
            match api::get_fleet(&farm, &fleet, config.as_ref()).await {
                Ok(resp) => {
                    print!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        Some(&farm),
                        None,
                        Some(&fleet),
                        config.as_ref(),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Fleet from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
    }
}
