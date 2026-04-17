use clap::Subcommand;
use deadline_api::api;
use deadline_config::config_file;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

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

fn setup(profile: Option<String>, farm_id: Option<String>, required: &[&str]) -> Result<deadline_config::ini::IniConfig, CliError> {
    let mut config = config_file::read_config()
        .map_err(|e| CliError::Operation(e.to_string()))?;
    crate::common::apply_cli_options_to_config(
        &mut config,
        &crate::common::CliOptions { profile, farm_id, queue_id: None, job_id: None, yes: false, ..Default::default() },
        required,
    )?;
    Ok(config)
}

async fn run_async(action: FleetAction) -> Result<(), CliError> {
    match action {
        FleetAction::List { profile, farm_id } => {
            let config = setup(profile, farm_id, &["farm_id"])?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let resp = api::list_fleets(&farm, Some(&config), None).await.map_err(|e| {
                CliError::Operation(format!("Failed to get Fleets from Deadline:\n{e}"))
            })?;
            let empty = vec![];
            let fleets = resp["fleets"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = fleets
                .iter()
                .map(|f| serde_json::json!({"fleetId": f["fleetId"], "displayName": f["displayName"]}))
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        FleetAction::Get { profile, farm_id, fleet_id } => {
            // fleet_id isn't in CliOptions, so handle it manually after setup
            let mut config = config_file::read_config()
                .map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions { profile, farm_id, queue_id: None, job_id: None, yes: false, ..Default::default() },
                &["farm_id"],
            )?;
            let farm = config_file::get_setting_with_config("defaults.farm_id", &config).unwrap_or_default();
            let fleet = fleet_id.ok_or_else(|| CliError::Operation(
                "Missing '--fleet-id' or default Fleet ID configuration".to_string()
            ))?;
            match api::get_fleet(&farm, &fleet, Some(&config), None).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        Some(&farm),
                        None,
                        Some(&fleet),
                        Some(&config),
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
