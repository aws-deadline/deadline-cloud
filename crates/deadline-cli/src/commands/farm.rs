use clap::Subcommand;
use deadline_api::api;
use deadline_config::config_file;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

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

async fn run_async(action: FarmAction) -> Result<(), CliError> {
    match action {
        FarmAction::List { profile } => {
            let config = setup(profile, None, &[])?;
            let resp = api::list_farms(Some(&config), None).await.map_err(|e| {
                CliError::Operation(format!("Failed to get Farms from Deadline:\n{e}"))
            })?;
            let empty = vec![];
            let farms = resp["farms"].as_array().unwrap_or(&empty);
            let structured: Vec<serde_json::Value> = farms
                .iter()
                .map(|f| serde_json::json!({"farmId": f["farmId"], "displayName": f["displayName"]}))
                .collect();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        FarmAction::Get { profile, farm_id } => {
            let config = setup(profile, farm_id, &["farm_id"])?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            match api::get_farm(&farm, Some(&config), None).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        "GetFarm",
                        Some(&farm),
                        None,
                        None,
                        Some(&config),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Farm from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
    }
}
