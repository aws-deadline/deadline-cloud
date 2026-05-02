use clap::Subcommand;
use deadline_api::{client, responses::FarmResponse, session};
use deadline_config::config_file;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

#[derive(Subcommand)]
pub(crate) enum FarmAction {
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

pub(crate) fn run(action: FarmAction) -> Result<(), CliError> {
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
            let dl = session::deadline_client(Some(&config)).await;
            let builder = client::apply_dcm_principal(dl.list_farms(), Some(&config));
            match client::collect_paginated(builder.into_paginator().send()).await {
                Ok(pages) => {
                    let structured: Vec<serde_json::Value> = pages
                        .iter()
                        .flat_map(aws_sdk_deadline::operation::list_farms::ListFarmsOutput::farms)
                        .map(|f| serde_json::json!({"farmId": f.farm_id(), "displayName": f.display_name()}))
                        .collect();
                    println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(), "ListFarms", None, None, None, Some(&config),
                    ).await;
                    Err(CliError::Operation(format!(
                        "Failed to get Farms from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        FarmAction::Get { profile, farm_id } => {
            let config = setup(profile, farm_id, &["farm_id"])?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let dl = session::deadline_client(Some(&config)).await;
            match dl.get_farm().farm_id(&farm).send().await {
                Ok(output) => {
                    let resp = FarmResponse::from(output);
                    let val = serde_json::to_value(&resp).map_err(|e| CliError::Operation(e.to_string()))?;
                    println!("{}", crate::common::cli_object_repr(&val));
                    Ok(())
                }
                Err(e) => {
                    let err = client::format_sdk_error(&e);
                    let suggestion = suggest_resources_on_client_error(
                        &err,
                        "GetFarm",
                        Some(&farm),
                        None,
                        None,
                        Some(&config),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Farm from Deadline:\n{err}{suggestion}"
                    )))
                }
            }
        }
    }
}
