use clap::Subcommand;
use deadline_api::{client, response_capture::ResponseBodyCapture, session};
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
            let dl = session::deadline_client(Some(&config)).await;
            let builder = client::apply_dcm_principal(dl.list_farms(), Some(&config));
            let resp = client::collect_paginated_raw("farms", |token| {
                let builder = builder.clone();
                async move {
                    let cap = ResponseBodyCapture::new();
                    let mut req = builder;
                    if let Some(t) = token { req = req.next_token(t); }
                    req.customize().interceptor(cap.clone())
                        .send().await.map_err(client::deadline_error)?;
                    cap.json().map_err(|e| deadline_api::errors::DeadlineError::OperationError(e.to_string()))
                }
            }).await.map_err(|e| {
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
            let dl = session::deadline_client(Some(&config)).await;
            let cap = ResponseBodyCapture::new();
            match dl.get_farm().farm_id(&farm)
                .customize().interceptor(cap.clone())
                .send().await
            {
                Ok(_) => {
                    let resp = cap.json().map_err(|e| CliError::Operation(e.to_string()))?;
                    println!("{}", crate::common::cli_object_repr(&resp));
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
