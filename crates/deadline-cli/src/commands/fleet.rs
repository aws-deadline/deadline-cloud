use clap::Subcommand;
use deadline_lib::api::{client, responses::FleetResponse, session};
use deadline_lib::config::config_file;

use super::config::CliError;
use super::helpers::suggest_resources_on_client_error;

#[derive(Subcommand)]
pub(crate) enum FleetAction {
    /// List available fleets
    List {
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
    },
    /// Get details of a specific fleet
    Get {
        #[arg(long)]
        profile: Option<String>,
        #[arg(long)]
        farm_id: Option<String>,
        #[arg(long)]
        fleet_id: Option<String>,
        #[arg(long)]
        queue_id: Option<String>,
    },
}

pub(crate) fn run(action: FleetAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

fn setup(
    profile: Option<String>,
    farm_id: Option<String>,
    required: &[&str],
) -> Result<deadline_lib::config::ini::IniConfig, CliError> {
    let mut config = config_file::read_config().map_err(|e| CliError::Operation(e.to_string()))?;
    crate::common::apply_cli_options_to_config(
        &mut config,
        &crate::common::CliOptions {
            profile,
            farm_id,
            queue_id: None,
            job_id: None,
            yes: false,
            ..Default::default()
        },
        required,
    )?;
    Ok(config)
}

async fn run_async(action: FleetAction) -> Result<(), CliError> {
    match action {
        FleetAction::List { profile, farm_id } => {
            let config = setup(profile, farm_id, &["farm_id"])?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let p = crate::common::extract_profile(&config);
            let dl = session::deadline_client(p.as_deref()).await;
            let builder =
                client::apply_dcm_principal(dl.list_fleets().farm_id(&farm), p.as_deref());
            match client::collect_paginated(builder.into_paginator().send()).await {
                Ok(pages) => {
                    let structured: Vec<serde_json::Value> = pages
                        .iter()
                        .flat_map(aws_sdk_deadline::operation::list_fleets::ListFleetsOutput::fleets)
                        .map(|f| serde_json::json!({"fleetId": f.fleet_id(), "displayName": f.display_name()}))
                        .collect();
                    println!(
                        "{}",
                        crate::common::cli_object_repr(&serde_json::json!(structured))
                    );
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        "ListFleets",
                        Some(&farm),
                        None,
                        None,
                        &config,
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Fleets from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
        FleetAction::Get {
            profile,
            farm_id,
            fleet_id,
            queue_id,
        } => {
            if fleet_id.is_some() && queue_id.is_some() {
                return Err(CliError::Operation(
                    "Only one of the --fleet-id and --queue-id options may be provided.".into(),
                ));
            }

            let mut config =
                config_file::read_config().map_err(|e| CliError::Operation(e.to_string()))?;
            crate::common::apply_cli_options_to_config(
                &mut config,
                &crate::common::CliOptions {
                    profile,
                    farm_id,
                    queue_id: None,
                    job_id: None,
                    yes: false,
                    ..Default::default()
                },
                &["farm_id"],
            )?;
            let farm = config_file::get_setting("defaults.farm_id", &config).unwrap_or_default();
            let p = crate::common::extract_profile(&config);

            if let Some(fleet) = fleet_id {
                // --fleet-id mode: typed get
                let dl = session::deadline_client(p.as_deref()).await;
                match dl.get_fleet().farm_id(&farm).fleet_id(&fleet).send().await {
                    Ok(output) => {
                        let resp = FleetResponse::from(output);
                        let val = serde_json::to_value(&resp)
                            .map_err(|e| CliError::Operation(e.to_string()))?;
                        println!("{}", crate::common::cli_object_repr(&val));
                        Ok(())
                    }
                    Err(e) => {
                        let err = client::format_sdk_error(&e);
                        let suggestion = suggest_resources_on_client_error(
                            &err,
                            "GetFleet",
                            Some(&farm),
                            None,
                            Some(&fleet),
                            &config,
                        )
                        .await;
                        Err(CliError::Operation(format!(
                            "Failed to get Fleet from Deadline:\n{err}{suggestion}"
                        )))
                    }
                }
            } else {
                // --queue-id mode (or default queue from config)
                let queue = queue_id
                    .or_else(|| {
                        config_file::get_setting("defaults.queue_id", &config)
                            .ok()
                            .filter(|s| !s.is_empty())
                    })
                    .ok_or_else(|| {
                        CliError::Operation(
                            "Missing '--fleet-id', '--queue-id', or default Queue ID configuration"
                                .into(),
                        )
                    })?;

                // Get queue display name
                let dl = session::deadline_client(p.as_deref()).await;
                let queue_output = match dl.get_queue().farm_id(&farm).queue_id(&queue).send().await
                {
                    Ok(output) => output,
                    Err(e) => {
                        let err = client::format_sdk_error(&e);
                        let suggestion = suggest_resources_on_client_error(
                            &err,
                            "GetQueue",
                            Some(&farm),
                            Some(&queue),
                            None,
                            &config,
                        )
                        .await;
                        return Err(CliError::Operation(format!(
                            "Failed to get Queue from Deadline:\n{err}{suggestion}"
                        )));
                    }
                };
                let queue_name = queue_output.display_name();

                // List queue-fleet associations
                let assoc_pages = client::collect_paginated(
                    dl.list_queue_fleet_associations()
                        .farm_id(&farm)
                        .queue_id(&queue)
                        .into_paginator()
                        .send(),
                )
                .await
                .map_err(|e| {
                    CliError::Operation(format!("Failed to list queue fleet associations:\n{e}"))
                })?;
                let associations: Vec<_> = assoc_pages.iter()
                    .flat_map(aws_sdk_deadline::operation::list_queue_fleet_associations::ListQueueFleetAssociationsOutput::queue_fleet_associations)
                    .collect();

                println!(
                    "Showing all fleets ({} total) associated with queue: {queue_name}",
                    associations.len()
                );

                // Get each fleet and print with association status
                for assoc in &associations {
                    let fleet_id_val = assoc.fleet_id();
                    let status = assoc.status().as_str();

                    let output = dl
                        .get_fleet()
                        .farm_id(&farm)
                        .fleet_id(fleet_id_val)
                        .send()
                        .await
                        .map_err(|e| {
                            CliError::Operation(format!(
                                "Failed to get Fleet from Deadline:\n{}",
                                client::format_sdk_error(&e)
                            ))
                        })?;
                    let resp = FleetResponse::from(output);
                    let mut val = serde_json::to_value(&resp)
                        .map_err(|e| CliError::Operation(e.to_string()))?;

                    // Add queueFleetAssociationStatus to the fleet response
                    if let Some(obj) = val.as_object_mut() {
                        obj.insert(
                            "queueFleetAssociationStatus".into(),
                            serde_json::Value::String(status.into()),
                        );
                    }

                    println!();
                    println!("{}", crate::common::cli_object_repr(&val));
                }

                Ok(())
            }
        }
    }
}
