use clap::Subcommand;
use deadline_client::api;

use super::config::CliError;
use super::helpers::{apply_profile, require_setting, suggest_resources_on_client_error};

#[derive(Subcommand)]
pub enum JobAction {
    /// List jobs in a queue
    List {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long, default_value = "5")]
        page_size: i32,
        #[arg(long, default_value = "0")]
        item_offset: i32,
    },
    /// Get details of a specific job
    Get {
        #[arg(long)] profile: Option<String>,
        #[arg(long)] farm_id: Option<String>,
        #[arg(long)] queue_id: Option<String>,
        #[arg(long)] job_id: Option<String>,
    },
}

pub fn run(action: JobAction) -> Result<(), CliError> {
    tokio::runtime::Runtime::new()
        .map_err(|e| CliError::Operation(e.to_string()))?
        .block_on(run_async(action))
}

async fn run_async(action: JobAction) -> Result<(), CliError> {
    match action {
        JobAction::List { profile, farm_id, queue_id, page_size, item_offset } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let resp = api::search_jobs(&farm, &[&queue], item_offset, page_size, config.as_ref())
                .await
                .map_err(|e| {
                    CliError::Operation(format!("Failed to get Jobs from Deadline:\n{e}"))
                })?;
            let total = resp["totalResults"].as_i64().unwrap_or(0);
            let empty = vec![];
            let jobs = resp["jobs"].as_array().unwrap_or(&empty);

            // Python uses "name" if present, falls back to "displayName"
            let name_field = if jobs.first().map_or(false, |j| j.get("name").is_some()) {
                "name"
            } else {
                "displayName"
            };

            let structured: Vec<serde_json::Value> = jobs
                .iter()
                .map(|j| {
                    let mut m = serde_json::Map::new();
                    for &field in &[name_field, "jobId", "taskRunStatus", "startedAt", "endedAt", "createdBy", "createdAt"] {
                        let v = j.get(field).and_then(|v| v.as_str()).unwrap_or("");
                        m.insert(field.into(), serde_json::Value::String(v.to_string()));
                    }
                    m.insert("estimatedTimeRemaining".into(),
                        serde_json::Value::String("N/A".into()));
                    serde_json::Value::Object(m)
                })
                .collect();

            println!(
                "Displaying {} of {} Jobs starting at {}",
                structured.len(), total, item_offset
            );
            println!();
            println!("{}", crate::common::cli_object_repr(&serde_json::json!(structured)));
            Ok(())
        }
        JobAction::Get { profile, farm_id, queue_id, job_id } => {
            let config = apply_profile(profile)?;
            let farm = require_setting("farm_id", farm_id, "defaults.farm_id", config.as_ref())?;
            let queue = require_setting("queue_id", queue_id, "defaults.queue_id", config.as_ref())?;
            let job = require_setting("job_id", job_id, "defaults.job_id", config.as_ref())?;
            match api::get_job(&farm, &queue, &job, config.as_ref()).await {
                Ok(resp) => {
                    println!("{}", crate::common::cli_object_repr(&resp));
                    Ok(())
                }
                Err(e) => {
                    let suggestion = suggest_resources_on_client_error(
                        &e.to_string(),
                        Some(&farm),
                        Some(&queue),
                        None,
                        config.as_ref(),
                    )
                    .await;
                    Err(CliError::Operation(format!(
                        "Failed to get Job from Deadline:\n{e}{suggestion}"
                    )))
                }
            }
        }
    }
}
