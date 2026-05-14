use deadline_lib::api::{client, session};
use deadline_lib::config::ini::IniConfig;

/// When an API call fails with AccessDenied/ResourceNotFound/ValidationException,
/// try to list available resources to help the user identify typos.
/// Dispatches suggestion chains based on which API operation failed,
/// matching Python's `_OPERATION_GROUPS` pattern.
/// Returns a suggestion string to append to the error message, or empty string.
pub(crate) async fn suggest_resources_on_client_error(
    error_msg: &str,
    operation_name: &str,
    farm_id: Option<&str>,
    queue_id: Option<&str>,
    fleet_id: Option<&str>,
    config: &IniConfig,
) -> String {
    // Only handle access/not-found/validation errors
    let is_suggestable = error_msg.contains("AccessDeniedException")
        || error_msg.contains("ResourceNotFoundException")
        || error_msg.contains("ValidationException");
    if !is_suggestable {
        return String::new();
    }

    let mut suggestions: Vec<String> = Vec::new();

    // Dispatch based on operation name (matching Python's _OPERATION_GROUPS)
    let found = match operation_name {
        "GetQueue" | "ListQueues" | "ListQueueEnvironments" => {
            if let Some(fid) = farm_id {
                try_list_queues(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else {
                try_list_farms(config, &mut suggestions).await
            }
        }
        "GetFleet" | "ListFleets" => {
            if let Some(fid) = farm_id {
                try_list_fleets(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else {
                try_list_farms(config, &mut suggestions).await
            }
        }
        "GetWorker" | "SearchWorkers" => {
            if let (Some(fid), Some(flid)) = (farm_id, fleet_id) {
                try_list_workers(fid, flid, config, &mut suggestions).await
                    || try_list_fleets(fid, config, &mut suggestions).await
            } else if let Some(fid) = farm_id {
                try_list_fleets(fid, config, &mut suggestions).await
            } else {
                false
            }
        }
        "GetJob" | "ListJobs" | "SearchJobs" | "CreateJob" => {
            if let (Some(fid), Some(qid)) = (farm_id, queue_id) {
                try_list_jobs(fid, qid, config, &mut suggestions).await
                    || try_list_queues(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else if let Some(fid) = farm_id {
                try_list_queues(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else {
                try_list_farms(config, &mut suggestions).await
            }
        }
        "GetStorageProfileForQueue" | "ListStorageProfilesForQueue" => {
            if let (Some(fid), Some(qid)) = (farm_id, queue_id) {
                try_list_storage_profiles(fid, qid, config, &mut suggestions).await
                    || try_list_queues(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else if let Some(fid) = farm_id {
                try_list_queues(fid, config, &mut suggestions).await
                    || try_list_farms(config, &mut suggestions).await
            } else {
                try_list_farms(config, &mut suggestions).await
            }
        }
        // Unknown operation: fall back to listing farms
        _ => try_list_farms(config, &mut suggestions).await,
    };

    if found {
        return suggestions.join("\n");
    }

    if suggestions.is_empty() {
        "\nCould not list available resources to suggest alternatives.\n\
         This may indicate your IAM policy is missing List permissions."
            .to_owned()
    } else {
        suggestions.join("\n")
    }
}

async fn try_list_farms(config: &IniConfig, out: &mut Vec<String>) -> bool {
    let dl = session::deadline_client(config).await;
    let builder = client::apply_dcm_principal(dl.list_farms(), config);
    let resp = client::collect_paginated(builder.into_paginator().send()).await;
    match resp {
        Ok(pages) => {
            let items: Vec<serde_json::Value> = pages
                .iter()
                .flat_map(aws_sdk_deadline::operation::list_farms::ListFarmsOutput::farms)
                .map(
                    |f| serde_json::json!({"farmId": f.farm_id(), "displayName": f.display_name()}),
                )
                .collect();
            format_suggestions(
                Some(&items),
                "farmId",
                "displayName",
                "Available farms:",
                out,
            )
        }
        Err(_) => false,
    }
}

async fn try_list_queues(farm_id: &str, config: &IniConfig, out: &mut Vec<String>) -> bool {
    let dl = session::deadline_client(config).await;
    let builder = client::apply_dcm_principal(dl.list_queues().farm_id(farm_id), config);
    let resp = client::collect_paginated(builder.into_paginator().send()).await;
    match resp {
        Ok(pages) => {
            let items: Vec<serde_json::Value> = pages
                .iter()
                .flat_map(aws_sdk_deadline::operation::list_queues::ListQueuesOutput::queues)
                .map(|q| serde_json::json!({"queueId": q.queue_id(), "displayName": q.display_name()}))
                .collect();
            format_suggestions(
                Some(&items),
                "queueId",
                "displayName",
                &format!("Available queues in farm {farm_id}:"),
                out,
            )
        }
        Err(_) => false,
    }
}

async fn try_list_fleets(farm_id: &str, config: &IniConfig, out: &mut Vec<String>) -> bool {
    let dl = session::deadline_client(config).await;
    let builder = client::apply_dcm_principal(dl.list_fleets().farm_id(farm_id), config);
    let resp = client::collect_paginated(builder.into_paginator().send()).await;
    match resp {
        Ok(pages) => {
            let items: Vec<serde_json::Value> = pages
                .iter()
                .flat_map(aws_sdk_deadline::operation::list_fleets::ListFleetsOutput::fleets)
                .map(|f| serde_json::json!({"fleetId": f.fleet_id(), "displayName": f.display_name()}))
                .collect();
            format_suggestions(
                Some(&items),
                "fleetId",
                "displayName",
                &format!("Available fleets in farm {farm_id}:"),
                out,
            )
        }
        Err(_) => false,
    }
}

async fn try_list_jobs(
    farm_id: &str,
    queue_id: &str,
    config: &IniConfig,
    out: &mut Vec<String>,
) -> bool {
    let dl = session::deadline_client(config).await;
    let builder =
        client::apply_dcm_principal(dl.list_jobs().farm_id(farm_id).queue_id(queue_id), config);
    match client::collect_paginated(builder.into_paginator().send()).await {
        Ok(pages) => {
            let items: Vec<serde_json::Value> = pages
                .iter()
                .flat_map(aws_sdk_deadline::operation::list_jobs::ListJobsOutput::jobs)
                .map(|j| serde_json::json!({"jobId": j.job_id(), "name": j.name()}))
                .collect();
            format_suggestions(
                Some(&items),
                "jobId",
                "name",
                &format!("Recent jobs in queue {queue_id}:"),
                out,
            )
        }
        Err(_) => false,
    }
}

async fn try_list_workers(
    farm_id: &str,
    fleet_id: &str,
    config: &IniConfig,
    out: &mut Vec<String>,
) -> bool {
    let dl = session::deadline_client(config).await;
    let resp = dl
        .search_workers()
        .farm_id(farm_id)
        .fleet_ids(fleet_id)
        .item_offset(0)
        .page_size(10)
        .send()
        .await;
    match resp {
        Ok(output) => {
            let workers = output.workers();
            if workers.is_empty() {
                return false;
            }
            out.push(format!("\nAvailable workers in fleet {fleet_id}:"));
            for w in workers.iter().take(10) {
                let id = w.worker_id().unwrap_or("");
                let status = w
                    .status()
                    .map_or("", aws_sdk_deadline::types::WorkerStatus::as_str);
                out.push(format!("  {id}  {status}"));
            }
            let total = i64::from(output.total_results());
            if total > 10 {
                out.push(format!("  ... and {} more", total - 10));
            }
            true
        }
        Err(_) => false,
    }
}

async fn try_list_storage_profiles(
    farm_id: &str,
    queue_id: &str,
    config: &IniConfig,
    out: &mut Vec<String>,
) -> bool {
    let client = session::deadline_client(config).await;
    match client::collect_paginated(
        client
            .list_storage_profiles_for_queue()
            .farm_id(farm_id)
            .queue_id(queue_id)
            .into_paginator()
            .send(),
    )
    .await
    {
        Ok(pages) => {
            let profiles: Vec<serde_json::Value> = pages.iter()
                .flat_map(aws_sdk_deadline::operation::list_storage_profiles_for_queue::ListStorageProfilesForQueueOutput::storage_profiles)
                .map(|sp| serde_json::json!({
                    "storageProfileId": sp.storage_profile_id(),
                    "displayName": sp.display_name(),
                }))
                .collect();
            format_suggestions(
                Some(&profiles),
                "storageProfileId",
                "displayName",
                &format!("Available storage profiles for queue {queue_id}:"),
                out,
            )
        }
        Err(_) => false,
    }
}

fn format_suggestions(
    items: Option<&Vec<serde_json::Value>>,
    id_field: &str,
    name_field: &str,
    header: &str,
    out: &mut Vec<String>,
) -> bool {
    let items = match items {
        Some(v) if !v.is_empty() => v,
        _ => return false,
    };
    out.push(format!("\n{header}"));
    for item in items.iter().take(10) {
        let id = item[id_field].as_str().unwrap_or("");
        let name = item[name_field].as_str().unwrap_or("");
        out.push(format!("  {id}  {name}"));
    }
    if items.len() > 10 {
        out.push(format!("  ... and {} more", items.len() - 10));
    }
    true
}
