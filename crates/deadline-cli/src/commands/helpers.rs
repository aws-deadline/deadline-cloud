use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use deadline_api::api;

use super::config::CliError;

/// Apply --profile option to config, returning Some(config) if profile was set.
pub fn apply_profile(profile: Option<String>) -> Result<Option<IniConfig>, CliError> {
    match profile {
        Some(p) => {
            let mut config = config_file::read_config()?;
            config_file::set_setting_in_config("defaults.aws_profile_name", &p, &mut config)?;
            Ok(Some(config))
        }
        None => Ok(None),
    }
}

/// When an API call fails with AccessDenied/ResourceNotFound/ValidationException,
/// try to list available resources to help the user identify typos.
/// Dispatches suggestion chains based on which API operation failed,
/// matching Python's `_OPERATION_GROUPS` pattern.
/// Returns a suggestion string to append to the error message, or empty string.
pub async fn suggest_resources_on_client_error(
    error_msg: &str,
    operation_name: &str,
    farm_id: Option<&str>,
    queue_id: Option<&str>,
    fleet_id: Option<&str>,
    config: Option<&IniConfig>,
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
        "GetFarm" | "ListFarms" => {
            try_list_farms(config, &mut suggestions).await
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
            .to_string()
    } else {
        suggestions.join("\n")
    }
}

async fn try_list_farms(config: Option<&IniConfig>, out: &mut Vec<String>) -> bool {
    match api::list_farms(config, None).await {
        Ok(resp) => format_suggestions(
            resp["farms"].as_array(),
            "farmId",
            "displayName",
            "Available farms:",
            out,
        ),
        Err(_) => false,
    }
}

async fn try_list_queues(farm_id: &str, config: Option<&IniConfig>, out: &mut Vec<String>) -> bool {
    match api::list_queues(farm_id, config, None).await {
        Ok(resp) => format_suggestions(
            resp["queues"].as_array(),
            "queueId",
            "displayName",
            &format!("Available queues in farm {farm_id}:"),
            out,
        ),
        Err(_) => false,
    }
}

async fn try_list_fleets(farm_id: &str, config: Option<&IniConfig>, out: &mut Vec<String>) -> bool {
    match api::list_fleets(farm_id, config, None).await {
        Ok(resp) => format_suggestions(
            resp["fleets"].as_array(),
            "fleetId",
            "displayName",
            &format!("Available fleets in farm {farm_id}:"),
            out,
        ),
        Err(_) => false,
    }
}

async fn try_list_jobs(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    out: &mut Vec<String>,
) -> bool {
    match api::list_jobs(farm_id, queue_id, config, None).await {
        Ok(resp) => format_suggestions(
            resp["jobs"].as_array(),
            "jobId",
            "name",
            &format!("Recent jobs in queue {queue_id}:"),
            out,
        ),
        Err(_) => false,
    }
}

async fn try_list_workers(
    farm_id: &str,
    fleet_id: &str,
    config: Option<&IniConfig>,
    out: &mut Vec<String>,
) -> bool {
    match api::search_workers(farm_id, &[fleet_id], 0, 10, config, None).await {
        Ok(resp) => {
            let workers = match resp["workers"].as_array() {
                Some(w) if !w.is_empty() => w,
                _ => return false,
            };
            out.push(format!("\nAvailable workers in fleet {fleet_id}:"));
            for w in workers.iter().take(10) {
                let id = w["workerId"].as_str().unwrap_or("");
                let status = w["status"].as_str().unwrap_or("");
                out.push(format!("  {id}  {status}"));
            }
            let total = resp["totalResults"].as_i64().unwrap_or(workers.len() as i64);
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
    config: Option<&IniConfig>,
    out: &mut Vec<String>,
) -> bool {
    match api::list_storage_profiles_for_queue(farm_id, queue_id, config, None).await {
        Ok(resp) => format_suggestions(
            resp["storageProfiles"].as_array(),
            "storageProfileId",
            "displayName",
            &format!("Available storage profiles for queue {queue_id}:"),
            out,
        ),
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
