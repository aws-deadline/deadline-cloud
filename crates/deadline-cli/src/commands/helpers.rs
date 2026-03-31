use deadline_config::config_file;
use deadline_config::ini::IniConfig;
use deadline_client::api;

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

/// Get a required setting from CLI arg, config, or return an error.
pub fn require_setting(
    name: &str,
    cli_arg: Option<String>,
    setting_name: &str,
    config: Option<&IniConfig>,
) -> Result<String, CliError> {
    if let Some(v) = cli_arg {
        return Ok(v);
    }
    let v = match config {
        Some(c) => config_file::get_setting_with_config(setting_name, c).unwrap_or_default(),
        None => config_file::get_setting(setting_name).unwrap_or_default(),
    };
    if v.is_empty() {
        Err(CliError::Operation(format!(
            "Missing '--{n}' or default {label} configuration",
            n = name.replace('_', "-"),
            label = name.replace('_', " ").replace("id", "ID"),
        )))
    } else {
        Ok(v)
    }
}

/// When an API call fails with AccessDenied/ResourceNotFound/ValidationException,
/// try to list available resources to help the user identify typos.
/// Returns a suggestion string to append to the error message, or empty string.
pub async fn suggest_resources_on_client_error(
    error_msg: &str,
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

    // Build a chain of fetchers based on what resource IDs we have.
    // Try the most specific first, fall back to broader.
    let mut suggestions: Vec<String> = Vec::new();

    // Try listing resources in order of specificity
    if let Some(fid) = farm_id {
        if let Some(qid) = queue_id {
            // We have farm + queue — try listing jobs, then queues, then farms
            if try_list_jobs(fid, qid, config, &mut suggestions).await {
                return suggestions.join("\n");
            }
        }
        if let Some(flid) = fleet_id {
            // We have farm + fleet — try listing workers, then fleets, then farms
            if try_list_workers(fid, flid, config, &mut suggestions).await {
                return suggestions.join("\n");
            }
        }
        // We have farm — try listing queues or fleets, then farms
        if try_list_queues(fid, config, &mut suggestions).await {
            return suggestions.join("\n");
        }
        if try_list_fleets(fid, config, &mut suggestions).await {
            return suggestions.join("\n");
        }
    }

    // Fall back to listing farms
    if try_list_farms(config, &mut suggestions).await {
        return suggestions.join("\n");
    }

    // All list calls failed
    if suggestions.is_empty() {
        return "\nCould not list available resources to suggest alternatives.\n\
                This may indicate your IAM policy is missing List permissions."
            .to_string();
    }

    suggestions.join("\n")
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
