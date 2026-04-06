//! Queue parameter definitions — fetches queue environment templates,
//! extracts parameterDefinitions, validates, deduplicates, and sets
//! groupLabel. Helper functions will move to deadline-job-bundle when
//! work item #7 is implemented.

use deadline_config::ini::IniConfig;
use deadline_models::errors::DeadlineError;
use deadline_common::telemetry::TelemetryClient;
use serde_json::Value;

use crate::api;

/// Fetch all queue parameter definitions for a queue.
pub async fn get_queue_parameter_definitions(
    farm_id: &str,
    queue_id: &str,
    config: Option<&IniConfig>,
    telemetry: Option<&TelemetryClient>,
) -> Result<Vec<Value>, DeadlineError> {
    let resp = api::list_queue_environments(farm_id, queue_id, config, telemetry).await?;
    let envs = resp["environments"].as_array().cloned().unwrap_or_default();

    // Fetch full environment details and sort by priority
    let mut full_envs = Vec::new();
    for env in &envs {
        let env_id = env["queueEnvironmentId"].as_str().unwrap_or("");
        let full = api::get_queue_environment(farm_id, queue_id, env_id, config, telemetry).await?;
        full_envs.push(full);
    }
    full_envs.sort_by_key(|e| e["priority"].as_i64().unwrap_or(0));

    // Parse templates and collect parameters
    let mut params: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
    for env in &full_envs {
        let template_str = env["template"].as_str().unwrap_or("");
        let template: Value = serde_yaml::from_str(template_str)
            .map_err(|e| DeadlineError::OperationError(format!("Failed to parse environment template: {e}")))?;

        let env_name = template.get("environment")
            .and_then(|e| e["name"].as_str())
            .unwrap_or("");

        let param_defs = match template.get("parameterDefinitions") {
            Some(Value::Array(arr)) => arr.clone(),
            Some(_) => return Err(DeadlineError::OperationError(
                "parameterDefinitions must be a list".into(),
            )),
            None => continue,
        };

        for mut param in param_defs {
            validate_job_parameter(&param, true, true)?;

            // Auto-set userInterface.control and groupLabel
            let needs_group_label = param.get("userInterface")
                .and_then(|ui| ui.get("groupLabel"))
                .and_then(|g| g.as_str())
                .map_or(true, |s| s.is_empty());

            if needs_group_label {
                let control = if param.get("userInterface").and_then(|ui| ui.get("control")).is_none() {
                    Some(get_ui_control(&param)?)
                } else {
                    None
                };
                let ui = param.as_object_mut().unwrap()
                    .entry("userInterface").or_insert_with(|| serde_json::json!({}));
                if let Some(ctrl) = control {
                    ui["control"] = Value::String(ctrl);
                }
                ui["groupLabel"] = Value::String(format!("Queue Environment: {env_name}"));
            }

            let name = param["name"].as_str().unwrap_or("").to_string();
            if let Some(existing) = params.get(&name) {
                let diffs = parameter_definition_difference(existing, &param);
                if !diffs.is_empty() {
                    return Err(DeadlineError::OperationError(format!(
                        "Job template parameter {} is duplicated across queue environments with mismatched fields:\n{}",
                        name, diffs.join(" ")
                    )));
                }
            } else {
                params.insert(name, param);
            }
        }
    }

    Ok(params.into_values().collect())
}

fn validate_job_parameter(param: &Value, type_required: bool, default_required: bool) -> Result<(), DeadlineError> {
    let obj = param.as_object().ok_or_else(|| {
        DeadlineError::OperationError(format!("Expected a dict for job parameter, but got {param}"))
    })?;

    let name = obj.get("name").and_then(|n| n.as_str()).ok_or_else(|| {
        DeadlineError::OperationError(format!("No \"name\" field in job parameter. Got {param}"))
    })?;
    if name.is_empty() {
        return Err(DeadlineError::OperationError("Job parameter has an empty name".into()));
    }

    if type_required && !obj.contains_key("type") {
        return Err(DeadlineError::OperationError(
            format!("Job parameter \"{name}\" is missing required key \"type\""),
        ));
    }
    if let Some(t) = obj.get("type").and_then(|t| t.as_str()) {
        if !["INT", "FLOAT", "STRING", "PATH"].contains(&t) {
            return Err(DeadlineError::OperationError(
                format!("Job parameter \"{name}\" had \"type\" {t} but expected one of (\"INT\", \"FLOAT\", \"STRING\", \"PATH\")"),
            ));
        }
    }

    if default_required && !obj.contains_key("default") {
        return Err(DeadlineError::OperationError(
            format!("Job parameter \"{name}\" is missing required key \"default\""),
        ));
    }

    Ok(())
}

fn get_ui_control(param: &Value) -> Result<String, DeadlineError> {
    if let Some(ctrl) = param.get("userInterface").and_then(|ui| ui.get("control")).and_then(|c| c.as_str()) {
        return Ok(ctrl.to_string());
    }
    let param_type = param["type"].as_str().unwrap_or("");
    if param.get("allowedValues").is_some() {
        return Ok("DROPDOWN_LIST".into());
    }
    match param_type {
        "STRING" => Ok("LINE_EDIT".into()),
        "PATH" => {
            let obj_type = param.get("objectType").and_then(|o| o.as_str()).unwrap_or("DIRECTORY");
            if obj_type == "FILE" {
                let data_flow = param.get("dataFlow").and_then(|d| d.as_str()).unwrap_or("NONE");
                if data_flow == "OUT" { Ok("CHOOSE_OUTPUT_FILE".into()) } else { Ok("CHOOSE_INPUT_FILE".into()) }
            } else {
                Ok("CHOOSE_DIRECTORY".into())
            }
        }
        "INT" | "FLOAT" => Ok("SPIN_BOX".into()),
        _ => Err(DeadlineError::OperationError(format!(
            "The job template parameter '{}' specifies an unsupported type '{param_type}'.",
            param.get("name").and_then(|n| n.as_str()).unwrap_or("<unnamed>")
        ))),
    }
}

fn parameter_definition_difference(lhs: &Value, rhs: &Value) -> Vec<String> {
    let fields = ["name", "type", "minValue", "maxValue", "minLength", "maxLength", "dataFlow", "objectType"];
    let mut diffs = Vec::new();
    for field in &fields {
        if lhs.get(field) != rhs.get(field) {
            diffs.push(field.to_string());
        }
    }
    // allowedValues: compare as sets
    if let (Some(Value::Array(a)), Some(Value::Array(b))) = (lhs.get("allowedValues"), rhs.get("allowedValues")) {
        let set_a: std::collections::HashSet<&Value> = a.iter().collect();
        let set_b: std::collections::HashSet<&Value> = b.iter().collect();
        if set_a != set_b {
            diffs.push("allowedValues".to_string());
        }
    } else if lhs.get("allowedValues") != rhs.get("allowedValues") {
        diffs.push("allowedValues".to_string());
    }
    diffs
}
