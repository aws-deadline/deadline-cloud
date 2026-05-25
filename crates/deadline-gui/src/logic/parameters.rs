//! Queue parameter logic.
//!
//! Fetches queue environment parameters from the API, parses them from
//! environment templates, and merges with job template parameters.

use std::collections::HashMap;

/// Parse parameter definitions from queue environment templates.
pub fn parse_queue_environment_parameters(
    environments: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    let mut params = Vec::new();
    for env in environments {
        let template_str = match env.get("template").and_then(|t| t.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let template: serde_json::Value = match serde_yaml::from_str(template_str) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Some(defs) = template
            .get("parameterDefinitions")
            .and_then(|d| d.as_array())
        {
            params.extend(defs.iter().cloned());
        }
    }
    params
}

/// Merge queue parameters with job parameters. Job params override queue params with same name.
pub fn merge_queue_and_job_parameters(
    queue_params: &[serde_json::Value],
    job_params: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    let mut result: Vec<serde_json::Value> = queue_params
        .iter()
        .map(|qp| {
            let name = qp.get("name").and_then(|n| n.as_str()).unwrap_or("");
            if let Some(jp) = job_params
                .iter()
                .find(|jp| jp.get("name").and_then(|n| n.as_str()) == Some(name))
            {
                let mut merged = qp.clone();
                if let Some(v) = jp.get("value") {
                    merged["value"] = v.clone();
                }
                merged
            } else {
                qp.clone()
            }
        })
        .collect();

    for jp in job_params {
        let name = jp.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if !queue_params
            .iter()
            .any(|qp| qp.get("name").and_then(|n| n.as_str()) == Some(name))
        {
            result.push(jp.clone());
        }
    }
    result
}

/// Apply initial values (from submitter) to queue parameters.
pub fn apply_initial_values(
    params: &mut [serde_json::Value],
    initial_values: &HashMap<String, String>,
) {
    for param in params.iter_mut() {
        let name = param
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(value) = initial_values.get(&name) {
            param["value"] = serde_json::json!(value);
        }
    }
}

/// Sort queue environments by their `priority` field (ascending).
pub fn sort_environments_by_priority(environments: &mut [serde_json::Value]) {
    environments.sort_by_key(|e| e.get("priority").and_then(|p| p.as_i64()).unwrap_or(0));
}

/// Assign `userInterface.groupLabel` to parameters that lack one.
/// Sets it to `"Queue Environment: {env_name}"`.
pub fn assign_group_labels(params: &mut [serde_json::Value], env_name: &str) {
    let label = format!("Queue Environment: {env_name}");
    for param in params.iter_mut() {
        let has_group_label = param
            .get("userInterface")
            .and_then(|ui| ui.get("groupLabel"))
            .and_then(|gl| gl.as_str())
            .is_some_and(|s| !s.is_empty());
        if !has_group_label {
            let ui = param
                .as_object_mut()
                .unwrap()
                .entry("userInterface")
                .or_insert_with(|| serde_json::json!({}));
            ui["groupLabel"] = serde_json::json!(label);
        }
    }
}

/// Check for duplicate parameter names with conflicting definitions.
/// Returns Ok(deduplicated params) or Err(description of conflicts).
pub fn detect_parameter_conflicts(
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let mut seen: HashMap<&str, &serde_json::Value> = HashMap::new();
    let mut result = Vec::new();
    let mut conflicts = Vec::new();

    for param in params {
        let name = param.get("name").and_then(|n| n.as_str()).unwrap_or("");
        if let Some(existing) = seen.get(name) {
            // Compare fields that must match (value comparison)
            for field in &[
                "type",
                "minValue",
                "maxValue",
                "minLength",
                "maxLength",
                "dataFlow",
                "objectType",
            ] {
                let a = existing.get(*field);
                let b = param.get(*field);
                if a != b {
                    conflicts.push(format!(
                        "Parameter \"{name}\" has conflicting \"{field}\" across queue environments"
                    ));
                    break;
                }
            }
            // Compare allowedValues as sets (order-insensitive)
            let a_vals = existing.get("allowedValues").and_then(|v| v.as_array());
            let b_vals = param.get("allowedValues").and_then(|v| v.as_array());
            match (a_vals, b_vals) {
                (Some(a), Some(b)) => {
                    let a_set: std::collections::HashSet<&serde_json::Value> = a.iter().collect();
                    let b_set: std::collections::HashSet<&serde_json::Value> = b.iter().collect();
                    if a_set != b_set {
                        conflicts.push(format!(
                            "Parameter \"{name}\" has conflicting \"allowedValues\" across queue environments"
                        ));
                    }
                }
                (None, None) => {}
                _ => {
                    conflicts.push(format!(
                        "Parameter \"{name}\" has conflicting \"allowedValues\" across queue environments"
                    ));
                }
            }
        } else {
            seen.insert(name, param);
            result.push(param.clone());
        }
    }

    if conflicts.is_empty() {
        Ok(result)
    } else {
        Err(conflicts.join("\n"))
    }
}

/// Resolve the UI control type for a parameter definition.
/// Returns one of: LINE_EDIT, MULTILINE_EDIT, SPIN_BOX, DROPDOWN_LIST,
/// CHECK_BOX, CHOOSE_INPUT_FILE, CHOOSE_OUTPUT_FILE, CHOOSE_DIRECTORY, HIDDEN.
pub fn get_ui_control(param: &serde_json::Value) -> &'static str {
    // Explicit control takes priority
    if let Some(control) = param
        .get("userInterface")
        .and_then(|ui| ui.get("control"))
        .and_then(|c| c.as_str())
    {
        return match control {
            "LINE_EDIT" => "LINE_EDIT",
            "MULTILINE_EDIT" => "MULTILINE_EDIT",
            "SPIN_BOX" => "SPIN_BOX",
            "DROPDOWN_LIST" => "DROPDOWN_LIST",
            "CHECK_BOX" => "CHECK_BOX",
            "CHOOSE_INPUT_FILE" => "CHOOSE_INPUT_FILE",
            "CHOOSE_OUTPUT_FILE" => "CHOOSE_OUTPUT_FILE",
            "CHOOSE_DIRECTORY" => "CHOOSE_DIRECTORY",
            "HIDDEN" => "HIDDEN",
            _ => "LINE_EDIT",
        };
    }

    // allowedValues → DROPDOWN_LIST
    if param.get("allowedValues").is_some() {
        return "DROPDOWN_LIST";
    }

    let param_type = param
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or("STRING");
    match param_type {
        "STRING" => "LINE_EDIT",
        "INT" | "FLOAT" => "SPIN_BOX",
        "PATH" => {
            let obj_type = param
                .get("objectType")
                .and_then(|o| o.as_str())
                .unwrap_or("DIRECTORY");
            if obj_type == "FILE" {
                let data_flow = param
                    .get("dataFlow")
                    .and_then(|d| d.as_str())
                    .unwrap_or("NONE");
                if data_flow == "OUT" {
                    "CHOOSE_OUTPUT_FILE"
                } else {
                    "CHOOSE_INPUT_FILE"
                }
            } else {
                "CHOOSE_DIRECTORY"
            }
        }
        _ => "LINE_EDIT",
    }
}

/// Find parameter names in `job_params` not recognized by template or queue. Returns sorted.
pub fn find_unrecognized_parameters(
    job_params: &[serde_json::Value],
    template_params: &[serde_json::Value],
    queue_params: &[serde_json::Value],
) -> Vec<String> {
    let template_names: std::collections::HashSet<&str> = template_params
        .iter()
        .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
        .collect();
    let queue_names: std::collections::HashSet<&str> = queue_params
        .iter()
        .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
        .collect();

    let mut unrecognized: Vec<String> = job_params
        .iter()
        .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
        .filter(|name| !template_names.contains(name) && !queue_names.contains(name))
        .map(String::from)
        .collect();
    unrecognized.sort();
    unrecognized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_queue_parameters_from_environments() {
        let environments = vec![serde_json::json!({
            "queueEnvironmentId": "env-001",
            "name": "Rez",
            "template": "specificationVersion: environment-2023-09\nenvironment:\n  name: Rez\nparameterDefinitions:\n- name: RezPackages\n  type: STRING\n  default: \"\"\n- name: CondaPackages\n  type: STRING\n  default: \"\"\n"
        })];
        let params = parse_queue_environment_parameters(&environments);
        assert_eq!(params.len(), 2);
        assert_eq!(params[0]["name"], "RezPackages");
        assert_eq!(params[1]["name"], "CondaPackages");
    }

    #[test]
    fn parse_queue_parameters_multiple_environments() {
        let environments = vec![
            serde_json::json!({
                "queueEnvironmentId": "env-001",
                "name": "Rez",
                "template": "specificationVersion: environment-2023-09\nenvironment:\n  name: Rez\nparameterDefinitions:\n- name: RezPackages\n  type: STRING\n  default: \"\"\n"
            }),
            serde_json::json!({
                "queueEnvironmentId": "env-002",
                "name": "Custom",
                "template": "specificationVersion: environment-2023-09\nenvironment:\n  name: Custom\nparameterDefinitions:\n- name: CustomParam\n  type: INT\n  default: \"42\"\n"
            }),
        ];
        let params = parse_queue_environment_parameters(&environments);
        assert_eq!(params.len(), 2);
        let names: Vec<&str> = params.iter().map(|p| p["name"].as_str().unwrap()).collect();
        assert!(names.contains(&"RezPackages"));
        assert!(names.contains(&"CustomParam"));
    }

    #[test]
    fn parse_queue_parameters_empty_environments() {
        let params = parse_queue_environment_parameters(&[]);
        assert!(params.is_empty());
    }

    #[test]
    fn parse_queue_parameters_no_parameter_definitions() {
        let environments = vec![serde_json::json!({
            "queueEnvironmentId": "env-001",
            "name": "NoParams",
            "template": "specificationVersion: environment-2023-09\nenvironment:\n  name: NoParams\n"
        })];
        assert!(parse_queue_environment_parameters(&environments).is_empty());
    }

    #[test]
    fn merge_parameters_job_overrides_queue() {
        let queue_params = vec![
            serde_json::json!({"name": "RezPackages", "type": "STRING", "value": ""}),
            serde_json::json!({"name": "Frames", "type": "STRING", "value": "1-100"}),
        ];
        let job_params = vec![serde_json::json!({"name": "Frames", "value": "1-10"})];
        let merged = merge_queue_and_job_parameters(&queue_params, &job_params);
        assert_eq!(
            merged.iter().find(|p| p["name"] == "RezPackages").unwrap()["value"],
            ""
        );
        assert_eq!(
            merged.iter().find(|p| p["name"] == "Frames").unwrap()["value"],
            "1-10"
        );
    }

    #[test]
    fn merge_parameters_job_only_params_appended() {
        let queue_params =
            vec![serde_json::json!({"name": "RezPackages", "type": "STRING", "value": ""})];
        let job_params =
            vec![serde_json::json!({"name": "SceneFile", "value": "/path/to/scene.ma"})];
        let merged = merge_queue_and_job_parameters(&queue_params, &job_params);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_parameters_empty_queue_params() {
        let job_params = vec![serde_json::json!({"name": "Frames", "value": "1-10"})];
        let merged = merge_queue_and_job_parameters(&[], &job_params);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0]["name"], "Frames");
    }

    #[test]
    fn merge_parameters_empty_job_params() {
        let queue_params = vec![
            serde_json::json!({"name": "RezPackages", "type": "STRING", "value": "maya-2024"}),
        ];
        let merged = merge_queue_and_job_parameters(&queue_params, &[]);
        assert_eq!(merged[0]["value"], "maya-2024");
    }

    #[test]
    fn apply_initial_values_overrides_defaults() {
        let mut params = vec![
            serde_json::json!({"name": "RezPackages", "type": "STRING", "value": ""}),
            serde_json::json!({"name": "Frames", "type": "STRING", "value": "1-100"}),
        ];
        let initial_values = [("RezPackages".to_string(), "maya-2024 arnold-5".to_string())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        apply_initial_values(&mut params, &initial_values);
        assert_eq!(
            params.iter().find(|p| p["name"] == "RezPackages").unwrap()["value"],
            "maya-2024 arnold-5"
        );
        assert_eq!(
            params.iter().find(|p| p["name"] == "Frames").unwrap()["value"],
            "1-100"
        );
    }

    #[test]
    fn apply_initial_values_unknown_names_ignored() {
        let mut params =
            vec![serde_json::json!({"name": "RezPackages", "type": "STRING", "value": ""})];
        let initial_values = [("NonExistent".to_string(), "value".to_string())]
            .into_iter()
            .collect::<HashMap<_, _>>();
        apply_initial_values(&mut params, &initial_values);
        assert_eq!(params[0]["value"], "");
    }

    #[test]
    fn validate_parameters_all_recognized() {
        let job_params = vec![serde_json::json!({"name": "Frames", "value": "1-10"})];
        let template_params = vec![serde_json::json!({"name": "Frames", "type": "STRING"})];
        let queue_params = vec![serde_json::json!({"name": "RezPackages", "type": "STRING"})];
        assert!(
            find_unrecognized_parameters(&job_params, &template_params, &queue_params).is_empty()
        );
    }

    #[test]
    fn validate_parameters_detects_unrecognized() {
        let job_params = vec![
            serde_json::json!({"name": "Frames", "value": "1-10"}),
            serde_json::json!({"name": "Bogus", "value": "x"}),
            serde_json::json!({"name": "AlsoBogus", "value": "y"}),
        ];
        let template_params = vec![serde_json::json!({"name": "Frames", "type": "STRING"})];
        let unrecognized = find_unrecognized_parameters(&job_params, &template_params, &[]);
        assert_eq!(unrecognized, vec!["AlsoBogus", "Bogus"]);
    }

    #[test]
    fn validate_parameters_recognized_in_queue() {
        let job_params = vec![serde_json::json!({"name": "RezPackages", "value": "maya-2024"})];
        let queue_params = vec![serde_json::json!({"name": "RezPackages", "type": "STRING"})];
        assert!(find_unrecognized_parameters(&job_params, &[], &queue_params).is_empty());
    }

    #[test]
    fn sort_environments_by_priority_ascending() {
        let mut envs = vec![
            serde_json::json!({"name": "High", "priority": 20}),
            serde_json::json!({"name": "Low", "priority": 5}),
            serde_json::json!({"name": "Mid", "priority": 10}),
        ];
        sort_environments_by_priority(&mut envs);
        let names: Vec<&str> = envs.iter().map(|e| e["name"].as_str().unwrap()).collect();
        assert_eq!(names, vec!["Low", "Mid", "High"]);
    }

    #[test]
    fn assign_group_labels() {
        let mut params = vec![
            serde_json::json!({"name": "A", "type": "STRING"}),
            serde_json::json!({"name": "B", "type": "INT", "userInterface": {"groupLabel": "Custom Group"}}),
            serde_json::json!({"name": "C", "type": "FLOAT", "userInterface": {"control": "SPIN_BOX"}}),
        ];
        super::assign_group_labels(&mut params, "Rez");

        // A had no userInterface → gets group label from env name
        assert_eq!(
            params[0]["userInterface"]["groupLabel"],
            "Queue Environment: Rez"
        );
        // B already had a groupLabel → preserved
        assert_eq!(params[1]["userInterface"]["groupLabel"], "Custom Group");
        // C had userInterface but no groupLabel → gets one
        assert_eq!(
            params[2]["userInterface"]["groupLabel"],
            "Queue Environment: Rez"
        );
    }

    #[test]
    fn detect_parameter_conflicts() {
        // Matching params → deduplicated (Ok with 1 entry)
        let matching = vec![
            serde_json::json!({"name": "Frames", "type": "STRING", "default": "1-100"}),
            serde_json::json!({"name": "Frames", "type": "STRING", "default": "1-100"}),
        ];
        let result = super::detect_parameter_conflicts(&matching);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);

        // Type mismatch → Err
        let type_mismatch = vec![
            serde_json::json!({"name": "Param", "type": "STRING"}),
            serde_json::json!({"name": "Param", "type": "INT"}),
        ];
        assert!(super::detect_parameter_conflicts(&type_mismatch).is_err());

        // allowedValues mismatch → Err
        let values_mismatch = vec![
            serde_json::json!({"name": "Mode", "type": "STRING", "allowedValues": ["A", "B"]}),
            serde_json::json!({"name": "Mode", "type": "STRING", "allowedValues": ["X", "Y"]}),
        ];
        assert!(super::detect_parameter_conflicts(&values_mismatch).is_err());

        // allowedValues same content different order → Ok (set comparison)
        let values_reordered = vec![
            serde_json::json!({"name": "Mode", "type": "STRING", "allowedValues": ["A", "B"]}),
            serde_json::json!({"name": "Mode", "type": "STRING", "allowedValues": ["B", "A"]}),
        ];
        assert!(super::detect_parameter_conflicts(&values_reordered).is_ok());

        // dataFlow mismatch → Err
        let dataflow_mismatch = vec![
            serde_json::json!({"name": "Path", "type": "PATH", "dataFlow": "IN"}),
            serde_json::json!({"name": "Path", "type": "PATH", "dataFlow": "OUT"}),
        ];
        assert!(super::detect_parameter_conflicts(&dataflow_mismatch).is_err());

        // objectType mismatch → Err
        let objtype_mismatch = vec![
            serde_json::json!({"name": "Path", "type": "PATH", "objectType": "FILE"}),
            serde_json::json!({"name": "Path", "type": "PATH", "objectType": "DIRECTORY"}),
        ];
        assert!(super::detect_parameter_conflicts(&objtype_mismatch).is_err());
    }

    #[test]
    fn get_ui_control_resolves_defaults() {
        // STRING with no control → LINE_EDIT
        assert_eq!(
            get_ui_control(&serde_json::json!({"name": "A", "type": "STRING"})),
            "LINE_EDIT"
        );
        // INT with no control → SPIN_BOX
        assert_eq!(
            get_ui_control(&serde_json::json!({"name": "B", "type": "INT"})),
            "SPIN_BOX"
        );
        // PATH with objectType FILE → CHOOSE_INPUT_FILE
        assert_eq!(
            get_ui_control(&serde_json::json!({"name": "C", "type": "PATH", "objectType": "FILE"})),
            "CHOOSE_INPUT_FILE"
        );
        // Any type with allowedValues → DROPDOWN_LIST
        assert_eq!(
            get_ui_control(
                &serde_json::json!({"name": "D", "type": "STRING", "allowedValues": ["X", "Y"]})
            ),
            "DROPDOWN_LIST"
        );
        // Explicit control override
        assert_eq!(
            get_ui_control(
                &serde_json::json!({"name": "E", "type": "STRING", "userInterface": {"control": "MULTILINE_EDIT"}})
            ),
            "MULTILINE_EDIT"
        );
    }
}
