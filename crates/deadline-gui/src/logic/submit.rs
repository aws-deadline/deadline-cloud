//! Submit dialog bundle preparation logic.
//!
//! Handles copying the job template, applying parameters, writing
//! asset references, and copying hooks — the Rust equivalent of
//! Python's `on_create_job_bundle_callback`.

use std::path::Path;

use super::attachments::AssetReferences;

/// Settings collected from the submit dialog UI.
#[derive(Debug, Clone)]
pub struct SubmitSettings {
    pub name: String,
    pub description: String,
    pub input_job_bundle_dir: String,
    pub priority: i32,
    pub initial_status: String,
    pub max_failed_tasks_count: i32,
    pub max_retries_per_task: i32,
    pub max_worker_count: i32, // -1 = unlimited
    pub parameters: Vec<serde_json::Value>,
}

impl Default for SubmitSettings {
    fn default() -> Self {
        Self {
            name: "Job bundle submission".to_string(),
            description: String::new(),
            input_job_bundle_dir: String::new(),
            priority: 50,
            initial_status: "READY".to_string(),
            max_failed_tasks_count: 20,
            max_retries_per_task: 5,
            max_worker_count: -1,
            parameters: Vec::new(),
        }
    }
}

/// Prepare a job bundle in `output_dir` from the source bundle.
///
/// Returns the merged parameter values list on success.
pub fn prepare_job_bundle(
    output_dir: &Path,
    settings: &SubmitSettings,
    queue_parameters: &[serde_json::Value],
    asset_references: &AssetReferences,
    host_requirements: Option<&serde_json::Value>,
) -> Result<Vec<serde_json::Value>, String> {
    let bundle_dir = Path::new(&settings.input_job_bundle_dir);

    let (template_content, file_type) = read_template(bundle_dir)?;
    let mut template: serde_json::Value =
        serde_json::from_str(&template_content).map_err(|e| format!("Invalid template: {e}"))?;

    template["name"] = serde_json::json!(settings.name);

    if settings.description.is_empty() {
        if let Some(obj) = template.as_object_mut() {
            obj.remove("description");
        }
    } else {
        template["description"] = serde_json::json!(settings.description);
    }

    if let Some(hr) = host_requirements
        && let Some(steps) = template.get_mut("steps").and_then(|s| s.as_array_mut())
    {
        for step in steps.iter_mut() {
            step["hostRequirements"] = hr.clone();
        }
    }

    // Merge parameters: queue params first, job template params override
    let job_param_names: std::collections::HashSet<&str> = settings
        .parameters
        .iter()
        .filter_map(|p| p.get("name").and_then(|n| n.as_str()))
        .collect();

    let mut parameter_values: Vec<serde_json::Value> = queue_parameters
        .iter()
        .filter(|qp| {
            let name = qp.get("name").and_then(|n| n.as_str()).unwrap_or("");
            !job_param_names.contains(name)
        })
        .map(|qp| {
            serde_json::json!({
                "name": qp.get("name").and_then(|n| n.as_str()).unwrap_or(""),
                "value": qp.get("value").cloned().unwrap_or(serde_json::json!("")),
            })
        })
        .collect();

    parameter_values.extend(settings.parameters.iter().map(|p| {
        serde_json::json!({
            "name": p.get("name").and_then(|n| n.as_str()).unwrap_or(""),
            "value": p.get("value").cloned().unwrap_or(serde_json::json!("")),
        })
    }));

    // Write template
    let ext = if file_type == "yaml" { "yaml" } else { "json" };
    let template_str =
        serde_json::to_string_pretty(&template).map_err(|e| format!("Serialize error: {e}"))?;
    std::fs::write(output_dir.join(format!("template.{ext}")), &template_str)
        .map_err(|e| format!("Write error: {e}"))?;

    // Write asset references
    let ar_str = serde_json::to_string_pretty(&asset_references.to_json())
        .map_err(|e| format!("Serialize error: {e}"))?;
    std::fs::write(output_dir.join(format!("asset_references.{ext}")), &ar_str)
        .map_err(|e| format!("Write error: {e}"))?;

    // Copy hooks if present
    for hooks_filename in &["hooks.yaml", "hooks.json"] {
        let hooks_src = bundle_dir.join(hooks_filename);
        if hooks_src.is_file() {
            std::fs::copy(&hooks_src, output_dir.join(hooks_filename))
                .map_err(|e| format!("Copy hooks: {e}"))?;
            let abs_bundle =
                std::fs::canonicalize(bundle_dir).unwrap_or_else(|_| bundle_dir.to_path_buf());
            std::fs::write(
                output_dir.join(".hooks_origin"),
                abs_bundle.to_string_lossy().as_bytes(),
            )
            .map_err(|e| format!("Write hooks_origin: {e}"))?;
            break;
        }
    }

    Ok(parameter_values)
}

/// Validate whether the submit button should be enabled. Returns issues (empty = ready).
pub fn validate_submit_readiness(
    farm_id: &str,
    queue_id: &str,
    api_available: bool,
) -> Vec<String> {
    let mut issues = Vec::new();
    if !api_available {
        issues.push(
            "AWS Deadline Cloud API is not accessible. Check your authentication status."
                .to_string(),
        );
    }
    if farm_id.is_empty() {
        issues.push(
            "No farm is configured. Click Settings to select a farm for job submission."
                .to_string(),
        );
    }
    if queue_id.is_empty() {
        issues.push(
            "No queue is configured. Click Settings to select a queue within your farm."
                .to_string(),
        );
    }
    issues
}

fn read_template(bundle_dir: &Path) -> Result<(String, &'static str), String> {
    let json_path = bundle_dir.join("template.json");
    if json_path.is_file() {
        let content =
            std::fs::read_to_string(&json_path).map_err(|e| format!("Read template: {e}"))?;
        return Ok((content, "json"));
    }
    let yaml_path = bundle_dir.join("template.yaml");
    if yaml_path.is_file() {
        let content =
            std::fs::read_to_string(&yaml_path).map_err(|e| format!("Read template: {e}"))?;
        let value: serde_json::Value =
            serde_yaml::from_str(&content).map_err(|e| format!("Parse YAML template: {e}"))?;
        let json_str =
            serde_json::to_string(&value).map_err(|e| format!("Convert to JSON: {e}"))?;
        return Ok((json_str, "yaml"));
    }
    Err(format!(
        "No template.json or template.yaml found in {}",
        bundle_dir.display()
    ))
}

/// Config-derived fields for job submission (extracted for testability).
#[derive(Debug, Clone)]
pub struct SubmitConfigFields {
    pub job_attachments_file_system: String,
    pub force_s3_check: bool,
    pub allow_bundle_hooks: bool,
    pub allow_environment_hooks: bool,
    pub known_config_paths: Vec<String>,
    pub s3_max_pool_connections: Option<usize>,
}

/// Read submission-related config fields from the config file.
pub fn read_submit_config_fields() -> SubmitConfigFields {
    let config = deadline_lib::config::config_file::read_config().unwrap_or_default();
    let ja_fs = deadline_lib::config::config_file::get_setting(
        "defaults.job_attachments_file_system",
        &config,
    )
    .unwrap_or_default();
    let force_s3 = deadline_lib::config::config_file::str2bool(
        &deadline_lib::config::config_file::get_setting("settings.force_s3_check", &config)
            .unwrap_or_default(),
    )
    .unwrap_or(false);
    let allow_bundle_hooks = deadline_lib::config::config_file::str2bool(
        &deadline_lib::config::config_file::get_setting("settings.allow_bundle_hooks", &config)
            .unwrap_or_default(),
    )
    .unwrap_or(false);
    let allow_env_hooks = deadline_lib::config::config_file::str2bool(
        &deadline_lib::config::config_file::get_setting(
            "settings.allow_environment_hooks",
            &config,
        )
        .unwrap_or_default(),
    )
    .unwrap_or(false);
    let known_config_paths = {
        let v =
            deadline_lib::config::config_file::get_setting("settings.known_asset_paths", &config)
                .unwrap_or_default();
        if v.is_empty() {
            Vec::new()
        } else {
            let sep = if cfg!(windows) { ';' } else { ':' };
            v.split(sep).map(String::from).collect()
        }
    };
    let s3_max_pool =
        deadline_lib::config::config_file::get_setting("settings.s3_max_pool_connections", &config)
            .ok()
            .and_then(|v| deadline_lib::attachments::s3::parse_s3_max_pool_connections(&v).ok());

    SubmitConfigFields {
        job_attachments_file_system: ja_fs,
        force_s3_check: force_s3,
        allow_bundle_hooks,
        allow_environment_hooks: allow_env_hooks,
        known_config_paths,
        s3_max_pool_connections: s3_max_pool,
    }
}

/// Resolve `target_task_run_status` from the initial status string.
pub fn resolve_target_task_run_status(initial_status: &str) -> Option<String> {
    if initial_status == "SUSPENDED" {
        Some("SUSPENDED".to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_bundle_dir(dir: &TempDir, template_content: &str) -> PathBuf {
        let bundle = dir.path().join("bundle");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("template.json"), template_content).unwrap();
        bundle
    }

    fn create_bundle_with_assets(dir: &TempDir) -> PathBuf {
        let bundle = dir.path().join("bundle");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(bundle.join("template.json"),
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"Test","steps":[{"name":"Step1","script":{"actions":{"onRun":{"command":"echo"}}}}]}"#).unwrap();
        std::fs::write(bundle.join("asset_references.json"),
            r#"{"inputFilePaths":["/tmp/input.exr"],"inputDirectoryPaths":[],"outputDirectoryPaths":["/tmp/output"]}"#).unwrap();
        bundle
    }

    #[test]
    fn prepare_job_bundle_copies_template_with_name() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_dir(
            &dir,
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"Original","steps":[]}"#,
        );
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "My Custom Job".to_string(),
            description: "A test job".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            ..Default::default()
        };
        let result = prepare_job_bundle(
            &output_dir,
            &settings,
            &[],
            &AssetReferences::default(),
            None,
        );
        assert!(result.is_ok());

        let content: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(output_dir.join("template.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(content["name"], "My Custom Job");
        assert_eq!(content["description"], "A test job");
    }

    #[test]
    fn prepare_job_bundle_removes_empty_description() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_dir(
            &dir,
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"X","description":"old","steps":[]}"#,
        );
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "X".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            ..Default::default()
        };
        prepare_job_bundle(
            &output_dir,
            &settings,
            &[],
            &AssetReferences::default(),
            None,
        )
        .unwrap();

        let content: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(output_dir.join("template.json")).unwrap(),
        )
        .unwrap();
        assert!(content.get("description").is_none());
    }

    #[test]
    fn prepare_job_bundle_injects_host_requirements_into_steps() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_dir(
            &dir,
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"X","steps":[{"name":"S1","script":{"actions":{"onRun":{"command":"echo"}}}},{"name":"S2","script":{"actions":{"onRun":{"command":"echo"}}}}]}"#,
        );
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "X".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            ..Default::default()
        };
        let host_req = serde_json::json!({
            "amounts": [{"name": "amount.worker.vcpu", "min": 4}],
            "attributes": [{"name": "attr.worker.os.family", "anyOf": ["linux"]}]
        });
        prepare_job_bundle(
            &output_dir,
            &settings,
            &[],
            &AssetReferences::default(),
            Some(&host_req),
        )
        .unwrap();

        let content: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(output_dir.join("template.json")).unwrap(),
        )
        .unwrap();
        let steps = content["steps"].as_array().unwrap();
        assert_eq!(steps[0]["hostRequirements"], host_req);
        assert_eq!(steps[1]["hostRequirements"], host_req);
    }

    #[test]
    fn prepare_job_bundle_writes_asset_references() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_with_assets(&dir);
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "Test".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            ..Default::default()
        };
        let assets = AssetReferences {
            input_file_paths: vec!["/extra/file.exr".to_string()],
            input_directory_paths: vec![],
            output_directory_paths: vec!["/out".to_string()],
        };
        prepare_job_bundle(&output_dir, &settings, &[], &assets, None).unwrap();

        let content: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(output_dir.join("asset_references.json")).unwrap(),
        )
        .unwrap();
        assert!(
            content["inputFilePaths"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("/extra/file.exr"))
        );
    }

    #[test]
    fn prepare_job_bundle_copies_hooks_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_dir(
            &dir,
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"X","steps":[]}"#,
        );
        std::fs::write(
            bundle.join("hooks.yaml"),
            "hooks:\n  - name: pre\n    command: echo pre\n",
        )
        .unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "X".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            ..Default::default()
        };
        prepare_job_bundle(
            &output_dir,
            &settings,
            &[],
            &AssetReferences::default(),
            None,
        )
        .unwrap();

        assert!(output_dir.join("hooks.yaml").exists());
        let origin = std::fs::read_to_string(output_dir.join(".hooks_origin")).unwrap();
        assert!(origin.contains("bundle"));
    }

    #[test]
    fn prepare_job_bundle_merges_parameters() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = create_bundle_dir(
            &dir,
            r#"{"specificationVersion":"jobtemplate-2023-09","name":"X","steps":[]}"#,
        );
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "X".to_string(),
            input_job_bundle_dir: bundle.to_string_lossy().to_string(),
            parameters: vec![
                serde_json::json!({"name": "Frames", "value": "1-10"}),
                serde_json::json!({"name": "Quality", "value": "high"}),
            ],
            ..Default::default()
        };
        let queue_params = vec![
            serde_json::json!({"name": "RezPackages", "value": "maya-2024"}),
            serde_json::json!({"name": "Frames", "value": "1-100"}),
        ];
        let params = prepare_job_bundle(
            &output_dir,
            &settings,
            &queue_params,
            &AssetReferences::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            params.iter().find(|p| p["name"] == "Frames").unwrap()["value"],
            "1-10"
        );
        assert_eq!(
            params.iter().find(|p| p["name"] == "RezPackages").unwrap()["value"],
            "maya-2024"
        );
    }

    #[test]
    fn prepare_job_bundle_invalid_bundle_dir_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let output_dir = dir.path().join("output");
        std::fs::create_dir_all(&output_dir).unwrap();

        let settings = SubmitSettings {
            name: "X".to_string(),
            input_job_bundle_dir: "/nonexistent/path".to_string(),
            ..Default::default()
        };
        assert!(
            prepare_job_bundle(
                &output_dir,
                &settings,
                &[],
                &AssetReferences::default(),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn submit_settings_default_values() {
        let s = SubmitSettings::default();
        assert_eq!(s.priority, 50);
        assert_eq!(s.initial_status, "READY");
        assert_eq!(s.max_failed_tasks_count, 20);
        assert_eq!(s.max_retries_per_task, 5);
        assert_eq!(s.max_worker_count, -1);
    }

    #[test]
    fn can_submit_requires_farm_and_queue() {
        let issues = validate_submit_readiness("", "", true);
        assert!(issues.iter().any(|i: &String| i.contains("farm")));
        assert!(issues.iter().any(|i: &String| i.contains("queue")));
    }

    #[test]
    fn can_submit_requires_api_available() {
        let issues = validate_submit_readiness("farm-abc", "queue-xyz", false);
        assert!(issues.iter().any(|i: &String| i.contains("API")));
    }

    #[test]
    fn can_submit_passes_when_all_configured() {
        assert!(validate_submit_readiness("farm-abc", "queue-xyz", true).is_empty());
    }

    #[test]
    fn resolve_target_task_run_status_ready_is_none() {
        assert_eq!(resolve_target_task_run_status("READY"), None);
    }

    #[test]
    fn resolve_target_task_run_status_suspended_returns_suspended() {
        assert_eq!(
            resolve_target_task_run_status("SUSPENDED"),
            Some("SUSPENDED".to_string())
        );
    }

    #[test]
    fn read_submit_config_fields_does_not_panic() {
        // Verify the function handles missing/empty config gracefully
        let fields = read_submit_config_fields();
        // All bool fields default to false when config is empty/missing
        // (exact values depend on what config file is present during test)
        let _ = fields.force_s3_check;
        let _ = fields.allow_bundle_hooks;
        let _ = fields.allow_environment_hooks;
        let _ = fields.known_config_paths;
        let _ = fields.s3_max_pool_connections;
    }

    #[test]
    fn load_template_name_yaml() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("bundle");
        std::fs::create_dir_all(&bundle).unwrap();
        std::fs::write(
            bundle.join("template.yaml"),
            "specificationVersion: jobtemplate-2023-09\nname: YAML Job\nsteps: []\n",
        )
        .unwrap();
        assert_eq!(
            crate::submit_model::load_template_name(&bundle),
            Some("YAML Job".to_string())
        );
    }
}
