//! : Job bundle — parameters: read_job_bundle_parameters (cases 66-80)

use deadline_job_bundle::parameters::read_job_bundle_parameters;
use std::fs;
use tempfile::TempDir;

/// Helper: create a bundle dir with a template and optional parameter_values.
fn make_bundle(
    template: &serde_json::Value,
    param_values: Option<&serde_json::Value>,
) -> TempDir {
    let dir = TempDir::new().unwrap();
    fs::write(
        dir.path().join("template.yaml"),
        serde_yaml::to_string(template).unwrap(),
    )
    .unwrap();
    if let Some(pv) = param_values {
        fs::write(
            dir.path().join("parameter_values.yaml"),
            serde_yaml::to_string(pv).unwrap(),
        )
        .unwrap();
    }
    dir
}

// .66: Valid template with parameters and parameter_values
#[test]
fn read_params_valid_template_with_values() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "Frame", "type": "INT", "default": 1}
        ]
    });
    let pv = serde_json::json!({
        "parameterValues": [{"name": "Frame", "value": "10"}]
    });
    let dir = make_bundle(&template, Some(&pv));
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0]["name"], "Frame");
    assert_eq!(params[0]["value"], "10");
}

// .67: Template is not a dict
#[test]
fn read_params_template_not_dict_returns_error() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("template.yaml"), "- item1\n- item2").unwrap();
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("top-level object"),
        "Expected 'top-level object' error, got: {err}"
    );
}

// .68: Template missing specificationVersion
#[test]
fn read_params_missing_spec_version_returns_error() {
    let template = serde_json::json!({"name": "Test"});
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("specificationVersion"),
        "Expected 'specificationVersion' error, got: {err}"
    );
}

// .69: Unsupported specificationVersion
#[test]
fn read_params_unsupported_spec_version_returns_error() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2099-01"
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("unsupported"),
        "Expected 'unsupported' error, got: {err}"
    );
}

// .70: parameterDefinitions is not a list
#[test]
fn read_params_param_defs_not_list_returns_error() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": "not a list"
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("must be a list"),
        "Expected 'must be a list' error, got: {err}"
    );
}

// .71: Template has no parameterDefinitions
#[test]
fn read_params_no_param_defs_returns_empty() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09"
    });
    let dir = make_bundle(&template, None);
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    assert!(params.is_empty());
}

// .72: Parameter value for name not in template (kept as-is)
#[test]
fn read_params_extra_param_value_kept() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "Frame", "type": "INT", "default": 1}
        ]
    });
    let pv = serde_json::json!({
        "parameterValues": [
            {"name": "Frame", "value": "5"},
            {"name": "deadline:priority", "value": "50"}
        ]
    });
    let dir = make_bundle(&template, Some(&pv));
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    assert_eq!(params.len(), 2);
    // The extra parameter should be present
    assert!(params.iter().any(|p| p["name"] == "deadline:priority"));
}

// .73: PATH parameter with relative default and no value → absolute
#[test]
fn read_params_path_relative_default_made_absolute() {
    let dir = TempDir::new().unwrap();
    // Create a subdir so the relative path resolves inside the bundle
    fs::create_dir(dir.path().join("subdir")).unwrap();
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "OutDir", "type": "PATH", "default": "subdir"}
        ]
    });
    fs::write(
        dir.path().join("template.yaml"),
        serde_yaml::to_string(&template).unwrap(),
    )
    .unwrap();
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    let value = params[0]["value"].as_str().unwrap();
    assert!(
        std::path::Path::new(value).is_absolute(),
        "Expected absolute path, got: {value}"
    );
}

// .74: PATH parameter with absolute default path → error
#[test]
fn read_params_path_absolute_default_returns_error() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "OutDir", "type": "PATH", "default": "/absolute/path"}
        ]
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("is absolute"),
        "Expected 'is absolute' error, got: {err}"
    );
}

// .75: PATH parameter with relative default resolving outside bundle → error
#[test]
fn read_params_path_default_outside_bundle_returns_error() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "OutDir", "type": "PATH", "default": "../../outside"}
        ]
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("outside"),
        "Expected 'outside' error, got: {err}"
    );
}

// .76: PATH parameter with allowedValues and relative default → NOT made absolute
#[test]
fn read_params_path_with_allowed_values_default_not_resolved() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {
                "name": "OutDir",
                "type": "PATH",
                "default": "subdir",
                "allowedValues": ["subdir", "other"]
            }
        ]
    });
    let dir = make_bundle(&template, None);
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    // With allowedValues, the default should NOT be made absolute
    // The parameter should not have a "value" set by path resolution
    let has_value = params[0].get("value").is_some();
    if has_value {
        let value = params[0]["value"].as_str().unwrap();
        assert!(
            !std::path::Path::new(value).is_absolute(),
            "PATH with allowedValues should not be made absolute, got: {value}"
        );
    }
}

// .77: HIDDEN parameter with no value and no default → error
#[test]
fn read_params_hidden_no_value_no_default_returns_error() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {
                "name": "Secret",
                "type": "STRING",
                "userInterface": {"control": "HIDDEN"}
            }
        ]
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    assert!(
        err.to_string().contains("Hidden parameter"),
        "Expected 'Hidden parameter' error, got: {err}"
    );
}

// .78: Multiple HIDDEN parameters missing values → error lists all names
#[test]
fn read_params_multiple_hidden_missing_lists_all() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "Secret1", "type": "STRING", "userInterface": {"control": "HIDDEN"}},
            {"name": "Secret2", "type": "STRING", "userInterface": {"control": "HIDDEN"}}
        ]
    });
    let dir = make_bundle(&template, None);
    let err = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("Secret1"), "Expected 'Secret1' in error, got: {msg}");
    assert!(msg.contains("Secret2"), "Expected 'Secret2' in error, got: {msg}");
}

// .79: HIDDEN parameter with a default value → no error
#[test]
fn read_params_hidden_with_default_succeeds() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {
                "name": "Secret",
                "type": "STRING",
                "default": "hidden_val",
                "userInterface": {"control": "HIDDEN"}
            }
        ]
    });
    let dir = make_bundle(&template, None);
    let result = read_job_bundle_parameters(dir.path().to_str().unwrap());
    assert!(result.is_ok());
}

// .80: No parameter_values file exists
#[test]
fn read_params_no_param_values_file_uses_defaults() {
    let template = serde_json::json!({
        "specificationVersion": "jobtemplate-2023-09",
        "parameterDefinitions": [
            {"name": "Frame", "type": "INT", "default": 1}
        ]
    });
    let dir = make_bundle(&template, None);
    let params = read_job_bundle_parameters(dir.path().to_str().unwrap()).unwrap();
    assert_eq!(params.len(), 1);
    // No "value" key set, only "default"
    assert_eq!(params[0]["default"], 1);
}
