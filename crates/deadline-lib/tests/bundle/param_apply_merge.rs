//! : Job bundle — parameters: apply, merge, UI control, difference (cases 81-118)

use deadline_lib::bundle::parameters::{
    apply_job_parameters, get_ui_control_for_parameter_definition, merge_queue_job_parameters,
    parameter_definition_difference,
};
use deadline_lib::bundle::submission::AssetReferences;
use std::path::Path;

// ── apply_job_parameters (cases 81-93) ──────────────────────────────

// .81: Job parameter provides a value for a template parameter
#[test]
fn apply_params_sets_value() {
    let job_params = vec![serde_json::json!({"name": "Frame", "value": "10"})];
    let mut params = vec![serde_json::json!({"name": "Frame", "type": "INT", "default": 1})];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert_eq!(params[0]["value"], "10");
}

// .82: PATH parameter value is relative → made absolute relative to CWD
#[test]
fn apply_params_path_relative_made_absolute() {
    let job_params = vec![serde_json::json!({"name": "Out", "value": "relative/path"})];
    let mut params = vec![serde_json::json!({"name": "Out", "type": "PATH"})];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    let value = params[0]["value"].as_str().unwrap();
    assert!(
        std::path::Path::new(value).is_absolute(),
        "Expected absolute path, got: {value}"
    );
}

// .83: PATH parameter with allowedValues and relative value → NOT made absolute
#[test]
fn apply_params_path_with_allowed_values_not_resolved() {
    let job_params = vec![serde_json::json!({"name": "Out", "value": "relative"})];
    let mut params = vec![serde_json::json!({
        "name": "Out",
        "type": "PATH",
        "allowedValues": ["relative", "other"]
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert_eq!(params[0]["value"], "relative");
}

// .84: PATH parameter value is empty string → skipped
#[test]
fn apply_params_path_empty_value_skipped() {
    let job_params = vec![serde_json::json!({"name": "Out", "value": ""})];
    let mut params = vec![serde_json::json!({"name": "Out", "type": "PATH", "dataFlow": "IN"})];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    // Empty PATH should not be added to asset references
    assert!(asset_refs.input_directories.is_empty());
    assert!(asset_refs.input_filenames.is_empty());
}

// .85: Parameter has no value and no default → error
#[test]
fn apply_params_no_value_no_default_returns_error() {
    let job_params: Vec<serde_json::Value> = vec![];
    let mut params = vec![serde_json::json!({"name": "Frame", "type": "INT"})];
    let mut asset_refs = AssetReferences::new();
    let err = apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("No parameter value provided"),
        "Expected 'No parameter value provided', got: {err}"
    );
}

// .86: PATH with dataFlow=IN and objectType=DIRECTORY → input_directories
#[test]
fn apply_params_path_in_directory_adds_to_input_dirs() {
    let job_params = vec![serde_json::json!({"name": "In", "value": "/input/dir"})];
    let mut params = vec![serde_json::json!({
        "name": "In", "type": "PATH", "dataFlow": "IN", "objectType": "DIRECTORY"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(asset_refs.input_directories.contains("/input/dir"));
}

// .87: PATH with dataFlow=IN and objectType=FILE → input_filenames
#[test]
fn apply_params_path_in_file_adds_to_input_filenames() {
    let job_params = vec![serde_json::json!({"name": "In", "value": "/input/file.txt"})];
    let mut params = vec![serde_json::json!({
        "name": "In", "type": "PATH", "dataFlow": "IN", "objectType": "FILE"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(asset_refs.input_filenames.contains("/input/file.txt"));
}

// .88: PATH with dataFlow=OUT and objectType=DIRECTORY → output_directories
#[test]
fn apply_params_path_out_directory_adds_to_output_dirs() {
    let job_params = vec![serde_json::json!({"name": "Out", "value": "/output/dir"})];
    let mut params = vec![serde_json::json!({
        "name": "Out", "type": "PATH", "dataFlow": "OUT", "objectType": "DIRECTORY"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(asset_refs.output_directories.contains("/output/dir"));
}

// .89: PATH with dataFlow=OUT and objectType=FILE → parent dir to output_directories
#[test]
fn apply_params_path_out_file_adds_parent_to_output_dirs() {
    let job_params = vec![serde_json::json!({"name": "Out", "value": "/output/dir/file.exr"})];
    let mut params = vec![serde_json::json!({
        "name": "Out", "type": "PATH", "dataFlow": "OUT", "objectType": "FILE"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(
        asset_refs.output_directories.contains("/output/dir"),
        "Expected parent dir '/output/dir', got: {:?}",
        asset_refs.output_directories
    );
}

// .90: PATH with dataFlow=INOUT → both input and output
#[test]
fn apply_params_path_inout_adds_to_both() {
    let job_params = vec![serde_json::json!({"name": "IO", "value": "/io/dir"})];
    let mut params = vec![serde_json::json!({
        "name": "IO", "type": "PATH", "dataFlow": "INOUT", "objectType": "DIRECTORY"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(asset_refs.input_directories.contains("/io/dir"));
    assert!(asset_refs.output_directories.contains("/io/dir"));
}

// .91: PATH with dataFlow=NONE → referenced_paths
#[test]
fn apply_params_path_none_adds_to_referenced() {
    let job_params = vec![serde_json::json!({"name": "Ref", "value": "/ref/path"})];
    let mut params = vec![serde_json::json!({
        "name": "Ref", "type": "PATH", "dataFlow": "NONE"
    })];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(asset_refs.referenced_paths.contains("/ref/path"));
}

// .92: PATH with invalid dataFlow → error
#[test]
fn apply_params_path_invalid_data_flow_returns_error() {
    let job_params = vec![serde_json::json!({"name": "P", "value": "/path"})];
    let mut params = vec![serde_json::json!({
        "name": "P", "type": "PATH", "dataFlow": "INVALID"
    })];
    let mut asset_refs = AssetReferences::new();
    let err = apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("dataFlow") || err.to_string().contains("NONE"),
        "Expected dataFlow error, got: {err}"
    );
}

// .93: Non-PATH parameter → no asset reference modification
#[test]
fn apply_params_non_path_no_asset_refs() {
    let job_params = vec![serde_json::json!({"name": "Frame", "value": "10"})];
    let mut params = vec![serde_json::json!({"name": "Frame", "type": "INT", "default": 1})];
    let mut asset_refs = AssetReferences::new();
    apply_job_parameters(
        &job_params,
        Path::new("/bundle"),
        &mut params,
        &mut asset_refs,
    )
    .unwrap();
    assert!(!asset_refs.is_non_empty());
}

// ── merge_queue_job_parameters (cases 94-103) ───────────────────────

// .94: No overlapping parameter names → union
#[test]
fn merge_params_no_overlap_returns_union() {
    let job = vec![serde_json::json!({"name": "A", "type": "STRING", "default": "a"})];
    let queue = vec![serde_json::json!({"name": "B", "type": "INT", "default": 1})];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result.len(), 2);
}

// .95: Overlapping parameter with same type → merged
#[test]
fn merge_params_same_type_merged() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "job"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "queue"})];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result.len(), 1);
    // Job bundle's default takes priority
    assert_eq!(result[0]["default"], "job");
}

// .96: Overlapping parameter with different types → error
#[test]
fn merge_params_different_types_returns_error() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "INT"})];
    let err = merge_queue_job_parameters(&job, &queue, None).unwrap_err();
    assert!(
        err.to_string().contains("conflicting"),
        "Expected 'conflicting' error, got: {err}"
    );
}

// .97: Job parameter has value, queue has same name → value copied
#[test]
fn merge_params_value_copied_to_merged() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING", "value": "hello"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "queue"})];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result[0]["value"], "hello");
}

// .98: Value-only parameter for a queue parameter → merged
#[test]
fn merge_params_value_only_for_queue_param() {
    let job = vec![serde_json::json!({"name": "P", "value": "hello"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "queue"})];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result[0]["value"], "hello");
}

// .99: Value-only parameter not in queue and no ":" → error
#[test]
fn merge_params_value_only_undefined_returns_error() {
    let job = vec![serde_json::json!({"name": "Unknown", "value": "x"})];
    let queue: Vec<serde_json::Value> = vec![];
    let err = merge_queue_job_parameters(&job, &queue, None).unwrap_err();
    assert!(
        err.to_string().contains("undefined"),
        "Expected 'undefined parameter' error, got: {err}"
    );
}

// .100: Value-only parameter with ":" in name → accepted
#[test]
fn merge_params_value_only_with_colon_accepted() {
    let job = vec![serde_json::json!({"name": "deadline:priority", "value": "50"})];
    let queue: Vec<serde_json::Value> = vec![];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result.len(), 1);
}

// .101: queue_id provided and mismatch → error includes queue_id
#[test]
fn merge_params_mismatch_with_queue_id_in_error() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "INT"})];
    let err = merge_queue_job_parameters(&job, &queue, Some("queue-abc123")).unwrap_err();
    assert!(
        err.to_string().contains("queue-abc123"),
        "Expected queue ID in error, got: {err}"
    );
}

// .102: queue_id not provided and mismatch → error says "queue" without ID
#[test]
fn merge_params_mismatch_without_queue_id_says_queue() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "INT"})];
    let err = merge_queue_job_parameters(&job, &queue, None).unwrap_err();
    assert!(
        err.to_string().contains("queue"),
        "Expected 'queue' in error, got: {err}"
    );
}

// .103: Overlapping parameter with different defaults but same type → merged
#[test]
fn merge_params_different_defaults_same_type_succeeds() {
    let job = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "a"})];
    let queue = vec![serde_json::json!({"name": "P", "type": "STRING", "default": "b"})];
    let result = merge_queue_job_parameters(&job, &queue, None).unwrap();
    assert_eq!(result.len(), 1);
    // Job bundle's default takes priority
    assert_eq!(result[0]["default"], "a");
}

// ── get_ui_control_for_parameter_definition (cases 104-113) ─────────

// .104: Explicit userInterface.control set
#[test]
fn ui_control_explicit_control_returned() {
    let param = serde_json::json!({
        "name": "P", "type": "STRING",
        "userInterface": {"control": "MULTILINE_EDIT"}
    });
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "MULTILINE_EDIT"
    );
}

// .105: STRING with no control and no allowedValues → LINE_EDIT
#[test]
fn ui_control_string_default_line_edit() {
    let param = serde_json::json!({"name": "P", "type": "STRING"});
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "LINE_EDIT"
    );
}

// .106: PATH with objectType=DIRECTORY → CHOOSE_DIRECTORY
#[test]
fn ui_control_path_directory_choose_directory() {
    let param = serde_json::json!({
        "name": "P", "type": "PATH", "objectType": "DIRECTORY"
    });
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "CHOOSE_DIRECTORY"
    );
}

// .107: PATH with objectType=FILE and dataFlow=OUT → CHOOSE_OUTPUT_FILE
#[test]
fn ui_control_path_file_out_choose_output() {
    let param = serde_json::json!({
        "name": "P", "type": "PATH", "objectType": "FILE", "dataFlow": "OUT"
    });
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "CHOOSE_OUTPUT_FILE"
    );
}

// .108: PATH with objectType=FILE and dataFlow=IN → CHOOSE_INPUT_FILE
#[test]
fn ui_control_path_file_in_choose_input() {
    let param = serde_json::json!({
        "name": "P", "type": "PATH", "objectType": "FILE", "dataFlow": "IN"
    });
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "CHOOSE_INPUT_FILE"
    );
}

// .109: INT with no control and no allowedValues → SPIN_BOX
#[test]
fn ui_control_int_default_spin_box() {
    let param = serde_json::json!({"name": "P", "type": "INT"});
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "SPIN_BOX"
    );
}

// .110: Any parameter with allowedValues and no explicit control → DROPDOWN_LIST
#[test]
fn ui_control_with_allowed_values_dropdown() {
    let param = serde_json::json!({
        "name": "P", "type": "STRING", "allowedValues": ["a", "b"]
    });
    assert_eq!(
        get_ui_control_for_parameter_definition(&param).unwrap(),
        "DROPDOWN_LIST"
    );
}

// .111: Explicit control SPIN_BOX for STRING → error
#[test]
fn ui_control_spin_box_for_string_returns_error() {
    let param = serde_json::json!({
        "name": "P", "type": "STRING",
        "userInterface": {"control": "SPIN_BOX"}
    });
    let err = get_ui_control_for_parameter_definition(&param).unwrap_err();
    assert!(
        err.to_string().contains("unsupported control"),
        "Expected 'unsupported control', got: {err}"
    );
}

// .112: Explicit control DROPDOWN_LIST but no allowedValues → error
#[test]
fn ui_control_dropdown_without_allowed_values_returns_error() {
    let param = serde_json::json!({
        "name": "P", "type": "STRING",
        "userInterface": {"control": "DROPDOWN_LIST"}
    });
    let err = get_ui_control_for_parameter_definition(&param).unwrap_err();
    assert!(
        err.to_string().contains("allowedValues"),
        "Expected 'allowedValues' error, got: {err}"
    );
}

// .113: Unsupported parameter type → error
#[test]
fn ui_control_unsupported_type_returns_error() {
    let param = serde_json::json!({"name": "P", "type": "BOOLEAN"});
    let err = get_ui_control_for_parameter_definition(&param).unwrap_err();
    assert!(
        err.to_string().contains("unsupported type"),
        "Expected 'unsupported type', got: {err}"
    );
}

// ── parameter_definition_difference (cases 114-118) ─────────────────

// .114: Two identical parameters → empty list
#[test]
fn param_diff_identical_returns_empty() {
    let a = serde_json::json!({"name": "P", "type": "STRING", "default": "x"});
    let b = serde_json::json!({"name": "P", "type": "STRING", "default": "x"});
    let diff = parameter_definition_difference(&a, &b, false);
    assert!(diff.is_empty());
}

// .115: Parameters differ in type → returns ["type"]
#[test]
fn param_diff_type_differs() {
    let a = serde_json::json!({"name": "P", "type": "STRING"});
    let b = serde_json::json!({"name": "P", "type": "INT"});
    let diff = parameter_definition_difference(&a, &b, false);
    assert!(diff.contains(&"type".to_owned()));
}

// .116: Parameters differ in allowedValues (compared as sets)
#[test]
fn param_diff_allowed_values_as_sets() {
    let a = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["a", "b"]});
    let b = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["b", "c"]});
    let diff = parameter_definition_difference(&a, &b, false);
    assert!(diff.contains(&"allowedValues".to_owned()));
}

// .116b: Same allowedValues in different order → no difference
#[test]
fn param_diff_allowed_values_same_set_no_diff() {
    let a = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["b", "a"]});
    let b = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["a", "b"]});
    let diff = parameter_definition_difference(&a, &b, false);
    assert!(!diff.contains(&"allowedValues".to_owned()));
}

// .117: ignore_missing=true and one parameter lacks a field → not reported
#[test]
fn param_diff_ignore_missing_skips_absent_field() {
    let a = serde_json::json!({"name": "P", "type": "STRING", "minLength": 5});
    let b = serde_json::json!({"name": "P", "type": "STRING"});
    let diff = parameter_definition_difference(&a, &b, true);
    assert!(!diff.contains(&"minLength".to_owned()));
}

// .118: ignore_missing=false and one parameter lacks a field → reported
#[test]
fn param_diff_no_ignore_missing_reports_absent_field() {
    let a = serde_json::json!({"name": "P", "type": "STRING", "minLength": 5});
    let b = serde_json::json!({"name": "P", "type": "STRING"});
    let diff = parameter_definition_difference(&a, &b, false);
    assert!(diff.contains(&"minLength".to_owned()));
}
