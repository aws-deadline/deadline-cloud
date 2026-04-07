//! §16: Job bundle — parameter validation (cases 1-48)
//!
//! Tests for validate_job_parameter, validate_user_interface_spec,
//! and validate_user_interface_file_filter.

use deadline_job_bundle::parameters::{
    validate_job_parameter, validate_user_interface_file_filter, validate_user_interface_spec,
};
use test_case::test_case;

// ── validate_job_parameter: valid types ─────────────────────────────

#[test_case("STRING" ; "string")]
#[test_case("PATH"   ; "path")]
#[test_case("INT"    ; "int")]
#[test_case("FLOAT"  ; "float")]
fn validate_param_valid_type(param_type: &str) {
    let input = serde_json::json!({"name": "P", "type": param_type});
    validate_job_parameter(&input, false, false).unwrap();
}

// ── validate_job_parameter: name field ──────────────────────────────

#[test]
fn validate_param_not_a_dict_returns_error() {
    let err = validate_job_parameter(&serde_json::json!("string"), false, false).unwrap_err();
    assert!(
        err.to_string().contains("dict") || err.to_string().contains("object"),
        "got: {err}"
    );
}

#[test]
fn validate_param_missing_name_returns_error() {
    let err = validate_job_parameter(&serde_json::json!({"type": "STRING"}), false, false).unwrap_err();
    assert!(err.to_string().contains("name"), "got: {err}");
}

#[test]
fn validate_param_name_not_string_returns_error() {
    let err = validate_job_parameter(&serde_json::json!({"name": 42}), false, false).unwrap_err();
    assert!(err.to_string().contains("name"), "got: {err}");
}

#[test]
fn validate_param_empty_name_returns_error() {
    let err = validate_job_parameter(&serde_json::json!({"name": ""}), false, false).unwrap_err();
    assert!(err.to_string().contains("empty"), "got: {err}");
}

// ── validate_job_parameter: type field ──────────────────────────────

#[test]
fn validate_param_unknown_type_returns_error() {
    let err = validate_job_parameter(&serde_json::json!({"name": "P", "type": "UNKNOWN"}), false, false).unwrap_err();
    assert!(
        err.to_string().contains("STRING") || err.to_string().contains("expected one of"),
        "got: {err}"
    );
}

#[test]
fn validate_param_type_required_missing_returns_error() {
    let err = validate_job_parameter(&serde_json::json!({"name": "P"}), true, false).unwrap_err();
    assert!(err.to_string().contains("type"), "got: {err}");
}

#[test]
fn validate_param_type_not_required_missing_succeeds() {
    validate_job_parameter(&serde_json::json!({"name": "P"}), false, false).unwrap();
}

// ── validate_job_parameter: default field ───────────────────────────

#[test]
fn validate_param_default_required_missing_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "STRING"}),
        false,
        true,
    )
    .unwrap_err();
    assert!(err.to_string().contains("default"), "got: {err}");
}

#[test]
fn validate_param_default_null_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "STRING", "default": null}),
        false,
        false,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("None") || err.to_string().contains("null"),
        "got: {err}"
    );
}

// ── validate_job_parameter: field type checks ───────────────────────

#[test]
fn validate_param_description_not_string_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "STRING", "description": 42}),
        false,
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("description"), "got: {err}");
}

#[test]
fn validate_param_allowed_values_not_list_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "STRING", "allowedValues": "not a list"}),
        false,
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("allowedValues"), "got: {err}");
}

// ── validate_job_parameter: dataFlow ────────────────────────────────

#[test]
fn validate_param_invalid_data_flow_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "PATH", "dataFlow": "INVALID"}),
        false,
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("dataFlow"), "got: {err}");
}

#[test]
fn validate_param_valid_data_flow_inout() {
    validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "PATH", "dataFlow": "INOUT"}),
        false,
        false,
    )
    .unwrap();
}

// ── validate_job_parameter: minLength/maxLength (parametrized) ──────

#[test_case("minLength" ; "minLength")]
#[test_case("maxLength" ; "maxLength")]
fn validate_param_length_not_int_returns_error(field: &str) {
    let input = serde_json::json!({"name": "P", "type": "STRING", field: "five"});
    let err = validate_job_parameter(&input, false, false).unwrap_err();
    assert!(err.to_string().contains(field), "got: {err}");
}

#[test_case("minLength" ; "minLength")]
#[test_case("maxLength" ; "maxLength")]
fn validate_param_length_negative_returns_error(field: &str) {
    let input = serde_json::json!({"name": "P", "type": "STRING", field: -1});
    let err = validate_job_parameter(&input, false, false).unwrap_err();
    assert!(err.to_string().contains("non-negative"), "got: {err}");
}

// ── validate_job_parameter: minValue/maxValue (parametrized) ────────

#[test_case("minValue" ; "minValue")]
#[test_case("maxValue" ; "maxValue")]
fn validate_param_value_bound_non_numeric_string_returns_error(field: &str) {
    let input = serde_json::json!({"name": "P", "type": "INT", field: "abc"});
    let err = validate_job_parameter(&input, false, false).unwrap_err();
    assert!(
        err.to_string().contains("non-numeric") || err.to_string().contains(field),
        "got: {err}"
    );
}

#[test]
fn validate_param_min_value_numeric_string_succeeds() {
    validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "FLOAT", "minValue": "3.14"}),
        false,
        false,
    )
    .unwrap();
}

#[test]
fn validate_param_min_value_bool_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "INT", "minValue": true}),
        false,
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("minValue"), "got: {err}");
}

// ── validate_job_parameter: objectType ──────────────────────────────

#[test]
fn validate_param_invalid_object_type_returns_error() {
    let err = validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "PATH", "objectType": "SYMLINK"}),
        false,
        false,
    )
    .unwrap_err();
    assert!(err.to_string().contains("objectType"), "got: {err}");
}

#[test]
fn validate_param_valid_object_type_file() {
    validate_job_parameter(
        &serde_json::json!({"name": "P", "type": "PATH", "objectType": "FILE"}),
        false,
        false,
    )
    .unwrap();
}

// ── validate_job_parameter: userInterface ────────────────────────────

#[test]
fn validate_param_with_valid_user_interface() {
    validate_job_parameter(
        &serde_json::json!({
            "name": "P", "type": "STRING",
            "userInterface": {"control": "LINE_EDIT", "label": "My Param"}
        }),
        false,
        false,
    )
    .unwrap();
}

// ── validate_user_interface_spec ────────────────────────────────────

#[test]
fn validate_ui_spec_valid_line_edit() {
    validate_user_interface_spec(&serde_json::json!({"control": "LINE_EDIT"}), "P").unwrap();
}

#[test]
fn validate_ui_spec_not_dict_returns_error() {
    let err = validate_user_interface_spec(&serde_json::json!("string"), "P").unwrap_err();
    assert!(
        err.to_string().contains("dict") || err.to_string().contains("object"),
        "got: {err}"
    );
}

#[test]
fn validate_ui_spec_invalid_control_returns_error() {
    let err =
        validate_user_interface_spec(&serde_json::json!({"control": "INVALID"}), "P").unwrap_err();
    assert!(
        err.to_string().contains("expected one of") || err.to_string().contains("control"),
        "got: {err}"
    );
}

#[test_case("label"      ; "label")]
#[test_case("groupLabel" ; "groupLabel")]
fn validate_ui_spec_string_field_not_string_returns_error(field: &str) {
    let input = serde_json::json!({field: 42});
    let err = validate_user_interface_spec(&input, "P").unwrap_err();
    assert!(err.to_string().contains(field), "got: {err}");
}

#[test]
fn validate_ui_spec_decimals_not_int_returns_error() {
    let err =
        validate_user_interface_spec(&serde_json::json!({"decimals": "five"}), "P").unwrap_err();
    assert!(err.to_string().contains("decimals"), "got: {err}");
}

#[test]
fn validate_ui_spec_decimals_negative_returns_error() {
    let err =
        validate_user_interface_spec(&serde_json::json!({"decimals": -1}), "P").unwrap_err();
    assert!(err.to_string().contains("non-negative"), "got: {err}");
}

#[test]
fn validate_ui_spec_single_step_delta_string_returns_error() {
    let err = validate_user_interface_spec(
        &serde_json::json!({"singleStepDelta": "fast"}),
        "P",
    )
    .unwrap_err();
    assert!(err.to_string().contains("singleStepDelta"), "got: {err}");
}

#[test_case(0   ; "zero")]
#[test_case(-1  ; "negative")]
fn validate_ui_spec_single_step_delta_non_positive_returns_error(val: i64) {
    let err = validate_user_interface_spec(
        &serde_json::json!({"singleStepDelta": val}),
        "P",
    )
    .unwrap_err();
    assert!(err.to_string().contains("positive"), "got: {err}");
}

#[test]
fn validate_ui_spec_file_filters_not_list_returns_error() {
    let err = validate_user_interface_spec(
        &serde_json::json!({"fileFilters": "not a list"}),
        "P",
    )
    .unwrap_err();
    assert!(err.to_string().contains("fileFilters"), "got: {err}");
}

#[test]
fn validate_ui_spec_valid_file_filters() {
    validate_user_interface_spec(
        &serde_json::json!({"fileFilters": [{"label": "Images", "patterns": ["*.png"]}]}),
        "P",
    )
    .unwrap();
}

// ── validate_user_interface_file_filter ──────────────────────────────

#[test]
fn validate_file_filter_valid() {
    validate_user_interface_file_filter(
        &serde_json::json!({"label": "Images", "patterns": ["*.png"]}),
        "P",
        "ff[0]",
    )
    .unwrap();
}

#[test]
fn validate_file_filter_not_dict_returns_error() {
    let err =
        validate_user_interface_file_filter(&serde_json::json!("string"), "P", "ff[0]").unwrap_err();
    assert!(
        err.to_string().contains("dict") || err.to_string().contains("object"),
        "got: {err}"
    );
}

#[test_case("label"    ; "label")]
#[test_case("patterns" ; "patterns")]
fn validate_file_filter_missing_required_field(field: &str) {
    let mut obj = serde_json::json!({"label": "L", "patterns": ["*.png"]});
    obj.as_object_mut().unwrap().remove(field);
    let err = validate_user_interface_file_filter(&obj, "P", "ff[0]").unwrap_err();
    assert!(err.to_string().contains(field), "got: {err}");
}

#[test]
fn validate_file_filter_label_not_string_returns_error() {
    let err = validate_user_interface_file_filter(
        &serde_json::json!({"label": 42, "patterns": ["*.png"]}),
        "P",
        "ff[0]",
    )
    .unwrap_err();
    assert!(err.to_string().contains("label"), "got: {err}");
}

#[test]
fn validate_file_filter_patterns_not_list_returns_error() {
    let err = validate_user_interface_file_filter(
        &serde_json::json!({"label": "L", "patterns": "*.png"}),
        "P",
        "ff[0]",
    )
    .unwrap_err();
    assert!(err.to_string().contains("patterns"), "got: {err}");
}

#[test]
fn validate_file_filter_pattern_not_string_returns_error() {
    let err = validate_user_interface_file_filter(
        &serde_json::json!({"label": "L", "patterns": [42]}),
        "P",
        "ff[0]",
    )
    .unwrap_err();
    assert!(err.to_string().contains("patterns"), "got: {err}");
}

#[test_case(""                        ; "empty")]
#[test_case("a]a]a]a]a]a]a]a]a]a]a]" ; "too_long")]
fn validate_file_filter_pattern_invalid_length(pattern: &str) {
    let err = validate_user_interface_file_filter(
        &serde_json::json!({"label": "L", "patterns": [pattern]}),
        "P",
        "ff[0]",
    )
    .unwrap_err();
    assert!(err.to_string().contains("1 and 20"), "got: {err}");
}
