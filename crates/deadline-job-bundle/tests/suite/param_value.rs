//! : Job bundle — parameter value validation & coercion (cases 49-65)

use deadline_job_bundle::parameters::validate_job_parameter_value;
use test_case::test_case;

// ── Type coercion ───────────────────────────────────────────────────

#[test_case("STRING", serde_json::json!("hello"), serde_json::json!("hello") ; "string_string")]
#[test_case("PATH",   serde_json::json!("/tmp"),  serde_json::json!("/tmp")  ; "path_string")]
#[test_case("INT",    serde_json::json!(42),      serde_json::json!(42)      ; "int_int")]
#[test_case("INT",    serde_json::json!("19"),     serde_json::json!(19)      ; "int_from_string")]
#[test_case("FLOAT",  serde_json::json!(3.14),    serde_json::json!(3.14)    ; "float_float")]
#[test_case("FLOAT",  serde_json::json!("3.14"),   serde_json::json!(3.14)    ; "float_from_string")]
fn validate_value_coercion_succeeds(
    param_type: &str,
    input: serde_json::Value,
    expected: serde_json::Value,
) {
    let param = serde_json::json!({"name": "P", "type": param_type});
    let result = validate_job_parameter_value(&param, &input).unwrap();
    assert_eq!(result, expected);
}

// ── Type errors ─────────────────────────────────────────────────────

#[test_case("INT",    serde_json::json!(3.7),    "not an integer"    ; "int_from_float")]
#[test_case("INT",    serde_json::json!("abc"),  "integer"           ; "int_from_bad_string")]
#[test_case("FLOAT",  serde_json::json!("abc"),  "floating"          ; "float_from_bad_string")]
#[test_case("STRING", serde_json::json!(42),     "STRING"            ; "string_from_int")]
fn validate_value_type_error(param_type: &str, input: serde_json::Value, expected_msg: &str) {
    let param = serde_json::json!({"name": "P", "type": param_type});
    let err = validate_job_parameter_value(&param, &input).unwrap_err();
    assert!(err.to_string().contains(expected_msg), "got: {err}");
}

#[test]
fn validate_value_unsupported_type_returns_error() {
    let param = serde_json::json!({"name": "P", "type": "BOOLEAN"});
    let err = validate_job_parameter_value(&param, &serde_json::json!(true)).unwrap_err();
    assert!(err.to_string().contains("unsupported type"), "got: {err}");
}

// ── Constraint checks ───────────────────────────────────────────────

#[test_case("minLength", 5,  serde_json::json!("ab"),     "shorter than minLength" ; "min_length")]
#[test_case("maxLength", 3,  serde_json::json!("abcdef"), "longer than maxLength"  ; "max_length")]
#[test_case("minValue",  10, serde_json::json!(5),        "less than minValue"     ; "min_value")]
#[test_case("maxValue",  10, serde_json::json!(15),       "greater than maxValue"  ; "max_value")]
fn validate_value_constraint_violation(
    constraint: &str,
    limit: i64,
    input: serde_json::Value,
    expected_msg: &str,
) {
    let param_type = if constraint.contains("Length") {
        "STRING"
    } else {
        "INT"
    };
    let param = serde_json::json!({"name": "P", "type": param_type, constraint: limit});
    let err = validate_job_parameter_value(&param, &input).unwrap_err();
    assert!(err.to_string().contains(expected_msg), "got: {err}");
}

#[test]
fn validate_value_not_in_allowed_values_returns_error() {
    let param = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["a", "b"]});
    let err = validate_job_parameter_value(&param, &serde_json::json!("c")).unwrap_err();
    assert!(
        err.to_string().contains("not an allowed value"),
        "got: {err}"
    );
}

#[test]
fn validate_value_in_allowed_values_succeeds() {
    let param = serde_json::json!({"name": "P", "type": "STRING", "allowedValues": ["a", "b"]});
    validate_job_parameter_value(&param, &serde_json::json!("a")).unwrap();
}
