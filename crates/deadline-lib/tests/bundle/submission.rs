//! : Job bundle — submission & asset references

use deadline_lib::bundle::submission::{AssetReferences, parse_frame_range, split_parameter_args};
use std::collections::BTreeSet;
use std::path::Path;
use test_case::test_case;

// ── AssetReferences ─────────────────────────────────────────────────

#[test]
fn asset_refs_default_is_empty() {
    let ar = AssetReferences::new();
    assert!(!ar.is_non_empty());
}

#[test]
fn asset_refs_with_any_field_is_non_empty() {
    let mut ar = AssetReferences::new();
    ar.referenced_paths.insert("/ref".into());
    assert!(ar.is_non_empty());
}

#[test]
fn asset_refs_union_merges_all_fields() {
    let a = AssetReferences {
        input_filenames: BTreeSet::from(["a.txt".into()]),
        output_directories: BTreeSet::from(["/out_a".into()]),
        ..Default::default()
    };
    let b = AssetReferences {
        input_filenames: BTreeSet::from(["b.txt".into()]),
        output_directories: BTreeSet::from(["/out_b".into()]),
        ..Default::default()
    };
    let u = a.union(&b);
    assert_eq!(u.input_filenames.len(), 2);
    assert_eq!(u.output_directories.len(), 2);
}

#[test]
fn asset_refs_from_dict_all_fields() {
    let obj = serde_json::json!({
        "assetReferences": {
            "inputs": {"filenames": ["a.txt", "b.txt"], "directories": ["/dir"]},
            "outputs": {"directories": ["/out"]},
            "referencedPaths": ["/ref"]
        }
    });
    let ar = AssetReferences::from_dict(Some(&obj));
    assert_eq!(ar.input_filenames.len(), 2);
    assert!(ar.input_directories.contains("/dir"));
    assert!(ar.output_directories.contains("/out"));
    assert!(ar.referenced_paths.contains("/ref"));
}

#[test]
fn asset_refs_from_dict_missing_fields_default_empty() {
    let obj = serde_json::json!({"assetReferences": {"inputs": {"filenames": ["a.txt"]}}});
    let ar = AssetReferences::from_dict(Some(&obj));
    assert!(ar.input_filenames.contains("a.txt"));
    assert!(ar.output_directories.is_empty());
}

#[test]
fn asset_refs_from_dict_none_returns_empty() {
    assert!(!AssetReferences::from_dict(None).is_non_empty());
}

#[test]
fn asset_refs_from_dict_normalizes_paths() {
    let obj = serde_json::json!({
        "assetReferences": {
            "inputs": {"filenames": ["dir/../file.txt", "a//b/c"], "directories": []},
            "outputs": {"directories": []},
            "referencedPaths": []
        }
    });
    let ar = AssetReferences::from_dict(Some(&obj));
    assert!(
        ar.input_filenames.contains("file.txt"),
        "got: {:?}",
        ar.input_filenames
    );
    assert!(
        ar.input_filenames.contains("a/b/c"),
        "got: {:?}",
        ar.input_filenames
    );
}

#[test]
fn asset_refs_to_dict_sorted_output() {
    let ar = AssetReferences {
        input_filenames: BTreeSet::from(["z.txt".into(), "a.txt".into()]),
        input_directories: BTreeSet::from(["/z".into(), "/a".into()]),
        output_directories: BTreeSet::from(["/out".into()]),
        referenced_paths: BTreeSet::from(["/ref".into()]),
    };
    let dict = ar.to_dict();
    let fns: Vec<&str> = dict["assetReferences"]["inputs"]["filenames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(fns, vec!["a.txt", "z.txt"]);
    let dirs: Vec<&str> = dict["assetReferences"]["inputs"]["directories"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(dirs, vec!["/a", "/z"]);
}

#[test]
fn asset_refs_to_dict_empty_has_empty_arrays() {
    let dict = AssetReferences::new().to_dict();
    assert!(
        dict["assetReferences"]["inputs"]["filenames"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        dict["assetReferences"]["outputs"]["directories"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        dict["assetReferences"]["referencedPaths"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

// ── split_parameter_args ────────────────────────────────────────────

#[test_case("deadline:priority",           "priority"           ; "priority")]
#[test_case("deadline:maxFailedTasksCount", "maxFailedTasksCount" ; "maxFailedTasksCount")]
#[test_case("deadline:targetTaskRunStatus", "targetTaskRunStatus" ; "targetTaskRunStatus")]
fn split_params_known_deadline_param(full_name: &str, expected_key: &str) {
    let params = vec![serde_json::json!({"name": full_name, "value": "50", "type": "INT"})];
    let (app, job) = split_parameter_args(&params, Path::new("/bundle"), None, None).unwrap();
    assert!(app.contains_key(expected_key));
    assert!(job.is_empty());
}

#[test]
fn split_params_regular_param_with_lowercased_type() {
    let params = vec![serde_json::json!({"name": "Frame", "value": "1", "type": "STRING"})];
    let (app, job) = split_parameter_args(&params, Path::new("/bundle"), None, None).unwrap();
    assert!(app.is_empty());
    assert!(
        job["Frame"].as_object().unwrap().contains_key("string"),
        "Expected lowercased type key, got: {:?}",
        job["Frame"]
    );
}

#[test]
fn split_params_unrecognized_deadline_param_returns_error() {
    let params =
        vec![serde_json::json!({"name": "deadline:unknownParam", "value": "x", "type": "STRING"})];
    let err = split_parameter_args(&params, Path::new("/bundle"), None, None).unwrap_err();
    assert!(
        err.to_string().contains("Unrecognized parameter"),
        "got: {err}"
    );
}

#[test]
fn split_params_other_app_prefix_silently_dropped() {
    let params =
        vec![serde_json::json!({"name": "maya:renderLayer", "value": "default", "type": "STRING"})];
    let (app, job) = split_parameter_args(&params, Path::new("/bundle"), None, None).unwrap();
    assert!(app.is_empty());
    assert!(job.is_empty());
}

#[test]
fn split_params_no_value_key_skipped() {
    let params = vec![serde_json::json!({"name": "Frame", "type": "INT"})];
    let (app, job) = split_parameter_args(&params, Path::new("/bundle"), None, None).unwrap();
    assert!(app.is_empty());
    assert!(job.is_empty());
}

#[test]
fn split_params_empty_list() {
    let (app, job) = split_parameter_args(&[], Path::new("/bundle"), None, None).unwrap();
    assert!(app.is_empty());
    assert!(job.is_empty());
}

#[test]
fn split_params_custom_app_name() {
    let params =
        vec![serde_json::json!({"name": "maya:renderLayer", "value": "default", "type": "STRING"})];
    let (app, _) =
        split_parameter_args(&params, Path::new("/bundle"), Some("maya"), Some(&["renderLayer"])).unwrap();
    assert_eq!(app["renderLayer"], "default");
}

// ── parse_frame_range ───────────────────────────────────────────────

#[test_case("1-10",    vec![1,2,3,4,5,6,7,8,9,10] ; "simple_range")]
#[test_case("1-10:2",  vec![1,3,5,7,9]             ; "with_step")]
#[test_case("5",       vec![5]                      ; "single_frame")]
#[test_case("-5-5",    (-5..=5).collect::<Vec<_>>() ; "negative_start")]
fn parse_frame_range_valid(input: &str, expected: Vec<i64>) {
    assert_eq!(parse_frame_range(input).unwrap(), expected);
}

#[test]
fn parse_frame_range_invalid_returns_error() {
    assert!(parse_frame_range("abc").is_err());
}
