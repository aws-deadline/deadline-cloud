//! : Job bundle — loading & parsing
//!
//! Tests for file discovery (JSON vs YAML, mutual exclusion), symlink
//! containment validation, YAML/JSON parsing, and `deadline_yaml_dump`.

use deadline_lib::bundle::loader::{
    deadline_yaml_dump, parse_yaml_or_json_content, read_yaml_or_json, read_yaml_or_json_object,
    save_yaml_or_json_to_file, validate_directory_symlink_containment,
};
use std::fs;
#[cfg(unix)]
use std::os::unix::fs as unix_fs;
use std::path::Path;
use tempfile::TempDir;

// ── validate_directory_symlink_containment ──────────────────────────

#[test]
fn validate_symlinks_no_symlinks_succeeds() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("template.yaml"), "spec: 1").unwrap();
    validate_directory_symlink_containment(dir.path()).unwrap();
}

#[cfg(unix)]
#[test]
fn validate_symlinks_bundle_is_symlink_succeeds() {
    let dir = TempDir::new().unwrap();
    let real_dir = dir.path().join("real");
    fs::create_dir(&real_dir).unwrap();
    fs::write(real_dir.join("template.yaml"), "spec: 1").unwrap();
    let link = dir.path().join("link");
    unix_fs::symlink(&real_dir, &link).unwrap();
    validate_directory_symlink_containment(&link).unwrap();
}

#[test]
fn validate_symlinks_not_a_directory_returns_error() {
    let dir = TempDir::new().unwrap();
    let file = dir.path().join("not_a_dir");
    fs::write(&file, "data").unwrap();
    let err = validate_directory_symlink_containment(&file).unwrap_err();
    assert!(err.to_string().contains("not a directory"), "got: {err}");
}

#[cfg(unix)]
#[test]
fn validate_symlinks_outside_bundle_returns_error() {
    let dir = TempDir::new().unwrap();
    let bundle = dir.path().join("bundle");
    fs::create_dir(&bundle).unwrap();
    let outside = dir.path().join("outside.txt");
    fs::write(&outside, "secret").unwrap();
    unix_fs::symlink(&outside, bundle.join("link.txt")).unwrap();
    let err = validate_directory_symlink_containment(&bundle).unwrap_err();
    assert!(err.to_string().contains("resolves outside"), "got: {err}");
}

#[cfg(unix)]
#[test]
fn validate_symlinks_inside_bundle_succeeds() {
    let dir = TempDir::new().unwrap();
    let bundle = dir.path().join("bundle");
    fs::create_dir(&bundle).unwrap();
    fs::write(bundle.join("real.txt"), "data").unwrap();
    unix_fs::symlink(bundle.join("real.txt"), bundle.join("link.txt")).unwrap();
    validate_directory_symlink_containment(&bundle).unwrap();
}

#[test]
fn validate_symlinks_nested_dirs_succeeds() {
    let dir = TempDir::new().unwrap();
    let sub = dir.path().join("a").join("b");
    fs::create_dir_all(&sub).unwrap();
    fs::write(sub.join("file.txt"), "data").unwrap();
    validate_directory_symlink_containment(dir.path()).unwrap();
}

// ── read_yaml_or_json ───────────────────────────────────────────────

#[test]
fn read_yaml_or_json_only_json_returns_json() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("template.json"), r#"{"key": "value"}"#).unwrap();
    let (contents, file_type) = read_yaml_or_json(dir.path(), "template", true).unwrap();
    assert_eq!(file_type, "JSON");
    assert_eq!(contents, r#"{"key": "value"}"#);
}

#[test]
fn read_yaml_or_json_only_yaml_returns_yaml() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("template.yaml"), "key: value").unwrap();
    let (contents, file_type) = read_yaml_or_json(dir.path(), "template", true).unwrap();
    assert_eq!(file_type, "YAML");
    assert_eq!(contents, "key: value");
}

#[test]
fn read_yaml_or_json_both_exist_returns_error() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("template.json"), "{}").unwrap();
    fs::write(dir.path().join("template.yaml"), "key: val").unwrap();
    let err = read_yaml_or_json(dir.path(), "template", true).unwrap_err();
    assert!(
        err.to_string().contains("only one is permitted"),
        "got: {err}"
    );
}

#[test]
fn read_yaml_or_json_neither_exists_required_returns_error() {
    let dir = TempDir::new().unwrap();
    let err = read_yaml_or_json(dir.path(), "template", true).unwrap_err();
    assert!(
        err.to_string()
            .contains("lacks a template.json or template.yaml"),
        "got: {err}"
    );
}

#[test]
fn read_yaml_or_json_neither_exists_optional_returns_empty() {
    let dir = TempDir::new().unwrap();
    let (contents, file_type) = read_yaml_or_json(dir.path(), "params", false).unwrap();
    assert_eq!(contents, "");
    assert_eq!(file_type, "");
}

#[test]
fn read_yaml_or_json_utf8_content_decoded() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("data.json"), r#"{"name": "café"}"#).unwrap();
    let (contents, _) = read_yaml_or_json(dir.path(), "data", true).unwrap();
    assert!(contents.contains("café"));
}

// ── parse_yaml_or_json_content ──────────────────────────────────────

#[test]
fn parse_valid_json() {
    let val = parse_yaml_or_json_content(r#"{"a": 1}"#, "JSON", Path::new("/tmp"), "test").unwrap();
    assert_eq!(val["a"], 1);
}

#[test]
fn parse_valid_yaml() {
    let val = parse_yaml_or_json_content("a: 1", "YAML", Path::new("/tmp"), "test").unwrap();
    assert_eq!(val["a"], 1);
}

#[test]
fn parse_invalid_json_returns_error() {
    let err = parse_yaml_or_json_content("{bad", "JSON", Path::new("/tmp"), "test").unwrap_err();
    assert!(
        err.to_string().contains("Error loading 'test.json'"),
        "got: {err}"
    );
}

#[test]
fn parse_invalid_yaml_returns_error() {
    let err = parse_yaml_or_json_content(":\n  :\n    - :", "YAML", Path::new("/tmp"), "test")
        .unwrap_err();
    assert!(
        err.to_string().contains("Error loading 'test.yaml'"),
        "got: {err}"
    );
}

#[test]
fn parse_unknown_type_returns_error() {
    let err = parse_yaml_or_json_content("<xml/>", "XML", Path::new("/tmp"), "test").unwrap_err();
    assert!(
        err.to_string().contains("Unexpected file type"),
        "got: {err}"
    );
}

// ── read_yaml_or_json_object ────────────────────────────────────────

#[test]
fn read_object_valid_returns_parsed() {
    let dir = TempDir::new().unwrap();
    fs::write(dir.path().join("data.json"), r#"{"x": 42}"#).unwrap();
    let val = read_yaml_or_json_object(dir.path(), "data", true)
        .unwrap()
        .unwrap();
    assert_eq!(val["x"], 42);
}

#[test]
fn read_object_missing_optional_returns_none() {
    let dir = TempDir::new().unwrap();
    assert!(
        read_yaml_or_json_object(dir.path(), "data", false)
            .unwrap()
            .is_none()
    );
}

// ── save_yaml_or_json_to_file ───────────────────────────────────────

#[test]
fn save_yaml_writes_file_with_block_literal_multiline() {
    let dir = TempDir::new().unwrap();
    let data = serde_json::json!({"script": "line1\nline2\nline3"});
    save_yaml_or_json_to_file(dir.path(), "out", "YAML", &data).unwrap();
    let contents = fs::read_to_string(dir.path().join("out.yaml")).unwrap();
    assert!(
        contents.contains("|\n") || contents.contains("|-\n"),
        "Expected block literal, got:\n{contents}"
    );
    assert!(contents.contains("line1"));
}

#[test]
fn save_json_writes_pretty_printed() {
    let dir = TempDir::new().unwrap();
    let data = serde_json::json!({"key": "value"});
    save_yaml_or_json_to_file(dir.path(), "out", "JSON", &data).unwrap();
    let contents = fs::read_to_string(dir.path().join("out.json")).unwrap();
    assert!(
        contents.contains("  \"key\""),
        "Expected indented JSON, got:\n{contents}"
    );
}

#[test]
fn save_unknown_type_returns_error() {
    let dir = TempDir::new().unwrap();
    let err =
        save_yaml_or_json_to_file(dir.path(), "out", "XML", &serde_json::json!({})).unwrap_err();
    assert!(
        err.to_string().contains("Unexpected file type"),
        "got: {err}"
    );
}

// ── deadline_yaml_dump ──────────────────────────────────────────────

#[test]
fn yaml_dump_multiline_uses_block_literal() {
    let data = serde_json::json!({"script": "line1\nline2\nline3"});
    let yaml = deadline_yaml_dump(&data);
    assert!(
        yaml.contains("|\n") || yaml.contains("|-\n"),
        "Expected block literal style, got:\n{yaml}"
    );
    assert!(yaml.contains("line1"));
    assert!(yaml.contains("line2"));
}

#[test]
fn yaml_dump_singleline_no_block_literal() {
    let data = serde_json::json!({"name": "hello"});
    let yaml = deadline_yaml_dump(&data);
    assert!(yaml.contains("name:"));
    assert!(
        !yaml.contains('|'),
        "Single-line should not use block literal"
    );
}

#[test]
fn yaml_dump_preserves_insertion_order() {
    let mut map = serde_json::Map::new();
    map.insert("zebra".into(), serde_json::json!("z"));
    map.insert("alpha".into(), serde_json::json!("a"));
    let yaml = deadline_yaml_dump(&serde_json::Value::Object(map));
    let z = yaml.find("zebra").unwrap();
    let a = yaml.find("alpha").unwrap();
    assert!(
        z < a,
        "Expected insertion order (zebra before alpha), got:\n{yaml}"
    );
}

// ── Roundtrip ───────────────────────────────────────────────────────

#[test]
fn roundtrip_yaml() {
    let dir = TempDir::new().unwrap();
    let data = serde_json::json!({"specificationVersion": "jobtemplate-2023-09", "name": "Test"});
    save_yaml_or_json_to_file(dir.path(), "t", "YAML", &data).unwrap();
    let val = read_yaml_or_json_object(dir.path(), "t", true)
        .unwrap()
        .unwrap();
    assert_eq!(val["specificationVersion"], "jobtemplate-2023-09");
    assert_eq!(val["name"], "Test");
}

#[test]
fn roundtrip_json() {
    let dir = TempDir::new().unwrap();
    let data = serde_json::json!({"key": [1, 2, 3]});
    save_yaml_or_json_to_file(dir.path(), "d", "JSON", &data).unwrap();
    let val = read_yaml_or_json_object(dir.path(), "d", true)
        .unwrap()
        .unwrap();
    assert_eq!(val["key"], serde_json::json!([1, 2, 3]));
}
