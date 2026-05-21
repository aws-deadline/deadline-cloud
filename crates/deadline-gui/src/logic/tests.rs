//! Tests for config dialog pure logic (Level 1).
//!
//! These test the business logic without any Qt dependency.

use super::*;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

// ─────────────────────────────────────────────────────────────────────
// load_config_state
// ─────────────────────────────────────────────────────────────────────

fn write_config(dir: &TempDir, content: &str) -> PathBuf {
    let path = dir.path().join("config");
    std::fs::write(&path, content).unwrap();
    path
}

#[test]
fn load_config_state_reads_profile() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(&dir, "[defaults]\naws_profile_name = my-profile\n");
    let state = load_config_state(&path);
    assert_eq!(state.aws_profile, "my-profile");
}

#[test]
fn load_config_state_reads_farm_and_queue() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        &dir,
        "[defaults]\naws_profile_name = prod\n\n\
         [profile-prod defaults]\nfarm_id = farm-abc\n\n\
         [profile-prod farm-abc defaults]\nqueue_id = queue-xyz\n",
    );
    let state = load_config_state(&path);
    assert_eq!(state.farm_id, "farm-abc");
    assert_eq!(state.queue_id, "queue-xyz");
}

#[test]
fn load_config_state_reads_checkboxes() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        &dir,
        "[defaults]\naws_profile_name = (default)\n\n\
         [settings]\nauto_accept = true\nforce_s3_check = false\n\
         [telemetry]\nopt_out = true\n",
    );
    let state = load_config_state(&path);
    assert!(state.auto_accept);
    assert!(!state.force_s3_check);
    assert!(state.telemetry_opt_out);
}

#[test]
fn load_config_state_reads_combobox_settings() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        &dir,
        "[defaults]\naws_profile_name = (default)\n\n\
         [settings]\nconflict_resolution = OVERWRITE\nlog_level = DEBUG\nlocale = ja_JP\n",
    );
    let state = load_config_state(&path);
    assert_eq!(state.conflict_resolution, "OVERWRITE");
    assert_eq!(state.log_level, "DEBUG");
    assert_eq!(state.locale, "ja_JP");
}

#[test]
fn load_config_state_reads_known_asset_paths() {
    let dir = tempfile::tempdir().unwrap();
    let sep = if cfg!(windows) { ";" } else { ":" };
    let paths_value = format!("/path/one{sep}/path/two");
    let content = format!(
        "[defaults]\naws_profile_name = (default)\n\n\
         [settings]\nknown_asset_paths = {}\n",
        paths_value
    );
    let path = write_config(&dir, &content);
    let state = load_config_state(&path);
    assert_eq!(state.known_asset_paths, vec!["/path/one", "/path/two"]);
}

#[test]
fn load_config_state_empty_config_returns_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(&dir, "");
    let state = load_config_state(&path);
    // aws_profile defaults to "(default)" per deadline-lib
    assert_eq!(state.aws_profile, "(default)");
    assert_eq!(state.farm_id, "");
    assert!(!state.auto_accept);
    assert!(state.known_asset_paths.is_empty());
}

#[test]
fn load_config_state_missing_file_returns_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nonexistent");
    let state = load_config_state(&path);
    // Missing file is treated same as empty — library defaults apply
    assert_eq!(state.aws_profile, "(default)");
    assert_eq!(state.farm_id, "");
}

// ─────────────────────────────────────────────────────────────────────
// apply_config_changes
// ─────────────────────────────────────────────────────────────────────

#[test]
fn apply_config_changes_writes_single_setting() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        &dir,
        "[defaults]\naws_profile_name = (default)\n\n[settings]\nlog_level = WARNING\n",
    );
    let mut changes = HashMap::new();
    changes.insert("settings.log_level".to_string(), "DEBUG".to_string());

    let new_state = apply_config_changes(&path, &changes).unwrap();
    assert_eq!(new_state.log_level, "DEBUG");

    // Verify persisted to disk
    let reloaded = load_config_state(&path);
    assert_eq!(reloaded.log_level, "DEBUG");
}

#[test]
fn apply_config_changes_writes_multiple_settings() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_config(
        &dir,
        "[defaults]\naws_profile_name = (default)\n\n[settings]\nauto_accept = false\n",
    );
    let mut changes = HashMap::new();
    changes.insert("settings.auto_accept".to_string(), "true".to_string());
    changes.insert("settings.log_level".to_string(), "ERROR".to_string());

    let new_state = apply_config_changes(&path, &changes).unwrap();
    assert!(new_state.auto_accept);
    assert_eq!(new_state.log_level, "ERROR");
}

#[test]
fn apply_config_changes_empty_changes_is_noop() {
    let dir = tempfile::tempdir().unwrap();
    let content = "[defaults]\naws_profile_name = test\n\n[settings]\nlog_level = INFO\n";
    let path = write_config(&dir, content);
    let changes = HashMap::new();

    let new_state = apply_config_changes(&path, &changes).unwrap();
    assert_eq!(new_state.log_level, "INFO");
    assert_eq!(new_state.aws_profile, "test");
}

// ─────────────────────────────────────────────────────────────────────
// compute_dirty_fields
// ─────────────────────────────────────────────────────────────────────

#[test]
fn compute_dirty_fields_no_changes() {
    let state = ConfigState {
        log_level: "INFO".to_string(),
        ..Default::default()
    };
    let dirty = compute_dirty_fields(&state, &state);
    assert!(dirty.is_empty());
}

#[test]
fn compute_dirty_fields_detects_string_change() {
    let baseline = ConfigState {
        log_level: "INFO".to_string(),
        ..Default::default()
    };
    let current = ConfigState {
        log_level: "DEBUG".to_string(),
        ..Default::default()
    };
    let dirty = compute_dirty_fields(&baseline, &current);
    assert!(dirty.contains(&"settings.log_level".to_string()));
}

#[test]
fn compute_dirty_fields_detects_bool_change() {
    let baseline = ConfigState::default();
    let current = ConfigState {
        auto_accept: true,
        ..Default::default()
    };
    let dirty = compute_dirty_fields(&baseline, &current);
    assert!(dirty.contains(&"settings.auto_accept".to_string()));
}

#[test]
fn compute_dirty_fields_detects_multiple_changes() {
    let baseline = ConfigState::default();
    let current = ConfigState {
        log_level: "ERROR".to_string(),
        farm_id: "farm-new".to_string(),
        telemetry_opt_out: true,
        ..Default::default()
    };
    let dirty = compute_dirty_fields(&baseline, &current);
    assert!(dirty.len() >= 3);
}

// ─────────────────────────────────────────────────────────────────────
// parse_aws_profiles
// ─────────────────────────────────────────────────────────────────────

#[test]
fn parse_aws_profiles_from_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let aws_dir = dir.path().join(".aws");
    std::fs::create_dir(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        "[default]\nregion = us-west-2\n\n[profile staging]\nregion = us-east-1\n",
    )
    .unwrap();

    let profiles = parse_aws_profiles(&aws_dir);
    assert_eq!(profiles[0], "(default)");
    assert!(profiles.contains(&"staging".to_string()));
}

#[test]
fn parse_aws_profiles_from_credentials_file() {
    let dir = tempfile::tempdir().unwrap();
    let aws_dir = dir.path().join(".aws");
    std::fs::create_dir(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("credentials"),
        "[default]\naws_access_key_id = AKIA...\n\n[prod]\naws_access_key_id = AKIA...\n",
    )
    .unwrap();

    let profiles = parse_aws_profiles(&aws_dir);
    assert_eq!(profiles[0], "(default)");
    assert!(profiles.contains(&"prod".to_string()));
}

#[test]
fn parse_aws_profiles_merges_both_files() {
    let dir = tempfile::tempdir().unwrap();
    let aws_dir = dir.path().join(".aws");
    std::fs::create_dir(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        "[profile alpha]\nregion = us-west-2\n",
    )
    .unwrap();
    std::fs::write(
        aws_dir.join("credentials"),
        "[beta]\naws_access_key_id = AKIA...\n",
    )
    .unwrap();

    let profiles = parse_aws_profiles(&aws_dir);
    assert_eq!(profiles[0], "(default)");
    assert!(profiles.contains(&"alpha".to_string()));
    assert!(profiles.contains(&"beta".to_string()));
}

#[test]
fn parse_aws_profiles_sorted_alphabetically() {
    let dir = tempfile::tempdir().unwrap();
    let aws_dir = dir.path().join(".aws");
    std::fs::create_dir(&aws_dir).unwrap();
    std::fs::write(
        aws_dir.join("config"),
        "[profile zebra]\n\n[profile alpha]\n\n[default]\n",
    )
    .unwrap();

    let profiles = parse_aws_profiles(&aws_dir);
    assert_eq!(profiles[0], "(default)");
    assert_eq!(profiles[1], "alpha");
    assert_eq!(profiles[2], "zebra");
}

#[test]
fn parse_aws_profiles_missing_dir_returns_default_only() {
    let dir = tempfile::tempdir().unwrap();
    let aws_dir = dir.path().join("nonexistent");
    let profiles = parse_aws_profiles(&aws_dir);
    assert_eq!(profiles, vec!["(default)"]);
}

// ─────────────────────────────────────────────────────────────────────
// extract_farms
// ─────────────────────────────────────────────────────────────────────

#[test]
fn extract_farms_from_single_page() {
    let page = serde_json::json!({
        "farms": [
            {"farmId": "farm-bbb", "displayName": "Beta Farm"},
            {"farmId": "farm-aaa", "displayName": "Alpha Farm"},
        ]
    });
    let farms = extract_farms(&[page]);
    assert_eq!(farms.len(), 2);
    assert_eq!(farms[0].display_name, "Alpha Farm");
    assert_eq!(farms[0].id, "farm-aaa");
    assert_eq!(farms[1].display_name, "Beta Farm");
}

#[test]
fn extract_farms_from_multiple_pages() {
    let page1 = serde_json::json!({"farms": [{"farmId": "farm-1", "displayName": "Zulu"}]});
    let page2 = serde_json::json!({"farms": [{"farmId": "farm-2", "displayName": "Alpha"}]});
    let farms = extract_farms(&[page1, page2]);
    assert_eq!(farms.len(), 2);
    assert_eq!(farms[0].display_name, "Alpha");
}

#[test]
fn extract_farms_empty_response() {
    let page = serde_json::json!({"farms": []});
    let farms = extract_farms(&[page]);
    assert!(farms.is_empty());
}

// ─────────────────────────────────────────────────────────────────────
// extract_queues
// ─────────────────────────────────────────────────────────────────────

#[test]
fn extract_queues_sorted_case_insensitive() {
    let page = serde_json::json!({
        "queues": [
            {"queueId": "queue-b", "displayName": "beta-queue"},
            {"queueId": "queue-a", "displayName": "Alpha-Queue"},
        ]
    });
    let queues = extract_queues(&[page]);
    assert_eq!(queues[0].display_name, "Alpha-Queue");
    assert_eq!(queues[1].display_name, "beta-queue");
}

// ─────────────────────────────────────────────────────────────────────
// extract_storage_profiles
// ─────────────────────────────────────────────────────────────────────

#[test]
fn extract_storage_profiles_filters_by_os() {
    let page = serde_json::json!({
        "storageProfiles": [
            {"storageProfileId": "sp-linux", "displayName": "Linux Profile", "osFamily": "LINUX"},
            {"storageProfileId": "sp-mac", "displayName": "Mac Profile", "osFamily": "MACOS"},
            {"storageProfileId": "sp-win", "displayName": "Win Profile", "osFamily": "WINDOWS"},
        ]
    });
    let profiles = extract_storage_profiles(&[page], "macos");
    // Should have "<none selected>" + the macOS profile
    assert_eq!(profiles.len(), 2);
    assert_eq!(profiles[0].display_name, "<none selected>");
    assert_eq!(profiles[0].id, "");
    assert_eq!(profiles[1].display_name, "Mac Profile");
    assert_eq!(profiles[1].id, "sp-mac");
}

#[test]
fn extract_storage_profiles_none_match_os() {
    let page = serde_json::json!({
        "storageProfiles": [
            {"storageProfileId": "sp-win", "displayName": "Win Only", "osFamily": "WINDOWS"},
        ]
    });
    let profiles = extract_storage_profiles(&[page], "linux");
    assert_eq!(profiles.len(), 1);
    assert_eq!(profiles[0].display_name, "<none selected>");
}

#[test]
fn extract_storage_profiles_sorted_with_none_first() {
    let page = serde_json::json!({
        "storageProfiles": [
            {"storageProfileId": "sp-z", "displayName": "Zulu Profile", "osFamily": "LINUX"},
            {"storageProfileId": "sp-a", "displayName": "Alpha Profile", "osFamily": "LINUX"},
        ]
    });
    let profiles = extract_storage_profiles(&[page], "linux");
    assert_eq!(profiles[0].display_name, "<none selected>");
    assert_eq!(profiles[1].display_name, "Alpha Profile");
    assert_eq!(profiles[2].display_name, "Zulu Profile");
}
