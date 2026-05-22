//! Pure business logic for the config dialog, testable without Qt.
//!
//! QObject methods in `config_model.rs`, `resource_model.rs`, and
//! `auth_model.rs` are thin wrappers that call these functions and
//! update Qt properties with the results.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

/// All settings displayed in the config dialog.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConfigState {
    pub aws_profile: String,
    pub job_history_dir: String,
    pub farm_id: String,
    pub queue_id: String,
    pub storage_profile_id: String,
    pub job_attachments_filesystem: String,
    pub auto_accept: bool,
    pub telemetry_opt_out: bool,
    pub force_s3_check: bool,
    pub submitter_update_notification: bool,
    pub conflict_resolution: String,
    pub log_level: String,
    pub locale: String,
    pub known_asset_paths: Vec<String>,
}

/// Load all config dialog settings from the config file at the given path.
pub fn load_config_state(config_path: &Path) -> ConfigState {
    let config = match deadline_lib::config::config_file::read_config_from(config_path) {
        Ok(c) => c,
        Err(_) => return ConfigState::default(),
    };

    let get = |name: &str| {
        deadline_lib::config::config_file::get_setting(name, &config).unwrap_or_default()
    };

    let str2bool = |s: &str| matches!(s.to_lowercase().as_str(), "true" | "yes" | "1");

    let known_paths_str = get("settings.known_asset_paths");
    let known_asset_paths = if known_paths_str.is_empty() {
        Vec::new()
    } else {
        let sep = if cfg!(windows) { ';' } else { ':' };
        known_paths_str.split(sep).map(|s| s.to_string()).collect()
    };

    ConfigState {
        aws_profile: get("defaults.aws_profile_name"),
        job_history_dir: get("settings.job_history_dir"),
        farm_id: get("defaults.farm_id"),
        queue_id: get("defaults.queue_id"),
        storage_profile_id: get("settings.storage_profile_id"),
        job_attachments_filesystem: get("defaults.job_attachments_file_system"),
        auto_accept: str2bool(&get("settings.auto_accept")),
        telemetry_opt_out: str2bool(&get("telemetry.opt_out")),
        force_s3_check: str2bool(&get("settings.force_s3_check")),
        submitter_update_notification: str2bool(&get("settings.submitter_update_notification")),
        conflict_resolution: get("settings.conflict_resolution"),
        log_level: get("settings.log_level"),
        locale: get("settings.locale"),
        known_asset_paths,
    }
}

/// Apply changed settings to disk. Only writes settings present in `changes`.
/// Returns the new state after applying.
pub fn apply_config_changes(
    config_path: &Path,
    changes: &HashMap<String, String>,
) -> Result<ConfigState, String> {
    let mut config = deadline_lib::config::config_file::read_config_from(config_path)
        .map_err(|e| e.to_string())?;

    for (setting_name, value) in changes {
        deadline_lib::config::config_file::set_setting(setting_name, value, &mut config)
            .map_err(|e| e.to_string())?;
    }

    deadline_lib::config::config_file::write_config_to(&config, config_path)
        .map_err(|e| e.to_string())?;

    Ok(load_config_state(config_path))
}

/// Compute which settings differ between two states.
/// Returns setting keys that changed.
pub fn compute_dirty_fields(baseline: &ConfigState, current: &ConfigState) -> Vec<String> {
    let mut dirty = Vec::new();

    if baseline.aws_profile != current.aws_profile {
        dirty.push("defaults.aws_profile_name".to_string());
    }
    if baseline.job_history_dir != current.job_history_dir {
        dirty.push("settings.job_history_dir".to_string());
    }
    if baseline.farm_id != current.farm_id {
        dirty.push("defaults.farm_id".to_string());
    }
    if baseline.queue_id != current.queue_id {
        dirty.push("defaults.queue_id".to_string());
    }
    if baseline.storage_profile_id != current.storage_profile_id {
        dirty.push("settings.storage_profile_id".to_string());
    }
    if baseline.job_attachments_filesystem != current.job_attachments_filesystem {
        dirty.push("defaults.job_attachments_file_system".to_string());
    }
    if baseline.auto_accept != current.auto_accept {
        dirty.push("settings.auto_accept".to_string());
    }
    if baseline.telemetry_opt_out != current.telemetry_opt_out {
        dirty.push("telemetry.opt_out".to_string());
    }
    if baseline.force_s3_check != current.force_s3_check {
        dirty.push("settings.force_s3_check".to_string());
    }
    if baseline.submitter_update_notification != current.submitter_update_notification {
        dirty.push("settings.submitter_update_notification".to_string());
    }
    if baseline.conflict_resolution != current.conflict_resolution {
        dirty.push("settings.conflict_resolution".to_string());
    }
    if baseline.log_level != current.log_level {
        dirty.push("settings.log_level".to_string());
    }
    if baseline.locale != current.locale {
        dirty.push("settings.locale".to_string());
    }
    if baseline.known_asset_paths != current.known_asset_paths {
        dirty.push("settings.known_asset_paths".to_string());
    }

    dirty
}

/// Parse AWS profile names from ~/.aws/config and ~/.aws/credentials files.
/// Returns sorted list with "(default)" as the first entry.
pub fn parse_aws_profiles(aws_dir: &Path) -> Vec<String> {
    let mut profiles = BTreeSet::new();

    for filename in &["config", "credentials"] {
        let path = aws_dir.join(filename);
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => continue,
        };
        for line in content.lines() {
            let line = line.trim();
            if line.starts_with('[') && line.ends_with(']') {
                let section = &line[1..line.len() - 1];
                let name = section.strip_prefix("profile ").unwrap_or(section);
                if name != "DEFAULT" && name != "default" {
                    profiles.insert(name.to_string());
                }
            }
        }
    }

    let mut result = vec!["(default)".to_string()];
    result.extend(profiles);
    result
}

/// A farm entry from the API.
#[derive(Debug, Clone, PartialEq)]
pub struct ResourceEntry {
    pub display_name: String,
    pub id: String,
}

/// Extract farm entries from paginated ListFarms API response pages.
/// Returns sorted by display_name (case-insensitive).
pub fn extract_farms(pages: &[serde_json::Value]) -> Vec<ResourceEntry> {
    let mut entries: Vec<ResourceEntry> = pages
        .iter()
        .filter_map(|p| p.get("farms"))
        .filter_map(|f| f.as_array())
        .flatten()
        .filter_map(|item| {
            Some(ResourceEntry {
                display_name: item.get("displayName")?.as_str()?.to_string(),
                id: item.get("farmId")?.as_str()?.to_string(),
            })
        })
        .collect();

    entries.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
    });
    entries
}

/// Extract queue entries from paginated ListQueues API response pages.
/// Returns sorted by display_name (case-insensitive).
pub fn extract_queues(pages: &[serde_json::Value]) -> Vec<ResourceEntry> {
    let mut entries: Vec<ResourceEntry> = pages
        .iter()
        .filter_map(|p| p.get("queues"))
        .filter_map(|f| f.as_array())
        .flatten()
        .filter_map(|item| {
            Some(ResourceEntry {
                display_name: item.get("displayName")?.as_str()?.to_string(),
                id: item.get("queueId")?.as_str()?.to_string(),
            })
        })
        .collect();

    entries.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
    });
    entries
}

/// Extract storage profile entries, filtered to the current OS.
/// Returns sorted by display_name (case-insensitive), with a
/// "<none selected>" entry (empty id) prepended.
pub fn extract_storage_profiles(
    pages: &[serde_json::Value],
    current_os: &str,
) -> Vec<ResourceEntry> {
    let mut entries: Vec<ResourceEntry> = pages
        .iter()
        .filter_map(|p| p.get("storageProfiles"))
        .filter_map(|f| f.as_array())
        .flatten()
        .filter_map(|item| {
            let os_family = item.get("osFamily")?.as_str()?;
            if os_family.to_lowercase() == current_os.to_lowercase() {
                Some(ResourceEntry {
                    display_name: item.get("displayName")?.as_str()?.to_string(),
                    id: item.get("storageProfileId")?.as_str()?.to_string(),
                })
            } else {
                None
            }
        })
        .collect();

    entries.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
    });

    let mut result = vec![ResourceEntry {
        display_name: "<none selected>".to_string(),
        id: String::new(),
    }];
    result.extend(entries);
    result
}

pub mod auth;
pub mod resources;
pub mod watcher;

#[cfg(test)]
mod tests;
