// Configuration file management for ~/.deadline/config
//
// INI config file read/write with hierarchical section naming,
// atomic writes, and environment variable override for file path.
//
// Design: The primary API operates on IniConfig values passed explicitly.
// CLI commands call read_config_from() once at startup, apply CLI flag
// overrides in memory via set_setting(), then thread the
// IniConfig through all downstream calls via get_setting().
// The _from_disk / _to_disk functions are convenience wrappers that
// read from / write to the default config file path.

use crate::ini::{IniConfig, IniParseError};
use crate::settings::{self, SettingDef, find_setting};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{0}")]
    InvalidSettingName(String),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("Malformed config file: {0}")]
    Parse(#[from] IniParseError),
    #[error("{0}")]
    InvalidBool(String),
}

// ---------------------------------------------------------------------------
// Config file path
// ---------------------------------------------------------------------------

const CONFIG_FILE_PATH_ENV_VAR: &str = "DEADLINE_CONFIG_FILE_PATH";
const DEFAULT_CONFIG_PATH: &str = "~/.deadline/config";
const DEFAULT_CACHE_DIR: &str = "~/.deadline/cache";

/// Expand `~` at the start of a path to the user's home directory.
fn expand_tilde(path: &str) -> PathBuf {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = home_dir() {
            return home.join(rest);
        }
    } else if path == "~" {
        if let Some(home) = home_dir() {
            return home;
        }
    }
    PathBuf::from(path)
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

/// Get the config file path, checking the env var first.
/// Falls back to `~/.deadline/config` if unset or empty.
pub fn get_config_file_path() -> PathBuf {
    match std::env::var(CONFIG_FILE_PATH_ENV_VAR) {
        Ok(val) if !val.is_empty() => expand_tilde(&val),
        _ => expand_tilde(DEFAULT_CONFIG_PATH),
    }
}

/// Get the cache directory path (`~/.deadline/cache`).
pub fn get_cache_directory() -> PathBuf {
    expand_tilde(DEFAULT_CACHE_DIR)
}

// ---------------------------------------------------------------------------
// Read / Write
// ---------------------------------------------------------------------------

/// Read a config file from a specific path.
/// Returns an empty config if the file doesn't exist.
pub fn read_config_from(path: &Path) -> Result<IniConfig, ConfigError> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(IniConfig::parse(&text)?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(IniConfig::new()),
        Err(e) => Err(ConfigError::Io(e)),
    }
}

/// Convenience: read from the default config file path (env var or ~/.deadline/config).
pub fn read_config() -> Result<IniConfig, ConfigError> {
    read_config_from(&get_config_file_path())
}

/// Write the config atomically to a specific path: write to a temp file, then rename.
/// Creates parent directories if needed. On POSIX, sets file permissions to 0o600.
pub fn write_config_to(config: &IniConfig, path: &Path) -> Result<(), ConfigError> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }

    // Temp file in the same directory ensures atomic rename works
    let parent = path.parent().unwrap_or(Path::new("."));
    let tmp_path = parent.join(format!(".config.tmp.{}", std::process::id()));

    fs::write(&tmp_path, config.to_string())?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&tmp_path, fs::Permissions::from_mode(0o600))?;
    }

    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// Convenience: write to the default config file path.
pub fn write_config(config: &IniConfig) -> Result<(), ConfigError> {
    write_config_to(config, &get_config_file_path())
}

// ---------------------------------------------------------------------------
// Section name resolution (hierarchical scoping)
// ---------------------------------------------------------------------------

/// Walk the dependency chain for a setting and return the list of
/// section name prefixes. For example, `defaults.queue_id` depends on
/// `defaults.farm_id` which depends on `defaults.aws_profile_name`.
/// If profile="myprofile" and farm="farm-abc", returns ["profile-myprofile", "farm-abc"].
fn get_section_prefixes(setting_def: &SettingDef, config: &IniConfig) -> Vec<String> {
    match setting_def.depend {
        Some(dep_name) => {
            let dep_def = find_setting(dep_name).expect("dependency setting must exist");
            let dep_section_format = dep_def
                .section_format
                .expect("dependency must have section_format");

            let (dep_section_part, dep_key) = dep_name.split_once('.').unwrap();
            let dep_prefixes = get_section_prefixes(dep_def, config);

            let dep_full_section = if dep_prefixes.is_empty() {
                dep_section_part.to_string()
            } else {
                format!("{} {}", dep_prefixes.join(" "), dep_section_part)
            };

            let dep_value = config
                .get(&dep_full_section, dep_key)
                .map(|s| s.to_string())
                .unwrap_or_else(|| resolve_default(dep_def, config));

            let formatted = dep_section_format.replace("{}", &dep_value);

            let mut prefixes = dep_prefixes;
            prefixes.push(formatted);
            prefixes
        }
        None => vec![],
    }
}

/// Resolve the default value for a setting, performing `{aws_profile_name}` substitution.
fn resolve_default(setting_def: &SettingDef, config: &IniConfig) -> String {
    let default = setting_def.default;
    if default.contains('{') {
        let profile = get_setting("defaults.aws_profile_name", config)
            .unwrap_or_else(|_| "(default)".to_string());
        default.replace("{aws_profile_name}", &profile)
    } else {
        default.to_string()
    }
}

/// Build the full INI section name for a setting.
fn full_section_name(setting_name: &str, setting_def: &SettingDef, config: &IniConfig) -> String {
    let section_part = setting_name.split('.').next().unwrap();
    let prefixes = get_section_prefixes(setting_def, config);
    if prefixes.is_empty() {
        section_part.to_string()
    } else {
        format!("{} {}", prefixes.join(" "), section_part)
    }
}

// ---------------------------------------------------------------------------
// Public API: get / set / clear / get_default
// ---------------------------------------------------------------------------

fn validate_setting(setting_name: &str) -> Result<&'static SettingDef, ConfigError> {
    if !setting_name.contains('.') {
        return Err(ConfigError::InvalidSettingName(format!(
            "The setting name '{setting_name}' is not valid."
        )));
    }
    find_setting(setting_name).ok_or_else(|| {
        ConfigError::InvalidSettingName(format!(
            "AWS Deadline Cloud configuration has no setting named '{setting_name}'."
        ))
    })
}

/// Get a setting value using an already-loaded config.
/// This is the primary API — most callers should use this.
pub fn get_setting(
    setting_name: &str,
    config: &IniConfig,
) -> Result<String, ConfigError> {
    let setting_def = validate_setting(setting_name)?;
    let key = setting_name.split('.').nth(1).unwrap();
    let section = full_section_name(setting_name, setting_def, config);

    match config.get(&section, key) {
        Some(value) => Ok(value.to_string()),
        None => Ok(resolve_default(setting_def, config)),
    }
}

/// Convenience: read from disk, then get the setting.
pub fn get_setting_from_disk(setting_name: &str) -> Result<String, ConfigError> {
    let config = read_config()?;
    get_setting(setting_name, &config)
}

/// Get the default value using an already-loaded config.
pub fn get_setting_default(
    setting_name: &str,
    config: &IniConfig,
) -> Result<String, ConfigError> {
    let setting_def = validate_setting(setting_name)?;
    Ok(resolve_default(setting_def, config))
}

/// Convenience: read from disk, then get the default.
pub fn get_setting_default_from_disk(setting_name: &str) -> Result<String, ConfigError> {
    let config = read_config()?;
    get_setting_default(setting_name, &config)
}

/// Set a setting value in a config object without writing to disk.
/// This is the primary API — most callers should use this.
pub fn set_setting(
    setting_name: &str,
    value: &str,
    config: &mut IniConfig,
) -> Result<(), ConfigError> {
    let setting_def = validate_setting(setting_name)?;
    let key = setting_name.split('.').nth(1).unwrap();
    let section = full_section_name(setting_name, setting_def, config);
    config.set(&section, key, value);
    Ok(())
}

/// Convenience: read from disk, set the value, write back.
pub fn set_setting_to_disk(setting_name: &str, value: &str) -> Result<(), ConfigError> {
    let mut config = read_config()?;
    set_setting(setting_name, value, &mut config)?;
    write_config(&config)?;
    Ok(())
}

/// Clear a setting in a config object without writing to disk.
pub fn clear_setting(
    setting_name: &str,
    config: &mut IniConfig,
) -> Result<(), ConfigError> {
    let default = get_setting_default(setting_name, config)?;
    set_setting(setting_name, &default, config)
}

/// Convenience: read from disk, clear the setting, write back.
pub fn clear_setting_to_disk(setting_name: &str) -> Result<(), ConfigError> {
    let default = get_setting_default_from_disk(setting_name)?;
    set_setting_to_disk(setting_name, &default)
}

// ---------------------------------------------------------------------------
// Profile resolution
// ---------------------------------------------------------------------------

/// Find the best AWS profile for a given farm and optional queue.
///
/// Priority:
/// 1. Default profile if its farm matches
/// 2. Any profile matching both farm and queue
/// 3. Any profile matching farm only
/// 4. Default profile (fallback)
///
/// Takes the profile list as a parameter so callers can source it from
/// the AWS SDK (boto3.Session in Python, aws-config in Rust) without
/// coupling this crate to the SDK.
pub fn get_best_profile_for_farm(
    config: &IniConfig,
    aws_profile_names: &[&str],
    farm_id: &str,
    queue_id: Option<&str>,
) -> String {
    // Work on a copy so we don't mutate the caller's config
    let mut scratch = config.clone();

    let default_profile =
        get_setting("defaults.aws_profile_name", &scratch).unwrap_or_default();

    // Priority 1: default profile's farm matches
    if get_setting("defaults.farm_id", &scratch).unwrap_or_default() == farm_id {
        return default_profile;
    }

    let mut first_farm_match: Option<String> = None;
    let queue_id = queue_id.filter(|q| !q.is_empty());

    for &profile in aws_profile_names {
        let _ = set_setting("defaults.aws_profile_name", profile, &mut scratch);

        let profile_farm =
            get_setting("defaults.farm_id", &scratch).unwrap_or_default();
        if profile_farm == farm_id {
            // Priority 2: farm + queue match
            if let Some(qid) = queue_id {
                let profile_queue =
                    get_setting("defaults.queue_id", &scratch).unwrap_or_default();
                if profile_queue == qid {
                    return profile.to_string();
                }
            }
            // Priority 3: first farm-only match
            if first_farm_match.is_none() {
                first_farm_match = Some(profile.to_string());
            }
        }
    }

    first_farm_match.unwrap_or(default_profile)
}

// ---------------------------------------------------------------------------
// str2bool
// ---------------------------------------------------------------------------

/// Convert a string to a boolean, accepting various true/false representations.
/// Accepted true: "yes", "on", "true", "1" (case-insensitive)
/// Accepted false: "no", "off", "false", "0" (case-insensitive)
pub fn str2bool(value: &str) -> Result<bool, ConfigError> {
    match value.to_lowercase().as_str() {
        "yes" | "on" | "true" | "1" => Ok(true),
        "no" | "off" | "false" | "0" => Ok(false),
        _ => Err(ConfigError::InvalidBool(format!(
            "{value:?} is not a valid boolean string value"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Iterating over all settings (used by `deadline config show`)
// ---------------------------------------------------------------------------

/// Returns an iterator over all setting names, in definition order.
pub fn setting_names() -> impl Iterator<Item = &'static str> {
    settings::SETTINGS.iter().map(|(name, _)| *name)
}

/// Get the description for a setting.
pub fn setting_description(setting_name: &str) -> &'static str {
    find_setting(setting_name)
        .map(|d| d.description)
        .unwrap_or("")
}

// ---------------------------------------------------------------------------
// Level 1 unit tests — CLI-unreachable behavior
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use test_case::test_case;


    #[test_case("true", Ok(true) ; "literal_true")]
    #[test_case("false", Ok(false) ; "literal_false")]
    #[test_case("on", Ok(true) ; "on")]
    #[test_case("off", Ok(false) ; "off")]
    #[test_case("yes", Ok(true) ; "yes")]
    #[test_case("no", Ok(false) ; "no")]
    #[test_case("1", Ok(true) ; "one")]
    #[test_case("0", Ok(false) ; "zero")]
    #[test_case("TrUe", Ok(true) ; "mixed case true")]
    #[test_case("FaLsE", Ok(false) ; "mixed case false")]
    fn str2bool_valid(input: &str, expected: Result<bool, ()>) {
        let result = str2bool(input);
        match expected {
            Ok(val) => assert_eq!(result.unwrap(), val),
            Err(_) => unreachable!(),
        }
    }

    #[test]
    fn str2bool_not_boolean_returns_error() {
        assert!(str2bool("not_boolean").is_err());
    }

    #[test]
    fn str2bool_empty_returns_error() {
        assert!(str2bool("").is_err());
    }

    #[test]
    fn str2bool_two_returns_error() {
        assert!(str2bool("2").is_err());
    }

    #[test]
    fn str2bool_maybe_returns_error() {
        assert!(str2bool("maybe").is_err());
    }

    // These tests must be serial because they mutate process-wide env vars.

    #[test]
    #[serial]
    fn get_config_file_path_default_no_env() {
        let _guard = EnvGuard::new(CONFIG_FILE_PATH_ENV_VAR);
        unsafe { std::env::remove_var(CONFIG_FILE_PATH_ENV_VAR) };
        let path = get_config_file_path();
        assert!(
            path.to_str().unwrap().ends_with(".deadline/config"),
            "expected path ending with .deadline/config, got: {path:?}"
        );
    }

    #[test]
    #[serial]
    fn get_config_file_path_env_override() {
        let _guard = EnvGuard::new(CONFIG_FILE_PATH_ENV_VAR);
        unsafe { std::env::set_var(CONFIG_FILE_PATH_ENV_VAR, "/tmp/my_config") };
        assert_eq!(get_config_file_path(), PathBuf::from("/tmp/my_config"));
    }

    #[test]
    #[serial]
    fn get_config_file_path_env_tilde() {
        let _guard = EnvGuard::new(CONFIG_FILE_PATH_ENV_VAR);
        unsafe { std::env::set_var(CONFIG_FILE_PATH_ENV_VAR, "~/custom/config") };
        let path = get_config_file_path();
        assert!(
            !path.to_str().unwrap().starts_with('~'),
            "tilde should be expanded, got: {path:?}"
        );
        assert!(path.to_str().unwrap().ends_with("custom/config"));
    }

    #[test]
    #[serial]
    fn get_config_file_path_env_empty_falls_back() {
        let _guard = EnvGuard::new(CONFIG_FILE_PATH_ENV_VAR);
        unsafe { std::env::set_var(CONFIG_FILE_PATH_ENV_VAR, "") };
        let path = get_config_file_path();
        assert!(path.to_str().unwrap().ends_with(".deadline/config"));
    }

    #[test]
    #[serial]
    fn get_config_file_path_env_unset_mid_session() {
        let _guard = EnvGuard::new(CONFIG_FILE_PATH_ENV_VAR);
        unsafe { std::env::set_var(CONFIG_FILE_PATH_ENV_VAR, "/tmp/custom") };
        assert_eq!(get_config_file_path(), PathBuf::from("/tmp/custom"));
        unsafe { std::env::remove_var(CONFIG_FILE_PATH_ENV_VAR) };
        let path = get_config_file_path();
        assert!(path.to_str().unwrap().ends_with(".deadline/config"));
    }


    #[test]
    fn read_config_from_valid_ini() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "[defaults]\naws_profile_name = test\n").unwrap();

        let config = read_config_from(&path).unwrap();
        assert_eq!(config.get("defaults", "aws_profile_name"), Some("test"));
    }

    #[test]
    fn read_config_from_missing_file_returns_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nonexistent");

        let config = read_config_from(&path).unwrap();
        assert_eq!(config.get("defaults", "aws_profile_name"), None);
    }

    #[test]
    fn read_config_from_empty_file_returns_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "").unwrap();

        let config = read_config_from(&path).unwrap();
        assert_eq!(config.get("defaults", "aws_profile_name"), None);
    }

    #[test]
    fn read_config_from_malformed_ini_returns_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");
        std::fs::write(&path, "key_without_section = bad\n").unwrap();

        assert!(read_config_from(&path).is_err());
    }


    #[test]
    fn write_config_to_writes_content() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");

        let mut config = IniConfig::new();
        config.set("defaults", "aws_profile_name", "test");
        write_config_to(&config, &path).unwrap();

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("aws_profile_name = test"));
    }

    #[test]
    fn write_config_to_creates_parent_dirs() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("subdir").join("nested").join("config");

        let config = IniConfig::new();
        write_config_to(&config, &path).unwrap();

        assert!(path.exists());
    }

    #[test]
    fn write_config_to_existing_parent_no_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");

        let config = IniConfig::new();
        write_config_to(&config, &path).unwrap();
        // Write again — parent already exists
        write_config_to(&config, &path).unwrap();

        assert!(path.exists());
    }

    #[cfg(unix)]
    #[test]
    fn write_config_to_sets_permissions_600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config");

        let config = IniConfig::new();
        write_config_to(&config, &path).unwrap();

        let perms = std::fs::metadata(&path).unwrap().permissions();
        assert_eq!(perms.mode() & 0o777, 0o600);
    }

    // -- get_setting_default --

    #[test]
    fn get_setting_default_aws_profile_name() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting_default("defaults.aws_profile_name", &config).unwrap(),
            "(default)"
        );
    }

    #[test]
    fn get_setting_default_auto_accept() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting_default("settings.auto_accept", &config).unwrap(),
            "false"
        );
    }

    #[test]
    fn get_setting_default_conflict_resolution() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting_default("settings.conflict_resolution", &config).unwrap(),
            "NOT_SELECTED"
        );
    }

    #[test]
    fn get_setting_default_log_level() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting_default("settings.log_level", &config).unwrap(),
            "WARNING"
        );
    }

    #[test]
    fn get_setting_default_job_history_dir_substitutes_profile() {
        let config = IniConfig::new();
        let val = get_setting_default("settings.job_history_dir", &config).unwrap();
        assert!(
            val.contains("(default)"),
            "should substitute aws_profile_name into default: {val}"
        );
    }

    #[test]
    fn get_setting_default_nonexistent_returns_error() {
        let config = IniConfig::new();
        assert!(get_setting_default("settings.nonexistent", &config).is_err());
    }

    // -- hierarchical get/set --

    #[test]
    fn get_farm_id_default_profile_returns_empty() {
        // farm_id under default profile "(default)" → empty default
        let config = IniConfig::new();
        let val = get_setting("defaults.farm_id", &config).unwrap();
        assert_eq!(val, "");
    }

    #[test]
    fn set_and_get_farm_id_under_default_profile() {
        let mut config = IniConfig::new();
        set_setting("defaults.farm_id", "farm-123", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.farm_id", &config).unwrap(),
            "farm-123"
        );
    }

    #[test]
    fn set_and_get_farm_id_under_custom_profile() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "MyProfile", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-abc", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.farm_id", &config).unwrap(),
            "farm-abc"
        );
    }

    #[test]
    fn get_queue_id_with_profile_and_farm_set() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "P1", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-1", &mut config).unwrap();
        set_setting("defaults.queue_id", "queue-1", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.queue_id", &config).unwrap(),
            "queue-1"
        );
    }

    #[test]
    fn get_job_id_with_full_hierarchy() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "P1", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-1", &mut config).unwrap();
        set_setting("defaults.queue_id", "queue-1", &mut config).unwrap();
        set_setting("defaults.job_id", "job-1", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.job_id", &config).unwrap(),
            "job-1"
        );
    }

    #[test]
    fn get_storage_profile_id_farm_scoped() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "P1", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-1", &mut config).unwrap();
        set_setting("settings.storage_profile_id", "sp-1", &mut config).unwrap();
        assert_eq!(
            get_setting("settings.storage_profile_id", &config).unwrap(),
            "sp-1"
        );
    }

    #[test]
    fn get_job_history_dir_with_custom_profile() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "MyProfile", &mut config).unwrap();
        let val = get_setting("settings.job_history_dir", &config).unwrap();
        assert!(
            val.contains("MyProfile"),
            "should substitute profile name: {val}"
        );
    }

    #[test]
    fn switch_profile_isolates_farm_id() {
        let mut config = IniConfig::new();
        // Set farm under profile A
        set_setting("defaults.aws_profile_name", "ProfileA", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-A", &mut config).unwrap();
        // Switch to profile B
        set_setting("defaults.aws_profile_name", "ProfileB", &mut config).unwrap();
        // farm_id should be empty under profile B
        assert_eq!(
            get_setting("defaults.farm_id", &config).unwrap(),
            ""
        );
    }

    #[test]
    fn switch_farm_isolates_queue_id() {
        let mut config = IniConfig::new();
        set_setting("defaults.farm_id", "farm-X", &mut config).unwrap();
        set_setting("defaults.queue_id", "queue-X", &mut config).unwrap();
        // Switch farm
        set_setting("defaults.farm_id", "farm-Y", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.queue_id", &config).unwrap(),
            ""
        );
    }

    #[test]
    fn set_with_explicit_config_does_not_write_disk() {
        let mut config = IniConfig::new();
        set_setting("defaults.farm_id", "farm-mem", &mut config).unwrap();
        // Value is in memory
        assert_eq!(
            get_setting("defaults.farm_id", &config).unwrap(),
            "farm-mem"
        );
        // No file was written — this is an in-memory-only operation
    }

    #[test]
    fn set_creates_new_section() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "NewProf", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-new", &mut config).unwrap();
        // The section "profile-NewProf defaults" should exist
        assert_eq!(config.get("profile-NewProf defaults", "farm_id"), Some("farm-new"));
    }

    #[test]
    fn clear_reverts_to_default() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "Prof", &mut config).unwrap();
        clear_setting("defaults.aws_profile_name", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.aws_profile_name", &config).unwrap(),
            "(default)"
        );
    }

    #[test]
    fn clear_farm_scoped_setting() {
        let mut config = IniConfig::new();
        set_setting("defaults.farm_id", "farm-1", &mut config).unwrap();
        set_setting("settings.storage_profile_id", "sp-1", &mut config).unwrap();
        clear_setting("settings.storage_profile_id", &mut config).unwrap();
        assert_eq!(
            get_setting("settings.storage_profile_id", &config).unwrap(),
            ""
        );
    }

    #[test]
    fn clear_with_explicit_config_does_not_write_disk() {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", "X", &mut config).unwrap();
        clear_setting("defaults.aws_profile_name", &mut config).unwrap();
        // Just verifying it doesn't panic — no disk write
        assert_eq!(
            get_setting("defaults.aws_profile_name", &config).unwrap(),
            "(default)"
        );
    }

    #[test]
    fn hierarchy_switch_profile_restores_previous_values() {
        let mut config = IniConfig::new();
        // Set up profile A with farm
        set_setting("defaults.aws_profile_name", "A", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-A", &mut config).unwrap();
        // Switch to B
        set_setting("defaults.aws_profile_name", "B", &mut config).unwrap();
        set_setting("defaults.farm_id", "farm-B", &mut config).unwrap();
        // Switch back to A
        set_setting("defaults.aws_profile_name", "A", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.farm_id", &config).unwrap(),
            "farm-A"
        );
    }

    #[test]
    fn hierarchy_clear_farm_reverts_queue_scope() {
        let mut config = IniConfig::new();
        set_setting("defaults.farm_id", "farm-1", &mut config).unwrap();
        set_setting("defaults.queue_id", "queue-1", &mut config).unwrap();
        // Clear farm → queue should now resolve under default empty farm
        clear_setting("defaults.farm_id", &mut config).unwrap();
        assert_eq!(
            get_setting("defaults.queue_id", &config).unwrap(),
            ""
        );
    }

    #[test]
    fn get_setting_no_dot_returns_error() {
        let config = IniConfig::new();
        let err = get_setting("bad_name", &config).unwrap_err();
        assert!(err.to_string().contains("is not valid"));
    }

    #[test]
    fn get_setting_unknown_name_returns_error() {
        let config = IniConfig::new();
        let err = get_setting("settings.nonexistent", &config).unwrap_err();
        assert!(err.to_string().contains("has no setting"));
    }

    #[test]
    fn set_setting_no_dot_returns_error() {
        let mut config = IniConfig::new();
        assert!(set_setting("bad", "val", &mut config).is_err());
    }

    #[test]
    fn set_setting_unknown_name_returns_error() {
        let mut config = IniConfig::new();
        assert!(set_setting("settings.fake", "val", &mut config).is_err());
    }

    #[test]
    fn clear_setting_no_dot_returns_error() {
        let mut config = IniConfig::new();
        assert!(clear_setting("bad", &mut config).is_err());
    }

    #[test]
    fn clear_setting_unknown_name_returns_error() {
        let mut config = IniConfig::new();
        assert!(clear_setting("settings.fake", &mut config).is_err());
    }

    // -- get_best_profile_for_farm --

    /// Helper: build a config with profiles and their farm/queue assignments.
    fn config_with_profiles(
        default_profile: &str,
        assignments: &[(&str, &str, &str)], // (profile, farm_id, queue_id)
    ) -> IniConfig {
        let mut config = IniConfig::new();
        set_setting("defaults.aws_profile_name", default_profile, &mut config).unwrap();

        for &(profile, farm, queue) in assignments {
            set_setting("defaults.aws_profile_name", profile, &mut config).unwrap();
            if !farm.is_empty() {
                set_setting("defaults.farm_id", farm, &mut config).unwrap();
            }
            if !queue.is_empty() {
                set_setting("defaults.queue_id", queue, &mut config).unwrap();
            }
        }

        // Restore default profile
        set_setting("defaults.aws_profile_name", default_profile, &mut config).unwrap();
        config
    }

    #[test]
    fn best_profile_default_farm_matches() {
        let config = config_with_profiles("ProfA", &[("ProfA", "farm-1", "")]);
        let profiles = ["ProfA", "ProfB"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", None),
            "ProfA"
        );
    }

    #[test]
    fn best_profile_other_matches_farm_and_queue() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-X", ""), ("Match", "farm-1", "queue-1")],
        );
        let profiles = ["Default", "Match"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", Some("queue-1")),
            "Match"
        );
    }

    #[test]
    fn best_profile_farm_only_match() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-X", ""), ("FarmMatch", "farm-1", "queue-other")],
        );
        let profiles = ["Default", "FarmMatch"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", Some("queue-1")),
            "FarmMatch"
        );
    }

    #[test]
    fn best_profile_no_match_returns_default() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-X", ""), ("Other", "farm-Y", "")],
        );
        let profiles = ["Default", "Other"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-missing", None),
            "Default"
        );
    }

    #[test]
    fn best_profile_exact_match_beats_farm_only() {
        let config = config_with_profiles(
            "Default",
            &[
                ("Default", "farm-X", ""),
                ("FarmOnly", "farm-1", "queue-other"),
                ("Exact", "farm-1", "queue-1"),
            ],
        );
        let profiles = ["Default", "FarmOnly", "Exact"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", Some("queue-1")),
            "Exact"
        );
    }

    #[test]
    fn best_profile_default_farm_match_wins_over_other() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-1", ""), ("Other", "farm-1", "")],
        );
        let profiles = ["Default", "Other"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", None),
            "Default"
        );
    }

    #[test]
    fn best_profile_no_queue_id_skips_queue_check() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-X", ""), ("Match", "farm-1", "queue-1")],
        );
        let profiles = ["Default", "Match"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", None),
            "Match"
        );
    }

    #[test]
    fn best_profile_empty_queue_id_treated_as_none() {
        let config = config_with_profiles(
            "Default",
            &[("Default", "farm-X", ""), ("Match", "farm-1", "queue-1")],
        );
        let profiles = ["Default", "Match"];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", Some("")),
            "Match"
        );
    }

    #[test]
    fn best_profile_does_not_modify_default_setting() {
        let config = config_with_profiles(
            "Original",
            &[("Original", "farm-X", ""), ("Other", "farm-1", "")],
        );
        let profiles = ["Original", "Other"];
        let _ = get_best_profile_for_farm(&config, &profiles, "farm-1", None);
        // Default profile should be unchanged
        assert_eq!(
            get_setting("defaults.aws_profile_name", &config).unwrap(),
            "Original"
        );
    }

    #[test]
    fn best_profile_empty_profile_list_returns_default() {
        let config = config_with_profiles("Default", &[("Default", "farm-X", "")]);
        let profiles: [&str; 0] = [];
        assert_eq!(
            get_best_profile_for_farm(&config, &profiles, "farm-1", None),
            "Default"
        );
    }

    /// Saves and restores an env var when dropped, preventing test interference.
    /// Only needed for the get_config_file_path tests that genuinely test env var behavior.
    struct EnvGuard {
        key: String,
        original: Option<String>,
    }

    impl EnvGuard {
        fn new(key: &str) -> Self {
            let original = std::env::var(key).ok();
            Self {
                key: key.to_string(),
                original,
            }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.original {
                Some(val) => unsafe { std::env::set_var(&self.key, val) },
                None => unsafe { std::env::remove_var(&self.key) },
            }
        }
    }

    // --- New config settings for submission defaults ---

    #[test]
    fn max_retries_per_task_setting_exists_with_default() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting("settings.max_retries_per_task", &config).unwrap(),
            "5"
        );
    }

    #[test]
    fn max_failed_tasks_count_setting_exists_with_default() {
        let config = IniConfig::new();
        assert_eq!(
            get_setting("settings.max_failed_tasks_count", &config).unwrap(),
            "20"
        );
    }
}
