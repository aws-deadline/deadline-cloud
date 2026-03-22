// Configuration file management for ~/.deadline/config
//
// INI config file read/write with hierarchical section naming,
// atomic writes, and environment variable override for file path.
//
// Design: The primary API operates on IniConfig values passed explicitly.
// CLI commands call read_config_from() once at startup, apply CLI flag
// overrides in memory via set_setting_in_config(), then thread the
// IniConfig through all downstream calls via get_setting_with_config().
// The free functions (get_setting, set_setting, etc.) are convenience
// wrappers that read from / write to the default config file path.

use crate::ini::{IniConfig, IniParseError};
use crate::settings::{self, SettingDef, SETTINGS, find_setting};
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
        let profile = get_setting_with_config("defaults.aws_profile_name", config)
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
            "The setting name {setting_name:?} is not valid."
        )));
    }
    find_setting(setting_name).ok_or_else(|| {
        let section = setting_name.split('.').next().unwrap();
        let known_sections: Vec<&str> = SETTINGS
            .iter()
            .map(|(n, _)| n.split('.').next().unwrap())
            .collect();
        if known_sections.contains(&section) {
            ConfigError::InvalidSettingName(format!(
                "AWS Deadline Cloud configuration section {section:?} has no setting named {setting_name:?}."
            ))
        } else {
            ConfigError::InvalidSettingName(format!(
                "AWS Deadline Cloud configuration has no setting named {setting_name:?}."
            ))
        }
    })
}

/// Get a setting value using an already-loaded config.
/// This is the primary API — most callers should use this.
pub fn get_setting_with_config(
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
pub fn get_setting(setting_name: &str) -> Result<String, ConfigError> {
    let config = read_config()?;
    get_setting_with_config(setting_name, &config)
}

/// Get the default value using an already-loaded config.
pub fn get_setting_default_with_config(
    setting_name: &str,
    config: &IniConfig,
) -> Result<String, ConfigError> {
    let setting_def = validate_setting(setting_name)?;
    Ok(resolve_default(setting_def, config))
}

/// Convenience: read from disk, then get the default.
pub fn get_setting_default(setting_name: &str) -> Result<String, ConfigError> {
    let config = read_config()?;
    get_setting_default_with_config(setting_name, &config)
}

/// Set a setting value in a config object without writing to disk.
/// This is the primary API — most callers should use this.
pub fn set_setting_in_config(
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
pub fn set_setting(setting_name: &str, value: &str) -> Result<(), ConfigError> {
    let mut config = read_config()?;
    set_setting_in_config(setting_name, value, &mut config)?;
    write_config(&config)?;
    Ok(())
}

/// Clear a setting in a config object without writing to disk.
pub fn clear_setting_in_config(
    setting_name: &str,
    config: &mut IniConfig,
) -> Result<(), ConfigError> {
    let default = get_setting_default_with_config(setting_name, config)?;
    set_setting_in_config(setting_name, &default, config)
}

/// Convenience: read from disk, clear the setting, write back.
pub fn clear_setting(setting_name: &str) -> Result<(), ConfigError> {
    let default = get_setting_default(setting_name)?;
    set_setting(setting_name, &default)
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
}
