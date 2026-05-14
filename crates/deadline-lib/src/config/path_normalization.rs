//! Level 1 tests for Windows config path normalization (#21f).
//!
//! On Windows, paths stored in the config file use forward slashes to prevent
//! corruption by tools (like Deadline Cloud Monitor) that interpret backslashes
//! as escape characters. When reading, forward slashes are converted back to
//! native backslashes.
//!
//! `normalize_path_for_config` and `normalize_path_from_config` are conditional
//! on `cfg!(windows)` — they are no-ops on Linux/macOS, matching Python's
//! `os.name == "nt"` guard.

#[cfg(test)]
mod tests {
    use crate::config::config_file::{normalize_path_for_config, normalize_path_from_config};

    // =========================================================================
    // normalize_path_for_config — writing paths (backslash → forward slash)
    // Only converts on Windows; no-op on other platforms.
    // =========================================================================

    #[test]
    #[cfg(windows)]
    fn for_config_converts_backslashes_to_forward_slashes() {
        assert_eq!(
            normalize_path_for_config(r"C:\Users\artist\.deadline\config"),
            "C:/Users/artist/.deadline/config"
        );
    }

    #[test]
    #[cfg(windows)]
    fn for_config_unc_path() {
        assert_eq!(
            normalize_path_for_config(r"\\server\share\folder"),
            "//server/share/folder"
        );
    }

    #[test]
    fn for_config_preserves_forward_slashes() {
        assert_eq!(
            normalize_path_for_config("/home/user/.deadline/config"),
            "/home/user/.deadline/config"
        );
    }

    #[test]
    fn for_config_empty_string() {
        assert_eq!(normalize_path_for_config(""), "");
    }

    // =========================================================================
    // normalize_path_from_config — reading paths (forward slash → backslash)
    // Only converts on Windows; no-op on other platforms.
    // =========================================================================

    #[test]
    #[cfg(windows)]
    fn from_config_converts_forward_slashes_to_backslashes() {
        assert_eq!(
            normalize_path_from_config("C:/Users/artist/.deadline/config"),
            r"C:\Users\artist\.deadline\config"
        );
    }

    #[test]
    #[cfg(windows)]
    fn from_config_unc_path() {
        assert_eq!(
            normalize_path_from_config("//server/share/folder"),
            r"\\server\share\folder"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn from_config_is_noop_on_unix() {
        // Forward slashes are native on Unix — no conversion
        assert_eq!(
            normalize_path_from_config("/home/user/.deadline/config"),
            "/home/user/.deadline/config"
        );
    }

    #[test]
    #[cfg(not(windows))]
    fn for_config_is_noop_on_unix() {
        // Backslashes are not path separators on Unix — no conversion
        assert_eq!(
            normalize_path_for_config(r"path\with\backslashes"),
            r"path\with\backslashes"
        );
    }

    #[test]
    fn from_config_empty_string() {
        assert_eq!(normalize_path_from_config(""), "");
    }

    // =========================================================================
    // Integration: get_setting applies normalization for is_path settings
    // =========================================================================

    #[test]
    fn get_setting_returns_path_setting_unchanged_on_unix_or_converted_on_windows() {
        use crate::config::config_file::get_setting;
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        // Directly write a forward-slash path into the config (as stored on disk).
        // deadline-cloud-monitor.path → section "deadline-cloud-monitor", key "path"
        config.set(
            "deadline-cloud-monitor",
            "path",
            "C:/Program Files/DCM/monitor.exe",
        );

        let val = get_setting("deadline-cloud-monitor.path", &config).unwrap();
        if cfg!(windows) {
            assert_eq!(val, r"C:\Program Files\DCM\monitor.exe");
        } else {
            assert_eq!(val, "C:/Program Files/DCM/monitor.exe");
        }
    }

    #[test]
    fn set_setting_stores_path_with_forward_slashes_on_windows() {
        use crate::config::config_file::set_setting;
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        set_setting(
            "deadline-cloud-monitor.path",
            r"C:\Program Files\DCM\monitor.exe",
            &mut config,
        )
        .unwrap();

        let raw = config.get("deadline-cloud-monitor", "path").unwrap_or("");
        if cfg!(windows) {
            assert_eq!(raw, "C:/Program Files/DCM/monitor.exe");
        } else {
            // On non-Windows, no conversion — backslashes preserved as-is
            assert_eq!(raw, r"C:\Program Files\DCM\monitor.exe");
        }
    }

    #[test]
    fn get_setting_normalizes_path_list_setting() {
        use crate::config::config_file::get_setting;
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        let stored = if cfg!(windows) {
            "C:/art/textures;D:/projects/renders"
        } else {
            "/art/textures:/projects/renders"
        };
        config.set("settings", "known_asset_paths", stored);

        let val = get_setting("settings.known_asset_paths", &config).unwrap();
        if cfg!(windows) {
            assert_eq!(val, r"C:\art\textures;D:\projects\renders");
        } else {
            assert_eq!(val, "/art/textures:/projects/renders");
        }
    }

    #[test]
    fn set_setting_normalizes_path_list_on_windows() {
        use crate::config::config_file::set_setting;
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        let input = if cfg!(windows) {
            r"C:\art\textures;D:\projects\renders"
        } else {
            "/art/textures:/projects/renders"
        };
        set_setting("settings.known_asset_paths", input, &mut config).unwrap();

        let raw = config.get("settings", "known_asset_paths").unwrap_or("");
        if cfg!(windows) {
            assert_eq!(raw, "C:/art/textures;D:/projects/renders");
        } else {
            assert_eq!(raw, "/art/textures:/projects/renders");
        }
    }

    #[test]
    fn non_path_setting_not_modified() {
        use crate::config::config_file::{get_setting, set_setting};
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        // aws_profile_name is NOT a path setting — slashes should never be modified
        set_setting("defaults.aws_profile_name", r"my/profile", &mut config).unwrap();
        let val = get_setting("defaults.aws_profile_name", &config).unwrap();
        assert_eq!(val, r"my/profile");
    }

    #[test]
    fn path_setting_roundtrips_correctly() {
        use crate::config::config_file::{get_setting, set_setting};
        use crate::config::ini::IniConfig;

        let mut config = IniConfig::new();
        let original = if cfg!(windows) {
            r"C:\Program Files\AWS\DeadlineCloudMonitor\monitor.exe"
        } else {
            "/usr/local/bin/DeadlineCloudMonitor"
        };
        set_setting("deadline-cloud-monitor.path", original, &mut config).unwrap();
        let result = get_setting("deadline-cloud-monitor.path", &config).unwrap();
        // Roundtrip must return the original native path
        assert_eq!(result, original);
    }
}
