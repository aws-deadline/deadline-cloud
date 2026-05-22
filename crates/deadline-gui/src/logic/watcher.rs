//! File watcher logic for credential/config change detection.
//!
//! Determines which paths to watch and provides a callback-based
//! interface for triggering auth refresh on file changes.

use std::path::PathBuf;

/// Returns the directories that should be watched for credential changes.
pub fn watch_paths() -> Vec<PathBuf> {
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Vec::new(),
    };
    vec![home.join(".aws"), home.join(".deadline")]
}

/// Determine if a changed path should trigger an auth refresh.
/// Returns true for changes in or under any of the watched directories.
pub fn should_trigger_refresh(changed_path: &std::path::Path, watch_paths: &[PathBuf]) -> bool {
    watch_paths.iter().any(|wp| changed_path.starts_with(wp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_paths_includes_aws_dir() {
        let paths = watch_paths();
        let aws_path = dirs::home_dir().unwrap().join(".aws");
        assert!(paths.contains(&aws_path));
    }

    #[test]
    fn watch_paths_includes_deadline_dir() {
        let paths = watch_paths();
        let deadline_path = dirs::home_dir().unwrap().join(".deadline");
        assert!(paths.contains(&deadline_path));
    }

    #[test]
    fn should_trigger_refresh_aws_config_change() {
        let home = dirs::home_dir().unwrap();
        let watch = vec![home.join(".aws"), home.join(".deadline")];
        let changed = home.join(".aws").join("credentials");
        assert!(should_trigger_refresh(&changed, &watch));
    }

    #[test]
    fn should_trigger_refresh_deadline_config_change() {
        let home = dirs::home_dir().unwrap();
        let watch = vec![home.join(".aws"), home.join(".deadline")];
        let changed = home.join(".deadline").join("config");
        assert!(should_trigger_refresh(&changed, &watch));
    }

    #[test]
    fn should_trigger_refresh_unrelated_path_false() {
        let home = dirs::home_dir().unwrap();
        let watch = vec![home.join(".aws"), home.join(".deadline")];
        let changed = home.join("Documents").join("file.txt");
        assert!(!should_trigger_refresh(&changed, &watch));
    }
}
