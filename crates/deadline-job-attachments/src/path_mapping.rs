// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

//! Path mapping rules: generate rules from storage profiles and apply them
//! via a trie-based longest-prefix matcher.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::errors::JobAttachmentsError;
use crate::models::{
    PathFormat, PathMappingRule, StorageProfile, StorageProfileOperatingSystemFamily,
};

/// Generate path mapping rules from source and destination storage profiles.
/// Returns one rule per matching file system location name. Returns empty
/// if both profiles have the same `storage_profile_id`.
pub fn generate_path_mapping_rules(
    source: &StorageProfile,
    destination: &StorageProfile,
) -> Vec<PathMappingRule> {
    if source.storage_profile_id == destination.storage_profile_id {
        return vec![];
    }

    let dest_locations: HashMap<&str, &str> = destination
        .file_system_locations
        .iter()
        .map(|loc| (loc.name.as_str(), loc.path.as_str()))
        .collect();

    let format = match source.os_family {
        StorageProfileOperatingSystemFamily::Windows => PathFormat::Windows.as_str(),
        _ => PathFormat::Posix.as_str(),
    };

    source
        .file_system_locations
        .iter()
        .filter_map(|loc| {
            dest_locations.get(loc.name.as_str()).map(|dest_path| PathMappingRule {
                source_path_format: format.to_string(),
                source_path: loc.path.clone(),
                destination_path: dest_path.to_string(),
            })
        })
        .collect()
}

/// Trie-based path mapper that selects the most specific (longest) matching
/// rule. Windows source paths are matched case-insensitively.
#[derive(Debug)]
pub struct PathMappingRuleApplier {
    pub source_path_format: Option<String>,
    pub path_mapping_rules: Vec<PathMappingRule>,
    trie: HashMap<String, TrieNode>,
    split_and_normalize: SplitMode,
}

#[derive(Clone, Debug)]
enum SplitMode {
    Posix,
    Windows,
    None,
}

#[derive(Default, Debug)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    destination: Option<PathBuf>,
}

impl PathMappingRuleApplier {
    /// Build a new applier from a list of rules. All rules must share the
    /// same `source_path_format`. Returns error if formats are mixed or
    /// unrecognized.
    pub fn new(rules: Vec<PathMappingRule>) -> Result<Self, JobAttachmentsError> {
        if rules.is_empty() {
            return Ok(Self {
                source_path_format: None,
                path_mapping_rules: rules,
                trie: HashMap::new(),
                split_and_normalize: SplitMode::None,
            });
        }

        let format = &rules[0].source_path_format;
        if !rules.iter().all(|r| r.source_path_format == *format) {
            let mut formats: Vec<&str> =
                rules.iter().map(|r| r.source_path_format.as_str()).collect();
            formats.sort();
            formats.dedup();
            return Err(JobAttachmentsError::AssetSync(format!(
                "The path mapping rules included multiple source path formats {}, only one is permitted.",
                formats.join(", ")
            )));
        }

        let mode = match format.as_str() {
            "posix" => SplitMode::Posix,
            "windows" => SplitMode::Windows,
            other => {
                return Err(JobAttachmentsError::AssetSync(format!(
                    "Unexpected source path format {other}"
                )))
            }
        };

        let mut root_children: HashMap<String, TrieNode> = HashMap::new();
        for rule in &rules {
            let parts = split_path(&mode, &rule.source_path);
            insert_into_trie(&mut root_children, &parts, &mode, &rule.destination_path);
        }

        Ok(Self {
            source_path_format: Some(format.clone()),
            path_mapping_rules: rules,
            trie: root_children,
            split_and_normalize: mode,
        })
    }

    /// Internal: attempt to transform, returning None if no rule matches.
    fn try_transform(&self, source_path: &str) -> Option<PathBuf> {
        if source_path.is_empty() {
            return None;
        }
        let parts = split_path(&self.split_and_normalize, source_path);
        if parts.is_empty() {
            return None;
        }

        let mut matched_destination: Option<&PathBuf> = None;
        let mut matched_remaining_start: usize = 0;

        let mut current_children = &self.trie;
        for (i, part) in parts.iter().enumerate() {
            let key = normalize_part(&self.split_and_normalize, part);
            match current_children.get(&key) {
                Some(node) => {
                    if node.destination.is_some() {
                        matched_destination = node.destination.as_ref();
                        matched_remaining_start = i + 1;
                    }
                    current_children = &node.children;
                }
                None => break,
            }
        }

        matched_destination.map(|dest| {
            let remaining = &parts[matched_remaining_start..];
            if remaining.is_empty() {
                dest.clone()
            } else {
                let mut result = dest.clone();
                for part in remaining {
                    result.push(part);
                }
                result
            }
        })
    }

    /// Transform `source_path` using the most specific matching rule.
    /// Returns the original path unchanged if no rule matches.
    pub fn transform(&self, source_path: &str) -> String {
        if self.source_path_format.is_none() {
            return source_path.to_string();
        }
        match self.try_transform(source_path) {
            Some(path) => path.to_string_lossy().into_owned(),
            None => source_path.to_string(),
        }
    }

    /// Transform `source_path` using the most specific matching rule.
    /// Returns error if no rule matches.
    pub fn strict_transform(&self, source_path: &str) -> Result<PathBuf, JobAttachmentsError> {
        if self.source_path_format.is_some() {
            if let Some(result) = self.try_transform(source_path) {
                return Ok(result);
            }
        }
        Err(JobAttachmentsError::AssetSync(
            "No path mapping rule could be applied".to_string(),
        ))
    }
}

/// Split a path into parts matching Python's PurePosixPath.parts / PureWindowsPath.parts.
fn split_path(mode: &SplitMode, path: &str) -> Vec<String> {
    match mode {
        SplitMode::Posix => {
            if path.is_empty() {
                return vec![];
            }
            let mut parts = Vec::new();
            if path.starts_with('/') {
                parts.push("/".to_string());
            }
            for component in path.split('/').filter(|s| !s.is_empty()) {
                parts.push(component.to_string());
            }
            parts
        }
        SplitMode::Windows => {
            if path.is_empty() {
                return vec![];
            }
            let mut parts = Vec::new();
            // Split on backslash
            let components: Vec<&str> = path.split('\\').collect();
            if components.len() >= 2 {
                // First component is drive letter (e.g. "C"), add trailing backslash
                parts.push(format!("{}\\", components[0]));
                for c in &components[1..] {
                    if !c.is_empty() {
                        parts.push(c.to_string());
                    }
                }
            } else {
                // Fallback: treat as single component
                parts.push(path.to_string());
            }
            parts
        }
        SplitMode::None => vec![],
    }
}

/// Normalize a trie key: lowercase for Windows, identity for POSIX.
fn normalize_part(mode: &SplitMode, part: &str) -> String {
    match mode {
        SplitMode::Windows => part.to_lowercase(),
        _ => part.to_string(),
    }
}

/// Insert a rule's destination into the trie at the path given by parts.
fn insert_into_trie(
    children: &mut HashMap<String, TrieNode>,
    parts: &[String],
    mode: &SplitMode,
    destination: &str,
) {
    if parts.is_empty() {
        return;
    }
    let key = normalize_part(mode, &parts[0]);
    let node = children.entry(key).or_default();
    if parts.len() == 1 {
        node.destination = Some(PathBuf::from(destination));
    } else {
        insert_into_trie(&mut node.children, &parts[1..], mode, destination);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        FileSystemLocation, FileSystemLocationType, PathFormat, StorageProfile,
        StorageProfileOperatingSystemFamily,
    };

    // --- Helper factories ---

    fn linux_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_string(),
            display_name: id.to_string(),
            os_family: StorageProfileOperatingSystemFamily::Linux,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_string(),
                    path: path.to_string(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn macos_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_string(),
            display_name: id.to_string(),
            os_family: StorageProfileOperatingSystemFamily::Macos,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_string(),
                    path: path.to_string(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn windows_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_string(),
            display_name: id.to_string(),
            os_family: StorageProfileOperatingSystemFamily::Windows,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_string(),
                    path: path.to_string(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn rule(fmt: &str, src: &str, dst: &str) -> PathMappingRule {
        PathMappingRule {
            source_path_format: fmt.to_string(),
            source_path: src.to_string(),
            destination_path: dst.to_string(),
        }
    }

    // ===================================================================
    // generate_path_mapping_rules tests
    // ===================================================================

    #[test]
    fn generate_rules_matching_names_returns_rules() {
        // matching location names produce one rule each
        let src = linux_profile("sp-1", vec![("shared", "/mnt/shared"), ("temp", "/tmp")]);
        let dst = linux_profile("sp-2", vec![("shared", "/opt/shared"), ("temp", "/var/tmp")]);
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 2);
        assert!(rules.contains(&rule("posix", "/mnt/shared", "/opt/shared")));
        assert!(rules.contains(&rule("posix", "/tmp", "/var/tmp")));
    }

    #[test]
    fn generate_rules_same_profile_returns_empty() {
        // same storageProfileId → empty
        let src = linux_profile("sp-same", vec![("shared", "/mnt/shared")]);
        let dst = linux_profile("sp-same", vec![("shared", "/opt/shared")]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    #[test]
    fn generate_rules_source_only_locations_no_rules() {
        // source locations not in destination produce no rules
        let src = linux_profile("sp-1", vec![("only_in_src", "/mnt/src")]);
        let dst = linux_profile("sp-2", vec![("only_in_dst", "/mnt/dst")]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    #[test]
    fn generate_rules_dest_only_locations_no_rules() {
        // destination locations not in source produce no rules
        let src = linux_profile("sp-1", vec![("a", "/a")]);
        let dst = linux_profile("sp-2", vec![("a", "/a2"), ("extra", "/extra")]);
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 1);
        assert!(rules.contains(&rule("posix", "/a", "/a2")));
    }

    #[test]
    fn generate_rules_windows_source_uses_windows_format() {
        // Windows source → WINDOWS format
        let src = windows_profile("sp-w", vec![("shared", "C:\\shared"), ("temp", "C:\\temp")]);
        let dst = windows_profile("sp-w2", vec![("shared", "D:\\shared"), ("temp", "D:\\temp")]);
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 2);
        for r in &rules {
            assert_eq!(r.source_path_format, "windows");
        }
    }

    #[test]
    fn generate_rules_linux_macos_source_uses_posix_format() {
        // Linux and macOS → POSIX format
        let linux_src = linux_profile("sp-l", vec![("shared", "/mnt/shared")]);
        let macos_src = macos_profile("sp-m", vec![("shared", "/Volumes/shared")]);
        let dst = linux_profile("sp-d", vec![("shared", "/opt/shared")]);

        for src in [&linux_src, &macos_src] {
            let rules = generate_path_mapping_rules(src, &dst);
            assert_eq!(rules.len(), 1);
            assert_eq!(rules[0].source_path_format, "posix");
        }
    }

    #[test]
    fn generate_rules_empty_locations_returns_empty() {
        // both profiles have empty fileSystemLocations
        let src = linux_profile("sp-1", vec![]);
        let dst = linux_profile("sp-2", vec![]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    // ===================================================================
    // PathMappingRuleApplier constructor tests
    // ===================================================================

    #[test]
    fn applier_mixed_source_formats_returns_error() {
        // mixed source path formats → error
        let rules = vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
            rule("windows", "D:\\tmp", "/var/tmp"),
        ];
        let err = PathMappingRuleApplier::new(rules).unwrap_err();
        assert!(
            err.to_string().contains("multiple source path formats"),
            "got: {err}"
        );
    }

    #[test]
    fn applier_unexpected_source_format_returns_error() {
        // unexpected format → error
        let rules = vec![rule("xisop", "/mnt/shared", "/opt/shared")];
        let err = PathMappingRuleApplier::new(rules).unwrap_err();
        assert!(
            err.to_string().contains("Unexpected source path format"),
            "got: {err}"
        );
    }

    // ===================================================================
    // transform tests
    // ===================================================================

    #[test]
    fn transform_exact_match_returns_destination() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/mnt/shared"), "/opt/shared");
    }

    #[test]
    fn transform_child_path_returns_joined_destination() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/subdir/file.txt"),
            "/opt/shared/subdir/file.txt"
        );
    }

    #[test]
    fn transform_no_match_returns_original() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/other/path"), "/other/path");
    }

    #[test]
    fn transform_most_specific_rule_wins() {
        // /mnt/Projects/Special is more specific than /mnt/Projects
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/Projects", "/dest/projects"),
            rule("posix", "/mnt/Projects/Special", "/dest/special"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("/mnt/Projects/Special/data.txt"),
            "/dest/special/data.txt"
        );
        // Shorter rule still works for non-Special paths
        assert_eq!(
            applier.transform("/mnt/Projects/other.txt"),
            "/dest/projects/other.txt"
        );
    }

    #[test]
    fn transform_windows_case_insensitive() {
        // Windows paths match case-insensitively
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\Shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("C:\\ShArEd"), "/opt/shared");
        assert_eq!(
            applier.transform("C:\\SHARED\\file.txt"),
            "/opt/shared/file.txt"
        );
    }

    #[test]
    fn transform_no_rules_returns_original() {
        let applier = PathMappingRuleApplier::new(vec![]).unwrap();
        assert_eq!(applier.transform("/some/path"), "/some/path");
    }

    // ===================================================================
    // strict_transform tests
    // ===================================================================

    #[test]
    fn strict_transform_match_returns_path() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.strict_transform("/mnt/shared/file.txt").unwrap(),
            PathBuf::from("/opt/shared/file.txt")
        );
    }

    #[test]
    fn strict_transform_no_match_returns_error() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        let err = applier.strict_transform("/other/path").unwrap_err();
        assert!(
            err.to_string()
                .contains("No path mapping rule could be applied"),
            "got: {err}"
        );
    }

    #[test]
    fn strict_transform_no_rules_returns_error() {
        // no rules (source_path_format is None)
        let applier = PathMappingRuleApplier::new(vec![]).unwrap();
        assert!(applier.strict_transform("/some/path").is_err());
    }

    #[test]
    fn strict_transform_overlapping_rules_uses_longest() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/dest/short"),
            rule("posix", "/mnt/shared/projects", "/dest/long"),
        ])
        .unwrap();
        assert_eq!(
            applier.strict_transform("/mnt/shared/projects/f.txt").unwrap(),
            PathBuf::from("/dest/long/f.txt")
        );
    }

    // ===================================================================
    // Additional edge cases from Python tests
    // ===================================================================

    #[test]
    fn transform_posix_empty_string_returns_empty() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform(""), "");
    }

    #[test]
    fn transform_posix_root_returns_root() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/"), "/");
    }

    #[test]
    fn transform_posix_case_sensitive() {
        // POSIX is case-sensitive: /Mnt/shared ≠ /mnt/shared
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/Mnt/shared"), "/Mnt/shared");
    }

    #[test]
    fn transform_posix_unicode_path() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/файл.txt"),
            "/opt/shared/файл.txt"
        );
    }

    #[test]
    fn transform_posix_spaces_in_path() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/file with spaces.txt"),
            "/opt/shared/file with spaces.txt"
        );
    }

    #[test]
    fn transform_windows_case_preserving_tail() {
        // Windows: case-insensitive match but tail preserves original case
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\proJects", "/dest/projects"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("C:\\PROJECTS\\Case\\Of\\tail\\PreServed"),
            "/dest/projects/Case/Of/tail/PreServed"
        );
    }

    #[test]
    fn transform_windows_unicode_path() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("C:\\shared\\файл.txt"),
            "/opt/shared/файл.txt"
        );
    }

    #[test]
    fn transform_windows_spaces_in_path() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("C:\\shared\\file with spaces.txt"),
            "/opt/shared/file with spaces.txt"
        );
    }

    #[test]
    fn transform_windows_overlapping_rules_most_specific_wins() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\shared", "/dest/short"),
            rule("windows", "C:\\shared\\projects", "/dest/long"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("C:\\shared\\projects"),
            "/dest/long"
        );
        assert_eq!(
            applier.transform("C:\\shared\\projects\\file.txt"),
            "/dest/long/file.txt"
        );
    }

    #[test]
    fn applier_posix_trie_structure() {
        // Verify trie is built correctly (mirrors Python test)
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/d1"),
            rule("posix", "/mnt/projects", "/d2"),
            rule("posix", "/tmp", "/d3"),
        ])
        .unwrap();
        assert_eq!(applier.source_path_format.as_deref(), Some("posix"));
        // Root trie should have "/" as only key
        assert_eq!(applier.trie.len(), 1);
        assert!(applier.trie.contains_key("/"));
    }

    #[test]
    fn applier_windows_trie_structure() {
        // Verify Windows trie keys are lowercased
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\Mnt\\Shared", "/d1"),
            rule("windows", "C:\\Mnt\\proJects", "/d2"),
            rule("windows", "D:\\tmp", "/d3"),
        ])
        .unwrap();
        assert_eq!(applier.source_path_format.as_deref(), Some("windows"));
        // Root trie should have "c:\\" and "d:\\" (lowercased)
        assert_eq!(applier.trie.len(), 2);
        assert!(applier.trie.contains_key("c:\\"));
        assert!(applier.trie.contains_key("d:\\"));
    }

    #[test]
    fn applier_empty_rules_has_none_format() {
        let applier = PathMappingRuleApplier::new(vec![]).unwrap();
        assert!(applier.source_path_format.is_none());
        assert!(applier.trie.is_empty());
    }

    #[test]
    fn transform_posix_partial_component_no_match() {
        // "/mnt/other/path" should NOT match rule for "/mnt/shared"
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/mnt/other/path"), "/mnt/other/path");
    }

    #[test]
    fn transform_windows_no_match_returns_original() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform("C:\\other\\path"), "C:\\other\\path");
    }

    #[test]
    fn strict_transform_windows_no_match_returns_error() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\Shared", "/opt/shared"),
        ])
        .unwrap();
        assert!(applier.strict_transform("C:\\other\\path").is_err());
    }

    #[test]
    fn transform_posix_three_rules_all_match() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/shared", "/d1"),
            rule("posix", "/mnt/projects", "/d2"),
            rule("posix", "/tmp", "/d3"),
        ])
        .unwrap();
        assert_eq!(applier.transform("/mnt/shared"), "/d1");
        assert_eq!(applier.transform("/mnt/projects"), "/d2");
        assert_eq!(applier.transform("/tmp"), "/d3");
        assert_eq!(applier.strict_transform("/mnt/shared").unwrap(), PathBuf::from("/d1"));
        assert_eq!(applier.strict_transform("/mnt/projects").unwrap(), PathBuf::from("/d2"));
        assert_eq!(applier.strict_transform("/tmp").unwrap(), PathBuf::from("/d3"));
    }

    #[test]
    fn transform_windows_three_rules_all_match() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\Shared", "/d1"),
            rule("windows", "C:\\proJects", "/d2"),
            rule("windows", "D:\\tmp", "/d3"),
        ])
        .unwrap();
        assert_eq!(applier.transform("C:\\Shared"), "/d1");
        assert_eq!(applier.transform("C:\\proJects"), "/d2");
        assert_eq!(applier.transform("D:\\tmp"), "/d3");
        assert_eq!(applier.strict_transform("C:\\Shared").unwrap(), PathBuf::from("/d1"));
        assert_eq!(applier.strict_transform("C:\\proJects").unwrap(), PathBuf::from("/d2"));
        assert_eq!(applier.strict_transform("D:\\tmp").unwrap(), PathBuf::from("/d3"));
    }

    #[test]
    fn transform_windows_empty_string_returns_empty() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("windows", "C:\\shared", "/opt/shared"),
        ])
        .unwrap();
        assert_eq!(applier.transform(""), "");
    }
}
