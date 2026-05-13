// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

//! Path mapping rules: generate rules from storage profiles and apply them
//! via openjd-expr's path mapping engine.

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
            dest_locations
                .get(loc.name.as_str())
                .map(|dest_path| PathMappingRule {
                    source_path_format: format.to_owned(),
                    source_path: loc.path.clone(),
                    destination_path: dest_path.to_string(),
                })
        })
        .collect()
}

/// Path mapper that delegates to openjd-expr's `apply_rules_with_format`.
/// Rules are sorted by `source_path` length (longest first) for correct
/// longest-prefix matching.
#[derive(Debug)]
pub struct PathMappingRuleApplier {
    pub source_path_format: Option<String>,
    pub path_mapping_rules: Vec<PathMappingRule>,
    openjd_rules: Vec<openjd_expr::path_mapping::PathMappingRule>,
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
                openjd_rules: Vec::new(),
            });
        }

        let format = &rules[0].source_path_format;
        if !rules.iter().all(|r| r.source_path_format == *format) {
            let mut formats: Vec<&str> = rules
                .iter()
                .map(|r| r.source_path_format.as_str())
                .collect();
            formats.sort_unstable();
            formats.dedup();
            return Err(JobAttachmentsError::AssetSync(format!(
                "The path mapping rules included multiple source path formats {}, only one is permitted.",
                formats.join(", ")
            )));
        }

        let openjd_format = match format.as_str() {
            "posix" => openjd_expr::path_mapping::PathFormat::Posix,
            "windows" => openjd_expr::path_mapping::PathFormat::Windows,
            other => {
                return Err(JobAttachmentsError::AssetSync(format!(
                    "Unexpected source path format {other}"
                )));
            }
        };

        // Convert and sort by source_path length descending (longest match first)
        let mut openjd_rules: Vec<openjd_expr::path_mapping::PathMappingRule> = rules
            .iter()
            .map(|r| openjd_expr::path_mapping::PathMappingRule {
                source_path_format: openjd_format,
                source_path: r.source_path.clone(),
                destination_path: r.destination_path.clone(),
            })
            .collect();
        openjd_rules.sort_by(|a, b| b.source_path.len().cmp(&a.source_path.len()));

        Ok(Self {
            source_path_format: Some(format.clone()),
            path_mapping_rules: rules,
            openjd_rules,
        })
    }

    /// Transform `source_path` using the most specific matching rule.
    /// Returns the original path unchanged if no rule matches.
    pub fn transform(&self, source_path: &str) -> String {
        if self.source_path_format.is_none() || source_path.is_empty() {
            return source_path.to_owned();
        }
        openjd_expr::path_mapping::apply_rules_with_format(
            &self.openjd_rules,
            source_path,
            openjd_expr::path_mapping::PathFormat::Posix,
        )
    }

    /// Transform `source_path` using the most specific matching rule.
    /// Returns error if no rule matches.
    pub fn strict_transform(&self, source_path: &str) -> Result<PathBuf, JobAttachmentsError> {
        if self.source_path_format.is_none() {
            return Err(JobAttachmentsError::AssetSync(
                "No path mapping rule could be applied".to_owned(),
            ));
        }
        let result = openjd_expr::path_mapping::apply_rules_with_format(
            &self.openjd_rules,
            source_path,
            openjd_expr::path_mapping::PathFormat::Posix,
        );
        if result == source_path {
            Err(JobAttachmentsError::AssetSync(
                "No path mapping rule could be applied".to_owned(),
            ))
        } else {
            Ok(PathBuf::from(result))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        FileSystemLocation, FileSystemLocationType, StorageProfile,
        StorageProfileOperatingSystemFamily,
    };

    // --- Helper factories ---

    fn linux_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_owned(),
            display_name: id.to_owned(),
            os_family: StorageProfileOperatingSystemFamily::Linux,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_owned(),
                    path: path.to_owned(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn macos_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_owned(),
            display_name: id.to_owned(),
            os_family: StorageProfileOperatingSystemFamily::Macos,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_owned(),
                    path: path.to_owned(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn windows_profile(id: &str, locations: Vec<(&str, &str)>) -> StorageProfile {
        StorageProfile {
            storage_profile_id: id.to_owned(),
            display_name: id.to_owned(),
            os_family: StorageProfileOperatingSystemFamily::Windows,
            file_system_locations: locations
                .into_iter()
                .map(|(name, path)| FileSystemLocation {
                    name: name.to_owned(),
                    path: path.to_owned(),
                    location_type: FileSystemLocationType::Shared,
                })
                .collect(),
        }
    }

    fn rule(fmt: &str, src: &str, dst: &str) -> PathMappingRule {
        PathMappingRule {
            source_path_format: fmt.to_owned(),
            source_path: src.to_owned(),
            destination_path: dst.to_owned(),
        }
    }

    // ===================================================================
    // generate_path_mapping_rules tests
    // ===================================================================

    #[test]
    fn generate_rules_matching_names_returns_rules() {
        let src = linux_profile("sp-1", vec![("shared", "/mnt/shared"), ("temp", "/tmp")]);
        let dst = linux_profile(
            "sp-2",
            vec![("shared", "/opt/shared"), ("temp", "/var/tmp")],
        );
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 2);
        assert!(rules.contains(&rule("posix", "/mnt/shared", "/opt/shared")));
        assert!(rules.contains(&rule("posix", "/tmp", "/var/tmp")));
    }

    #[test]
    fn generate_rules_same_profile_returns_empty() {
        let src = linux_profile("sp-same", vec![("shared", "/mnt/shared")]);
        let dst = linux_profile("sp-same", vec![("shared", "/opt/shared")]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    #[test]
    fn generate_rules_source_only_locations_no_rules() {
        let src = linux_profile("sp-1", vec![("only_in_src", "/mnt/src")]);
        let dst = linux_profile("sp-2", vec![("only_in_dst", "/mnt/dst")]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    #[test]
    fn generate_rules_dest_only_locations_no_rules() {
        let src = linux_profile("sp-1", vec![("a", "/a")]);
        let dst = linux_profile("sp-2", vec![("a", "/a2"), ("extra", "/extra")]);
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 1);
        assert!(rules.contains(&rule("posix", "/a", "/a2")));
    }

    #[test]
    fn generate_rules_windows_source_uses_windows_format() {
        let src = windows_profile("sp-w", vec![("shared", "C:\\shared"), ("temp", "C:\\temp")]);
        let dst = windows_profile(
            "sp-w2",
            vec![("shared", "D:\\shared"), ("temp", "D:\\temp")],
        );
        let rules = generate_path_mapping_rules(&src, &dst);
        assert_eq!(rules.len(), 2);
        for r in &rules {
            assert_eq!(r.source_path_format, "windows");
        }
    }

    #[test]
    fn generate_rules_linux_macos_source_uses_posix_format() {
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
        let src = linux_profile("sp-1", vec![]);
        let dst = linux_profile("sp-2", vec![]);
        assert!(generate_path_mapping_rules(&src, &dst).is_empty());
    }

    // ===================================================================
    // PathMappingRuleApplier constructor tests
    // ===================================================================

    #[test]
    fn applier_mixed_source_formats_returns_error() {
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
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(applier.transform("/mnt/shared"), "/opt/shared");
    }

    #[test]
    fn transform_child_path_returns_joined_destination() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/subdir/file.txt"),
            "/opt/shared/subdir/file.txt"
        );
    }

    #[test]
    fn transform_no_match_returns_original() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(applier.transform("/other/path"), "/other/path");
    }

    #[test]
    fn transform_most_specific_rule_wins() {
        let applier = PathMappingRuleApplier::new(vec![
            rule("posix", "/mnt/Projects", "/dest/projects"),
            rule("posix", "/mnt/Projects/Special", "/dest/special"),
        ])
        .unwrap();
        assert_eq!(
            applier.transform("/mnt/Projects/Special/data.txt"),
            "/dest/special/data.txt"
        );
        assert_eq!(
            applier.transform("/mnt/Projects/other.txt"),
            "/dest/projects/other.txt"
        );
    }

    #[test]
    fn transform_windows_case_insensitive() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\Shared", "/opt/shared")])
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
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(
            applier.strict_transform("/mnt/shared/file.txt").unwrap(),
            PathBuf::from("/opt/shared/file.txt")
        );
    }

    #[test]
    fn strict_transform_no_match_returns_error() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        let err = applier.strict_transform("/other/path").unwrap_err();
        assert!(
            err.to_string()
                .contains("No path mapping rule could be applied"),
            "got: {err}"
        );
    }

    #[test]
    fn strict_transform_no_rules_returns_error() {
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
            applier
                .strict_transform("/mnt/shared/projects/f.txt")
                .unwrap(),
            PathBuf::from("/dest/long/f.txt")
        );
    }

    // ===================================================================
    // Additional edge cases
    // ===================================================================

    #[test]
    fn transform_posix_empty_string_returns_empty() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(applier.transform(""), "");
    }

    #[test]
    fn transform_posix_case_sensitive() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(applier.transform("/Mnt/shared"), "/Mnt/shared");
    }

    #[test]
    fn transform_posix_unicode_path() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/файл.txt"),
            "/opt/shared/файл.txt"
        );
    }

    #[test]
    fn transform_posix_spaces_in_path() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(
            applier.transform("/mnt/shared/file with spaces.txt"),
            "/opt/shared/file with spaces.txt"
        );
    }

    #[test]
    fn transform_windows_case_preserving_tail() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\proJects", "/dest/projects")])
                .unwrap();
        assert_eq!(
            applier.transform("C:\\PROJECTS\\Case\\Of\\tail\\PreServed"),
            "/dest/projects/Case/Of/tail/PreServed"
        );
    }

    #[test]
    fn transform_windows_unicode_path() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\shared", "/opt/shared")])
                .unwrap();
        assert_eq!(
            applier.transform("C:\\shared\\файл.txt"),
            "/opt/shared/файл.txt"
        );
    }

    #[test]
    fn transform_windows_spaces_in_path() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\shared", "/opt/shared")])
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
        assert_eq!(applier.transform("C:\\shared\\projects"), "/dest/long");
        assert_eq!(
            applier.transform("C:\\shared\\projects\\file.txt"),
            "/dest/long/file.txt"
        );
    }

    #[test]
    fn transform_posix_partial_component_no_match() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("posix", "/mnt/shared", "/opt/shared")]).unwrap();
        assert_eq!(applier.transform("/mnt/other/path"), "/mnt/other/path");
    }

    #[test]
    fn transform_windows_no_match_returns_original() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\shared", "/opt/shared")])
                .unwrap();
        assert_eq!(applier.transform("C:\\other\\path"), "C:\\other\\path");
    }

    #[test]
    fn strict_transform_windows_no_match_returns_error() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\Shared", "/opt/shared")])
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
    }

    #[test]
    fn transform_windows_empty_string_returns_empty() {
        let applier =
            PathMappingRuleApplier::new(vec![rule("windows", "C:\\shared", "/opt/shared")])
                .unwrap();
        assert_eq!(applier.transform(""), "");
    }
}
