use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use deadline_models::errors::JobAttachmentsError;

use crate::asset_manifests::{hash_file, AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion};
use crate::caches::{HashCache, HashCacheEntry};
use crate::models::{
    AssetRootGroup, AssetRootManifest, AssetUploadGroup, FileSystemLocationType, StorageProfile,
};
use crate::progress_tracker::{ProgressReportMetadata, ProgressStatus, ProgressTracker};

fn is_relative_to(path: &Path, base: &str) -> bool {
    let base_path = Path::new(base);
    path.starts_with(base_path)
}

/// Normalizes a path to absolute without following symlinks, filtering empty
/// strings and paths relative to SHARED locations. Returns None if filtered.
fn resolve_and_filter(p: &str, shared_locations: &[&str]) -> Option<PathBuf> {
    if p.is_empty() {
        return None;
    }
    let abs_path = normalize_absolute(p);
    if shared_locations.iter().any(|s| is_relative_to(&abs_path, s)) {
        return None;
    }
    Some(abs_path)
}

fn normalize_absolute(p: &str) -> PathBuf {
    use std::path::Component;
    let path = Path::new(p);
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().unwrap_or_default().join(path)
    };
    // Normalize: resolve `.` and `..` without following symlinks
    let mut components = Vec::new();
    for c in abs.components() {
        match c {
            Component::ParentDir => { components.pop(); }
            Component::CurDir => {}
            _ => components.push(c),
        }
    }
    components.iter().collect()
}

pub fn prepare_paths_for_upload(
    input_paths: &[String],
    output_paths: &[String],
    referenced_paths: &[String],
    storage_profile: Option<&StorageProfile>,
    require_paths_exist: bool,
) -> Result<AssetUploadGroup, JobAttachmentsError> {
    let mut local_locations: Vec<(&str, &str)> = Vec::new(); // (path, name)
    let mut shared_locations: Vec<&str> = Vec::new();

    if let Some(profile) = storage_profile {
        for loc in &profile.file_system_locations {
            match loc.location_type {
                FileSystemLocationType::Local => {
                    local_locations.push((&loc.path, &loc.name));
                }
                FileSystemLocationType::Shared => {
                    shared_locations.push(&loc.path);
                }
            }
        }
    }

    let mut groupings: Vec<(String, AssetRootGroup)> = Vec::new(); // (key, group)
    let mut missing_inputs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut misconfigured_dirs: BTreeSet<PathBuf> = BTreeSet::new();
    let mut extra_referenced: Vec<String> = Vec::new();

    // Process input paths
    for p in input_paths {
        if p.is_empty() {
            continue;
        }
        let abs_path = normalize_absolute(p);

        if !abs_path.exists() {
            if require_paths_exist {
                missing_inputs.insert(abs_path);
            } else {
                log::warn!(
                    "Input path '{}' resolving to '{}' does not exist. Adding to referenced paths.",
                    p,
                    abs_path.display()
                );
                extra_referenced.push(p.clone());
            }
            continue;
        }
        if abs_path.is_dir() {
            misconfigured_dirs.insert(abs_path);
            continue;
        }
        if shared_locations.iter().any(|s| is_relative_to(&abs_path, s)) {
            continue;
        }
        let key = find_group_key(&abs_path, &local_locations, &mut groupings);
        let group = get_group_mut(&key, &mut groupings);
        group.inputs.insert(abs_path);
    }

    if !missing_inputs.is_empty() || !misconfigured_dirs.is_empty() {
        let mut msg = "Job submission contains missing input files or directories specified as files. All inputs must exist and be classified properly.".to_string();
        if !missing_inputs.is_empty() {
            let list: Vec<String> = missing_inputs.iter().map(|p| p.display().to_string()).collect();
            msg.push_str(&format!("\nMissing input files:\n\t{}", list.join("\n\t")));
        }
        if !misconfigured_dirs.is_empty() {
            let list: Vec<String> = misconfigured_dirs.iter().map(|p| p.display().to_string()).collect();
            msg.push_str(&format!("\nDirectories classified as files:\n\t{}", list.join("\n\t")));
        }
        return Err(JobAttachmentsError::MisconfiguredInputs(msg));
    }

    // Process output paths
    for p in output_paths {
        if let Some(abs_path) = resolve_and_filter(p, &shared_locations) {
            let key = find_group_key(&abs_path, &local_locations, &mut groupings);
            get_group_mut(&key, &mut groupings).outputs.insert(abs_path);
        }
    }

    // Process referenced paths (including extras from missing inputs)
    let all_referenced = referenced_paths.iter().chain(extra_referenced.iter());
    for p in all_referenced {
        if let Some(abs_path) = resolve_and_filter(p, &shared_locations) {
            let key = find_group_key(&abs_path, &local_locations, &mut groupings);
            get_group_mut(&key, &mut groupings).references.insert(abs_path);
        }
    }

    // Compute root_path for each group as common_path of all paths
    for (_, group) in &mut groupings {
        let all_paths: Vec<&PathBuf> = group
            .inputs
            .iter()
            .chain(group.outputs.iter())
            .chain(group.references.iter())
            .collect();
        if all_paths.is_empty() {
            continue;
        }
        let common = common_path(&all_paths);
        let root = if common.is_file() {
            common.parent().unwrap_or(&common).to_path_buf()
        } else {
            common
        };
        group.root_path = root.to_string_lossy().into_owned();
    }

    // Sort groups by (root_path, file_system_location_name)
    groupings.sort_by(|a, b| {
        (&a.1.root_path, &a.1.file_system_location_name)
            .cmp(&(&b.1.root_path, &b.1.file_system_location_name))
    });

    let asset_groups: Vec<AssetRootGroup> = groupings.into_iter().map(|(_, g)| g).collect();

    // Compute totals
    let mut total_input_files: u64 = 0;
    let mut total_input_bytes: u64 = 0;
    for group in &asset_groups {
        for input in &group.inputs {
            total_input_files += 1;
            total_input_bytes += std::fs::metadata(input).map(|m| m.len()).unwrap_or(0);
        }
    }

    Ok(AssetUploadGroup {
        asset_groups,
        total_input_files,
        total_input_bytes,
    })
}

fn find_group_key(
    abs_path: &Path,
    local_locations: &[(&str, &str)],
    groupings: &mut Vec<(String, AssetRootGroup)>,
) -> String {
    // Find most specific LOCAL location match
    let mut best_match: Option<(&str, &str)> = None;
    for &(loc_path, loc_name) in local_locations {
        if is_relative_to(abs_path, loc_path) {
            if best_match.is_none() || loc_path.len() > best_match.unwrap().0.len() {
                best_match = Some((loc_path, loc_name));
            }
        }
    }

    if let Some((loc_path, loc_name)) = best_match {
        // Ensure group exists for this local location
        if !groupings.iter().any(|(k, _)| k == loc_path) {
            groupings.push((
                loc_path.to_string(),
                AssetRootGroup {
                    file_system_location_name: Some(loc_name.to_string()),
                    root_path: String::new(),
                    inputs: BTreeSet::new(),
                    outputs: BTreeSet::new(),
                    references: BTreeSet::new(),
                },
            ));
        }
        loc_path.to_string()
    } else {
        // Use top-level directory component as key
        let top = top_directory(abs_path);
        if !groupings.iter().any(|(k, _)| k.eq_ignore_ascii_case(&top)) {
            groupings.push((
                top.clone(),
                AssetRootGroup {
                    file_system_location_name: None,
                    root_path: String::new(),
                    inputs: BTreeSet::new(),
                    outputs: BTreeSet::new(),
                    references: BTreeSet::new(),
                },
            ));
        }
        top
    }
}

fn get_group_mut<'a>(key: &str, groupings: &'a mut Vec<(String, AssetRootGroup)>) -> &'a mut AssetRootGroup {
    let idx = groupings
        .iter()
        .position(|(k, _)| k.eq_ignore_ascii_case(key))
        .expect("group must exist");
    &mut groupings[idx].1
}

fn top_directory(path: &Path) -> String {
    path.components()
        .next()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn common_path(paths: &[&PathBuf]) -> PathBuf {
    if paths.is_empty() {
        return PathBuf::new();
    }
    let first = paths[0];
    let mut prefix: Vec<_> = first.components().collect();
    for p in &paths[1..] {
        let comps: Vec<_> = p.components().collect();
        let len = prefix.len().min(comps.len());
        let mut common_len = 0;
        for i in 0..len {
            if prefix[i] == comps[i] {
                common_len = i + 1;
            } else {
                break;
            }
        }
        prefix.truncate(common_len);
    }
    prefix.iter().collect()
}

/// Hashes a file using the cache. Returns the hash and sets `was_cached` if the
/// cached entry was reused without re-hashing.
fn hash_with_cache(
    cache: &HashCache,
    full_path: &str,
    file_path: &Path,
    hash_alg: HashAlgorithm,
    mtime_ns: i64,
    was_cached: &mut bool,
) -> Result<String, JobAttachmentsError> {
    if let Some(entry) = cache.get_entry(full_path, hash_alg, 0, -1) {
        if entry.last_modified_time == mtime_ns {
            *was_cached = true;
            return Ok(entry.file_hash);
        }
    }
    let h = hash_file(file_path, hash_alg)?;
    cache.put_entry(&HashCacheEntry {
        file_path: full_path.to_string(),
        hash_algorithm: hash_alg,
        file_hash: h.clone(),
        last_modified_time: mtime_ns,
        range_start: 0,
        range_end: -1,
    });
    Ok(h)
}

pub fn hash_assets_and_create_manifest(
    asset_groups: &[AssetRootGroup],
    total_input_files: u64,
    total_input_bytes: u64,
    hash_cache_dir: Option<&str>,
    on_preparing_to_submit: Option<Box<dyn Fn(ProgressReportMetadata) -> bool + Send>>,
) -> Result<(crate::progress_tracker::SummaryStatistics, Vec<AssetRootManifest>), JobAttachmentsError> {
    let start = std::time::Instant::now();

    let progress_tracker = ProgressTracker::new(
        ProgressStatus::PreparingInProgress,
        total_input_files,
        total_input_bytes,
        on_preparing_to_submit,
    );

    let cache_dir = hash_cache_dir
        .map(|s| s.to_string())
        .or_else(|| crate::caches::default_cache_dir());

    let mut asset_root_manifests = Vec::new();

    for group in asset_groups {
        let asset_manifest = if !group.inputs.is_empty() {
            let cache = cache_dir
                .as_deref()
                .map(HashCache::new)
                .transpose()?;

            let mut paths = Vec::new();
            let sorted_inputs: Vec<_> = group.inputs.iter().cloned().collect();

            for input_path in &sorted_inputs {
                // Check cancellation
                if !progress_tracker.continue_reporting() {
                    return Err(JobAttachmentsError::Cancelled {
                        message: "File hashing cancelled.".into(),
                    });
                }

                let full_path = input_path.to_string_lossy().into_owned();
                let meta = std::fs::metadata(input_path).map_err(|e| {
                    JobAttachmentsError::AssetSync(format!(
                        "Failed to stat {}: {e}",
                        input_path.display()
                    ))
                })?;
                let file_size = meta.len() as i64;

                #[cfg(unix)]
                let mtime_ns = {
                    use std::os::unix::fs::MetadataExt;
                    meta.mtime() * 1_000_000_000 + meta.mtime_nsec()
                };
                #[cfg(not(unix))]
                let mtime_ns = {
                    meta.modified()
                        .unwrap_or(std::time::UNIX_EPOCH)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos() as i64
                };

                let mtime_us = mtime_ns / 1000; // truncate to microseconds

                let hash_alg = HashAlgorithm::Xxh128;
                let mut was_cached = false;

                let file_hash = match cache {
                    Some(ref cache) => {
                        hash_with_cache(cache, &full_path, input_path, hash_alg, mtime_ns, &mut was_cached)?
                    }
                    None => hash_file(input_path, hash_alg)?,
                };

                // Relative POSIX path
                let root = Path::new(&group.root_path);
                let rel_path = input_path
                    .strip_prefix(root)
                    .unwrap_or(input_path)
                    .to_string_lossy()
                    .replace('\\', "/");

                paths.push(ManifestPath {
                    path: rel_path,
                    hash: file_hash,
                    size: file_size,
                    mtime: mtime_us,
                });

                if was_cached {
                    progress_tracker.increase_skipped(1, file_size as u64);
                } else {
                    progress_tracker.increase_processed(1, file_size as u64);
                }
                if !progress_tracker.report_progress() {
                    return Err(JobAttachmentsError::Cancelled {
                        message: "File hashing cancelled.".into(),
                    });
                }
            }

            let total_size: i64 = paths.iter().map(|p| p.size).sum();
            Some(AssetManifest::new(
                HashAlgorithm::Xxh128,
                ManifestVersion::V2023_03_03,
                total_size,
                paths,
            )?)
        } else {
            None
        };

        asset_root_manifests.push(AssetRootManifest {
            file_system_location_name: group.file_system_location_name.clone(),
            root_path: group.root_path.clone(),
            asset_manifest,
            outputs: group.outputs.iter().cloned().collect(),
        });
    }

    let elapsed = start.elapsed().as_secs_f64();
    progress_tracker.set_total_time(elapsed);

    Ok((progress_tracker.get_summary_statistics(), asset_root_manifests))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{
        AssetRootGroup, FileSystemLocation, FileSystemLocationType,
        StorageProfile,
    };
    use crate::progress_tracker::ProgressReportMetadata;
    use std::fs;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use tempfile::TempDir;

    fn create_test_file(dir: &TempDir, name: &str, content: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&path, content).unwrap();
        path
    }

    // === §20 case 1: Happy path — input files, output dirs, referenced paths ===

    #[test]
    fn prepare_paths_groups_inputs_outputs_references() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"hello");
        let f2 = create_test_file(&dir, "b.txt", b"world");
        let out_dir = dir.path().join("output");
        fs::create_dir_all(&out_dir).unwrap();

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            &[out_dir.to_string_lossy().into()],
            &[f1.to_string_lossy().into()],
            None,
            false,
        )
        .unwrap();

        assert!(result.total_input_files > 0);
        assert!(result.total_input_bytes > 0);
        assert!(!result.asset_groups.is_empty());
    }

    // === §20 case 2: Input path relative to SHARED location is excluded ===

    #[test]
    fn prepare_paths_shared_location_input_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();
        let f1 = create_test_file(&dir, "shared/a.txt", b"data");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into()],
            &[],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert_eq!(result.total_input_files, 0);
    }

    // === §20 case 3: Input path relative to LOCAL location grouped under that root ===

    #[test]
    fn prepare_paths_local_location_groups_under_root() {
        let dir = TempDir::new().unwrap();
        let local_dir = dir.path().join("local");
        fs::create_dir_all(&local_dir).unwrap();
        let f1 = create_test_file(&dir, "local/a.txt", b"data");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "LocalLoc".into(),
                path: local_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Local,
            }],
        };

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into()],
            &[],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert_eq!(result.asset_groups.len(), 1);
        assert_eq!(
            result.asset_groups[0].file_system_location_name.as_deref(),
            Some("LocalLoc")
        );
    }

    // === §20 case 4: Multiple input files share common parent ===

    #[test]
    fn prepare_paths_common_parent_grouped_together() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "sub/a.txt", b"aaa");
        let f2 = create_test_file(&dir, "sub/b.txt", b"bbb");

        let result = prepare_paths_for_upload(
            &[f1.to_string_lossy().into(), f2.to_string_lossy().into()],
            &[],
            &[],
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.asset_groups.len(), 1);
        let root = &result.asset_groups[0].root_path;
        assert!(
            root.contains("sub"),
            "root should contain 'sub' directory, got: {root}"
        );
    }

    // === §20 case 6: Non-existent input with require_paths_exist=true errors ===

    #[test]
    fn prepare_paths_missing_input_require_exist_errors() {
        let result = prepare_paths_for_upload(
            &["/nonexistent/file.txt".into()],
            &[],
            &[],
            None,
            true,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Missing input") || err.contains("missing") || err.contains("not exist"),
            "expected missing file error, got: {err}"
        );
    }

    // === §20 case 7: Non-existent input with require_paths_exist=false moves to referenced ===

    #[test]
    fn prepare_paths_missing_input_no_require_moves_to_referenced() {
        let dir = TempDir::new().unwrap();
        let existing = create_test_file(&dir, "exists.txt", b"data");
        let missing = dir.path().join("missing.txt");

        let result = prepare_paths_for_upload(
            &[
                existing.to_string_lossy().into(),
                missing.to_string_lossy().into(),
            ],
            &[],
            &[],
            None,
            false,
        )
        .unwrap();

        assert_eq!(result.total_input_files, 1);
    }

    // === §20 case 8: Directory classified as input file errors ===

    #[test]
    fn prepare_paths_directory_as_input_file_errors() {
        let dir = TempDir::new().unwrap();
        let sub = dir.path().join("subdir");
        fs::create_dir_all(&sub).unwrap();

        let result = prepare_paths_for_upload(
            &[sub.to_string_lossy().into()],
            &[],
            &[],
            None,
            true,
        );
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("irectories classified as files") || err.contains("irectory"),
            "expected directory error, got: {err}"
        );
    }

    // === §20 case 9: Empty inputs/outputs/references returns empty group ===

    #[test]
    fn prepare_paths_all_empty_returns_empty() {
        let result = prepare_paths_for_upload(&[], &[], &[], None, false).unwrap();
        assert!(result.asset_groups.is_empty());
        assert_eq!(result.total_input_files, 0);
        assert_eq!(result.total_input_bytes, 0);
    }

    // === §20 case 10: Empty strings in input paths are filtered out ===

    #[test]
    fn prepare_paths_empty_strings_filtered() {
        let result = prepare_paths_for_upload(
            &["".into(), "".into()],
            &[],
            &[],
            None,
            false,
        )
        .unwrap();
        assert!(result.asset_groups.is_empty());
        assert_eq!(result.total_input_files, 0);
    }

    // === §20 case 11: Output path relative to SHARED location excluded ===

    #[test]
    fn prepare_paths_shared_location_output_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[],
            &[shared_dir.to_string_lossy().into()],
            &[],
            Some(&profile),
            false,
        )
        .unwrap();

        assert!(result.asset_groups.is_empty());
    }

    // === §20 case 12: Referenced path relative to SHARED location excluded ===

    #[test]
    fn prepare_paths_shared_location_reference_excluded() {
        let dir = TempDir::new().unwrap();
        let shared_dir = dir.path().join("shared");
        fs::create_dir_all(&shared_dir).unwrap();
        let ref_path = dir.path().join("shared/ref.txt");

        let profile = StorageProfile {
            storage_profile_id: "sp-test".into(),
            display_name: "Test".into(),
            os_family: crate::models::StorageProfileOperatingSystemFamily::host(),
            file_system_locations: vec![FileSystemLocation {
                name: "SharedLoc".into(),
                path: shared_dir.to_string_lossy().into(),
                location_type: FileSystemLocationType::Shared,
            }],
        };

        let result = prepare_paths_for_upload(
            &[],
            &[],
            &[ref_path.to_string_lossy().into()],
            Some(&profile),
            false,
        )
        .unwrap();

        assert!(result.asset_groups.is_empty());
    }

    // === §20 case 13: hash_assets_and_create_manifest single group with inputs ===

    #[test]
    fn hash_assets_creates_manifest_for_single_group() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"hello");
        let f2 = create_test_file(&dir, "b.txt", b"world");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1, f2].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (stats, manifests) = hash_assets_and_create_manifest(
            &[group],
            2,
            10,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_some());
        assert_eq!(stats.processed_files + stats.skipped_files, 2);
    }

    // === §20 case 14: Group with outputs but no inputs has None manifest ===

    #[test]
    fn hash_assets_output_only_group_has_no_manifest() {
        let dir = TempDir::new().unwrap();
        let out_dir = dir.path().join("output");
        fs::create_dir_all(&out_dir).unwrap();

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: Default::default(),
            outputs: [out_dir].into_iter().collect(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_stats, manifests) = hash_assets_and_create_manifest(
            &[group],
            0,
            0,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_none());
    }

    // === §20 case 15: Multiple groups produce multiple manifests ===

    #[test]
    fn hash_assets_multiple_groups_multiple_manifests() {
        let dir1 = TempDir::new().unwrap();
        let dir2 = TempDir::new().unwrap();
        let f1 = create_test_file(&dir1, "a.txt", b"aaa");
        let f2 = create_test_file(&dir2, "b.txt", b"bbb");

        let groups = vec![
            AssetRootGroup {
                root_path: dir1.path().to_string_lossy().into(),
                file_system_location_name: None,
                inputs: [f1].into_iter().collect(),
                outputs: Default::default(),
                references: Default::default(),
            },
            AssetRootGroup {
                root_path: dir2.path().to_string_lossy().into(),
                file_system_location_name: None,
                inputs: [f2].into_iter().collect(),
                outputs: Default::default(),
                references: Default::default(),
            },
        ];

        let cache_dir = TempDir::new().unwrap();
        let (_stats, manifests) = hash_assets_and_create_manifest(
            &groups,
            2,
            6,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 2);
        assert!(manifests[0].asset_manifest.is_some());
        assert!(manifests[1].asset_manifest.is_some());
    }

    // === §20 case 16: New file (not in cache) is hashed and cached ===

    #[test]
    fn hash_assets_new_file_hashed_and_cached() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "new.txt", b"new content");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (stats, _) = hash_assets_and_create_manifest(
            &[group],
            1,
            11,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(stats.processed_files, 1);
        assert_eq!(stats.skipped_files, 0);
    }

    // === §20 case 17: Cached unmodified file uses cached hash (skipped) ===

    #[test]
    fn hash_assets_cached_unmodified_file_skipped() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "cached.txt", b"cached content");

        let cache_dir = TempDir::new().unwrap();

        // First pass: hash the file (populates cache)
        let group1 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1.clone()].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats1, _) = hash_assets_and_create_manifest(
            &[group1],
            1,
            14,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats1.processed_files, 1);

        // Second pass: same file, same mtime — should be skipped
        let group2 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats2, _) = hash_assets_and_create_manifest(
            &[group2],
            1,
            14,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats2.skipped_files, 1);
        assert_eq!(stats2.processed_files, 0);
    }

    // === §20 case 18: Cached file with different mtime is re-hashed ===

    #[test]
    fn hash_assets_modified_file_rehashed() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "modify.txt", b"original");

        let cache_dir = TempDir::new().unwrap();

        // First pass
        let group1 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1.clone()].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        hash_assets_and_create_manifest(
            &[group1],
            1,
            8,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        // Modify the file (changes mtime)
        std::thread::sleep(std::time::Duration::from_millis(50));
        fs::write(&f1, b"modified content").unwrap();

        // Second pass: mtime changed, should be re-hashed (processed, not skipped)
        let group2 = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };
        let (stats2, _) = hash_assets_and_create_manifest(
            &[group2],
            1,
            16,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(stats2.processed_files, 1);
        assert_eq!(stats2.skipped_files, 0);
    }

    // === §20 case 19: Callback returns false cancels with error ===

    #[test]
    fn hash_assets_callback_cancel_returns_error() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"data");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let cancel_callback = |_: ProgressReportMetadata| -> bool { false };

        let result = hash_assets_and_create_manifest(
            &[group],
            1,
            4,
            Some(cache_dir.path().to_str().unwrap()),
            Some(Box::new(cancel_callback)),
        );
        assert!(result.is_err());
    }

    // === §20 case 20: Progress tracker reports progress for each file ===

    #[test]
    fn hash_assets_reports_progress() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"aaa");
        let f2 = create_test_file(&dir, "b.txt", b"bbb");

        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone = call_count.clone();
        let callback = move |_: ProgressReportMetadata| -> bool {
            count_clone.fetch_add(1, Ordering::SeqCst);
            true
        };

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1, f2].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        hash_assets_and_create_manifest(
            &[group],
            2,
            6,
            Some(cache_dir.path().to_str().unwrap()),
            Some(Box::new(callback)),
        )
        .unwrap();

        assert!(call_count.load(Ordering::SeqCst) >= 1);
    }

    // === §20 case 21: Manifest paths are POSIX-style relative paths ===

    #[test]
    fn hash_assets_manifest_paths_are_posix_relative() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "sub/file.txt", b"content");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            1,
            7,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        let manifest = manifests[0].asset_manifest.as_ref().unwrap();
        let path = &manifest.paths[0].path;
        assert!(
            path.contains('/') || !path.contains('\\'),
            "path should use forward slashes: {path}"
        );
        assert!(
            !path.starts_with('/'),
            "path should be relative: {path}"
        );
    }

    // === §20 case 22: File mtime stored as microseconds (integer) ===

    #[test]
    fn hash_assets_mtime_is_microseconds_integer() {
        let dir = TempDir::new().unwrap();
        let f1 = create_test_file(&dir, "a.txt", b"data");

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: [f1].into_iter().collect(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            1,
            4,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        let manifest = manifests[0].asset_manifest.as_ref().unwrap();
        let mtime = manifest.paths[0].mtime;
        assert!(
            mtime > 1_000_000_000_000,
            "mtime should be in microseconds since epoch, got: {mtime}"
        );
    }

    // === §20 case 24: Empty file list returns None manifest ===

    #[test]
    fn hash_assets_empty_inputs_returns_none_manifest() {
        let dir = TempDir::new().unwrap();

        let group = AssetRootGroup {
            root_path: dir.path().to_string_lossy().into(),
            file_system_location_name: None,
            inputs: Default::default(),
            outputs: Default::default(),
            references: Default::default(),
        };

        let cache_dir = TempDir::new().unwrap();
        let (_, manifests) = hash_assets_and_create_manifest(
            &[group],
            0,
            0,
            Some(cache_dir.path().to_str().unwrap()),
            None,
        )
        .unwrap();

        assert_eq!(manifests.len(), 1);
        assert!(manifests[0].asset_manifest.is_none());
    }
}
