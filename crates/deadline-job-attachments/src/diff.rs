//! Manifest comparison: detect new, modified, deleted, and unchanged files.

use std::collections::HashMap;
use std::path::Path;

use crate::asset_manifests::{AssetManifest, ManifestPath};

/// Status of a local file relative to a manifest entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStatus {
    Unchanged,
    New,
    Modified,
    Deleted,
}

/// Fast diff using file size and mtime. Returns root-relative POSIX paths
/// with their status. Mtime tolerance: 1 microsecond.
pub fn fast_diff(
    root: &str,
    current_files: &[String],
    reference_manifest: &AssetManifest,
) -> Vec<(String, FileStatus)> {
    let root_path = Path::new(root);
    let mut manifest_map: HashMap<String, &ManifestPath> = HashMap::new();
    for entry in &reference_manifest.paths {
        let normalized = entry.path.replace('\\', "/");
        manifest_map.insert(normalized, entry);
    }

    let mut results = Vec::new();
    let mut seen_relative: Vec<String> = Vec::new();

    for file in current_files {
        let file_path = Path::new(file);
        let relative = file_path
            .strip_prefix(root_path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .replace('\\', "/");
        seen_relative.push(relative.clone());

        let Ok(meta) = std::fs::metadata(file_path) else {
            continue;
        };

        match manifest_map.get(&relative) {
            None => {
                results.push((relative, FileStatus::New));
            }
            Some(entry) => {
                let file_size = meta.len();
                if file_size != entry.size {
                    results.push((relative, FileStatus::Modified));
                    continue;
                }

                #[cfg(unix)]
                let mtime_us = {
                    use std::os::unix::fs::MetadataExt;
                    (meta.mtime() * 1_000_000) + (meta.mtime_nsec() / 1000)
                };
                #[cfg(not(unix))]
                let mtime_us = {
                    meta.modified()
                        .unwrap_or(std::time::UNIX_EPOCH)
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_micros() as i64
                };

                if (mtime_us - entry.mtime).abs() > 1 {
                    results.push((relative, FileStatus::Modified));
                }
            }
        }
    }

    // Deleted: in manifest but not on disk
    for entry in &reference_manifest.paths {
        let normalized = entry.path.replace('\\', "/");
        if !seen_relative.contains(&normalized) {
            results.push((normalized, FileStatus::Deleted));
        }
    }

    results
}

/// Hash-based diff between two manifests. Returns status for every path
/// in either manifest.
pub fn hash_diff(
    reference: &AssetManifest,
    compare: &AssetManifest,
) -> Vec<(FileStatus, ManifestPath)> {
    let ref_map: HashMap<&str, &ManifestPath> = reference
        .paths
        .iter()
        .map(|p| (p.path.as_str(), p))
        .collect();
    let cmp_map: HashMap<&str, &ManifestPath> =
        compare.paths.iter().map(|p| (p.path.as_str(), p)).collect();

    let mut results = Vec::new();

    for (path, cmp_entry) in &cmp_map {
        match ref_map.get(path) {
            None => results.push((FileStatus::New, (*cmp_entry).clone())),
            Some(ref_entry) => {
                if ref_entry.hash == cmp_entry.hash {
                    results.push((FileStatus::Unchanged, (*cmp_entry).clone()));
                } else {
                    results.push((FileStatus::Modified, (*cmp_entry).clone()));
                }
            }
        }
    }

    for (path, ref_entry) in &ref_map {
        if !cmp_map.contains_key(path) {
            results.push((FileStatus::Deleted, (*ref_entry).clone()));
        }
    }

    results
}
