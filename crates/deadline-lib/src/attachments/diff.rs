//! Manifest comparison: detect new, modified, deleted, and unchanged files.
//!
//! `hash_diff` delegates to `openjd_snapshots::diff_snapshots`.
//! `fast_diff` is filesystem-based (no openjd equivalent) and stays here.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use openjd_snapshots::{DiffOptions, FileEntry, Snapshot, diff_snapshots};

// =========================================================================
// Public API
// =========================================================================

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
///
/// This is filesystem-based (no openjd equivalent).
pub fn fast_diff(
    root: &str,
    current_files: &[String],
    reference_manifest: &Snapshot,
) -> Vec<(String, FileStatus)> {
    let root_path = Path::new(root);
    let mut manifest_map: HashMap<String, &FileEntry> = HashMap::new();
    for entry in &reference_manifest.files {
        let normalized = entry.path.replace('\\', "/");
        manifest_map.insert(normalized, entry);
    }

    let mut results = Vec::new();
    let mut seen_relative: HashSet<String> = HashSet::new();

    for file in current_files {
        let file_path = Path::new(file);
        let relative = file_path
            .strip_prefix(root_path)
            .unwrap_or(file_path)
            .to_string_lossy()
            .replace('\\', "/");
        seen_relative.insert(relative.clone());

        let Ok(meta) = std::fs::metadata(file_path) else {
            continue;
        };

        match manifest_map.get(&relative) {
            None => {
                results.push((relative, FileStatus::New));
            }
            Some(entry) => {
                let file_size = meta.len();
                if file_size != entry.size.unwrap_or(0) {
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

                let entry_mtime = entry.mtime.unwrap_or(0) as i64;
                if (mtime_us - entry_mtime).abs() > 1 {
                    results.push((relative, FileStatus::Modified));
                }
            }
        }
    }

    // Deleted: in manifest but not on disk
    for entry in &reference_manifest.files {
        let normalized = entry.path.replace('\\', "/");
        if !seen_relative.contains(&normalized) {
            results.push((normalized, FileStatus::Deleted));
        }
    }

    results
}

/// Hash-based diff between two manifests. Returns status for every path
/// in either manifest.
///
/// Delegates to `openjd_snapshots::diff_snapshots`.
pub fn hash_diff(reference: &Snapshot, compare: &Snapshot) -> Vec<(FileStatus, FileEntry)> {
    let opts = DiffOptions {
        ignore_hashes: false,
        ..Default::default()
    };

    let diff_manifest = diff_snapshots(reference, compare, &opts)
        .expect("diff_snapshots should not fail on valid snapshots");

    let mut results = Vec::new();

    // New/modified entries from the diff
    for file in &diff_manifest.files {
        if file.deleted {
            // Deleted: look up original entry from reference
            let fe = reference
                .files
                .iter()
                .find(|f| f.path == file.path)
                .cloned()
                .unwrap_or_else(|| FileEntry::new(&file.path));
            results.push((FileStatus::Deleted, fe));
        } else {
            // New or modified — check if it existed in reference
            let status = if reference.files.iter().any(|f| f.path == file.path) {
                FileStatus::Modified
            } else {
                FileStatus::New
            };
            results.push((status, file.clone()));
        }
    }

    // Unchanged: entries in compare that are NOT in the diff output
    for cf in &compare.files {
        if !diff_manifest.files.iter().any(|f| f.path == cf.path) {
            results.push((FileStatus::Unchanged, cf.clone()));
        }
    }

    results
}
