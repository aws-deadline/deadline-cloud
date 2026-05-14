//! Manifest comparison: detect new, modified, deleted, and unchanged files.
//!
//! `hash_diff` delegates to `openjd_snapshots::diff_snapshots` via the type bridge.
//! `fast_diff` is filesystem-based (no openjd equivalent) and stays here.

use std::collections::HashMap;
use std::path::Path;

use openjd_snapshots::{DiffOptions, FileEntry, Snapshot, diff_snapshots};

use crate::attachments::asset_manifests::{AssetManifest, ManifestPath};

// =========================================================================
// Type bridge: AssetManifest ↔ Snapshot
// =========================================================================

/// Convert an `AssetManifest` to an openjd `Snapshot` (relative-path, full manifest).
pub fn asset_manifest_to_snapshot(manifest: &AssetManifest) -> Snapshot {
    let mut snap = Snapshot::new(
        openjd_snapshots::HashAlgorithm::Xxh128,
        openjd_snapshots::WHOLE_FILE_CHUNK_SIZE,
    );
    for p in &manifest.paths {
        let mut entry = FileEntry::file(&p.path, p.size, p.mtime as u64);
        entry.hash = Some(p.hash.clone());
        snap.files.push(entry);
    }
    snap.total_size = manifest.total_size;
    snap
}

/// Convert an openjd `Snapshot` back to an `AssetManifest`.
pub fn snapshot_to_asset_manifest(snap: &Snapshot) -> AssetManifest {
    use crate::attachments::asset_manifests::{HashAlgorithm, ManifestVersion};
    let paths: Vec<ManifestPath> = snap
        .files
        .iter()
        .filter(|f| !f.deleted && f.symlink_target.is_none())
        .map(|f| ManifestPath {
            path: f.path.clone(),
            hash: f.hash.clone().unwrap_or_default(),
            size: f.size.unwrap_or(0),
            mtime: f.mtime.unwrap_or(0) as i64,
        })
        .collect();
    let total_size = paths.iter().map(|p| p.size).sum();
    // unwrap is safe: paths are well-formed from a valid Snapshot
    AssetManifest::new(HashAlgorithm::Xxh128, ManifestVersion::V2023_03_03, total_size, paths)
        .expect("valid manifest from snapshot conversion")
}

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
///
/// Delegates to `openjd_snapshots::diff_snapshots` via the type bridge.
pub fn hash_diff(
    reference: &AssetManifest,
    compare: &AssetManifest,
) -> Vec<(FileStatus, ManifestPath)> {
    let ref_snap = asset_manifest_to_snapshot(reference);
    let cmp_snap = asset_manifest_to_snapshot(compare);

    let opts = DiffOptions {
        ignore_hashes: false,
        ..Default::default()
    };

    let diff_manifest = diff_snapshots(&ref_snap, &cmp_snap, &opts)
        .expect("diff_snapshots should not fail on valid snapshots");

    let mut results = Vec::new();

    // New/modified entries from the diff
    for file in &diff_manifest.files {
        if file.deleted {
            // Deleted: look up original entry from reference
            let mp = reference
                .paths
                .iter()
                .find(|p| p.path == file.path)
                .cloned()
                .unwrap_or_else(|| ManifestPath {
                    path: file.path.clone(),
                    hash: String::new(),
                    size: 0,
                    mtime: 0,
                });
            results.push((FileStatus::Deleted, mp));
        } else {
            // New or modified — check if it existed in reference
            let status = if reference.paths.iter().any(|p| p.path == file.path) {
                FileStatus::Modified
            } else {
                FileStatus::New
            };
            let mp = ManifestPath {
                path: file.path.clone(),
                hash: file.hash.clone().unwrap_or_default(),
                size: file.size.unwrap_or(0),
                mtime: file.mtime.unwrap_or(0) as i64,
            };
            results.push((status, mp));
        }
    }

    // Unchanged: entries in compare that are NOT in the diff output
    for cp in &compare.paths {
        if !diff_manifest.files.iter().any(|f| f.path == cp.path) {
            results.push((FileStatus::Unchanged, cp.clone()));
        }
    }

    results
}
