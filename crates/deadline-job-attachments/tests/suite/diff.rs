//! Level 1 tests for diff module (batch 9e-1).

use deadline_job_attachments::asset_manifests::{
    AssetManifest, HashAlgorithm, ManifestPath, ManifestVersion,
};
use deadline_job_attachments::diff::{fast_diff, hash_diff, FileStatus};
use std::fs;
use tempfile::TempDir;

fn make_manifest(entries: &[(&str, &str, i64, i64)]) -> AssetManifest {
    let paths: Vec<ManifestPath> = entries
        .iter()
        .map(|(p, h, s, m)| ManifestPath {
            path: p.to_string(),
            hash: h.to_string(),
            size: *s,
            mtime: *m,
        })
        .collect();
    let total_size: i64 = paths.iter().map(|p| p.size).sum();
    AssetManifest::new(HashAlgorithm::Xxh128, ManifestVersion::V2023_03_03, total_size, paths)
        .unwrap()
}

fn create_file(dir: &TempDir, name: &str, content: &[u8]) -> String {
    let path = dir.path().join(name);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(&path, content).unwrap();
    path.to_string_lossy().into_owned()
}

// =====================================================================
// fast_diff
// =====================================================================

#[test]
fn fast_diff_new_file_detected() {
    let dir = TempDir::new().unwrap();
    let f1 = create_file(&dir, "new_file.txt", b"hello");
    let _manifest = make_manifest(&[]);
    // Empty manifest, one file on disk → file is New
    // fast_diff needs a non-empty manifest to work; use one with a different file
    let manifest = make_manifest(&[("other.txt", "aa".repeat(16).as_str(), 5, 1000000)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[f1], &manifest);
    assert!(result.iter().any(|(_, s)| *s == FileStatus::New));
}

#[test]
fn fast_diff_deleted_file_detected() {
    let dir = TempDir::new().unwrap();
    // Manifest has a file, disk doesn't
    let manifest = make_manifest(&[("gone.txt", "aa".repeat(16).as_str(), 10, 1000000)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[], &manifest);
    assert!(result.iter().any(|(p, s)| p == "gone.txt" && *s == FileStatus::Deleted));
}

#[test]
fn fast_diff_unchanged_file_not_reported_as_modified() {
    let dir = TempDir::new().unwrap();
    let f1 = create_file(&dir, "same.txt", b"hello");
    let meta = fs::metadata(&f1).unwrap();
    #[cfg(unix)]
    let mtime_us = {
        use std::os::unix::fs::MetadataExt;
        (meta.mtime() * 1_000_000) + (meta.mtime_nsec() / 1000)
    };
    #[cfg(not(unix))]
    let mtime_us = {
        meta.modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    };
    let manifest = make_manifest(&[("same.txt", "aa".repeat(16).as_str(), 5, mtime_us)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[f1], &manifest);
    // Should not be Modified — size and mtime match
    assert!(!result.iter().any(|(_, s)| *s == FileStatus::Modified));
}

#[test]
fn fast_diff_size_change_detected_as_modified() {
    let dir = TempDir::new().unwrap();
    let f1 = create_file(&dir, "changed.txt", b"longer content");
    let meta = fs::metadata(&f1).unwrap();
    #[cfg(unix)]
    let mtime_us = {
        use std::os::unix::fs::MetadataExt;
        (meta.mtime() * 1_000_000) + (meta.mtime_nsec() / 1000)
    };
    #[cfg(not(unix))]
    let mtime_us = {
        meta.modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    };
    // Manifest says size is 5, actual is 14
    let manifest = make_manifest(&[("changed.txt", "aa".repeat(16).as_str(), 5, mtime_us)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[f1], &manifest);
    assert!(result.iter().any(|(_, s)| *s == FileStatus::Modified));
}

// =====================================================================
// hash_diff
// =====================================================================

#[test]
fn hash_diff_new_file_detected() {
    let reference = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let compare = make_manifest(&[
        ("a.txt", "aa".repeat(16).as_str(), 10, 1000),
        ("b.txt", "bb".repeat(16).as_str(), 20, 2000),
    ]);
    let result = hash_diff(&reference, &compare);
    assert!(result.iter().any(|(s, p)| *s == FileStatus::New && p.path == "b.txt"));
}

#[test]
fn hash_diff_deleted_file_detected() {
    let reference = make_manifest(&[
        ("a.txt", "aa".repeat(16).as_str(), 10, 1000),
        ("b.txt", "bb".repeat(16).as_str(), 20, 2000),
    ]);
    let compare = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(result.iter().any(|(s, p)| *s == FileStatus::Deleted && p.path == "b.txt"));
}

#[test]
fn hash_diff_modified_file_detected() {
    let reference = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let compare = make_manifest(&[("a.txt", "cc".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(result.iter().any(|(s, p)| *s == FileStatus::Modified && p.path == "a.txt"));
}

#[test]
fn hash_diff_unchanged_file_detected() {
    let reference = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let compare = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(result.iter().any(|(s, _)| *s == FileStatus::Unchanged));
}
