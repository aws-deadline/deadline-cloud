//! Level 1 tests for diff module (batch 9e-1).

use deadline_lib::attachments::diff::{FileStatus, fast_diff, hash_diff};
use openjd_snapshots::{FileEntry, HashAlgorithm, Snapshot, WHOLE_FILE_CHUNK_SIZE};
use std::fs;
use tempfile::TempDir;

fn make_manifest(entries: &[(&str, &str, u64, i64)]) -> Snapshot {
    let files: Vec<FileEntry> = entries
        .iter()
        .map(|(p, h, s, m)| {
            let mut e = FileEntry::file(*p, *s, *m as u64);
            e.hash = Some(h.to_string());
            e
        })
        .collect();
    let total_size: u64 = files.iter().map(|f| f.size.unwrap_or(0)).sum();
    let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    snap.files = files;
    snap.total_size = total_size;
    snap
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
    let manifest = make_manifest(&[("other.txt", "aa".repeat(16).as_str(), 5, 1_000_000)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[f1], &manifest);
    assert!(result.iter().any(|(_, s)| *s == FileStatus::New));
}

#[test]
fn fast_diff_deleted_file_detected() {
    let dir = TempDir::new().unwrap();
    // Manifest has a file, disk doesn't
    let manifest = make_manifest(&[("gone.txt", "aa".repeat(16).as_str(), 10, 1_000_000)]);
    let result = fast_diff(dir.path().to_str().unwrap(), &[], &manifest);
    assert!(
        result
            .iter()
            .any(|(p, s)| p == "gone.txt" && *s == FileStatus::Deleted)
    );
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
    assert!(
        result
            .iter()
            .any(|(s, p)| *s == FileStatus::New && p.path == "b.txt")
    );
}

#[test]
fn hash_diff_deleted_file_detected() {
    let reference = make_manifest(&[
        ("a.txt", "aa".repeat(16).as_str(), 10, 1000),
        ("b.txt", "bb".repeat(16).as_str(), 20, 2000),
    ]);
    let compare = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(
        result
            .iter()
            .any(|(s, p)| *s == FileStatus::Deleted && p.path == "b.txt")
    );
}

#[test]
fn hash_diff_modified_file_detected() {
    let reference = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let compare = make_manifest(&[("a.txt", "cc".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(
        result
            .iter()
            .any(|(s, p)| *s == FileStatus::Modified && p.path == "a.txt")
    );
}

#[test]
fn hash_diff_unchanged_file_detected() {
    let reference = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let compare = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let result = hash_diff(&reference, &compare);
    assert!(result.iter().any(|(s, _)| *s == FileStatus::Unchanged));
}

// =====================================================================
// Batch F: F1 — HashSet correctness and performance for large manifests
// =====================================================================

#[test]
fn fast_diff_large_manifest_deleted_detection_is_correct() {
    // The deleted-file detection loop iterates manifest entries and checks
    // if each was seen on disk. With Vec::contains this is O(N*M) where
    // N=manifest entries and M=files on disk. When both are large, it's slow.
    //
    // We use 10K files on disk + 10K manifest entries (5K overlap, 5K deleted).
    // With Vec::contains at 10K×10K = 100M comparisons in debug → several seconds.
    // With HashSet at 10K lookups → <1s in debug.
    let dir = TempDir::new().unwrap();

    // Create 10K files on disk
    let disk_files: Vec<String> = (0..10_000)
        .map(|i| create_file(&dir, &format!("file_{i:05}.txt"), b"x"))
        .collect();

    // Manifest has 10K entries: 5K match disk files, 5K are "deleted" (not on disk)
    let files: Vec<FileEntry> = (0..5_000)
        .map(|i| {
            let mut e = FileEntry::file(&format!("file_{i:05}.txt"), 1, 1_000_000);
            e.hash = Some(format!("{i:032x}"));
            e
        })
        .chain((0..5_000).map(|i| {
            let mut e = FileEntry::file(&format!("deleted_{i:05}.txt"), 1, 1_000_000);
            e.hash = Some(format!("{:032x}", i + 50_000));
            e
        }))
        .collect();
    let mut manifest = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    manifest.files = files;
    manifest.total_size = 10_000;

    let start = std::time::Instant::now();
    let result = fast_diff(dir.path().to_str().unwrap(), &disk_files, &manifest);
    let elapsed = start.elapsed();

    let deleted_count = result
        .iter()
        .filter(|(_, s)| *s == FileStatus::Deleted)
        .count();
    assert_eq!(
        deleted_count, 5_000,
        "Expected 5,000 deleted files, got {deleted_count}"
    );

    // Measured on dev machine: ~250ms total (240ms file I/O + 5ms HashSet lookup).
    // Vec::contains would add ~700ms → ~940ms total.
    // Threshold at 400ms: allows 1.6x I/O variance across machines while
    // clearly catching the ~700ms regression from O(N²).
    assert!(
        elapsed.as_millis() < 400,
        "Deleted-file detection took {elapsed:?} — likely O(N²). Expected <400ms with HashSet."
    );
}
