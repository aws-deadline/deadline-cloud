//! Level 1 tests for `manifest_ops` module (batch 9e-1).

use deadline_lib::attachments::manifest_ops::{
    GlobConfig, glob_files, manifest_diff, manifest_merge, manifest_snapshot, resolve_glob_config,
    write_manifest,
};
use openjd_snapshots::{
    FileEntry, HashAlgorithm, Snapshot, WHOLE_FILE_CHUNK_SIZE, encode_snapshot_v2023,
};
use std::fs;
use std::path::Path;
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
// resolve_glob_config
// =====================================================================

#[test]
fn resolve_glob_config_defaults_to_all_files() {
    let config = resolve_glob_config(&[], &[], None).unwrap();
    assert_eq!(config.include, vec!["**/*"]);
    assert!(config.exclude.is_empty());
}

#[test]
fn resolve_glob_config_include_exclude_override_config() {
    let config =
        resolve_glob_config(&["*.exr".into()], &["*.tmp".into()], Some("ignored")).unwrap();
    assert_eq!(config.include, vec!["*.exr"]);
    assert_eq!(config.exclude, vec!["*.tmp"]);
}

#[test]
fn resolve_glob_config_json_string_parsed() {
    let json = r#"{"include": ["*.png"], "exclude": ["thumb*"]}"#;
    let config = resolve_glob_config(&[], &[], Some(json)).unwrap();
    assert_eq!(config.include, vec!["*.png"]);
    assert_eq!(config.exclude, vec!["thumb*"]);
}

#[test]
fn resolve_glob_config_json_file_parsed() {
    let dir = TempDir::new().unwrap();
    let config_path = dir.path().join("glob.json");
    fs::write(&config_path, r#"{"include": ["*.exr"]}"#).unwrap();
    let config = resolve_glob_config(&[], &[], Some(config_path.to_str().unwrap())).unwrap();
    assert_eq!(config.include, vec!["*.exr"]);
}

// =====================================================================
// glob_files
// =====================================================================

#[test]
fn glob_files_returns_all_files_with_default_config() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"a");
    create_file(&dir, "sub/b.txt", b"b");
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let files = glob_files(dir.path(), &config).unwrap();
    assert_eq!(files.len(), 2);
}

#[test]
fn glob_files_include_filter_works() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"a");
    create_file(&dir, "b.exr", b"b");
    let config = GlobConfig {
        include: vec!["*.exr".into()],
        exclude: vec![],
    };
    let files = glob_files(dir.path(), &config).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].contains("b.exr"));
}

#[test]
fn glob_files_exclude_filter_works() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"a");
    create_file(&dir, "b.tmp", b"b");
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec!["*.tmp".into()],
    };
    let files = glob_files(dir.path(), &config).unwrap();
    assert_eq!(files.len(), 1);
    assert!(files[0].contains("a.txt"));
}

#[test]
fn glob_files_empty_dir_returns_empty() {
    let dir = TempDir::new().unwrap();
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let files = glob_files(dir.path(), &config).unwrap();
    assert!(files.is_empty());
}

// =====================================================================
// write_manifest
// =====================================================================

#[test]
fn write_manifest_creates_file_with_name() {
    let dir = TempDir::new().unwrap();
    let manifest = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let path = write_manifest(
        Path::new("/some/root"),
        &manifest,
        dir.path(),
        Some("myname"),
    )
    .unwrap();
    assert!(path.is_file());
    assert!(path.to_string_lossy().contains("myname-"));
    assert!(path.to_string_lossy().ends_with(".manifest"));
}

#[test]
fn write_manifest_derives_name_from_root() {
    let dir = TempDir::new().unwrap();
    let manifest = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let path = write_manifest(Path::new("/some/root"), &manifest, dir.path(), None).unwrap();
    // /some/root → _some_root → some_root (strip leading _)
    assert!(path.to_string_lossy().contains("some_root-"));
}

#[test]
fn write_manifest_replaces_slashes_backslashes_colons() {
    let dir = TempDir::new().unwrap();
    let manifest = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let path = write_manifest(
        Path::new("C:\\Users\\test:data/files"),
        &manifest,
        dir.path(),
        None,
    )
    .unwrap();
    let filename = path.file_name().unwrap().to_str().unwrap();
    assert!(!filename.contains('/'));
    assert!(!filename.contains('\\'));
    assert!(!filename.contains(':'));
}

#[test]
fn write_manifest_creates_parent_dirs() {
    let dir = TempDir::new().unwrap();
    let dest = dir.path().join("deep/nested/dir");
    let manifest = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let path = write_manifest(Path::new("/root"), &manifest, &dest, Some("test")).unwrap();
    assert!(path.is_file());
}

// =====================================================================
// manifest_snapshot
// =====================================================================

#[test]
fn manifest_snapshot_creates_manifest_from_files() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"hello");
    create_file(&dir, "b.txt", b"world");
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let result = manifest_snapshot(dir.path(), dir.path(), None, &config, None, false).unwrap();
    assert!(result.is_some());
    let snap = result.unwrap();
    assert!(snap.manifest.is_file());
}

#[test]
fn manifest_snapshot_empty_dir_returns_none() {
    let dir = TempDir::new().unwrap();
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let result = manifest_snapshot(dir.path(), dir.path(), None, &config, None, false).unwrap();
    assert!(result.is_none());
}

#[test]
fn manifest_snapshot_relative_root_produces_relative_paths() {
    use std::path::PathBuf;
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"hello");
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    // Simulate what the CLI does: pass a relative root path
    // glob_files will absolutize it, but hash_files_to_manifest gets the relative string
    let saved_cwd = std::env::current_dir().unwrap();
    std::env::set_current_dir(dir.path()).unwrap();

    let result = manifest_snapshot(&PathBuf::from("."), dir.path(), None, &config, None, false);

    std::env::set_current_dir(&saved_cwd).unwrap();

    let result = result.unwrap();
    assert!(result.is_some());
    let snap = result.unwrap();
    let content = fs::read_to_string(&snap.manifest).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
    let path = parsed["paths"][0]["path"].as_str().unwrap();
    assert_eq!(
        path, "a.txt",
        "manifest path should be relative to root, got: {path}"
    );
}

// =====================================================================
// manifest_diff
// =====================================================================

#[test]
fn manifest_diff_detects_new_file() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "existing.txt", b"old");
    create_file(&dir, "new_file.txt", b"new");

    // Create a manifest with only existing.txt
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let snap = manifest_snapshot(dir.path(), dir.path(), None, &config, None, false)
        .unwrap()
        .unwrap();

    // Add another file
    create_file(&dir, "added.txt", b"added");

    let diff = manifest_diff(&snap.manifest.to_string_lossy(), dir.path(), &config, false).unwrap();
    assert!(diff.new.iter().any(|p| p.contains("added")));
}

// =====================================================================
// manifest_merge
// =====================================================================

#[test]
fn manifest_merge_two_files_produces_result() {
    let dir = TempDir::new().unwrap();

    // Create two manifest files on disk
    let m1 = make_manifest(&[("a.txt", "aa".repeat(16).as_str(), 10, 1000)]);
    let m2 = make_manifest(&[("b.txt", "bb".repeat(16).as_str(), 20, 2000)]);

    let m1_path = dir.path().join("m1.manifest");
    let m2_path = dir.path().join("m2.manifest");
    fs::write(&m1_path, encode_snapshot_v2023(&m1).unwrap()).unwrap();
    fs::write(&m2_path, encode_snapshot_v2023(&m2).unwrap()).unwrap();

    let dest = dir.path().join("output");
    fs::create_dir_all(&dest).unwrap();

    let result = manifest_merge(
        Path::new("/root"),
        &[
            m1_path.to_str().unwrap().into(),
            m2_path.to_str().unwrap().into(),
        ],
        &dest,
        Some("merged"),
    )
    .unwrap();
    assert!(result.is_some());
    let merge = result.unwrap();
    assert!(merge.local_manifest_path.is_file());
}

#[test]
fn manifest_merge_nonexistent_file_returns_error() {
    let dir = TempDir::new().unwrap();
    let result = manifest_merge(
        Path::new("/root"),
        &["/nonexistent/path.manifest".into()],
        dir.path(),
        None,
    );
    assert!(result.is_err());
}

// =====================================================================
// Batch F: F3 — resolve_glob_config missing file gives clear error
// =====================================================================

#[test]
fn resolve_glob_config_missing_file_gives_clear_error() {
    // When a path that looks like a file (contains / or ends in .json) doesn't exist,
    // the error should say "not found" rather than a confusing JSON parse error.
    let result = resolve_glob_config(&[], &[], Some("/nonexistent/path/glob_config.json"));
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not found") || msg.contains("No such file"),
        "Expected 'not found' error for missing file path, got: {msg}"
    );
}

// =====================================================================
// Error case tests for manifest operations (Batch G2)
// =====================================================================

#[test]
fn manifest_snapshot_nonexistent_root_returns_error() {
    let dest = TempDir::new().unwrap();
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let result = manifest_snapshot(
        Path::new("/nonexistent/root"),
        dest.path(),
        None,
        &config,
        None,
        false,
    );
    assert!(result.is_err() || result.unwrap().is_none());
}

#[test]
fn manifest_snapshot_nonexistent_diff_file_returns_error() {
    let dir = TempDir::new().unwrap();
    create_file(&dir, "a.txt", b"data");
    let dest = TempDir::new().unwrap();
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let result = manifest_snapshot(
        dir.path(),
        dest.path(),
        None,
        &config,
        Some("/nonexistent/diff.manifest"),
        false,
    );
    assert!(result.is_err());
}

#[test]
fn manifest_diff_nonexistent_manifest_returns_error() {
    let dir = TempDir::new().unwrap();
    let config = GlobConfig {
        include: vec!["**/*".into()],
        exclude: vec![],
    };
    let result = manifest_diff("/nonexistent/manifest.json", dir.path(), &config, false);
    assert!(result.is_err());
}
