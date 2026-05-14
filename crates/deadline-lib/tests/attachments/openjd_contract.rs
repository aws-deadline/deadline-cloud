//! Contract tests verifying openjd-snapshots produces output compatible with
//! the Deadline Cloud service wire format. These tests were originally in our
//! wrapper layer (asset_manifests.rs) and are retained to catch regressions if
//! openjd's behavior changes in ways that break service compatibility.

use openjd_snapshots::{
    FileEntry, HashAlgorithm, Snapshot, WHOLE_FILE_CHUNK_SIZE, decode_v2023,
    encode_snapshot_v2023, hash,
};

// =========================================================================
// Hashing contract: output format and determinism
// =========================================================================

#[test]
fn hash_data_produces_32_char_hex() {
    let h = hash::hash_data(b"hello world");
    assert_eq!(h.len(), 32);
    assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn hash_data_empty_input_produces_32_char_hex() {
    let h = hash::hash_data(b"");
    assert_eq!(h.len(), 32);
}

#[test]
fn hash_data_is_deterministic() {
    let h1 = hash::hash_data(b"test");
    let h2 = hash::hash_data(b"test");
    assert_eq!(h1, h2);
}

#[test]
fn hash_data_different_inputs_produce_different_hashes() {
    let h1 = hash::hash_data(b"aaa");
    let h2 = hash::hash_data(b"bbb");
    assert_ne!(h1, h2);
}

#[test]
fn hash_file_produces_32_char_hex() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("test.txt");
    std::fs::write(&path, b"hello world").unwrap();

    let h = hash::hash_file(&path).unwrap();
    assert_eq!(h.len(), 32);
    assert!(h.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn hash_file_empty_matches_hash_data_empty() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.txt");
    std::fs::write(&path, b"").unwrap();

    let file_hash = hash::hash_file(&path).unwrap();
    let data_hash = hash::hash_data(b"");
    assert_eq!(file_hash, data_hash);
}

#[test]
fn hash_file_large_matches_hash_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("large.bin");
    let data = vec![0xABu8; 1_000_000];
    std::fs::write(&path, &data).unwrap();

    let file_hash = hash::hash_file(&path).unwrap();
    let data_hash = hash::hash_data(&data);
    assert_eq!(file_hash, data_hash);
}

// =========================================================================
// Encode contract: wire format compatibility with Deadline Cloud service
// =========================================================================

fn make_snapshot(files: Vec<FileEntry>, total_size: u64) -> Snapshot {
    let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    snap.files = files;
    snap.total_size = total_size;
    snap
}

#[test]
fn encode_produces_canonical_json_no_whitespace() {
    let snap = make_snapshot(
        vec![
            {
                let mut e = FileEntry::file("b.txt", 50, 2000);
                e.hash = Some("bbbb".into());
                e
            },
            {
                let mut e = FileEntry::file("a.txt", 50, 1000);
                e.hash = Some("aaaa".into());
                e
            },
        ],
        100,
    );

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    assert!(!encoded.contains(' '), "canonical JSON has no spaces");
    assert!(!encoded.contains('\n'), "canonical JSON has no newlines");
}

#[test]
fn encode_sorts_paths_alphabetically() {
    let snap = make_snapshot(
        vec![
            {
                let mut e = FileEntry::file("b.txt", 50, 2000);
                e.hash = Some("bbbb".into());
                e
            },
            {
                let mut e = FileEntry::file("a.txt", 50, 1000);
                e.hash = Some("aaaa".into());
                e
            },
        ],
        100,
    );

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    let a_pos = encoded.find("a.txt").unwrap();
    let b_pos = encoded.find("b.txt").unwrap();
    assert!(a_pos < b_pos, "paths should be sorted: a.txt before b.txt");
}

#[test]
fn encode_sorts_top_level_keys() {
    let snap = make_snapshot(
        vec![{
            let mut e = FileEntry::file("f.txt", 10, 1000);
            e.hash = Some("aabb".into());
            e
        }],
        10,
    );

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    let hash_pos = encoded.find("\"hashAlg\"").unwrap();
    let version_pos = encoded.find("\"manifestVersion\"").unwrap();
    let paths_pos = encoded.find("\"paths\"").unwrap();
    let total_pos = encoded.find("\"totalSize\"").unwrap();
    assert!(hash_pos < version_pos);
    assert!(version_pos < paths_pos);
    assert!(paths_pos < total_pos);
}

#[test]
fn encode_produces_ascii_only_output() {
    let snap = make_snapshot(
        vec![{
            let mut e = FileEntry::file("日本語.txt", 10, 1000);
            e.hash = Some("aabb".into());
            e
        }],
        10,
    );

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    assert!(
        encoded.is_ascii(),
        "encoded manifest should be ASCII-only, got: {encoded}"
    );
    assert!(encoded.contains("\\u"), "non-ASCII chars should be escaped");
}

#[test]
fn encode_sorts_paths_by_utf16_be_order() {
    let snap = make_snapshot(
        vec![
            {
                let mut e = FileEntry::file("é.txt", 10, 1000);
                e.hash = Some("aaaa".into());
                e
            },
            {
                let mut e = FileEntry::file("a.txt", 10, 1000);
                e.hash = Some("bbbb".into());
                e
            },
        ],
        20,
    );

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    let a_pos = encoded.find("a.txt").unwrap();
    let e_pos = encoded
        .find("\\u00e9")
        .unwrap_or_else(|| encoded.find("\\u00E9").unwrap());
    assert!(
        a_pos < e_pos,
        "a.txt should sort before é.txt in UTF-16 BE order"
    );
}

// =========================================================================
// Decode contract: rejects malformed manifests
// =========================================================================

fn valid_manifest_json() -> String {
    serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 100,
        "paths": [
            {"path": "file1.txt", "hash": "abcdef1234567890abcdef1234567890", "size": 50, "mtime": 1_000_000},
            {"path": "file2.txt", "hash": "1234567890abcdef1234567890abcdef", "size": 50, "mtime": 2_000_000}
        ]
    })
    .to_string()
}

#[test]
fn decode_valid_manifest() {
    let snap = decode_v2023(&valid_manifest_json()).unwrap();
    assert_eq!(snap.hash_alg, HashAlgorithm::Xxh128);
    assert_eq!(snap.total_size, 100);
    assert_eq!(snap.files.len(), 2);
}

#[test]
fn decode_rejects_missing_manifest_version() {
    let json = r#"{"hashAlg":"xxh128","totalSize":0,"paths":[{"path":"f","hash":"abc123","size":0,"mtime":0}]}"#;
    let err = decode_v2023(json).unwrap_err();
    assert!(
        err.to_string().contains("manifestVersion"),
        "expected error about manifestVersion, got: {err}"
    );
}

#[test]
fn decode_rejects_unknown_manifest_version() {
    let json = serde_json::json!({
        "manifestVersion": "2099-01-01",
        "hashAlg": "xxh128",
        "totalSize": 0,
        "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("manifestVersion") || msg.contains("2023-03-03"),
        "expected version error, got: {msg}"
    );
}

#[test]
fn decode_rejects_missing_hash_alg() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "totalSize": 0,
        "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("hashAlg") || err.to_string().contains("hash"),
        "expected hashAlg error, got: {err}"
    );
}

#[test]
fn decode_rejects_missing_paths() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 0
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("paths"),
        "expected paths error, got: {err}"
    );
}

#[test]
fn decode_rejects_missing_total_size() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("totalSize"),
        "expected totalSize error, got: {err}"
    );
}

#[test]
fn decode_rejects_paths_not_a_list() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 0,
        "paths": "not-a-list"
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("paths"),
        "expected paths error, got: {err}"
    );
}

#[test]
fn decode_rejects_path_entry_missing_hash() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 0,
        "paths": [{"path": "f", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("hash"),
        "expected hash error, got: {err}"
    );
}

#[test]
fn decode_rejects_path_size_not_integer() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": 0,
        "paths": [{"path": "f", "hash": "abc123", "size": "not-int", "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("size"),
        "expected size error, got: {err}"
    );
}

#[test]
fn decode_rejects_unsupported_hash_alg() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "sha256",
        "totalSize": 0,
        "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("hash") || msg.contains("sha256"),
        "expected hash algorithm error, got: {msg}"
    );
}

#[test]
fn decode_rejects_total_size_not_integer() {
    let json = serde_json::json!({
        "manifestVersion": "2023-03-03",
        "hashAlg": "xxh128",
        "totalSize": "not-int",
        "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
    })
    .to_string();
    let err = decode_v2023(&json).unwrap_err();
    assert!(
        err.to_string().contains("totalSize"),
        "expected totalSize error, got: {err}"
    );
}

// =========================================================================
// Round-trip contract: encode → decode preserves data
// =========================================================================

#[test]
fn encode_decode_round_trip_preserves_data() {
    let mut snap = Snapshot::new(HashAlgorithm::Xxh128, WHOLE_FILE_CHUNK_SIZE);
    snap.total_size = 150;
    snap.files = vec![
        {
            let mut e = FileEntry::file("dir/file1.exr", 100, 1_710_000_000_000_000);
            e.hash = Some("abcdef1234567890abcdef1234567890".into());
            e
        },
        {
            let mut e = FileEntry::file("dir/file2.exr", 50, 1_710_000_001_000_000);
            e.hash = Some("1234567890abcdef1234567890abcdef".into());
            e
        },
    ];

    let encoded = encode_snapshot_v2023(&snap).unwrap();
    let decoded = decode_v2023(&encoded).unwrap();

    assert_eq!(decoded.hash_alg, snap.hash_alg);
    assert_eq!(decoded.total_size, snap.total_size);
    assert_eq!(decoded.files.len(), snap.files.len());
    for (a, b) in decoded.files.iter().zip(snap.files.iter()) {
        assert_eq!(a.path, b.path);
        assert_eq!(a.hash, b.hash);
        assert_eq!(a.size, b.size);
        assert_eq!(a.mtime, b.mtime);
    }
}
