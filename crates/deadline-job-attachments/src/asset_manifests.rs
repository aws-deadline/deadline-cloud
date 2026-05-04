use std::fmt;
use std::io::{BufReader, Read};
use std::path::Path;

use crate::errors::JobAttachmentsError;

// --- HashAlgorithm ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    Xxh128,
}

impl fmt::Display for HashAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl HashAlgorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Xxh128 => "xxh128",
        }
    }
}

impl std::str::FromStr for HashAlgorithm {
    type Err = JobAttachmentsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "xxh128" => Ok(Self::Xxh128),
            other => Err(JobAttachmentsError::UnsupportedHashAlgorithm(format!(
                "Unsupported hashing algorithm provided: {other}"
            ))),
        }
    }
}

// --- Hashing ---

/// Hashes a byte slice with the given algorithm. Returns a 32-char lowercase hex string.
pub fn hash_data(data: &[u8], _alg: HashAlgorithm) -> String {
    format!("{:032x}", xxhash_rust::xxh3::xxh3_128(data))
}

/// Hashes a file by reading it in chunks. Returns a 32-char lowercase hex string.
pub fn hash_file(
    path: &Path,
    _alg: HashAlgorithm,
) -> Result<String, JobAttachmentsError> {
    let file = std::fs::File::open(path).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to open file {}: {e}", path.display()))
    })?;
    let mut reader = BufReader::new(file);
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = reader.read(&mut buf).map_err(|e| {
            JobAttachmentsError::AssetSync(format!(
                "Failed to read file {}: {e}",
                path.display()
            ))
        })?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(format!("{:032x}", hasher.digest128()))
}

// --- ManifestVersion ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestVersion {
    V2023_03_03,
}

impl fmt::Display for ManifestVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::V2023_03_03 => write!(f, "2023-03-03"),
        }
    }
}

impl std::str::FromStr for ManifestVersion {
    type Err = JobAttachmentsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "2023-03-03" => Ok(Self::V2023_03_03),
            other => Err(JobAttachmentsError::ManifestDecode(format!(
                "Unknown manifest version: {other} (Currently supported Manifest versions: 2023-03-03)"
            ))),
        }
    }
}

// --- ManifestPath ---

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestPath {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mtime: i64,
}

// --- AssetManifest ---

#[derive(Debug, Clone)]
pub struct AssetManifest {
    pub hash_alg: HashAlgorithm,
    pub manifest_version: ManifestVersion,
    pub total_size: u64,
    pub paths: Vec<ManifestPath>,
}

/// Sort key for canonical path ordering per RFC 8785: UTF-16 BE bytes.
fn utf16_be_sort_key(s: &str) -> Vec<u8> {
    s.encode_utf16()
        .flat_map(u16::to_be_bytes)
        .collect()
}

/// Escape non-ASCII characters to `\uXXXX` sequences (equivalent to Python's
/// `json.dumps(..., ensure_ascii=True)`). Handles surrogate pairs for chars above U+FFFF.
pub(crate) fn escape_to_ascii(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii() {
            out.push(ch);
        } else {
            // Encode as UTF-16 units (handles surrogate pairs for chars > U+FFFF)
            let mut buf = [0u16; 2];
            let encoded = ch.encode_utf16(&mut buf);
            for &unit in encoded.iter() {
                use std::fmt::Write;
                write!(out, "\\u{unit:04x}").expect("write to String");
            }
        }
    }
    out
}

impl AssetManifest {
    pub fn new(
        hash_alg: HashAlgorithm,
        manifest_version: ManifestVersion,
        total_size: u64,
        mut paths: Vec<ManifestPath>,
    ) -> Result<Self, JobAttachmentsError> {
        // Sort paths by canonical UTF-16 BE ordering at construction time.
        // This fixes the Python bug where paths are sorted twice (reverse lex, then UTF-16 BE).
        paths.sort_by(|a, b| utf16_be_sort_key(&a.path).cmp(&utf16_be_sort_key(&b.path)));

        Ok(Self {
            hash_alg,
            manifest_version,
            total_size,
            paths,
        })
    }

    pub fn encode(&self) -> String {
        // Build JSON with sorted keys using BTreeMap (not serde_json::Map
        // which uses IndexMap with preserve_order feature)
        use std::collections::BTreeMap;

        let mut map = BTreeMap::new();
        map.insert(
            "hashAlg",
            serde_json::Value::String(self.hash_alg.to_string()),
        );
        map.insert(
            "manifestVersion",
            serde_json::Value::String(self.manifest_version.to_string()),
        );

        // Paths are already sorted at construction time
        let paths_json: Vec<serde_json::Value> = self
            .paths
            .iter()
            .map(|p| {
                let mut m = BTreeMap::new();
                m.insert("hash", serde_json::Value::String(p.hash.clone()));
                m.insert("mtime", serde_json::Value::Number(p.mtime.into()));
                m.insert("path", serde_json::Value::String(p.path.clone()));
                m.insert("size", serde_json::Value::Number(p.size.into()));
                serde_json::to_value(m).expect("JSON serialization")
            })
            .collect();
        map.insert("paths", serde_json::Value::Array(paths_json));
        map.insert(
            "totalSize",
            serde_json::Value::Number(self.total_size.into()),
        );

        let json = serde_json::to_string(&map).expect("JSON serialization");

        // Apply ensure_ascii: escape non-ASCII characters
        escape_to_ascii(&json)
    }
}

// --- decode_manifest ---

pub fn decode_manifest(json_str: &str) -> Result<AssetManifest, JobAttachmentsError> {
    let doc: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| JobAttachmentsError::ManifestDecode(format!("Invalid JSON: {e}")))?;

    let obj = doc
        .as_object()
        .ok_or_else(|| JobAttachmentsError::ManifestDecode("Manifest must be a JSON object".into()))?;

    // Check manifestVersion
    let version_val = obj.get("manifestVersion").ok_or_else(|| {
        JobAttachmentsError::ManifestDecode(
            "Manifest is missing the required \"manifestVersion\" field".into(),
        )
    })?;
    let version_str = version_val.as_str().ok_or_else(|| {
        JobAttachmentsError::ManifestDecode("manifestVersion must be a string".into())
    })?;
    let version: ManifestVersion = version_str.parse()?;

    // Validate required fields
    let mut missing = Vec::new();
    if !obj.contains_key("hashAlg") {
        missing.push("hashAlg");
    }
    if !obj.contains_key("paths") {
        missing.push("paths");
    }
    if !obj.contains_key("totalSize") {
        missing.push("totalSize");
    }
    if !missing.is_empty() {
        return Err(JobAttachmentsError::ManifestDecode(format!(
            "manifest is missing required field(s) {missing:?}"
        )));
    }

    // Validate hashAlg
    let hash_alg_str = obj["hashAlg"]
        .as_str()
        .ok_or_else(|| JobAttachmentsError::ManifestDecode("hashAlg must be a string".into()))?;
    if hash_alg_str != "xxh128" {
        return Err(JobAttachmentsError::ManifestDecode("hashAlg must be one of {\"xxh128\"}".to_owned()));
    }
    let hash_alg: HashAlgorithm = hash_alg_str.parse()?;

    // Validate totalSize
    let total_size = obj["totalSize"]
        .as_u64()
        .ok_or_else(|| JobAttachmentsError::ManifestDecode("totalSize must be a non-negative integer".into()))?;

    // Validate paths
    let paths_val = &obj["paths"];
    let paths_arr = paths_val
        .as_array()
        .ok_or_else(|| JobAttachmentsError::ManifestDecode("paths must be a list".into()))?;
    if paths_arr.is_empty() {
        return Err(JobAttachmentsError::ManifestDecode(
            "paths must have a least one item".into(),
        ));
    }

    let mut paths = Vec::with_capacity(paths_arr.len());
    for entry in paths_arr {
        let entry_obj = entry.as_object().ok_or_else(|| {
            JobAttachmentsError::ManifestDecode("path entry must be an object".into())
        })?;

        // Check required path fields
        let mut path_missing = Vec::new();
        for field in &["path", "hash", "size", "mtime"] {
            if !entry_obj.contains_key(*field) {
                path_missing.push(*field);
            }
        }
        if !path_missing.is_empty() {
            return Err(JobAttachmentsError::ManifestDecode(format!(
                "path is missing required field(s) {path_missing:?}"
            )));
        }

        let path = entry_obj["path"]
            .as_str()
            .ok_or_else(|| JobAttachmentsError::ManifestDecode("path must be a string".into()))?;
        let hash = entry_obj["hash"]
            .as_str()
            .ok_or_else(|| JobAttachmentsError::ManifestDecode("hash must be a string".into()))?;
        let size = entry_obj["size"]
            .as_u64()
            .ok_or_else(|| JobAttachmentsError::ManifestDecode("size must be a non-negative integer".into()))?;
        let mtime = entry_obj["mtime"]
            .as_i64()
            .ok_or_else(|| JobAttachmentsError::ManifestDecode("mtime must be an integer".into()))?;

        paths.push(ManifestPath {
            path: path.to_owned(),
            hash: hash.to_owned(),
            size,
            mtime,
        });
    }

    // Validate hashes are alphanumeric
    for p in &paths {
        if !p.hash.chars().all(|c: char| c.is_ascii_alphanumeric()) {
            return Err(JobAttachmentsError::ManifestDecode(format!(
                "The hash {} for path {} is not alphanumeric",
                p.hash, p.path
            )));
        }
    }

    AssetManifest::new(hash_alg, version, total_size, paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    // === : HashAlgorithm ===

    #[test]
    fn hash_algorithm_from_str_xxh128() {
        let alg: HashAlgorithm = "xxh128".parse().unwrap();
        assert_eq!(alg, HashAlgorithm::Xxh128);
    }

    #[test]
    fn hash_algorithm_from_str_unsupported() {
        let err = "sha256".parse::<HashAlgorithm>().unwrap_err();
        assert!(err.to_string().contains("sha256"));
    }

    #[test]
    fn hash_algorithm_display() {
        assert_eq!(HashAlgorithm::Xxh128.to_string(), "xxh128");
    }

    // === : hash_file ===

    #[test]
    fn hash_file_basic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        std::fs::write(&path, b"hello world").unwrap();

        let hash = hash_file(&path, HashAlgorithm::Xxh128).unwrap();
        assert_eq!(hash.len(), 32);
        assert!(hash.chars().all(|c: char| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.txt");
        std::fs::write(&path, b"").unwrap();

        let hash = hash_file(&path, HashAlgorithm::Xxh128).unwrap();
        let expected = hash_data(b"", HashAlgorithm::Xxh128);
        assert_eq!(hash, expected);
    }

    #[test]
    fn hash_file_large_multi_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");
        let data = vec![0xABu8; 1_000_000];
        std::fs::write(&path, &data).unwrap();

        let file_hash = hash_file(&path, HashAlgorithm::Xxh128).unwrap();
        let data_hash = hash_data(&data, HashAlgorithm::Xxh128);
        assert_eq!(file_hash, data_hash);
    }

    // === : hash_data ===

    #[test]
    fn hash_data_basic() {
        let hash = hash_data(b"hello world", HashAlgorithm::Xxh128);
        assert_eq!(hash.len(), 32);
        assert!(hash.chars().all(|c: char| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_data_empty() {
        let hash = hash_data(b"", HashAlgorithm::Xxh128);
        assert_eq!(hash.len(), 32);
    }

    #[test]
    fn hash_data_deterministic() {
        let h1 = hash_data(b"test", HashAlgorithm::Xxh128);
        let h2 = hash_data(b"test", HashAlgorithm::Xxh128);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_data_different_inputs_different_hashes() {
        let h1 = hash_data(b"aaa", HashAlgorithm::Xxh128);
        let h2 = hash_data(b"bbb", HashAlgorithm::Xxh128);
        assert_ne!(h1, h2);
    }

    // === : ManifestVersion ===

    #[test]
    fn manifest_version_from_str() {
        let v: ManifestVersion = "2023-03-03".parse().unwrap();
        assert_eq!(v, ManifestVersion::V2023_03_03);
    }

    #[test]
    fn manifest_version_display() {
        assert_eq!(ManifestVersion::V2023_03_03.to_string(), "2023-03-03");
    }

    // === : decode_manifest ===

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
    fn decode_manifest_valid() {
        let manifest = decode_manifest(&valid_manifest_json()).unwrap();
        assert_eq!(manifest.hash_alg, HashAlgorithm::Xxh128);
        assert_eq!(manifest.manifest_version, ManifestVersion::V2023_03_03);
        assert_eq!(manifest.total_size, 100);
        assert_eq!(manifest.paths.len(), 2);
    }

    #[test]
    fn decode_manifest_missing_version() {
        let json = r#"{"hashAlg":"xxh128","totalSize":0,"paths":[{"path":"f","hash":"abc123","size":0,"mtime":0}]}"#;
        let err = decode_manifest(json).unwrap_err();
        assert!(
            err.to_string().contains("manifestVersion"),
            "expected error about manifestVersion, got: {err}"
        );
    }

    #[test]
    fn decode_manifest_unknown_version() {
        let json = serde_json::json!({
            "manifestVersion": "2099-01-01",
            "hashAlg": "xxh128",
            "totalSize": 0,
            "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Unknown manifest version"), "got: {msg}");
        assert!(msg.contains("2023-03-03"), "should list supported versions, got: {msg}");
    }

    #[test]
    fn decode_manifest_missing_hash_alg() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "totalSize": 0,
            "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("missing required field"));
    }

    #[test]
    fn decode_manifest_missing_paths() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 0
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("missing required field"));
    }

    #[test]
    fn decode_manifest_missing_total_size() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("missing required field"));
    }

    #[test]
    fn decode_manifest_empty_paths() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 0,
            "paths": []
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("a least one item"));
    }

    #[test]
    fn decode_manifest_paths_not_a_list() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 0,
            "paths": "not-a-list"
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("must be a list"));
    }

    #[test]
    fn decode_manifest_path_entry_missing_hash() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 0,
            "paths": [{"path": "f", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("missing required field"));
    }

    #[test]
    fn decode_manifest_path_size_not_integer() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 0,
            "paths": [{"path": "f", "hash": "abc123", "size": "not-int", "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("size must be a non-negative integer"));
    }

    #[test]
    fn decode_manifest_hash_not_alphanumeric() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": 10,
            "paths": [{"path": "f", "hash": "abc/def", "size": 10, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("not alphanumeric"));
    }

    #[test]
    fn decode_manifest_unsupported_hash_alg() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "sha256",
            "totalSize": 0,
            "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("hashAlg"));
    }

    #[test]
    fn decode_manifest_total_size_not_integer() {
        let json = serde_json::json!({
            "manifestVersion": "2023-03-03",
            "hashAlg": "xxh128",
            "totalSize": "not-int",
            "paths": [{"path": "f", "hash": "abc123", "size": 0, "mtime": 0}]
        })
        .to_string();
        let err = decode_manifest(&json).unwrap_err();
        assert!(err.to_string().contains("totalSize must be a non-negative integer"));
    }

    // === : AssetManifest::encode ===

    #[test]
    fn encode_manifest_canonical_json() {
        let manifest = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            100,
            vec![
                ManifestPath {
                    path: "b.txt".into(),
                    hash: "bbbb".into(),
                    size: 50,
                    mtime: 2000,
                },
                ManifestPath {
                    path: "a.txt".into(),
                    hash: "aaaa".into(),
                    size: 50,
                    mtime: 1000,
                },
            ],
        )
        .unwrap();

        let encoded = manifest.encode();
        assert!(!encoded.contains(' '));
        assert!(!encoded.contains('\n'));
        let a_pos = encoded.find("a.txt").unwrap();
        let b_pos = encoded.find("b.txt").unwrap();
        assert!(a_pos < b_pos, "paths should be sorted: a.txt before b.txt");
    }

    #[test]
    fn encode_manifest_sorted_keys() {
        let manifest = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            10,
            vec![ManifestPath {
                path: "f.txt".into(),
                hash: "aabb".into(),
                size: 10,
                mtime: 1000,
            }],
        )
        .unwrap();

        let encoded = manifest.encode();
        let hash_pos = encoded.find("\"hashAlg\"").unwrap();
        let version_pos = encoded.find("\"manifestVersion\"").unwrap();
        let paths_pos = encoded.find("\"paths\"").unwrap();
        let total_pos = encoded.find("\"totalSize\"").unwrap();
        assert!(hash_pos < version_pos);
        assert!(version_pos < paths_pos);
        assert!(paths_pos < total_pos);
    }

    #[test]
    fn encode_manifest_ascii_output() {
        let manifest = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            10,
            vec![ManifestPath {
                path: "日本語.txt".into(),
                hash: "aabb".into(),
                size: 10,
                mtime: 1000,
            }],
        )
        .unwrap();

        let encoded = manifest.encode();
        assert!(
            encoded.is_ascii(),
            "encoded manifest should be ASCII-only, got: {encoded}"
        );
        assert!(encoded.contains("\\u"));
    }

    #[test]
    fn encode_manifest_utf16_be_path_sort() {
        let manifest = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            20,
            vec![
                ManifestPath {
                    path: "é.txt".into(),
                    hash: "aaaa".into(),
                    size: 10,
                    mtime: 1000,
                },
                ManifestPath {
                    path: "a.txt".into(),
                    hash: "bbbb".into(),
                    size: 10,
                    mtime: 1000,
                },
            ],
        )
        .unwrap();

        let encoded = manifest.encode();
        let a_pos = encoded.find("a.txt").unwrap();
        let e_pos = encoded.find("\\u00e9").unwrap_or_else(|| encoded.find("\\u00E9").unwrap());
        assert!(a_pos < e_pos, "a.txt should sort before é.txt in UTF-16 BE");
    }

    // === : AssetManifest::new validation ===

    #[test]
    fn asset_manifest_new_unsupported_hash_alg() {
        let result = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            0,
            vec![ManifestPath {
                path: "f".into(),
                hash: "aa".into(),
                size: 0,
                mtime: 0,
            }],
        );
        assert!(result.is_ok());
    }

    // === : Round-trip encode/decode ===

    #[test]
    fn manifest_encode_decode_round_trip() {
        let original = AssetManifest::new(
            HashAlgorithm::Xxh128,
            ManifestVersion::V2023_03_03,
            150,
            vec![
                ManifestPath {
                    path: "dir/file1.exr".into(),
                    hash: "abcdef1234567890abcdef1234567890".into(),
                    size: 100,
                    mtime: 1_710_000_000_000_000,
                },
                ManifestPath {
                    path: "dir/file2.exr".into(),
                    hash: "1234567890abcdef1234567890abcdef".into(),
                    size: 50,
                    mtime: 1_710_000_001_000_000,
                },
            ],
        )
        .unwrap();

        let encoded = original.encode();
        let decoded = decode_manifest(&encoded).unwrap();

        assert_eq!(decoded.hash_alg, original.hash_alg);
        assert_eq!(decoded.manifest_version, original.manifest_version);
        assert_eq!(decoded.total_size, original.total_size);
        assert_eq!(decoded.paths.len(), original.paths.len());
        for (a, b) in decoded.paths.iter().zip(original.paths.iter()) {
            assert_eq!(a.path, b.path);
            assert_eq!(a.hash, b.hash);
            assert_eq!(a.size, b.size);
            assert_eq!(a.mtime, b.mtime);
        }
    }
}
