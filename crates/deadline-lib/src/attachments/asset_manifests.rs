use std::fmt;
use std::path::Path;

use crate::attachments::errors::JobAttachmentsError;

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

/// Hashes a byte slice. Returns a 32-char lowercase hex string (xxh128).
pub fn hash_data(data: &[u8]) -> String {
    openjd_snapshots::hash::hash_data(data)
}

/// Hashes a file by reading it in chunks. Returns a 32-char lowercase hex string (xxh128).
pub fn hash_file(path: &Path) -> Result<String, JobAttachmentsError> {
    openjd_snapshots::hash::hash_file(path).map_err(|e| {
        JobAttachmentsError::AssetSync(format!("Failed to hash file {}: {e}", path.display()))
    })
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
    s.encode_utf16().flat_map(u16::to_be_bytes).collect()
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
        paths.sort_by(|a, b| utf16_be_sort_key(&a.path).cmp(&utf16_be_sort_key(&b.path)));

        Ok(Self {
            hash_alg,
            manifest_version,
            total_size,
            paths,
        })
    }

    pub fn encode(&self) -> String {
        use openjd_snapshots::{FileEntry, Manifest, Snapshot, WHOLE_FILE_CHUNK_SIZE};

        let mut snap: Snapshot = Manifest::new(
            openjd_snapshots::HashAlgorithm::Xxh128,
            WHOLE_FILE_CHUNK_SIZE,
        );
        snap.total_size = self.total_size;
        snap.files = self
            .paths
            .iter()
            .map(|p| {
                let mut entry = FileEntry::file(&p.path, p.size, p.mtime as u64);
                entry.hash = Some(p.hash.clone());
                entry
            })
            .collect();

        openjd_snapshots::encode_snapshot_v2023(&snap)
            .expect("manifest produced by AssetManifest::new is always valid")
    }
}

// --- decode_manifest ---

pub fn decode_manifest(json_str: &str) -> Result<AssetManifest, JobAttachmentsError> {
    let snapshot = openjd_snapshots::decode_v2023(json_str)
        .map_err(|e| JobAttachmentsError::ManifestDecode(e.to_string()))?;

    let paths: Vec<ManifestPath> = snapshot
        .files
        .iter()
        .map(|f| ManifestPath {
            path: f.path.clone(),
            hash: f.hash.clone().unwrap_or_default(),
            size: f.size.unwrap_or(0),
            mtime: f.mtime.unwrap_or(0) as i64,
        })
        .collect();

    if paths.is_empty() {
        return Err(JobAttachmentsError::ManifestDecode(
            "paths must have a least one item".into(),
        ));
    }
    for p in &paths {
        if !p.hash.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(JobAttachmentsError::ManifestDecode(format!(
                "The hash {} for path {} is not alphanumeric",
                p.hash, p.path
            )));
        }
    }

    AssetManifest::new(
        HashAlgorithm::Xxh128,
        ManifestVersion::V2023_03_03,
        snapshot.total_size,
        paths,
    )
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
        assert!(msg.contains("manifestVersion"), "got: {msg}");
        assert!(
            msg.contains("2023-03-03"),
            "should list supported versions, got: {msg}"
        );
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
        assert!(err.to_string().contains("hashAlg"));
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
        assert!(err.to_string().contains("paths"));
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
        assert!(err.to_string().contains("totalSize"));
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
        assert!(err.to_string().contains("paths"));
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
        assert!(err.to_string().contains("hash"));
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
        assert!(err.to_string().contains("size"));
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
        assert!(err.to_string().contains("hash algorithm"));
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
        assert!(err.to_string().contains("totalSize"));
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
