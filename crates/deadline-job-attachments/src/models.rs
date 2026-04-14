use deadline_models::errors::JobAttachmentsError;
pub use deadline_models::job_attachments::FileSystemLocationType;
use deadline_models::job_attachments::JobAttachmentsFileSystem;
use deadline_models::path_format::PathFormat;

use crate::asset_manifests::{hash_data, HashAlgorithm};

// --- Helper functions ---

pub fn join_s3_paths(parts: &[&str]) -> String {
    parts.join("/")
}

pub fn generate_random_guid() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Converts a Unix timestamp (seconds as f64) to ISO 8601 datetime string.
/// Format: `2024-01-15T10:30:00.123456Z` (always 6 decimal places).
/// Matches Python's `_float_to_iso_datetime_string`.
pub fn float_to_iso_datetime_string(time: f64) -> String {
    let seconds = time as i64;
    let microseconds = ((time - seconds as f64) * 1_000_000.0) as u32;
    let dt = chrono::DateTime::from_timestamp(seconds, microseconds * 1000)
        .unwrap_or_default();
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

// --- StorageProfileOperatingSystemFamily ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageProfileOperatingSystemFamily {
    Windows,
    Linux,
    Macos,
}

impl std::str::FromStr for StorageProfileOperatingSystemFamily {
    type Err = JobAttachmentsError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "windows" => Ok(Self::Windows),
            "linux" => Ok(Self::Linux),
            "macos" => Ok(Self::Macos),
            other => Err(JobAttachmentsError::AssetSync(format!(
                "Unsupported OS family: {other}"
            ))),
        }
    }
}

impl StorageProfileOperatingSystemFamily {
    pub fn host() -> Self {
        if cfg!(target_os = "windows") {
            Self::Windows
        } else if cfg!(target_os = "macos") {
            Self::Macos
        } else {
            Self::Linux
        }
    }
}

// --- PathFormat extensions ---

/// Extension trait adding host detection to PathFormat from deadline-models.
pub trait PathFormatExt {
    fn host() -> PathFormat;
    fn as_str(&self) -> &'static str;
}

impl PathFormatExt for PathFormat {
    fn host() -> PathFormat {
        if cfg!(target_os = "windows") {
            PathFormat::Windows
        } else {
            PathFormat::Posix
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            PathFormat::Posix => "posix",
            PathFormat::Windows => "windows",
        }
    }
}

// --- JobAttachmentS3Settings ---

#[derive(Debug, Clone)]
pub struct JobAttachmentS3Settings {
    pub s3_bucket_name: String,
    pub root_prefix: String,
}

impl JobAttachmentS3Settings {
    pub fn from_root_path(root_path: &str) -> Result<Self, JobAttachmentsError> {
        let (bucket, prefix) = root_path.split_once('/').ok_or_else(|| {
            JobAttachmentsError::MalformedAttachment(
                "Invalid root path format, should be s3BucketName/rootPrefix.".into(),
            )
        })?;
        Ok(Self {
            s3_bucket_name: bucket.into(),
            root_prefix: prefix.into(),
        })
    }

    pub fn from_s3_root_uri(uri: &str) -> Result<Self, JobAttachmentsError> {
        let rest = uri.strip_prefix("s3://").ok_or_else(|| {
            JobAttachmentsError::MalformedAttachment(
                "Invalid root uri format, should be s3://s3BucketName/rootPrefix.".into(),
            )
        })?;
        let (bucket, prefix) = rest.split_once('/').ok_or_else(|| {
            JobAttachmentsError::MalformedAttachment(
                "Invalid root uri format, should be s3://s3BucketName/rootPrefix.".into(),
            )
        })?;
        if bucket.is_empty() || prefix.is_empty() {
            return Err(JobAttachmentsError::MalformedAttachment(
                "Invalid root uri format, should be s3://s3BucketName/rootPrefix.".into(),
            ));
        }
        Ok(Self {
            s3_bucket_name: bucket.into(),
            root_prefix: prefix.into(),
        })
    }

    pub fn to_root_path(&self) -> String {
        join_s3_paths(&[&self.s3_bucket_name, &self.root_prefix])
    }

    pub fn to_s3_root_uri(&self) -> String {
        format!("s3://{}", self.to_root_path())
    }

    fn validate_root_prefix(&self) -> Result<(), JobAttachmentsError> {
        if self.root_prefix.is_empty() {
            return Err(JobAttachmentsError::MissingRootPrefix(
                "Missing S3 root prefix".into(),
            ));
        }
        Ok(())
    }

    pub fn full_cas_prefix(&self) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[&self.root_prefix, "Data"]))
    }

    pub fn full_job_output_prefix(
        &self,
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
    ) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[
            &self.root_prefix,
            "Manifests",
            farm_id,
            queue_id,
            job_id,
        ]))
    }

    pub fn full_step_output_prefix(
        &self,
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: &str,
    ) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[
            &self.root_prefix,
            "Manifests",
            farm_id,
            queue_id,
            job_id,
            step_id,
        ]))
    }

    pub fn full_task_output_prefix(
        &self,
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: &str,
        task_id: &str,
    ) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[
            &self.root_prefix,
            "Manifests",
            farm_id,
            queue_id,
            job_id,
            step_id,
            task_id,
        ]))
    }

    pub fn full_output_prefix(
        &self,
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: &str,
        task_id: &str,
        session_action_id: &str,
    ) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[
            &self.root_prefix,
            "Manifests",
            farm_id,
            queue_id,
            job_id,
            step_id,
            task_id,
            session_action_id,
        ]))
    }

    pub fn partial_manifest_prefix(&self, farm_id: &str, queue_id: &str) -> String {
        let guid = generate_random_guid();
        join_s3_paths(&[farm_id, queue_id, "Inputs", &guid])
    }

    /// Builds the partial S3 prefix for output manifests with task_id.
    /// Pattern: `farm/queue/job/step/task/{iso_time}_{session_action_id}`
    pub fn partial_session_action_manifest_prefix(
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: &str,
        task_id: &str,
        session_action_id: &str,
        time: f64,
    ) -> String {
        let ts = float_to_iso_datetime_string(time);
        let last_segment = format!("{ts}_{session_action_id}");
        join_s3_paths(&[farm_id, queue_id, job_id, step_id, task_id, &last_segment])
    }

    /// Builds the partial S3 prefix for output manifests without task_id (task chunking).
    /// Pattern: `farm/queue/job/step/{iso_time}_{session_action_id}`
    pub fn partial_session_action_manifest_prefix_without_task(
        farm_id: &str,
        queue_id: &str,
        job_id: &str,
        step_id: &str,
        session_action_id: &str,
        time: f64,
    ) -> String {
        let ts = float_to_iso_datetime_string(time);
        let last_segment = format!("{ts}_{session_action_id}");
        join_s3_paths(&[farm_id, queue_id, job_id, step_id, &last_segment])
    }

    pub fn add_root_and_manifest_folder_prefix(
        &self,
        path: &str,
    ) -> Result<String, JobAttachmentsError> {
        self.validate_root_prefix()?;
        Ok(join_s3_paths(&[&self.root_prefix, "Manifests", path]))
    }
}

// --- ManifestProperties ---

#[derive(Debug, Clone)]
pub struct ManifestProperties {
    pub root_path: String,
    pub root_path_format: PathFormat,
    pub file_system_location_name: Option<String>,
    pub input_manifest_path: Option<String>,
    pub input_manifest_hash: Option<String>,
    pub output_relative_directories: Option<Vec<String>>,
}

impl ManifestProperties {
    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert(
            "rootPath".into(),
            serde_json::Value::String(self.root_path.clone()),
        );
        if let Some(ref name) = self.file_system_location_name {
            map.insert(
                "fileSystemLocationName".into(),
                serde_json::Value::String(name.clone()),
            );
        }
        map.insert(
            "rootPathFormat".into(),
            serde_json::Value::String(self.root_path_format.as_str().into()),
        );
        if let Some(ref path) = self.input_manifest_path {
            map.insert(
                "inputManifestPath".into(),
                serde_json::Value::String(path.clone()),
            );
        }
        if let Some(ref hash) = self.input_manifest_hash {
            map.insert(
                "inputManifestHash".into(),
                serde_json::Value::String(hash.clone()),
            );
        }
        if let Some(ref dirs) = self.output_relative_directories {
            map.insert(
                "outputRelativeDirectories".into(),
                serde_json::Value::Array(
                    dirs.iter()
                        .map(|d| serde_json::Value::String(d.clone()))
                        .collect(),
                ),
            );
        }
        serde_json::Value::Object(map)
    }

    pub fn as_output_metadata(&self) -> serde_json::Value {
        let mut metadata = serde_json::Map::new();

        if self.root_path.is_ascii() {
            metadata.insert(
                "asset-root".into(),
                serde_json::Value::String(self.root_path.clone()),
            );
        } else {
            // S3 metadata must be ASCII. JSON-encode with \u escapes.
            let json_root = serde_json::to_string(&self.root_path).unwrap();
            // serde_json outputs UTF-8 by default; we need ASCII \u escapes
            let ascii_json = crate::asset_manifests::escape_to_ascii(&json_root);
            metadata.insert(
                "asset-root-json".into(),
                serde_json::Value::String(ascii_json.clone()),
            );
            metadata.insert(
                "asset-root".into(),
                serde_json::Value::String(ascii_json),
            );
        }

        if let Some(ref name) = self.file_system_location_name {
            metadata.insert(
                "file-system-location-name".into(),
                serde_json::Value::String(name.clone()),
            );
        }

        let mut outer = serde_json::Map::new();
        outer.insert(
            "Metadata".into(),
            serde_json::Value::Object(metadata),
        );
        serde_json::Value::Object(outer)
    }
}

// --- Attachments ---

#[derive(Debug, Clone)]
pub struct Attachments {
    pub manifests: Vec<ManifestProperties>,
    pub file_system: JobAttachmentsFileSystem,
}

impl Default for Attachments {
    fn default() -> Self {
        Self {
            manifests: Vec::new(),
            file_system: JobAttachmentsFileSystem::Copied,
        }
    }
}

impl Attachments {
    pub fn to_json(&self) -> serde_json::Value {
        let fs_str = match self.file_system {
            JobAttachmentsFileSystem::Copied => "COPIED",
            JobAttachmentsFileSystem::Virtual => "VIRTUAL",
        };
        serde_json::json!({
            "manifests": self.manifests.iter().map(|m| m.to_json()).collect::<Vec<_>>(),
            "fileSystem": fs_str,
        })
    }
}

// --- PathMappingRule ---

#[derive(Debug, Clone)]
pub struct PathMappingRule {
    pub source_path_format: String,
    pub source_path: String,
    pub destination_path: String,
}

impl PathMappingRule {
    pub fn get_hashed_source_path(&self, alg: HashAlgorithm) -> String {
        hash_data(self.source_path.as_bytes(), alg)
    }
}

// --- FileConflictResolution ---

/// How to resolve conflicts when a downloaded file already exists locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileConflictResolution {
    Skip,
    Overwrite,
    CreateCopy,
}

// --- UploadManifestInfo ---

/// Result of uploading a single manifest via `attachment_upload`.
#[derive(Debug, Clone)]
pub struct UploadManifestInfo {
    pub output_manifest_path: String,
    pub output_manifest_hash: String,
    pub source_path: Option<String>,
}

// --- FileSystemLocation ---

#[derive(Debug, Clone)]
pub struct FileSystemLocation {
    pub name: String,
    pub path: String,
    pub location_type: FileSystemLocationType,
}

// --- StorageProfile ---

#[derive(Debug, Clone)]
pub struct StorageProfile {
    pub storage_profile_id: String,
    pub display_name: String,
    pub os_family: StorageProfileOperatingSystemFamily,
    pub file_system_locations: Vec<FileSystemLocation>,
}

// --- AssetRootGroup ---

#[derive(Debug, Clone)]
pub struct AssetRootGroup {
    pub file_system_location_name: Option<String>,
    pub root_path: String,
    pub inputs: std::collections::BTreeSet<std::path::PathBuf>,
    pub outputs: std::collections::BTreeSet<std::path::PathBuf>,
    pub references: std::collections::BTreeSet<std::path::PathBuf>,
}

// --- AssetUploadGroup ---

#[derive(Debug, Clone)]
pub struct AssetUploadGroup {
    pub asset_groups: Vec<AssetRootGroup>,
    pub total_input_files: u64,
    pub total_input_bytes: u64,
}

// --- AssetRootManifest ---

#[derive(Debug, Clone)]
pub struct AssetRootManifest {
    pub file_system_location_name: Option<String>,
    pub root_path: String,
    pub asset_manifest: Option<crate::asset_manifests::AssetManifest>,
    pub outputs: Vec<std::path::PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // === : JobAttachmentS3Settings ===

    #[test]
    fn s3_settings_from_root_path_simple() {
        let s = JobAttachmentS3Settings::from_root_path("my-bucket/my-prefix").unwrap();
        assert_eq!(s.s3_bucket_name, "my-bucket");
        assert_eq!(s.root_prefix, "my-prefix");
    }

    #[test]
    fn s3_settings_from_root_path_deep_prefix() {
        let s = JobAttachmentS3Settings::from_root_path("my-bucket/deep/nested/prefix").unwrap();
        assert_eq!(s.s3_bucket_name, "my-bucket");
        assert_eq!(s.root_prefix, "deep/nested/prefix");
    }

    #[test]
    fn s3_settings_from_root_path_no_slash_errors() {
        let err = JobAttachmentS3Settings::from_root_path("no-slash").unwrap_err();
        assert!(err.to_string().contains("Invalid root path format"));
    }

    #[test]
    fn s3_settings_from_s3_root_uri_valid() {
        let s = JobAttachmentS3Settings::from_s3_root_uri("s3://my-bucket/my-prefix").unwrap();
        assert_eq!(s.s3_bucket_name, "my-bucket");
        assert_eq!(s.root_prefix, "my-prefix");
    }

    #[test]
    fn s3_settings_from_s3_root_uri_wrong_scheme_errors() {
        let err =
            JobAttachmentS3Settings::from_s3_root_uri("https://my-bucket/my-prefix").unwrap_err();
        assert!(err.to_string().contains("Invalid root uri format"));
    }

    #[test]
    fn s3_settings_from_s3_root_uri_no_prefix_errors() {
        let err = JobAttachmentS3Settings::from_s3_root_uri("s3://my-bucket").unwrap_err();
        assert!(err.to_string().contains("Invalid root uri format"));
    }

    #[test]
    fn s3_settings_to_root_path() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "bucket".into(),
            root_prefix: "prefix".into(),
        };
        assert_eq!(s.to_root_path(), "bucket/prefix");
    }

    #[test]
    fn s3_settings_to_s3_root_uri() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "bucket".into(),
            root_prefix: "prefix".into(),
        };
        assert_eq!(s.to_s3_root_uri(), "s3://bucket/prefix");
    }

    #[test]
    fn s3_settings_full_cas_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "bucket".into(),
            root_prefix: "prefix".into(),
        };
        assert_eq!(s.full_cas_prefix().unwrap(), "prefix/Data");
    }

    #[test]
    fn s3_settings_full_cas_prefix_empty_root_errors() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "bucket".into(),
            root_prefix: "".into(),
        };
        let err = s.full_cas_prefix().unwrap_err();
        assert!(err.to_string().contains("Missing S3 root prefix"));
    }

    #[test]
    fn s3_settings_full_job_output_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        assert_eq!(
            s.full_job_output_prefix("farm-1", "queue-1", "job-1")
                .unwrap(),
            "rp/Manifests/farm-1/queue-1/job-1"
        );
    }

    #[test]
    fn s3_settings_full_step_output_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        assert_eq!(
            s.full_step_output_prefix("f", "q", "j", "s").unwrap(),
            "rp/Manifests/f/q/j/s"
        );
    }

    #[test]
    fn s3_settings_full_task_output_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        assert_eq!(
            s.full_task_output_prefix("f", "q", "j", "s", "t").unwrap(),
            "rp/Manifests/f/q/j/s/t"
        );
    }

    #[test]
    fn s3_settings_full_output_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        assert_eq!(
            s.full_output_prefix("f", "q", "j", "s", "t", "sa")
                .unwrap(),
            "rp/Manifests/f/q/j/s/t/sa"
        );
    }

    #[test]
    fn s3_settings_partial_manifest_prefix_has_guid() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        let prefix = s.partial_manifest_prefix("farm-1", "queue-1");
        assert!(prefix.starts_with("farm-1/queue-1/Inputs/"));
        let guid = prefix.rsplit('/').next().unwrap();
        assert_eq!(guid.len(), 32);
        assert!(guid.chars().all(|c: char| c.is_ascii_hexdigit()));
    }

    #[test]
    fn s3_settings_add_root_and_manifest_folder_prefix() {
        let s = JobAttachmentS3Settings {
            s3_bucket_name: "b".into(),
            root_prefix: "rp".into(),
        };
        assert_eq!(
            s.add_root_and_manifest_folder_prefix("some/path").unwrap(),
            "rp/Manifests/some/path"
        );
    }

    // === : ManifestProperties ===

    #[test]
    fn manifest_properties_to_json_all_fields() {
        let mp = ManifestProperties {
            root_path: "/tmp/root".into(),
            root_path_format: PathFormat::Posix,
            file_system_location_name: Some("loc1".into()),
            input_manifest_path: Some("manifests/abc".into()),
            input_manifest_hash: Some("deadbeef".into()),
            output_relative_directories: Some(vec!["out1".into(), "out2".into()]),
        };
        let json = mp.to_json();
        assert_eq!(json["rootPath"], "/tmp/root");
        assert_eq!(json["rootPathFormat"], "posix");
        assert_eq!(json["fileSystemLocationName"], "loc1");
        assert_eq!(json["inputManifestPath"], "manifests/abc");
        assert_eq!(json["inputManifestHash"], "deadbeef");
        assert_eq!(json["outputRelativeDirectories"][0], "out1");
    }

    #[test]
    fn manifest_properties_to_json_optional_fields_omitted() {
        let mp = ManifestProperties {
            root_path: "/tmp/root".into(),
            root_path_format: PathFormat::Posix,
            file_system_location_name: None,
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: None,
        };
        let json = mp.to_json();
        assert_eq!(json["rootPath"], "/tmp/root");
        assert_eq!(json["rootPathFormat"], "posix");
        assert!(json.get("fileSystemLocationName").is_none());
        assert!(json.get("inputManifestPath").is_none());
        assert!(json.get("inputManifestHash").is_none());
        assert!(json.get("outputRelativeDirectories").is_none());
    }

    #[test]
    fn manifest_properties_as_output_metadata_ascii() {
        let mp = ManifestProperties {
            root_path: "/tmp/root".into(),
            root_path_format: PathFormat::Posix,
            file_system_location_name: None,
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: None,
        };
        let meta = mp.as_output_metadata();
        let metadata = meta["Metadata"].as_object().unwrap();
        assert_eq!(metadata["asset-root"], "/tmp/root");
        assert!(!metadata.contains_key("asset-root-json"));
    }

    #[test]
    fn manifest_properties_as_output_metadata_non_ascii() {
        let mp = ManifestProperties {
            root_path: "/tmp/日本語".into(),
            root_path_format: PathFormat::Posix,
            file_system_location_name: None,
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: None,
        };
        let meta = mp.as_output_metadata();
        let metadata = meta["Metadata"].as_object().unwrap();
        assert!(metadata.contains_key("asset-root"));
        assert!(metadata.contains_key("asset-root-json"));
        let json_root = metadata["asset-root-json"].as_str().unwrap();
        assert!(json_root.contains("\\u"));
    }

    #[test]
    fn manifest_properties_as_output_metadata_with_location() {
        let mp = ManifestProperties {
            root_path: "/tmp/root".into(),
            root_path_format: PathFormat::Posix,
            file_system_location_name: Some("my-location".into()),
            input_manifest_path: None,
            input_manifest_hash: None,
            output_relative_directories: None,
        };
        let meta = mp.as_output_metadata();
        let metadata = meta["Metadata"].as_object().unwrap();
        assert_eq!(metadata["file-system-location-name"], "my-location");
    }

    // === : StorageProfileOperatingSystemFamily ===

    #[test_case("windows", StorageProfileOperatingSystemFamily::Windows ; "lowercase windows")]
    #[test_case("WINDOWS", StorageProfileOperatingSystemFamily::Windows ; "uppercase windows")]
    #[test_case("linux", StorageProfileOperatingSystemFamily::Linux ; "lowercase linux")]
    #[test_case("macos", StorageProfileOperatingSystemFamily::Macos ; "lowercase macos")]
    fn storage_profile_os_family_case_insensitive(input: &str, expected: StorageProfileOperatingSystemFamily) {
        let parsed: StorageProfileOperatingSystemFamily = input.parse().unwrap();
        assert_eq!(parsed, expected);
    }

    #[test]
    fn storage_profile_os_family_host() {
        let host = StorageProfileOperatingSystemFamily::host();
        #[cfg(target_os = "macos")]
        assert_eq!(host, StorageProfileOperatingSystemFamily::Macos);
        #[cfg(target_os = "linux")]
        assert_eq!(host, StorageProfileOperatingSystemFamily::Linux);
        #[cfg(target_os = "windows")]
        assert_eq!(host, StorageProfileOperatingSystemFamily::Windows);
    }

    // === : PathFormat ===

    #[test]
    fn path_format_host() {
        let host = PathFormat::host();
        #[cfg(target_os = "windows")]
        assert_eq!(host, PathFormat::Windows);
        #[cfg(not(target_os = "windows"))]
        assert_eq!(host, PathFormat::Posix);
    }

    // === : Attachments ===

    #[test]
    fn attachments_default_file_system_is_copied() {
        let a = Attachments::default();
        assert_eq!(a.file_system, JobAttachmentsFileSystem::Copied);
    }

    #[test]
    fn attachments_to_json_with_manifests() {
        let a = Attachments {
            manifests: vec![ManifestProperties {
                root_path: "/root".into(),
                root_path_format: PathFormat::Posix,
                file_system_location_name: None,
                input_manifest_path: None,
                input_manifest_hash: None,
                output_relative_directories: None,
            }],
            file_system: JobAttachmentsFileSystem::Copied,
        };
        let json = a.to_json();
        assert_eq!(json["fileSystem"], "COPIED");
        assert_eq!(json["manifests"].as_array().unwrap().len(), 1);
    }

    // === : PathMappingRule ===

    #[test]
    fn path_mapping_rule_hashed_source_path() {
        let rule = PathMappingRule {
            source_path_format: "posix".into(),
            source_path: "/mnt/shared".into(),
            destination_path: "/local/shared".into(),
        };
        let hash = rule.get_hashed_source_path(HashAlgorithm::Xxh128);
        assert_eq!(hash.len(), 32);
        assert!(hash.chars().all(|c: char| c.is_ascii_hexdigit()));

        let hash2 = rule.get_hashed_source_path(HashAlgorithm::Xxh128);
        assert_eq!(hash, hash2);
    }

    // === : Helper functions ===

    #[test]
    fn join_s3_paths_basic() {
        assert_eq!(join_s3_paths(&["root", "Data"]), "root/Data");
    }

    #[test]
    fn join_s3_paths_multiple() {
        assert_eq!(
            join_s3_paths(&["rp", "Manifests", "farm", "queue"]),
            "rp/Manifests/farm/queue"
        );
    }

    #[test]
    fn generate_random_guid_format() {
        let guid = generate_random_guid();
        assert_eq!(guid.len(), 32);
        assert!(guid.chars().all(|c: char| c.is_ascii_hexdigit()));
        let guid2 = generate_random_guid();
        assert_ne!(guid, guid2);
    }

    // === float_to_iso_datetime_string ===

    #[test]
    fn float_to_iso_datetime_string_basic() {
        let ts = 1705312200.0;
        let result = float_to_iso_datetime_string(ts);
        assert_eq!(result, "2024-01-15T09:50:00.000000Z");
    }

    #[test]
    fn float_to_iso_datetime_string_with_microseconds() {
        let ts = 1705312200.123456;
        let result = float_to_iso_datetime_string(ts);
        assert_eq!(result, "2024-01-15T09:50:00.123456Z");
    }

    #[test]
    fn float_to_iso_datetime_string_epoch() {
        let result = float_to_iso_datetime_string(0.0);
        assert_eq!(result, "1970-01-01T00:00:00.000000Z");
    }

    // === partial_session_action_manifest_prefix ===

    #[test]
    fn partial_session_action_manifest_prefix_with_task() {
        let ts = 1705312200.5;
        let result = JobAttachmentS3Settings::partial_session_action_manifest_prefix(
            "farm-1", "queue-1", "job-1", "step-1", "task-1", "sa-1", ts,
        );
        assert_eq!(
            result,
            "farm-1/queue-1/job-1/step-1/task-1/2024-01-15T09:50:00.500000Z_sa-1"
        );
    }

    #[test]
    fn partial_session_action_manifest_prefix_without_task() {
        let ts = 1705312200.5;
        let result = JobAttachmentS3Settings::partial_session_action_manifest_prefix_without_task(
            "farm-1", "queue-1", "job-1", "step-1", "sa-1", ts,
        );
        assert_eq!(
            result,
            "farm-1/queue-1/job-1/step-1/2024-01-15T09:50:00.500000Z_sa-1"
        );
    }

    // === StorageProfile full fields ===

    #[test]
    fn storage_profile_has_identity_fields() {
        let profile = StorageProfile {
            storage_profile_id: "sp-abc123".into(),
            display_name: "My Profile".into(),
            os_family: StorageProfileOperatingSystemFamily::Linux,
            file_system_locations: vec![],
        };
        assert_eq!(profile.storage_profile_id, "sp-abc123");
        assert_eq!(profile.display_name, "My Profile");
        assert_eq!(profile.os_family, StorageProfileOperatingSystemFamily::Linux);
    }
}
