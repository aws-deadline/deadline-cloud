// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

//! Incremental download state: checkpoint persistence for `queue sync-output`.
//! Tracks per-job download progress at three levels: job → session → session action index.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

#[allow(unused_imports, reason = "HashAlgorithm used conditionally in tests")]
use crate::attachments::asset_manifests::{AssetManifest, HashAlgorithm, ManifestPath, hash_data};
use crate::attachments::errors::JobAttachmentsError;
use crate::attachments::path_mapping::PathMappingRuleApplier;

/// Upper bound for `SearchJobs` eventual consistency, in seconds.
pub const EVENTUAL_CONSISTENCY_MAX_SECONDS: i64 = 120;

fn default_eventual_consistency() -> i64 {
    EVENTUAL_CONSISTENCY_MAX_SECONDS
}

/// A job in the incremental download checkpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncrementalDownloadJob {
    pub job: Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub session_ended_timestamp: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub session_completed_indexes: HashMap<String, i64>,
}

/// Full checkpoint for incremental downloads.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncrementalDownloadState {
    pub local_storage_profile_id: Option<String>,
    pub downloads_started_timestamp: DateTime<Utc>,
    pub downloads_completed_timestamp: DateTime<Utc>,
    #[serde(default = "default_eventual_consistency")]
    pub eventual_consistency_max_seconds: i64,
    #[serde(default)]
    pub jobs: Vec<IncrementalDownloadJob>,
}

impl IncrementalDownloadJob {
    pub fn new(
        job: Value,
        session_ended_timestamp: Option<DateTime<Utc>>,
        session_completed_indexes: Option<HashMap<String, i64>>,
    ) -> Self {
        Self {
            job,
            session_ended_timestamp,
            session_completed_indexes: session_completed_indexes.unwrap_or_default(),
        }
    }

    pub fn job_id(&self) -> &str {
        self.job["jobId"].as_str().unwrap_or("")
    }
}

impl IncrementalDownloadState {
    pub fn new(
        local_storage_profile_id: Option<String>,
        downloads_started_timestamp: DateTime<Utc>,
        downloads_completed_timestamp: Option<DateTime<Utc>>,
        jobs: Option<Vec<IncrementalDownloadJob>>,
        eventual_consistency_max_seconds: Option<i64>,
    ) -> Self {
        Self {
            local_storage_profile_id,
            downloads_started_timestamp,
            downloads_completed_timestamp: downloads_completed_timestamp
                .unwrap_or(downloads_started_timestamp),
            eventual_consistency_max_seconds: eventual_consistency_max_seconds
                .unwrap_or(EVENTUAL_CONSISTENCY_MAX_SECONDS),
            jobs: jobs.unwrap_or_default(),
        }
    }

    pub fn from_file(path: &Path) -> Result<Self, JobAttachmentsError> {
        let contents = fs::read_to_string(path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to read checkpoint file: {e}"))
        })?;
        serde_json::from_str(&contents).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to parse checkpoint file: {e}"))
        })
    }

    pub fn save_file(&self, path: &Path) -> Result<(), JobAttachmentsError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| {
                JobAttachmentsError::AssetSync(format!("Failed to create directory: {e}"))
            })?;
        }
        let json = serde_json::to_string_pretty(self).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to serialize checkpoint: {e}"))
        })?;
        let dir = path.parent().unwrap_or(Path::new("."));
        let mut tmp = NamedTempFile::new_in(dir).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to create temp file: {e}"))
        })?;
        tmp.write_all(json.as_bytes()).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to write checkpoint: {e}"))
        })?;
        tmp.persist(path).map_err(|e| {
            JobAttachmentsError::AssetSync(format!("Failed to persist checkpoint: {e}"))
        })?;
        Ok(())
    }
}

// =========================================================================
// Manifest S3 download pipeline (C2)
// =========================================================================

/// Regex to extract session action ID from an S3 manifest key.
/// Matches `sessionaction-{id}-{index}` segments in the key path.
fn session_action_id_regex() -> Regex {
    Regex::new(r"(sessionaction-[^/-]+-[^/-]+)/").expect("valid regex")
}

/// Populate session actions with output manifest S3 keys by matching
/// manifest keys from S3 to session actions by session action ID and
/// root path hash.
///
/// Modifies `session_action_list` in place, adding a `"manifests"` field
/// to each session action that lacks one.
pub fn add_output_manifests_from_s3(
    _farm_id: &str,
    _queue: &Value,
    job: &Value,
    manifest_keys: &[String],
    session_action_list: &mut [Value],
) -> Result<(), JobAttachmentsError> {
    // If the job has no attachments, nothing to add
    let attachments = match job.get("attachments") {
        Some(a) if !a.is_null() => a,
        _ => return Ok(()),
    };
    let job_manifests = attachments["manifests"].as_array().ok_or_else(|| {
        JobAttachmentsError::AssetSync("Job attachments missing manifests".into())
    })?;
    let job_manifests_len = job_manifests.len();

    // Filter to actions that lack a "manifests" field
    let needs_manifests: Vec<usize> = session_action_list
        .iter()
        .enumerate()
        .filter(|(_, sa)| sa.get("manifests").is_none())
        .map(|(i, _)| i)
        .collect();
    if needs_manifests.is_empty() {
        return Ok(());
    }

    // Initialize empty manifests arrays on actions that need them
    for &idx in &needs_manifests {
        session_action_list[idx]["manifests"] = Value::Array(vec![json!({}); job_manifests_len]);
    }

    // Build index of root path hashes → manifest position
    let indexed_root_path_hashes: Vec<(usize, String)> = job_manifests
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let loc_name = m
                .get("fileSystemLocationName")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let root_path = m["rootPath"].as_str().unwrap_or("");
            let input = format!("{loc_name}{root_path}");
            (i, hash_data(input.as_bytes()))
        })
        .collect();

    if manifest_keys.is_empty() {
        // No keys to process — remove the empty manifests arrays we just added
        for &idx in &needs_manifests {
            if let Value::Object(map) = &mut session_action_list[idx] {
                map.remove("manifests");
            }
        }
        return Ok(());
    }

    // Index session actions by ID for fast lookup
    let sa_by_id: HashMap<String, usize> = needs_manifests
        .iter()
        .filter_map(|&idx| {
            session_action_list[idx]["sessionActionId"]
                .as_str()
                .map(|id| (id.to_owned(), idx))
        })
        .collect();

    let re = session_action_id_regex();
    let job_name = job
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let job_id = job
        .get("jobId")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    for key in manifest_keys {
        // Extract session action ID from key
        let sa_id = re
            .captures(key)
            .and_then(|c| c.get(1))
            .map(|m| m.as_str().to_owned())
            .ok_or_else(|| {
                JobAttachmentsError::AssetSync(format!(
                    "Job attachments manifest key for job {job_name} ({job_id}) lacks a session action id"
                ))
            })?;

        // Find which root path hash matches this key
        let manifest_index = indexed_root_path_hashes
            .iter()
            .find(|(_, hash)| key.contains(hash.as_str()))
            .map(|(idx, _)| *idx)
            .ok_or_else(|| {
                let hashes: Vec<&str> = indexed_root_path_hashes.iter().map(|(_, h)| h.as_str()).collect();
                JobAttachmentsError::AssetSync(format!(
                    "Job attachments manifest key for job {job_name} ({job_id}) does not contain any of the rootPath hashes {}: {key}",
                    hashes.join(", ")
                ))
            })?;

        // If this session action is in our list, set the manifest path
        if let Some(&sa_idx) = sa_by_id.get(&sa_id)
            && let Some(arr) = session_action_list[sa_idx]["manifests"].as_array_mut()
            && manifest_index < arr.len()
        {
            arr[manifest_index] = json!({"outputManifestPath": key});
        }
    }

    Ok(())
}

/// Download manifests from S3, make paths absolute by joining with root path,
/// and optionally apply path mapping. Returns `(last_modified, manifest)` pairs.
/// Unmapped paths are recorded in `output_unmapped_paths`.
pub fn make_manifest_paths_absolute(
    root_path: &str,
    manifest: &mut AssetManifest,
    path_mapping_rule_applier: Option<&PathMappingRuleApplier>,
    source_path_format: Option<&str>,
    output_unmapped_paths: &mut Vec<String>,
) -> Result<(), JobAttachmentsError> {
    let is_windows = source_path_format == Some("windows");

    // Join each manifest path with root_path using source OS conventions,
    // then normalize
    for mp in &mut manifest.paths {
        let joined = if is_windows {
            // Windows: join with backslash, normalize
            let full = format!("{}\\{}", root_path.trim_end_matches('\\'), mp.path);
            full.replace('/', "\\")
        } else {
            // POSIX: join with forward slash, normalize
            format!("{}/{}", root_path.trim_end_matches('/'), mp.path)
        };
        mp.path = joined;
    }

    // Apply path mapping if provided
    if let Some(applier) = path_mapping_rule_applier {
        let mut mapped_paths = Vec::new();
        for mp in manifest.paths.drain(..) {
            match applier.strict_transform(&mp.path) {
                Ok(transformed) => {
                    mapped_paths.push(ManifestPath {
                        path: transformed.to_string_lossy().into_owned(),
                        ..mp
                    });
                }
                Err(_) => {
                    output_unmapped_paths.push(mp.path);
                }
            }
        }
        manifest.paths = mapped_paths;
        manifest.total_size = manifest.paths.iter().map(|p| p.size).sum();
    }

    Ok(())
}

/// Merge manifests ordered by last-modified timestamp. Later manifests'
/// files overwrite earlier ones. Uses case-insensitive path keys for dedup.
pub fn merge_absolute_path_manifest_list(
    downloaded_manifests: &mut [(DateTime<Utc>, AssetManifest)],
) -> Vec<ManifestPath> {
    // Sort by timestamp so earlier manifests are processed first
    downloaded_manifests.sort_by_key(|(ts, _)| *ts);

    // Insert into map keyed by lowercased path; later entries overwrite earlier
    let mut merged: HashMap<String, ManifestPath> = HashMap::new();
    for (_, manifest) in downloaded_manifests.iter() {
        for mp in &manifest.paths {
            merged.insert(mp.path.to_lowercase(), mp.clone());
        }
    }
    merged.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn utc(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32) -> DateTime<Utc> {
        chrono::NaiveDate::from_ymd_opt(y, m, d)
            .unwrap()
            .and_hms_opt(h, min, s)
            .unwrap()
            .and_utc()
    }

    fn sample_job_dict() -> Value {
        json!({
            "jobId": "job-abc123",
            "name": "Test Job",
            "taskRunStatusCounts": {"SUCCEEDED": 5, "FAILED": 0}
        })
    }

    // ===================================================================
    // IncrementalDownloadJob tests
    // ===================================================================

    #[test]
    fn job_construct_with_all_fields() {
        let ts = utc(2024, 6, 15, 10, 30, 0);
        let indexes = HashMap::from([("session-1".to_owned(), 5i64)]);
        let job = IncrementalDownloadJob::new(sample_job_dict(), Some(ts), Some(indexes.clone()));
        assert_eq!(job.session_ended_timestamp, Some(ts));
        assert_eq!(job.session_completed_indexes, indexes);
    }

    #[test]
    fn job_construct_none_indexes_defaults_to_empty() {
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        assert!(job.session_completed_indexes.is_empty());
    }

    #[test]
    fn job_id_returns_job_id_field() {
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        assert_eq!(job.job_id(), "job-abc123");
    }

    #[test]
    fn job_round_trip_serde() {
        let ts = utc(2024, 6, 15, 10, 30, 0);
        let indexes = HashMap::from([("session-1".to_owned(), 5i64)]);
        let original = IncrementalDownloadJob::new(sample_job_dict(), Some(ts), Some(indexes));
        let json = serde_json::to_string(&original).unwrap();
        let restored: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn job_deserialize_not_an_object_returns_error() {
        let result = serde_json::from_str::<IncrementalDownloadJob>("\"not a dict\"");
        assert!(result.is_err());
    }

    #[test]
    fn job_deserialize_missing_required_job_field_returns_error() {
        let result = serde_json::from_str::<IncrementalDownloadJob>(r#"{"other": 1}"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("job"));
    }

    #[test]
    fn job_deserialize_missing_optional_session_ended_timestamp() {
        let json = json!({"job": sample_job_dict()}).to_string();
        let job: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert!(job.session_ended_timestamp.is_none());
    }

    #[test]
    fn job_deserialize_missing_optional_session_completed_indexes() {
        let json = json!({"job": sample_job_dict()}).to_string();
        let job: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert!(job.session_completed_indexes.is_empty());
    }

    #[test]
    fn job_serialize_none_timestamp_omits_key() {
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        let val: Value = serde_json::to_value(&job).unwrap();
        assert!(val.get("sessionEndedTimestamp").is_none());
    }

    #[test]
    fn job_serialize_empty_indexes_omits_key() {
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, Some(HashMap::new()));
        let val: Value = serde_json::to_value(&job).unwrap();
        assert!(val.get("sessionCompletedIndexes").is_none());
    }

    // ===================================================================
    // IncrementalDownloadState tests
    // ===================================================================

    #[test]
    fn state_construct_required_fields_only() {
        let ts = utc(2024, 6, 15, 10, 0, 0);
        let state = IncrementalDownloadState::new(Some("sp-123".to_owned()), ts, None, None, None);
        assert_eq!(state.downloads_completed_timestamp, ts);
        assert!(state.jobs.is_empty());
        assert_eq!(
            state.eventual_consistency_max_seconds,
            EVENTUAL_CONSISTENCY_MAX_SECONDS
        );
    }

    #[test]
    fn state_construct_all_fields() {
        let started = utc(2024, 6, 15, 10, 0, 0);
        let completed = utc(2024, 6, 15, 11, 0, 0);
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        let state = IncrementalDownloadState::new(
            Some("sp-123".to_owned()),
            started,
            Some(completed),
            Some(vec![job]),
            Some(60),
        );
        assert_eq!(state.downloads_started_timestamp, started);
        assert_eq!(state.downloads_completed_timestamp, completed);
        assert_eq!(state.jobs.len(), 1);
        assert_eq!(state.eventual_consistency_max_seconds, 60);
    }

    #[test]
    fn state_construct_none_storage_profile() {
        let ts = utc(2024, 6, 15, 10, 0, 0);
        let state = IncrementalDownloadState::new(None, ts, None, None, None);
        assert!(state.local_storage_profile_id.is_none());
    }

    #[test]
    fn state_round_trip_serde() {
        let started = utc(2024, 6, 15, 10, 0, 0);
        let completed = utc(2024, 6, 15, 11, 0, 0);
        let job = IncrementalDownloadJob::new(
            sample_job_dict(),
            Some(utc(2024, 6, 15, 10, 30, 0)),
            Some(HashMap::from([("s-1".to_owned(), 3i64)])),
        );
        let original = IncrementalDownloadState::new(
            Some("sp-123".to_owned()),
            started,
            Some(completed),
            Some(vec![job]),
            Some(120),
        );
        let json = serde_json::to_string(&original).unwrap();
        let restored: IncrementalDownloadState = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn state_deserialize_not_an_object_returns_error() {
        let result = serde_json::from_str::<IncrementalDownloadState>("42");
        assert!(result.is_err());
    }

    #[test]
    fn state_deserialize_missing_required_fields_returns_error() {
        let result = serde_json::from_str::<IncrementalDownloadState>(r#"{"jobs": []}"#);
        assert!(result.is_err());
    }

    #[test]
    fn state_deserialize_multiple_jobs() {
        let started = utc(2024, 6, 15, 10, 0, 0);
        let job1 = IncrementalDownloadJob::new(
            json!({"jobId": "job-1", "name": "Job 1", "taskRunStatusCounts": {"SUCCEEDED": 1}}),
            None,
            None,
        );
        let job2 = IncrementalDownloadJob::new(
            json!({"jobId": "job-2", "name": "Job 2", "taskRunStatusCounts": {"SUCCEEDED": 2}}),
            Some(utc(2024, 6, 15, 10, 30, 0)),
            Some(HashMap::from([("s-1".to_owned(), 1i64)])),
        );
        let state = IncrementalDownloadState::new(
            Some("sp-1".to_owned()),
            started,
            None,
            Some(vec![job1, job2]),
            None,
        );
        let json = serde_json::to_string(&state).unwrap();
        let restored: IncrementalDownloadState = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.jobs.len(), 2);
        assert_eq!(restored.jobs[0].job_id(), "job-1");
        assert_eq!(restored.jobs[1].job_id(), "job-2");
        assert!(restored.jobs[1].session_ended_timestamp.is_some());
    }

    // ===================================================================
    // File persistence tests
    // ===================================================================

    #[test]
    fn state_save_and_load_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("checkpoint.json");
        let state = IncrementalDownloadState::new(
            Some("sp-1".to_owned()),
            utc(2024, 6, 15, 10, 0, 0),
            Some(utc(2024, 6, 15, 11, 0, 0)),
            Some(vec![IncrementalDownloadJob::new(
                sample_job_dict(),
                Some(utc(2024, 6, 15, 10, 30, 0)),
                Some(HashMap::from([("s-1".to_owned(), 5i64)])),
            )]),
            None,
        );
        state.save_file(&path).unwrap();
        let loaded = IncrementalDownloadState::from_file(&path).unwrap();
        assert_eq!(state, loaded);
    }

    #[test]
    fn state_save_creates_parent_directories() {
        let tmp = TempDir::new().unwrap();
        let path = tmp
            .path()
            .join("nested")
            .join("dir")
            .join("checkpoint.json");
        let state =
            IncrementalDownloadState::new(None, utc(2024, 6, 15, 10, 0, 0), None, None, None);
        state.save_file(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn state_save_is_atomic() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("checkpoint.json");

        let state1 = IncrementalDownloadState::new(
            Some("sp-1".to_owned()),
            utc(2024, 6, 15, 10, 0, 0),
            None,
            None,
            None,
        );
        state1.save_file(&path).unwrap();

        let state2 = IncrementalDownloadState::new(
            Some("sp-2".to_owned()),
            utc(2024, 6, 16, 10, 0, 0),
            None,
            None,
            None,
        );
        state2.save_file(&path).unwrap();

        let loaded = IncrementalDownloadState::from_file(&path).unwrap();
        assert_eq!(loaded.local_storage_profile_id, Some("sp-2".to_owned()));

        // No temp files left behind
        let dir_entries: Vec<_> = fs::read_dir(tmp.path()).unwrap().collect();
        assert_eq!(dir_entries.len(), 1);
    }

    #[test]
    fn state_load_nonexistent_file_returns_error() {
        assert!(IncrementalDownloadState::from_file(Path::new("/nonexistent/path.json")).is_err());
    }

    #[test]
    fn state_load_invalid_json_returns_error() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.json");
        fs::write(&path, "not valid json {{{").unwrap();
        assert!(IncrementalDownloadState::from_file(&path).is_err());
    }

    // ===================================================================
    // add_output_manifests_from_s3 tests
    // ===================================================================

    fn sample_job_with_attachments() -> Value {
        json!({
            "jobId": "job-abc123",
            "name": "Test Job",
            "attachments": {
                "manifests": [
                    {
                        "rootPath": "/mnt/shared",
                        "rootPathFormat": "posix",
                        "fileSystemLocationName": ""
                    }
                ]
            }
        })
    }

    fn sample_session_action(id: &str, has_manifests: bool) -> Value {
        let mut sa = json!({"sessionActionId": id});
        if has_manifests {
            sa["manifests"] = json!([{"outputManifestPath": "some/path"}]);
        }
        sa
    }

    #[test]
    fn add_manifests_matches_keys_to_session_actions() {
        // session actions lacking manifests get populated
        let job = sample_job_with_attachments();
        let root_path_hash = hash_data("/mnt/shared".as_bytes());
        let keys = vec![format!(
            "prefix/Manifests/sessionaction-abc-0/{root_path_hash}/manifest.json"
        )];
        let mut actions = vec![sample_session_action("sessionaction-abc-0", false)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        add_output_manifests_from_s3("farm-1", &queue, &job, &keys, &mut actions).unwrap();
        assert!(actions[0].get("manifests").is_some());
    }

    #[test]
    fn add_manifests_skips_actions_with_existing_manifests() {
        // session action already has manifests → skipped
        let job = sample_job_with_attachments();
        let mut actions = vec![sample_session_action("sessionaction-abc-0", true)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        add_output_manifests_from_s3("farm-1", &queue, &job, &[], &mut actions).unwrap();
        // manifests field unchanged
        assert!(
            actions[0]["manifests"][0]
                .get("outputManifestPath")
                .is_some()
        );
    }

    #[test]
    fn add_manifests_no_attachments_returns_immediately() {
        // job has no attachments
        let job = json!({"jobId": "job-1", "name": "No Attachments"});
        let mut actions = vec![sample_session_action("sessionaction-abc-0", false)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        add_output_manifests_from_s3("farm-1", &queue, &job, &[], &mut actions).unwrap();
        assert!(actions[0].get("manifests").is_none());
    }

    #[test]
    fn add_manifests_all_have_manifests_returns_immediately() {
        // all session actions already have manifests
        let job = sample_job_with_attachments();
        let mut actions = vec![
            sample_session_action("sessionaction-abc-0", true),
            sample_session_action("sessionaction-abc-1", true),
        ];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        add_output_manifests_from_s3("farm-1", &queue, &job, &[], &mut actions).unwrap();
    }

    #[test]
    fn add_manifests_key_missing_session_action_id_returns_error() {
        // key lacks session action ID
        let job = sample_job_with_attachments();
        let keys = vec!["prefix/Manifests/no-session-action-id/hash/manifest.json".to_owned()];
        let mut actions = vec![sample_session_action("sessionaction-abc-0", false)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        let err =
            add_output_manifests_from_s3("farm-1", &queue, &job, &keys, &mut actions).unwrap_err();
        assert!(err.to_string().contains("session action id"), "got: {err}");
    }

    #[test]
    fn add_manifests_key_no_matching_root_hash_returns_error() {
        // key doesn't contain any root path hash
        let job = sample_job_with_attachments();
        let keys = vec!["prefix/Manifests/sessionaction-abc-0/wronghash/manifest.json".to_owned()];
        let mut actions = vec![sample_session_action("sessionaction-abc-0", false)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        let err =
            add_output_manifests_from_s3("farm-1", &queue, &job, &keys, &mut actions).unwrap_err();
        assert!(err.to_string().contains("root"), "got: {err}");
    }

    #[test]
    fn add_manifests_no_keys_leaves_actions_unchanged() {
        // no manifests found in S3
        let job = sample_job_with_attachments();
        let mut actions = vec![sample_session_action("sessionaction-abc-0", false)];
        let queue =
            json!({"jobAttachmentSettings": {"rootPrefix": "prefix", "s3BucketName": "bucket"}});
        add_output_manifests_from_s3("farm-1", &queue, &job, &[], &mut actions).unwrap();
        // No manifests field added since no keys to process
        assert!(actions[0].get("manifests").is_none());
    }

    // ===================================================================
    // make_manifest_paths_absolute tests
    // ===================================================================

    fn make_manifest(paths: Vec<(&str, &str, u64)>) -> AssetManifest {
        AssetManifest::new(
            HashAlgorithm::Xxh128,
            crate::attachments::asset_manifests::ManifestVersion::V2023_03_03,
            paths.iter().map(|(_, _, s)| *s).sum(),
            paths
                .iter()
                .map(|(p, h, s)| ManifestPath {
                    path: p.to_string(),
                    hash: h.to_string(),
                    size: *s,
                    mtime: 1_000_000,
                })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn absolute_paths_joined_with_root() {
        // paths made absolute by joining with root path
        let mut manifest = make_manifest(vec![("subdir/file.txt", "aaa", 100)]);
        let mut unmapped = vec![];
        make_manifest_paths_absolute("/mnt/shared", &mut manifest, None, None, &mut unmapped)
            .unwrap();
        assert_eq!(manifest.paths[0].path, "/mnt/shared/subdir/file.txt");
        assert!(unmapped.is_empty());
    }

    #[test]
    fn absolute_paths_with_path_mapping() {
        // path mapping applied after absolutization
        use crate::attachments::models::PathMappingRule;
        let applier = PathMappingRuleApplier::new(vec![PathMappingRule {
            source_path_format: "posix".to_owned(),
            source_path: "/mnt/shared".to_owned(),
            destination_path: "/local/mapped".to_owned(),
        }])
        .unwrap();
        let mut manifest = make_manifest(vec![("subdir/file.txt", "aaa", 100)]);
        let mut unmapped = vec![];
        make_manifest_paths_absolute(
            "/mnt/shared",
            &mut manifest,
            Some(&applier),
            Some("posix"),
            &mut unmapped,
        )
        .unwrap();
        assert_eq!(manifest.paths[0].path, "/local/mapped/subdir/file.txt");
        assert!(unmapped.is_empty());
    }

    #[test]
    fn absolute_paths_no_mapping_uses_host_conventions() {
        // no path mapping → join with root using host OS
        let mut manifest = make_manifest(vec![("a/b.txt", "aaa", 50)]);
        let mut unmapped = vec![];
        make_manifest_paths_absolute("/root", &mut manifest, None, None, &mut unmapped).unwrap();
        assert!(manifest.paths[0].path.starts_with("/root/"));
    }

    #[test]
    fn absolute_paths_windows_source_format() {
        // Windows source paths joined with Windows conventions
        use crate::attachments::models::PathMappingRule;
        let applier = PathMappingRuleApplier::new(vec![PathMappingRule {
            source_path_format: "windows".to_owned(),
            source_path: "C:\\shared".to_owned(),
            destination_path: "/local/mapped".to_owned(),
        }])
        .unwrap();
        let mut manifest = make_manifest(vec![("subdir\\file.txt", "aaa", 100)]);
        let mut unmapped = vec![];
        make_manifest_paths_absolute(
            "C:\\shared",
            &mut manifest,
            Some(&applier),
            Some("windows"),
            &mut unmapped,
        )
        .unwrap();
        assert_eq!(manifest.paths[0].path, "/local/mapped/subdir/file.txt");
    }

    #[test]
    fn absolute_paths_unmapped_paths_excluded() {
        // paths that fail mapping are excluded and recorded
        use crate::attachments::models::PathMappingRule;
        let applier = PathMappingRuleApplier::new(vec![PathMappingRule {
            source_path_format: "posix".to_owned(),
            source_path: "/mnt/shared".to_owned(),
            destination_path: "/local/mapped".to_owned(),
        }])
        .unwrap();
        let _manifest = make_manifest(vec![
            ("subdir/file.txt", "aaa", 100),
            ("other/file.txt", "bbb", 200),
        ]);
        // The second file's absolute path will be /mnt/other/other/file.txt
        // which won't match the /mnt/shared rule
        let mut manifest2 = make_manifest(vec![("file.txt", "bbb", 200)]);
        let mut unmapped = vec![];
        make_manifest_paths_absolute(
            "/mnt/other",
            &mut manifest2,
            Some(&applier),
            Some("posix"),
            &mut unmapped,
        )
        .unwrap();
        assert!(manifest2.paths.is_empty());
        assert_eq!(unmapped.len(), 1);
        assert!(unmapped[0].contains("/mnt/other"));
    }

    // ===================================================================
    // merge_absolute_path_manifest_list tests
    // ===================================================================

    #[test]
    fn merge_non_overlapping_files() {
        // two manifests with different files → all included
        let ts1 = utc(2024, 6, 15, 10, 0, 0);
        let ts2 = utc(2024, 6, 15, 11, 0, 0);
        let m1 = make_manifest(vec![("/a/file1.txt", "hash1", 100)]);
        let m2 = make_manifest(vec![("/b/file2.txt", "hash2", 200)]);
        let mut manifests = vec![(ts1, m1), (ts2, m2)];
        let result = merge_absolute_path_manifest_list(&mut manifests);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn merge_same_path_later_wins() {
        // same file path, later timestamp wins
        let ts1 = utc(2024, 6, 15, 10, 0, 0);
        let ts2 = utc(2024, 6, 15, 11, 0, 0);
        let m1 = make_manifest(vec![("/a/file.txt", "old_hash", 100)]);
        let m2 = make_manifest(vec![("/a/file.txt", "new_hash", 150)]);
        let mut manifests = vec![(ts1, m1), (ts2, m2)];
        let result = merge_absolute_path_manifest_list(&mut manifests);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, "new_hash");
    }

    #[test]
    fn merge_case_insensitive_keys() {
        // paths differing only in case treated as same file
        let ts1 = utc(2024, 6, 15, 10, 0, 0);
        let ts2 = utc(2024, 6, 15, 11, 0, 0);
        let m1 = make_manifest(vec![("/a/File.txt", "hash1", 100)]);
        let m2 = make_manifest(vec![("/a/file.txt", "hash2", 200)]);
        let mut manifests = vec![(ts1, m1), (ts2, m2)];
        let result = merge_absolute_path_manifest_list(&mut manifests);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, "hash2"); // later wins
    }

    #[test]
    fn merge_empty_list() {
        // empty → empty
        let mut manifests: Vec<(DateTime<Utc>, AssetManifest)> = vec![];
        let result = merge_absolute_path_manifest_list(&mut manifests);
        assert!(result.is_empty());
    }

    #[test]
    fn merge_sorted_by_timestamp_before_merging() {
        // manifests sorted by timestamp; earlier processed first
        let ts_early = utc(2024, 6, 15, 10, 0, 0);
        let ts_late = utc(2024, 6, 15, 11, 0, 0);
        // Provide in reverse order — merge should sort first
        let m_late = make_manifest(vec![("/a/file.txt", "late_hash", 200)]);
        let m_early = make_manifest(vec![("/a/file.txt", "early_hash", 100)]);
        let mut manifests = vec![(ts_late, m_late), (ts_early, m_early)];
        let result = merge_absolute_path_manifest_list(&mut manifests);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].hash, "late_hash"); // later timestamp wins even if provided first
    }
}
