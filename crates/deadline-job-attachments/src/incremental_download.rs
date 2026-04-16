// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

//! Incremental download state: checkpoint persistence for `queue sync-output`.
//! Tracks per-job download progress at three levels: job → session → session action index.

use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tempfile::NamedTempFile;

use crate::errors::JobAttachmentsError;

/// Upper bound for SearchJobs eventual consistency, in seconds.
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
    // IncrementalDownloadJob tests (spec cases 1-10)
    // ===================================================================

    #[test]
    fn job_construct_with_all_fields() {
        // Spec #1
        let ts = utc(2024, 6, 15, 10, 30, 0);
        let indexes = HashMap::from([("session-1".to_string(), 5i64)]);
        let job = IncrementalDownloadJob::new(
            sample_job_dict(),
            Some(ts),
            Some(indexes.clone()),
        );
        assert_eq!(job.session_ended_timestamp, Some(ts));
        assert_eq!(job.session_completed_indexes, indexes);
    }

    #[test]
    fn job_construct_none_indexes_defaults_to_empty() {
        // Spec #2
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        assert!(job.session_completed_indexes.is_empty());
    }

    #[test]
    fn job_id_returns_job_id_field() {
        // Spec #3
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        assert_eq!(job.job_id(), "job-abc123");
    }

    #[test]
    fn job_round_trip_serde() {
        // Spec #4
        let ts = utc(2024, 6, 15, 10, 30, 0);
        let indexes = HashMap::from([("session-1".to_string(), 5i64)]);
        let original = IncrementalDownloadJob::new(
            sample_job_dict(),
            Some(ts),
            Some(indexes),
        );
        let json = serde_json::to_string(&original).unwrap();
        let restored: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn job_deserialize_not_an_object_returns_error() {
        // Spec #5
        let result = serde_json::from_str::<IncrementalDownloadJob>("\"not a dict\"");
        assert!(result.is_err());
    }

    #[test]
    fn job_deserialize_missing_required_job_field_returns_error() {
        // Spec #6
        let result = serde_json::from_str::<IncrementalDownloadJob>(r#"{"other": 1}"#);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("job"));
    }

    #[test]
    fn job_deserialize_missing_optional_session_ended_timestamp() {
        // Spec #7
        let json = json!({"job": sample_job_dict()}).to_string();
        let job: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert!(job.session_ended_timestamp.is_none());
    }

    #[test]
    fn job_deserialize_missing_optional_session_completed_indexes() {
        // Spec #8
        let json = json!({"job": sample_job_dict()}).to_string();
        let job: IncrementalDownloadJob = serde_json::from_str(&json).unwrap();
        assert!(job.session_completed_indexes.is_empty());
    }

    #[test]
    fn job_serialize_none_timestamp_omits_key() {
        // Spec #9
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        let val: Value = serde_json::to_value(&job).unwrap();
        assert!(val.get("sessionEndedTimestamp").is_none());
    }

    #[test]
    fn job_serialize_empty_indexes_omits_key() {
        // Spec #10
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, Some(HashMap::new()));
        let val: Value = serde_json::to_value(&job).unwrap();
        assert!(val.get("sessionCompletedIndexes").is_none());
    }

    // ===================================================================
    // IncrementalDownloadState tests (spec cases 11-17)
    // ===================================================================

    #[test]
    fn state_construct_required_fields_only() {
        // Spec #11
        let ts = utc(2024, 6, 15, 10, 0, 0);
        let state = IncrementalDownloadState::new(
            Some("sp-123".to_string()),
            ts,
            None,
            None,
            None,
        );
        assert_eq!(state.downloads_completed_timestamp, ts);
        assert!(state.jobs.is_empty());
        assert_eq!(state.eventual_consistency_max_seconds, EVENTUAL_CONSISTENCY_MAX_SECONDS);
    }

    #[test]
    fn state_construct_all_fields() {
        // Spec #12
        let started = utc(2024, 6, 15, 10, 0, 0);
        let completed = utc(2024, 6, 15, 11, 0, 0);
        let job = IncrementalDownloadJob::new(sample_job_dict(), None, None);
        let state = IncrementalDownloadState::new(
            Some("sp-123".to_string()),
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
        // Spec #13
        let ts = utc(2024, 6, 15, 10, 0, 0);
        let state = IncrementalDownloadState::new(None, ts, None, None, None);
        assert!(state.local_storage_profile_id.is_none());
    }

    #[test]
    fn state_round_trip_serde() {
        // Spec #14
        let started = utc(2024, 6, 15, 10, 0, 0);
        let completed = utc(2024, 6, 15, 11, 0, 0);
        let job = IncrementalDownloadJob::new(
            sample_job_dict(),
            Some(utc(2024, 6, 15, 10, 30, 0)),
            Some(HashMap::from([("s-1".to_string(), 3i64)])),
        );
        let original = IncrementalDownloadState::new(
            Some("sp-123".to_string()),
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
        // Spec #15
        let result = serde_json::from_str::<IncrementalDownloadState>("42");
        assert!(result.is_err());
    }

    #[test]
    fn state_deserialize_missing_required_fields_returns_error() {
        // Spec #16
        let result = serde_json::from_str::<IncrementalDownloadState>(r#"{"jobs": []}"#);
        assert!(result.is_err());
    }

    #[test]
    fn state_deserialize_multiple_jobs() {
        // Spec #17
        let started = utc(2024, 6, 15, 10, 0, 0);
        let job1 = IncrementalDownloadJob::new(
            json!({"jobId": "job-1", "name": "Job 1", "taskRunStatusCounts": {"SUCCEEDED": 1}}),
            None, None,
        );
        let job2 = IncrementalDownloadJob::new(
            json!({"jobId": "job-2", "name": "Job 2", "taskRunStatusCounts": {"SUCCEEDED": 2}}),
            Some(utc(2024, 6, 15, 10, 30, 0)),
            Some(HashMap::from([("s-1".to_string(), 1i64)])),
        );
        let state = IncrementalDownloadState::new(
            Some("sp-1".to_string()),
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
    // File persistence tests (spec cases 18-22)
    // ===================================================================

    #[test]
    fn state_save_and_load_round_trip() {
        // Spec #18
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("checkpoint.json");
        let state = IncrementalDownloadState::new(
            Some("sp-1".to_string()),
            utc(2024, 6, 15, 10, 0, 0),
            Some(utc(2024, 6, 15, 11, 0, 0)),
            Some(vec![IncrementalDownloadJob::new(
                sample_job_dict(),
                Some(utc(2024, 6, 15, 10, 30, 0)),
                Some(HashMap::from([("s-1".to_string(), 5i64)])),
            )]),
            None,
        );
        state.save_file(&path).unwrap();
        let loaded = IncrementalDownloadState::from_file(&path).unwrap();
        assert_eq!(state, loaded);
    }

    #[test]
    fn state_save_creates_parent_directories() {
        // Spec #19
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested").join("dir").join("checkpoint.json");
        let state = IncrementalDownloadState::new(
            None,
            utc(2024, 6, 15, 10, 0, 0),
            None, None, None,
        );
        state.save_file(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn state_save_is_atomic() {
        // Spec #20
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("checkpoint.json");

        let state1 = IncrementalDownloadState::new(
            Some("sp-1".to_string()),
            utc(2024, 6, 15, 10, 0, 0),
            None, None, None,
        );
        state1.save_file(&path).unwrap();

        let state2 = IncrementalDownloadState::new(
            Some("sp-2".to_string()),
            utc(2024, 6, 16, 10, 0, 0),
            None, None, None,
        );
        state2.save_file(&path).unwrap();

        let loaded = IncrementalDownloadState::from_file(&path).unwrap();
        assert_eq!(loaded.local_storage_profile_id, Some("sp-2".to_string()));

        // No temp files left behind
        let dir_entries: Vec<_> = fs::read_dir(tmp.path()).unwrap().collect();
        assert_eq!(dir_entries.len(), 1);
    }

    #[test]
    fn state_load_nonexistent_file_returns_error() {
        // Spec #21
        assert!(IncrementalDownloadState::from_file(Path::new("/nonexistent/path.json")).is_err());
    }

    #[test]
    fn state_load_invalid_json_returns_error() {
        // Spec #22
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bad.json");
        fs::write(&path, "not valid json {{{").unwrap();
        assert!(IncrementalDownloadState::from_file(&path).is_err());
    }
}
