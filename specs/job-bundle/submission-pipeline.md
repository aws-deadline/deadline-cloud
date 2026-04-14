# Submission Pipeline

## Entry Point

`create_job_from_job_bundle(SubmitJobParams) -> Result<Option<String>>`

Returns the job ID on success, or `None` if submission was canceled.

## SubmitJobParams

All inputs for the submission pipeline:
- `job_bundle_dir` — path to the bundle directory
- `job_parameters` — `Vec<serde_json::Value>` of `{name, value}` overrides
- `name` — optional job name override
- `priority` — optional (default 50)
- `max_failed_tasks_count`, `max_retries_per_task`, `max_worker_count`
- `target_task_run_status` — READY or SUSPENDED
- `job_attachments_file_system` — COPIED or VIRTUAL
- `require_paths_exist` — error if input paths are missing
- `submitter_name` — for telemetry and history
- `known_asset_paths` — paths that should not generate warnings
- `auto_accept` — skip confirmation prompts
- `force_s3_check` — override S3 verification behavior
- `config` — `Option<&IniConfig>`
- `print_callback` — `Box<dyn Fn(&str)>` for status messages
- `hashing_progress_callback`, `upload_progress_callback` — progress reporting
- `continue_callback` — cancellation check

## Orchestration Flow

```
create_job_from_job_bundle(params)
  │
  ├── 1. Validate bundle directory (symlink containment)
  ├── 2. Load template (template.yaml or template.json)
  │      Validate specificationVersion = jobtemplate-2023-09
  ├── 3. Load parameter_values (optional file)
  ├── 4. Fetch queue parameter definitions from queue environments
  ├── 5. Merge parameters: queue env params + template params + CLI overrides
  │      Validate types, coerce values, check constraints
  ├── 6. Extract asset references from PATH parameters
  ├── 7. Expand input directories to file lists (recursive walk)
  │      Empty dirs → referenced_paths
  │      Missing dirs → error if require_paths_exist, else warn
  ├── 8. Build known asset paths list
  │      Merge from: config setting + CLI --known-asset-path args
  │      TRIE-based prefix deduplication
  ├── 9. Check for files outside known paths
  │      auto_accept → proceed silently
  │      else → print_callback with warning, check continue_callback
  ├── 10. Hash assets and create manifests (via deadline-job-attachments)
  │       Progress reported via hashing_progress_callback
  ├── 11. Upload assets to S3 CAS (via deadline-job-attachments)
  │       Progress reported via upload_progress_callback
  ├── 12. Build CreateJob request (template + parameters + attachment metadata)
  ├── 13. Call CreateJob API (via deadline-api)
  ├── 14. Save job history snapshot to disk
  └── 15. Poll until job exits CREATE_IN_PROGRESS
          Exponential backoff: 0.3s initial, doubles each iteration,
          capped at 5s, 300s total timeout
```

## Job Creation Polling

`poll_create_job` checks `lifecycleStatus` first, falls back to legacy `state`
field. Returns success/failure with status message. Supports cancellation via
caller-provided callback.

## Job History

After submission, saves bundle snapshot to:
`{history_dir}/YYYY-MM/YYYY-MM-DD-{NN}-{submitter}-{job_name}`

- `history_dir` from `settings.job_history_dir` (profile-scoped)
- NN is sequential (determined by scanning existing directories for the same date)
- Submitter and job names sanitized: only alphanumeric, space, hyphen, underscore
- Job name truncated to 128 characters

## AssetReferences

Four `BTreeSet<String>` fields:
- `input_filenames` — individual files to upload
- `input_directories` — directories to walk and upload
- `output_directories` — directories where workers write output
- `referenced_paths` — paths referenced but not uploaded (e.g., NONE dataFlow)

`union()` combines two `AssetReferences` instances. `from_dict()` parses from
the JSON format used in job attachment metadata. `to_dict()` serializes back.
