# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#13: Job download & sync-output** — Batches A, B, C1 complete. C2 next.

### Completed: Batch A (`job download-output`)

- CLI command: `deadline job download-output`
- 17 Level 2 CLI tests, all passing

### Completed: Batch B (Path mapping from storage profiles)

- `path_mapping.rs`: `generate_path_mapping_rules`, `PathMappingRuleApplier`
- 37 Level 1 tests, all passing

### Completed: Batch C1 (IncrementalDownloadState)

- `incremental_download.rs`: `IncrementalDownloadJob`, `IncrementalDownloadState`
- Serde-based serialization (owned file format pattern, documented in `patterns.md`)
- Atomic file persistence via `tempfile::NamedTempFile` + `persist()`
- 22 Level 1 tests, all passing
- Added `tempfile` as regular dependency (was dev-only)
- Added serialization pattern guidance to `specs/patterns.md`

### Next: Batch C2 (Manifest S3 download pipeline)

- `add_output_manifests_from_s3` — match S3 manifest keys to session actions
- `download_all_manifests_with_absolute_paths` — download + path mapping
- `merge_absolute_path_manifest_list` — timestamp-ordered merge
- `download_file` — S3 CAS download with conflict resolution
- `download_manifest_paths` — orchestrate parallel downloads with progress
- Test spec: section 35 cases 23-61 (39 cases)
- Python source: `_incremental_downloads/_manifest_s3_downloads.py` (667 lines)

### Then: Batch C3 (CLI `queue sync-output`)

- Full orchestration, job categorization, PID lock, CLI command
- Test spec: section 42 cases 14-26 (13 cases)

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).

2. **All tests pass.** 949 tests across all crates.

3. **Development workflow is in `specs/workflow.md`.** Follow the
   7-step loop. Step 3 now includes a CLI comparison gate.

4. **Specs are in `specs/`.** Per-crate specs in `specs/{crate}/`,
   CLI command docs in `specs/cli/`.

5. **DCM test infrastructure exists.** `cli_dcm.rs` has
   `write_dcm_aws_config()` helper. `cli_credential_scoping.rs` has
   the pattern for testing DCM vs non-DCM credential paths.

6. **`deadline-job-bundle` owns submission orchestration.** It depends
   on `deadline-api` (API calls) and `deadline-job-attachments` (S3
   upload). See `specs/architecture.md` for the dependency graph.

7. **S3 mock specificity matters.** `mock_s3_list_empty` uses
   `query_param("list-type", "2")` to avoid intercepting Deadline API
   GET requests on the shared test server.

8. **Serialization patterns.** API responses use `serde_json::Value`
   via `ResponseBodyCapture`. Owned file formats (checkpoints) use
   `#[derive(Serialize, Deserialize)]`. See `specs/patterns.md`.
