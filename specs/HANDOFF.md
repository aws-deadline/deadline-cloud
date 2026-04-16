# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#13: Job download & sync-output** — Batch A complete. Batch B complete. Batch C next.

### Completed: Batch A (`job download-output`)

- CLI command: `deadline job download-output` with `--step-id`, `--task-id`,
  `--conflict-resolution`, `--yes`, `--output verbose|json`
- Library: `OutputDownloader` struct in `deadline-job-attachments/src/download.rs`
- API: `get_step`, `get_task` in `deadline-api/src/api.rs`
- Utility: `summarize_path_list` in `deadline-api/src/path_utils.rs`
- Bug fix: `download_file` now passes actual bytes to progress tracker
- Bug fix: `human_readable_file_size` now uses 2 decimal places matching Python
- 17 Level 2 CLI tests, all passing
- CLI output verified against Python CLI with real API

### Deferred from Batch A

- Interactive root-editing loop (prompt to change download root directories)
- Cross-OS root path mismatch interactive prompt
- Conflict resolution interactive prompt (when neither flag nor `--yes`)
- Test case 44-10 (happy path with full S3 download through stubs)

### Completed: Batch B (Path mapping from storage profiles)

- New module: `crates/deadline-job-attachments/src/path_mapping.rs`
- `generate_path_mapping_rules()`: matches storage profile locations by name,
  determines format from OS family
- `PathMappingRuleApplier`: trie-based longest-prefix matcher with
  `transform()` and `strict_transform()`
- Windows paths matched case-insensitively, output preserves original case
- 37 Level 1 tests, all passing
- Spec: `specs/job-attachments/path-mapping.md`
- Added `PartialEq` derive to `PathMappingRule` in `models.rs`

### Next: Batch C (`queue sync-output` + incremental downloads)

- `IncrementalDownloadState`, checkpoint persistence, job categorization,
  PID file lock, full incremental download orchestration
- Test spec: `test_specs/cli.md` section 42 cases 14-26,
  `test_specs/job_attachments_data_transfer.md` section 35 (61 cases)

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).

2. **All tests pass.** 927 tests across all crates.

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
