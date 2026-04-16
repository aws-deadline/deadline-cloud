# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#13: Job download & sync-output** — Batches A, B, C1, C2 complete. C3 next.

### Completed: Batch A (`job download-output`)

- CLI command: `deadline job download-output`
- 17 Level 2 CLI tests, all passing

### Completed: Batch B (Path mapping from storage profiles)

- `path_mapping.rs`: `generate_path_mapping_rules`, `PathMappingRuleApplier`
- 37 Level 1 tests, all passing

### Completed: Batch C1 (IncrementalDownloadState)

- `incremental_download.rs`: `IncrementalDownloadJob`, `IncrementalDownloadState`
- Serde-based serialization, atomic file persistence
- 22 Level 1 tests, all passing

### Completed: Batch C2 (Manifest S3 download pipeline)

- `add_output_manifests_from_s3`: match S3 keys to session actions by
  session action ID regex and root path hash
- `make_manifest_paths_absolute`: join paths with root, apply path mapping,
  collect unmapped paths
- `merge_absolute_path_manifest_list`: timestamp-ordered case-insensitive merge
- Reuses existing `download_file` from `download.rs` — no fork
- 17 Level 1 tests, all passing

### Next: Batch C3 (CLI `queue sync-output`)

- Full orchestration: job categorization (7 categories), session retrieval,
  storage profile loading, path mapping rule creation, checkpoint management
- PID file lock for concurrency control
- CLI argument parsing, dry-run
- Test spec: section 42 cases 14-26 (13 cases)
- Python source: `cli/_incremental_download.py` (1,271 lines)

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).

2. **All tests pass.** 966 tests across all crates.

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
