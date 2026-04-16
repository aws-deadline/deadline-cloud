# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#13: Job download & sync-output** — Wiring orchestration for `queue sync-output`.

### Context

Batches A, B, C1, C2, C3 built the pieces:
- Checkpoint persistence (C1)
- Manifest S3 pipeline: key matching, path absolutization, merging (C2)
- CLI command shell: args, validation, PID lock, checkpoint management (C3)
- Path mapping (B)
- File download engine (A)

But the C3 happy-path tests accepted placeholder output ("no files to
download") instead of testing the full orchestration. The orchestration
function that connects all pieces is not implemented.

### Current step: Step 1 complete

**What needs to be wired (the orchestration):**
1. `_get_download_candidate_jobs` → 2 SearchJobs calls (active + recently ended)
2. `_categorize_jobs_in_checkpoint` → compare checkpoint jobs vs candidates,
   call GetJob for new jobs, categorize into 7 categories
3. `_get_job_sessions` → ListSessions + ListSessionActions per job,
   filter by checkpoint indexes, add manifest S3 keys
4. `_create_path_mapping_rule_appliers` → GetStorageProfileForQueue per
   unique profile, generate rules
5. `_update_checkpoint_jobs_list` → update checkpoint with new indexes
6. `_download_all_manifests_with_absolute_paths` → download manifests,
   absolutize paths, apply mapping (C2 function exists)
7. `_merge_absolute_path_manifest_list` → merge by timestamp (C2 exists)
8. `_download_manifest_paths` → download files (reuses download.rs)

**API call sequence for minimum happy path:**
GetStorageProfileForQueue → GetQueue → SearchJobs (×2) → GetJob →
ListSessions → ListSessionActions → S3 ListObjects → S3 GetObject (manifest)
→ S3 GetObject (file) → save checkpoint

**Missing test infrastructure:**
- `mock_list_session_actions` helper in test server
- S3 GetObject mock for manifest content
- S3 ListObjects mock for manifest keys (existing `mock_s3_list_empty` only returns empty)
- S3 GetObject mock for file content

**Plan:**
1. Add missing mock helpers to test server
2. Rewrite 7 happy-path tests with full mock chains (red)
3. Implement orchestration function (green)
4. Spec, audit, commit

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 979 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
