# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#13: Job download & sync-output** — Batches A, B, C1, C2, C3 complete.

### Completed: Batch A (`job download-output`)
- 17 Level 2 CLI tests

### Completed: Batch B (Path mapping)
- 37 Level 1 tests

### Completed: Batch C1 (IncrementalDownloadState)
- 22 Level 1 tests

### Completed: Batch C2 (Manifest S3 download pipeline)
- 17 Level 1 tests

### Completed: Batch C3 (CLI `queue sync-output`)
- `SyncOutput` variant added to `QueueAction` with all CLI args
- `PidFileLock` RAII struct with atomic acquire/release
- Full validation: mutual exclusion, writable dir, storage profile,
  job attachment settings, checkpoint storage profile match
- Checkpoint load/bootstrap/save with dry-run support
- 13 Level 2 CLI tests, all passing

### Remaining work for #13
- Full orchestration wiring: job categorization, session retrieval,
  manifest download, file download. Currently stubbed with
  "no files to download" placeholder.
- This is the API integration layer that connects C1/C2 functions
  to the CLI command.

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 979 tests across all crates.
3. **Development workflow is in `specs/workflow.md`.**
4. **Serialization patterns.** See `specs/patterns.md`.
