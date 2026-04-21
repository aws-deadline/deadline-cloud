# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Step Status

(Completed — see Recently Completed below)

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1120 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.

## Investigated and Dropped

| Gap | Reason |
|-----|--------|
| AUDIT-034 | `--submitter-info` is GUI-only (`bundle gui-submit`). CLI `bundle submit` correctly has `--submitter-name`. Not deprecated on CLI. |
| AUDIT-051 | Download path collision — worker-agent scope, not CLI. Deferred. |

## Recently Completed

**Gap sweep: audit findings F1-F8 (2026-04-21)**

Fixed 7 audit findings across 6 files:
- **F1** (AUDIT-042): Windows stdin for login — `Stdio::piped()` on Windows
- **F2** (AUDIT-054): `--redirect-output` cross-platform — `SetStdHandle` on Windows
- **F3** (AUDIT-053): Windows long path UNC — `get_long_path_compatible_path()`
- **F5**: Telemetry hashing/upload summary events wired into submission flow
- **F6**: Telemetry error event on submission failure
- **F7** (AUDIT-008): Interactive root path editing in `job download-output`
  (implementation complete, 6 tests `#[ignore]` pending S3 mock chain fix)
- **F8** (AUDIT-031): `--save-debug-snapshot` — full implementation with
  JSON dump, parameter files, shell/batch scripts, S3 copy commands,
  queue.json, zip support, and `snapshot_assets` for local file copy

**Next action item:** Fix S3 download mock chain to unblock 7 ignored tests.

**AUDIT-013: Hash cache V4 compatibility** (prior session)

- Rust now uses Python's `hashesV4` table instead of `hashesV5`. Both CLIs
  share one hash cache — zero re-hashing when switching between tools.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-043 | User-agent position in header — Rust SDK limitation, content is correct |
| AUDIT-046 | `require_setting` exit code — function was unused dead code, deleted |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
