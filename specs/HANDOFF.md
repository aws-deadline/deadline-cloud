# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Step Status

(Completed — see Recently Completed below)

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1127 tests across all crates, 0 ignored.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.
7. **S3 mock encoding:** `encode_s3_path()` in `deadline-test-server`
   handles percent-encoding of colons in S3 key paths. Callers of
   `mock_s3_get_object` and `mock_s3_get_object_with_metadata` should
   pass raw keys — encoding is the mock helper's responsibility.

## Investigated and Dropped

| Gap | Reason |
|-----|--------|
| AUDIT-034 | `--submitter-info` is GUI-only (`bundle gui-submit`). CLI `bundle submit` correctly has `--submitter-name`. Not deprecated on CLI. |
| AUDIT-051 | Download path collision — worker-agent scope, not CLI. Deferred. |

## Recently Completed

**S3 download mock chain fix (2026-04-21)**

Fixed test infrastructure bug: S3 SDK percent-encodes colons (`:` → `%3A`)
in HTTP paths with `force_path_style(true)`, but wiremock `path()` matcher
compared against raw encoded URLs. Added `encode_s3_path()` helper to
`deadline-test-server` that handles encoding centrally. Removed manual
`.replace(':', "%3A")` from 4 call sites in `queue_sync_output.rs`.
Unblocked 7 `#[ignore]` tests in `job_download.rs`. Result: 1127 tests,
0 ignored.

**Gap sweep: audit findings F1-F8 (2026-04-21)**

Fixed 7 audit findings across 6 files:
- **F1** (AUDIT-042): Windows stdin for login — `Stdio::piped()` on Windows
- **F2** (AUDIT-054): `--redirect-output` cross-platform — `SetStdHandle` on Windows
- **F3** (AUDIT-053): Windows long path UNC — `get_long_path_compatible_path()`
- **F5**: Telemetry hashing/upload summary events wired into submission flow
- **F6**: Telemetry error event on submission failure
- **F7** (AUDIT-008): Interactive root path editing in `job download-output`
- **F8** (AUDIT-031): `--save-debug-snapshot` — full implementation

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
