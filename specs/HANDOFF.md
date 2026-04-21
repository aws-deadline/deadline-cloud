# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**S3 download mock chain fix** — Technical debt item from `progress.md`.
Unblocks 7 `#[ignore]` tests in `job_download.rs` (F7 tests + conflict
prompt test).

## Step Status

| Step | Status |
|------|--------|
| 1. Study Python | ✅ Complete |
| 2. Write tests | ⏳ Pending review |
| 3. Implement | Not started |
| 4. Write spec | Not started |
| 5. Audit | Not started |
| 6. Fix | Not started |
| 7. Commit | Not started |

## Root Cause Analysis

### Problem

7 Level 2 tests in `crates/deadline-cli/tests/cli/job_download.rs` are
`#[ignore]` because the S3 GetObject mock doesn't match what the AWS
SDK actually sends.

### Root cause: URL percent-encoding mismatch

The S3 SDK with `force_path_style(true)` (enabled when `AWS_ENDPOINT_URL_S3`
is set — see `crates/deadline-job-attachments/src/s3.rs:57`) sends
GetObject requests as:

```
GET /test-bucket/root-prefix/Manifests/.../2024-01-01T00%3A00%3A00Z_sa-1/output.manifest
```

The colons in the timestamp `2024-01-01T00:00:00Z` are percent-encoded
to `%3A`. The wiremock `path()` matcher (`PathExactMatcher`) compares
against `request.url.path()`, which returns the **raw percent-encoded**
path from the `url::Url` type. So:

- Mock registers: `/test-bucket/.../2024-01-01T00:00:00Z_sa-1/output.manifest`
- SDK sends:      `/test-bucket/.../2024-01-01T00%3A00%3A00Z_sa-1/output.manifest`
- **No match → 404**

### Evidence

- Level 1 tests in `download.rs` work because their keys have no special
  characters (e.g. `sa-1/output_old.json`, `manifest-key`).
- Running the ignored test produces:
  `Error downloading binary file in bucket 'test-bucket', Target key or
  prefix: '...2024-01-01T00:00:00Z_sa-1/output.manifest', HTTP Status
  Code: 404, unhandled error (NotFound)`
- The `url::Url::path()` method returns the percent-encoded path (verified
  from `url` crate source).

### Affected tests (7 total)

| Test | What it tests |
|------|---------------|
| `job_download_output_existing_files_shows_conflict_prompt` | Conflict detection when files exist |
| `job_download_output_cross_os_root_prompts_for_new_path` | F7: Cross-OS root mismatch prompt |
| `job_download_output_root_editing_loop_accepts_y_to_proceed` | F7: Root editing — accept |
| `job_download_output_root_editing_loop_n_cancels` | F7: Root editing — cancel |
| `job_download_output_root_editing_select_index_then_proceed` | F7: Root editing — select index |
| `job_download_output_json_mode_cross_os_root_emits_json` | F7: JSON mode cross-OS |
| `job_download_output_yes_skips_root_editing_but_shows_cross_os_prompt` | F7: --yes with cross-OS |

## Implementation Plan

### Fix approach

Two changes needed:

1. **Fix `mock_s3_get_object_with_metadata` in `deadline-test-server/src/deadline_api/s3.rs`**:
   Use `path_regex` instead of `path` for the matcher, with the key
   pattern regex-escaped but colons matching both literal and
   percent-encoded forms. OR simpler: percent-encode the key in the
   mock registration to match what the SDK sends.

   Simplest fix: the mock helper should percent-encode the path the
   same way the SDK does. Use `percent_encoding::utf8_percent_encode`
   with the S3 path encoding set (encode `:` but preserve `/`).

   Same fix needed for `mock_s3_get_object`.

2. **Fix test manifest keys in `job_download.rs`**: The keys use
   `2024-01-01T00:00:00Z_sa-1` which contains colons. Two options:
   - (a) Change keys to avoid colons: `2024-01-01T000000Z_sa-1` — but
     this doesn't match real S3 key format.
   - (b) Keep realistic keys and fix the mock to handle encoding — this
     is the correct approach since it makes the test infrastructure
     robust for any key format.

   Also fix the metadata header: tests use `x-amz-meta-asset-root` but
   the code checks `asset-root-json` first, then `asset-root`. The
   Level 1 tests use `x-amz-meta-asset-root-json` with JSON-encoded
   values. The Level 2 tests should match.

### Crates/modules changed

| File | Change |
|------|--------|
| `crates/deadline-test-server/src/deadline_api/s3.rs` | Fix `mock_s3_get_object` and `mock_s3_get_object_with_metadata` to percent-encode the path |
| `crates/deadline-test-server/Cargo.toml` | Add `percent-encoding` dependency (if not already present) |
| `crates/deadline-cli/tests/cli/job_download.rs` | Remove `#[ignore]` from 7 tests, fix metadata header to use `asset-root-json` |

### Test spec → Rust test mapping

| Test spec (cli.md) | Rust test | Status |
|---------------------|-----------|--------|
| §44 case 10 (happy path download) | `job_download_output_existing_files_shows_conflict_prompt` | Blocked → fix |
| §44 case 13 (conflict resolution) | `job_download_output_existing_files_shows_conflict_prompt` | Blocked → fix |
| §44 case 14 (--yes flag) | `job_download_output_yes_flag_defaults_to_create_copy` | ✅ Passes |
| F7 AUDIT-008 (cross-OS root) | 6 tests listed above | Blocked → fix |

### Batching

Single batch — all changes are small and tightly coupled. No need to
split.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1120 tests across all crates (excluding 7 ignored).
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
