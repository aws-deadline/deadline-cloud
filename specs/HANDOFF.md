# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Step Status

(Completed — see Recently Completed below)

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1108 tests across all crates.
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
| AUDIT-008 | Interactive root path editing — deferred (fancy interactive feature). |

## Recently Completed

**AUDIT-013: Hash cache V4 compatibility**

- Rust now uses Python's `hashesV4` table instead of `hashesV5`. Both CLIs
  share one hash cache — zero re-hashing when switching between tools.
- `HashCacheEntry.last_modified_time` changed from `i64` (nanoseconds) to
  `String` matching Python's `str(datetime.fromtimestamp(st_mtime))` format.
- New `format_mtime_for_cache(secs, nsec)` converts through `f64` to replicate
  Python's float-precision loss from `os.stat().st_mtime`.
- Edge case fixed: nanoseconds near 1 second (e.g., 999999500ns) round
  microseconds to 1000000, which carries into the seconds field.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-043 | User-agent position in header — Rust SDK limitation, content is correct |
| AUDIT-046 | `require_setting` exit code — function was unused dead code, deleted |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
