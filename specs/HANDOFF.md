# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1085 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.

## Recently Completed

**#4 — `fleet get --queue-id` mode (AUDIT-006)**

- Added `list_queue_fleet_associations` API function (paginated, ResponseBodyCapture)
- Added `--queue-id` option to `fleet get` CLI command
- Queue mode: get_queue → list associations → get each fleet → print with status
- 3 new Level 2 tests, all passing
- CLI comparison verified against real API — only pre-existing accepted
  differences (field order, extra `arn` field)
- Python bug found: `--queue-id` CLI arg overwritten by config default.
  Rust correctly prioritizes CLI arg.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-046 | `require_setting` exit code — function is unused dead code |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
