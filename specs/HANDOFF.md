# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1086 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.

## Recently Completed

**Quick wins batch — AUDIT-046, SYNC-005, AUDIT-056, AUDIT-043**

- AUDIT-046: Deleted unused `require_setting` function (dead code)
- SYNC-005: Added progress messages to `queue sync-output`:
  "Found N new session action(s) across N job(s)" and
  "Populating manifest S3 keys for N jobs..."
- AUDIT-056: Implemented storage profile suggestion chain:
  `list_storage_profiles_for_queue` API function, `try_list_storage_profiles`
  helper, wired match arm, added `suggest_resources_on_client_error` to
  `queue get-storage-profile` command. 1 new Level 2 test.
- AUDIT-043: Reclassified as accepted difference — Rust SDK `app_name()`
  vs Python `user_agent_extra`. Content identical, header position differs.

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-030 | False finding — Python also doesn't support macOS |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-043 | User-agent position in header — Rust SDK limitation, content is correct |
| AUDIT-046 | `require_setting` exit code — function was unused dead code, deleted |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
