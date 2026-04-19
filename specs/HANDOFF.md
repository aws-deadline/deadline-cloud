# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None — pick from `specs/progress.md`.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1082 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.
5. **`serde_json` has `preserve_order` enabled** — `json!()` preserves
   insertion order, not alphabetical.
6. **Test fixtures for CLI comparison.** `test_fixtures/job_bundles/`
   contains sample job bundles for manual comparison between Python and
   Rust CLIs. See `test_fixtures/README.md`.

## Recently Completed

**15e — Behavioral parity audit (Batch 2)** — all sub-batches A–F done.

- Sub-batches A–C: INI parser fixes, path traversal, sync-output S3 pipeline
- Sub-batch D: Job pagination, suggest_resources rewrite, manifest merge order
- Sub-batch E: Output parity (session action count, manifest paths, WARNING, path summary)
- Sub-batch F: Duration format days, dry-run file/byte counts

All audit-found bugs are fixed. Remaining feature gaps are tracked on
their original work items in `progress.md` under "Audit gaps in
completed items".

### Accepted Differences

| ID | Reason |
|----|--------|
| AUDIT-024 | YAML key ordering — accepted per `patterns.md` |
| AUDIT-039 | `job wait` verbose to stderr — Rust approach is better |
| AUDIT-046 | `require_setting` exit code — function is unused dead code |
| SYNC-004 | Path summary groups by directory (Rust) vs flat full-path (Python) — same data, Rust more readable |
| (no ID) | `sync-output` job order — HashMap iteration vs Python dict insertion order |
| (no ID) | `sync-output` WARNING placement — same text, slightly different position in output |
