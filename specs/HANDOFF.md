# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None. Pick the next "Not started" item from `specs/progress.md`.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1045 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.

## Notes from #15e

**Behavioral parity audit complete.** 56 findings documented in
`specs/audit_reports/2026-04-17-behavioral-parity.md`. First fix batch
addressed 8 findings (AUDIT-002, 003, 004, 005, 015, 027, 028, 029).
48 findings remain. Status is "In progress" — audit done, remaining
fixes tracked in the audit report.

**Deferred from fix batch:** AUDIT-001 (sync-output download pipeline)
and AUDIT-014 (multipart upload) are too large for a bug-fix batch —
they need their own work items.
