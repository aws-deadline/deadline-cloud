# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

**#15e — Behavioral parity audit** (Tier 2: code review)

### Status: Audit complete — 56 findings documented

### Report
`specs/audit_reports/2026-04-17-behavioral-parity.md`

### Summary
| Priority | Count |
|----------|-------|
| Critical | 4     |
| High     | 10    |
| Medium   | 22    |
| Low      | 20    |

### Critical bugs (fix first)
1. **AUDIT-001:** `queue sync-output` doesn't download files
2. **AUDIT-002:** `known_asset_paths` separator uses `/` instead of `:`
3. **AUDIT-003:** `auto_accept` + unknown paths proceeds (should cancel)
4. **AUDIT-004:** INI key case sensitivity — Rust is case-sensitive, Python lowercases

### Recommended fix order
1. Fix the 4 Critical bugs (AUDIT-001 through AUDIT-004) — correctness/safety
2. Fix High bugs affecting correctness: AUDIT-007 (sync-output pagination),
   AUDIT-014 (5GB upload limit), AUDIT-005 (DCM default profile)
3. Fix High UX gaps: AUDIT-006 (fleet get --queue-id), AUDIT-008 (download
   root editing), AUDIT-009 (upload confirmation)
4. Address Medium validation bugs: AUDIT-027, AUDIT-028, AUDIT-029
5. Remaining Medium and Low items per priority

### Next steps
- Pick up Critical fixes as the next work item (or create a new #15e-F1
  work item for the fix batch)
- Each fix follows the standard workflow: test → implement → verify
- Update each finding's Resolution field as fixes are applied

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1036 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.

## Notes from #15d

**Behavioral gap fixed:** `queue export-credentials` now validates
credential response fields before formatting. Empty/missing credentials
produce an error instead of null JSON output (matching Python behavior).
