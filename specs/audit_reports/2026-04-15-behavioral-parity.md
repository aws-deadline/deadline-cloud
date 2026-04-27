# Audit: Behavioral Parity (post-April-10 changes)

**Date:** 2026-04-15
**Scope:** Code added/changed since the 2026-04-10 audit: `bundle submit` implementation,
credential scoping wiring, crate consolidation, and verification of previous fixes.
**Status:** Complete

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 0     | 0     | 0         |
| High     | 1     | 1     | 0         |
| Medium   | 2     | 2     | 0         |
| Low      | 2     | 2     | 0         |

All 7 fixes from the April 10 audit verified as correctly applied (including F-1).
Crate consolidation verified clean — no regressions.
## Crate Consolidation Verification

✅ All public APIs from dissolved `deadline-models` and `deadline-common` properly
re-homed in `deadline-api` and `deadline-job-attachments`. Zero stale imports.
Zero missing re-exports. All downstream `Cargo.toml` files correct. No regressions.
## Not parity gaps (removed from action items)

| ID | Original finding | Why removed |
|----|-----------------|-------------|
| AUDIT-020 | Job history not saved in CLI | Python CLI doesn't save history either — GUI only. Matches Python. Spec updated. |
| AUDIT-024 | CREATE_FAILED missing job ID | Python also omits job ID in this error. Our fix is a Rust improvement, not a parity fix. Already applied — keeping it. |
| AUDIT-025 | Submitter name "CLI" vs "deadline-cloud-cli" | Python also defaults to `"CLI"`. No difference exists. |
| AUDIT-027 | Priority hardcoded before overwrite | Python uses the same pattern (`"priority": 50` then overwrite). Not dead code — it's the default. |
| AUDIT-028 | Fleet-scoped config missing endpoint URL | Worker logs not exposed via CLI. Not a parity gap for current scope. |
| AUDIT-029 | Missing CloudWatch AccessDeniedException test | Test coverage gap, not a behavioral parity issue. Moved to Low as regression guard for F-1 fix. |
