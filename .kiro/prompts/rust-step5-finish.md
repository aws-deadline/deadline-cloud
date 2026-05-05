You are doing Steps 5-7 (Audit & Fix, Spec, Commit) of the development workflow.

Read these files first:
- `specs/HANDOFF.md` — current work item, implementation details, CLI comparison results
- `specs/{crate}/README.md` — existing specs for the target crate

**Step 5 — Audit & Fix (code ↔ tests):**
Read the implementation and tests for the feature you just built:
1. The implementation in `crates/{crate}/src/`
2. The tests (L1 unit tests and L2 CLI subprocess tests)

Check for alignment:
- Does the code handle all edge cases the Python source handles?
- Do the tests cover every code path, including error paths?
- Do all tests pass for the right reasons (not vacuously)?
- Are there code paths with no test coverage?
- Do the CLI comparison results from Step 4 reveal anything the tests missed?

Produce a written findings list. For each finding, categorize as: Bug,
Missing coverage, or Improvement. If no findings, state "Audit clean —
no findings." If bugs are outside the scope of your changes, still
document them as bugs to fix in this iteration.

Fix findings: bugs → fix code + verify tests catch it. Missing
coverage → add or strengthen tests. Improvements → implement if
low-risk, defer if not. If fixes were significant, re-audit the
changed areas. Loop until clean.

If any fix resolves an existing audit finding (AUDIT-NNN), update the
finding's entry in `specs/audit_reports/<date>-behavioral-parity.md`:
mark it as Fixed, update the Rust behavior description, and update the
summary table and remaining open list at the top and bottom of the report.
Audit finding status lives in the audit report, not in `progress.md` or
`HANDOFF.md`.

**Step 6 — Write spec:**
Write or update spec files in `specs/{crate}/` to describe the final
audited state of the code. Include: behavioral contract, data flows,
edge cases, design decisions, differences from Python. Exclude:
implementation mechanics, internal APIs, and forego update if changes are minor.
Update `specs/{crate}/README.md` index if new files were created.

**Step 7 — Commit:**
1. Run pre-commit checks (all must pass before committing):
   ```bash
   make fmt          # cargo fmt --check (no formatting drift)
   make lint         # cargo clippy -- -D warnings (no lint warnings)
   make test         # cargo test + pytest gui/tests/ (all tests green)
   ```
   If `make fmt` fails, run `cargo fmt` and include the fixes in your commit.
2. Verify specs are updated
3. Update `specs/progress.md` work items table (status only — no audit details)
4. If audit findings were resolved, verify they are updated in
   `specs/audit_reports/2026-04-17-behavioral-parity.md` (the single
   source of truth for audit finding status)
5. Clear `specs/HANDOFF.md` active work item
6. Commit with conventional commit message covering the full batch

⛔ GATE: Present the audit findings and final commit summary. Stop
and wait for review. Update `specs/HANDOFF.md` with step status once review is complete.
