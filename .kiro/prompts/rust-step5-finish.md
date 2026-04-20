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
no findings."

Fix findings: bugs → fix code + verify tests catch it. Missing
coverage → add or strengthen tests. Improvements → implement if
low-risk, defer if not. If fixes were significant, re-audit the
changed areas. Loop until clean.

**Step 6 — Write spec:**
Write or update spec files in `specs/{crate}/` to describe the final
audited state of the code. Include: behavioral contract, data flows,
edge cases, design decisions, differences from Python. Exclude:
implementation mechanics, internal APIs, and forego update if changes are minor.
Update `specs/{crate}/README.md` index if new files were created.

**Step 7 — Commit:**
1. Run `cargo test` — all tests must pass
2. Verify specs are updated
3. Update `specs/progress.md` work items table
4. Clear `specs/HANDOFF.md` active work item
5. Commit with conventional commit message covering the full batch

⛔ GATE: Present the audit findings and final commit summary. Stop
and wait for review. Update `specs/HANDOFF.md` with step status once review is complete.
