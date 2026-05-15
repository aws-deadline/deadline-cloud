You are doing Step 5 (Audit & Fix) of the development workflow.

Read these files first:
- `specs/HANDOFF.md` — current work item, implementation details, CLI comparison results
- `specs/{crate}/README.md` — existing specs for the target crate

**Resumability:** If HANDOFF shows Step 5 was already started for this
work item, review what was audited and continue from where it left off.
Do NOT restart from scratch.

**Audit the implementation and tests:**
Read the code you just built:
1. The implementation in `crates/{crate}/src/`
2. The tests (L1 unit tests and L2 CLI subprocess tests)

Check for alignment:
- Does the code handle all edge cases the Python source handles?
- Do the tests cover every code path, including error paths?
- Do all tests pass for the right reasons (not vacuously)?
- Are there code paths with no test coverage?
- Do the CLI comparison results from Step 4 reveal anything the tests missed?

**Produce a findings list.** For each finding, categorize as:
Bug, Missing coverage, or Improvement.
If no findings, state "Audit clean — no findings."

**Constraints:**
- Do NOT skip the audit even if you believe the code is correct because fresh eyes catch bugs
- Do NOT fix findings without presenting them first because the human may disagree on priority
- Do NOT suppress findings that are outside scope — document them as known issues

⛔ GATE: Present the audit findings list. Stop and wait for human to
approve which findings to fix now vs defer.

**Fix approved findings:**
- Bugs → fix code + verify tests catch it
- Missing coverage → add or strengthen tests
- Improvements → implement if low-risk, defer if not
- If fixes were significant, re-audit the changed areas. Loop until clean.

If any fix resolves an existing audit finding (AUDIT-NNN), update the
finding's entry in `audit_reports/archive/<date>-behavioral-parity.md`:
mark it as Fixed, update the Rust behavior description, and update the
summary table at the top of the report.

**Update `specs/HANDOFF.md` with:**
- Findings list (fixed and deferred)
- "Status: Step 5 complete, awaiting review"

⛔ GATE: Present final audit state (all findings resolved or deferred).
Stop and wait for review.
