You are doing Step 2 (Write Tests) of the development workflow.

Read these files first and abide by them:
- `specs/HANDOFF.md` — current work item, plan, and test case mapping
- `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow

**Resumability:** If HANDOFF shows Step 2 was already started for this
work item, review what was written and continue from where it left off.
Do NOT restart from scratch.

⛔ GATE: Before writing any test code, present the full list of tests you
plan to write (test names, what each tests, which module/file). Stop and
wait for human approval. Do NOT write test code until approved.

**Write tests that define the behavioral contract before implementation.**
This may mean new tests or updating existing tests with gaps. Derive from:
1. Python implementation code to determine testable behavior
2. Python unit tests (port to Rust equivalents)
3. Python CLI output (run it, capture it — this is the reference)
4. Edge cases from Step 1
5. Error paths — every error the Python code can produce

**Constraints:**
- Do NOT write any implementation code because this step is tests only
- Do NOT modify existing passing tests to make them fail artificially because that corrupts the test suite
- Do NOT skip error path tests because error behavior is part of the contract
- Evaluate existing tests to see if there are tests worth removing OR updating
  to assert this new behavior to minimize redundancy
- Consider adding new tests if no existing tests are worth updating if the new
  behavior being added is too unique to do so
- Prefer Level 2 (CLI subprocess + stub server) when CLI-reachable
- For GUI features: Level 2 tests go in `pytests/ui_accessibility/` using xa11y
  (see DEVELOPMENT.md)
- Use Level 1 (library unit) for precision or non-CLI-reachable behavior
- CLI output tests use `insta-cmd` snapshots
- Run tests and confirm they all fail — if any passes, it's not testing
  anything new and needs fixing

**Update `specs/HANDOFF.md` with:**
- List of test files created/modified
- Test count (how many new tests)
- "Status: Step 2 complete, awaiting review"

⛔ GATE: Present the test suite (file names, test names, what each tests).
Stop and wait for approval before writing any implementation code.
