You are doing Step 2 (Write Tests) of the development workflow.

Read these files first and abide by them:
- `specs/HANDOFF.md` — current work item, plan, and test case mapping
- `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow

Write tests that define the behavioral contract before any implementation.
This may mean new tests or updating existing tests with gaps. Derive from:
1. Test spec section for this work item
2. Python unit tests (port to Rust equivalents)
3. Python CLI output (run it, capture it — this is the reference)
4. Edge cases from Step 1
5. Error paths — every error the Python code can produce

Rules:
- Prefer Level 2 (CLI subprocess + stub server) when CLI-reachable
- Use Level 1 (library unit) for precision or non-CLI-reachable behavior
- CLI output tests use `insta-cmd` snapshots
- Run tests and confirm they all fail — if any passes, it's not testing
  anything new and needs fixing

⛔ GATE: Present the test suite. Stop and wait for approval before writing any implementation code.
Update `specs/HANDOFF.md` with step status once review is complete.
