You are doing Step 3 (Implement) of the development workflow.

Read these files first and abide by them:
- `specs/HANDOFF.md` — current work item, plan, approved test suite
- `specs/patterns.md` — AWS SDK usage patterns, coding conventions

**Resumability:** If HANDOFF shows Step 3 was already started for this
work item, review what was implemented and continue from where it left off.
Do NOT restart from scratch.

**Write the minimum code to make the tests pass.** Then:
1. Run `cargo build` (full workspace)
2. Run `make test` (full test suite — Rust tests, xa11y UI tests, PyO3 binding tests)
3. If snapshots were created, run `cargo insta review` — verify each against
   Python implementation before accepting.

The `make test` target runs:
- `cargo test` — all Rust crate tests
- `pytest pytests/ui_accessibility/` — xa11y UI tests (requires macOS
  Accessibility permission; catches QML type registration errors, missing
  Accessible.name attributes, and runtime failures that `cargo test`
  cannot detect)
- `pytest pytests/bindings/` — PyO3 binding tests (requires `maturin`)

If any test category cannot run (no display server, no accessibility
permission, maturin not installed), explicitly state this limitation and
do NOT claim the implementation is complete without verification.

**Constraints:**
- Do NOT use `INSTA_UPDATE=always` because each snapshot must be manually verified against Python output
- Do NOT modify test assertions to make them pass because the tests define the contract — fix the implementation instead
- Do NOT add features beyond what the tests require because scope creep delays review
- Do NOT over-abstract — if the Python is a simple function, keep it simple in Rust

For CLI commands calling AWS APIs, follow patterns in `specs/patterns.md`:
- Callers own their SDK calls — no wrapper functions in api.rs
- All API calls return typed SDK output; callers use typed accessors
- List functions use SDK paginator via `client::collect_paginated`
- Search functions (offset-based) call SDK `.send()` directly
- Display paths build serializable response structs from typed output
- DateTime formatting: `dt.fmt(DateTimeWithOffset).replace('T', " ").replace('Z', "+00:00")`
- HashMap keys must be sorted for deterministic display output
- Mock responses: include all fields the real API returns

Refactor for clarity once green — but don't over-abstract.

**Update `specs/HANDOFF.md` with:**
- Files created/modified
- Test results summary (pass/fail counts)
- "Status: Step 3 complete, awaiting review"

⛔ GATE: All tests must pass and full workspace must build. Present results
(test output summary, files changed). Stop and wait for review.
