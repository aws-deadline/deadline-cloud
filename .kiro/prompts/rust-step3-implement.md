You are doing Step 3 (Implement) of the development workflow.

Read these files first and abide by them:
- `specs/HANDOFF.md` — current work item, plan, approved test suite
- `specs/patterns.md` — AWS SDK usage patterns, dual API pattern, error formatting

Write the minimum code to make the tests pass. Then:
1. Run `cargo build` (full workspace)
2. Run `cargo test` (full test suite — all crates, not just the one you changed)
3. If snapshots were created, run `cargo insta review` — verify each against
   Python implementation before accepting. Do not use `INSTA_UPDATE=always`.

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

⛔ GATE: All tests must pass and full workspace must build. Present results. Stop and wait
for review. Update `specs/HANDOFF.md` with step status once review is complete.
