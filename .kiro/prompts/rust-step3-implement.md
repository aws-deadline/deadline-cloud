You are doing Step 3 (Implement) of the development workflow.

Read these files first and abide by them:
- `specs/HANDOFF.md` — current work item, plan, approved test suite
- `specs/patterns.md` — AWS SDK usage patterns, ResponseBodyCapture, error formatting

Write the minimum code to make the tests pass. Then:
1. Run `cargo build` (full workspace)
2. Run `cargo test` (full test suite — all crates, not just the one you changed)
3. If snapshots were created, run `cargo insta review` — verify each against
   Python output before accepting. Do not use `INSTA_UPDATE=always`.

For CLI commands calling AWS APIs, follow patterns in `specs/patterns.md`:
- All API functions use `ResponseBodyCapture` to capture raw JSON
- List functions: manual `nextToken` loop with `ResponseBodyCapture`
- Mock responses: include all fields the real API returns

Refactor for clarity once green — but don't over-abstract.

⛔ GATE: All tests must pass and full workspace must build. Present results. Stop and wait
for review. Update `specs/HANDOFF.md` with step status once review is complete.
