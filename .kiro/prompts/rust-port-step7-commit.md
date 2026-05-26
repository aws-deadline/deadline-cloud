You are doing Step 7 (Commit) of the development workflow.

Read `specs/HANDOFF.md` for the current work item and all step results.

**Resumability:** If HANDOFF shows Step 7 was already started for this
work item, check if a commit was already made. If so, present it and stop.
Do NOT create duplicate commits.

**Pre-commit checks (all must pass):**
```bash
make fmt          # cargo fmt --check (no formatting drift)
make lint         # cargo clippy -- -D warnings (no lint warnings)
make test         # all tests: Rust + xa11y UI + PyO3 bindings
```
If `make fmt` fails, run `cargo fmt` and include the fixes in your commit.
Do NOT commit if any check fails.

**Constraints:**
- Do NOT commit if any pre-commit check fails because broken code must not be committed
- Do NOT push the commit because it needs human review before sharing
- Do NOT include unrelated changes in the commit because that makes review harder

**Verify docs are updated:**
- `specs/progress.md` — update work item status to ✅ Done
- If audit findings were resolved, verify they are marked as Fixed,
  Deferred, or Accepted in the relevant report under `audit_reports/`

**Clean up `specs/HANDOFF.md`:**
- Remove all intermediate step notes for this work item (plans, test
  mappings, step statuses, findings lists) because they are stale now
- Add a one-line entry to the "Completed items" section
- Set active work item back to "None"

**Commit:**
- Stage only relevant files (implementation, tests, specs, Cargo files)
- Use conventional commit message format
- Include test count in body if tests were added

**Update `specs/HANDOFF.md` with:**
- Active work item: None
- One-line completion entry in Completed items

⛔ GATE: Present the commit message and summary of all changes.
Stop and wait for final review.
