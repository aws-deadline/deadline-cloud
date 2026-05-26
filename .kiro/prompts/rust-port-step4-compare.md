You are doing Step 4 (Compare CLIs) of the development workflow.

Read `specs/HANDOFF.md` for the current work item and what was implemented.

**Resumability:** If HANDOFF shows Step 4 was already started for this
work item, review what was compared and continue from where it left off.
Do NOT restart from scratch.

**Run the Python CLI and the Rust CLI with the same arguments and diff output.**
This catches differences that stub-server tests miss.

**Process:**
1. Check auth: `deadline auth status` — if not authenticated, ask the
   human to log in.
   Do NOT run `deadline auth login` yourself because it requires browser interaction.

⛔ GATE: If not authenticated, stop and ask the human to log in.
Do not proceed until auth is confirmed.

2. Discover resources: `deadline farm list`, `deadline queue list`, etc.
3. Use the resources you found to test each key case that you implemented. Diff both CLIs:
   ```bash
   diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
   ```
4. For bundle submit comparisons, use the committed test fixtures:
   ```bash
   diff <(deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1) \
        <(./target/debug/deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1)
   ```
   See `test_fixtures/README.md` for available bundles and more examples.
5. If the command has no API calls (pure validation / URL parsing),
   compare error messages and exit codes for all error paths.
6. Fix any output differences.

**For GUI work items:** CLI diff doesn't apply to interactive dialogs.
Instead, run the xa11y tests which verify equivalent behavior:
```bash
pytest pytests/ui_accessibility/ -v --tb=short
```
If they fail, investigate whether it's a real regression or a test
needing update for the Rust-native dialog.

**Constraints:**
- Do NOT accept output differences without documenting the rationale because undocumented divergences become bugs later
- Do NOT skip error path comparisons because error messages are part of the user contract
- Do NOT modify Python CLI behavior because Python is the reference implementation

**If a difference is found:**
Present it and ask whether to fix or accept.
Do NOT decide on your own whether a difference is acceptable because the human owns that decision.

⛔ GATE: If any differences are found, present them and STOP.
Wait for human to say "fix" or "accept" for each one.

**Long-lived processes (MCP server, etc.):**
`diff` doesn't work for processes that never exit. Instead, compare
Python and Rust source code directly: tool names, parameter names/types,
required vs optional, instructions text, and error handling shape.

**Update `specs/HANDOFF.md` with:**
- Commands compared and results
- Any accepted differences with rationale
- "Status: Step 4 complete, awaiting review"

⛔ GATE: Present comparison results and any accepted differences.
Stop and wait for review.
