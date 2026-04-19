You are doing Step 4 (Compare CLIs) of the development workflow.

Read `specs/HANDOFF.md` for the current work item and what was implemented.

Run the Python CLI and the Rust CLI with the same arguments and diff output.
This catches differences that stub-server tests miss.

Process:
1. Check auth: `deadline auth status` — if not authenticated, ask human to log in
2. Discover resources: `deadline farm list`, `deadline queue list`, etc.
3. Use the resources you found to test each key case that you implemented. Then, diff both CLIs:
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
   compare error messages and exit codes for all error paths
6. Fix any output differences. If a difference is accepted, document
   the rationale.

⛔ GATE: Present comparison results and any accepted differences. Stop and wait for
review. Update `specs/HANDOFF.md` with step status once review is complete.
