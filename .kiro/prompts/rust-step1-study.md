You are doing Step 1 (Study Python) of the development workflow.

Read these files first and abide by them:
- `AGENTS.md` — repo conventions
- `specs/HANDOFF.md` — resume in-flight work or pick a new work item
- `specs/progress.md` — work items table (if no active work in HANDOFF)
- `specs/patterns.md` — design principles and coding conventions

If no active work item, present candidate items and wait for human to choose.

Then study the feature deeply:
- `specs/{crate}/README.md` and topic files for the target crate
- `specs/test_specs/` — find the section for this work item's test cases
- `specs/cli/{command}.md` — if the work item involves CLI commands
- `specs/python-observations.md` — known behavioral notes
- Python source at `../deadline-cloud-python` for the feature being ported
- Python tests for the feature being ported
- For CLI commands calling AWS APIs: run the Python CLI and capture exact output

Update `specs/HANDOFF.md` with:
- High-level implementation plan (which crates/modules change, new types)
- Cross-reference table mapping test spec cases to planned Rust test names
- Batching strategy if the work item is large

⛔ GATE: Present your findings and plan. Stop and wait for review.
Update `specs/HANDOFF.md` with step status once review is complete.