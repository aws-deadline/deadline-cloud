You are doing Step 1 (Study Python) of the development workflow.

Read these files first and abide by them:
- `AGENTS.md` — repo conventions
- `specs/HANDOFF.md` — resume in-flight work or pick a new work item
- `specs/progress.md` — work items table (if no active work in HANDOFF)
- `audit_reports/*` — check for unfinished action items to address
- `specs/patterns.md` — design principles and coding conventions

**Resumability:** If HANDOFF shows Step 1 was already started for this
work item, review what was done and continue from where it left off.
Do NOT restart from scratch.

**Pick work item:**
If no active work item, present candidate items and wait for human to choose.
Do NOT pick a work item yourself because the human decides priority.
Do NOT begin studying any feature until the human has confirmed which work item to do.

⛔ GATE: If no active work item in HANDOFF, present candidates and STOP.
Do not proceed until the human picks one.

**Record baseline:**
Before studying anything, run the full test suite:
```bash
make test 2>&1 | tail -20
```
This runs `cargo test` (Rust), `pytest pytests/ui_accessibility/` (xa11y UI),
and `pytest pytests/bindings/` (PyO3 bindings). The Makefile auto-detects
`.venv/` — no manual activation needed. If `.venv/` doesn't exist, create
it first (`python3 -m venv .venv && .venv/bin/pip install -e ".[test]"`).
If only Rust tests matter, `cargo test` works without a venv.

Record the current pass counts for all three. This is the baseline — if
it's not green, fix it before proceeding because you cannot port on top
of a broken build.

**Study the feature deeply:**
- `specs/{crate}/README.md` and topic files for the target crate
- `specs/deadline-cli/{command}.md` — if the work item involves CLI commands
- `specs/python-observations.md` — known behavioral notes
- Python source at `../deadline-cloud-python` for the feature being ported
- Python tests for the feature being ported
- For CLI commands calling AWS APIs: run the Python CLI and capture exact output

**Constraints:**
- Do NOT write any implementation code because this step is study only
- Do NOT write any test code because that is Step 2
- Do NOT modify any existing files other than HANDOFF because this step is read-only

**Update `specs/HANDOFF.md` with:**
- Active work item and baseline test count
- High-level implementation plan (which crates/modules change, new types, tests to write)
- Cross-reference table mapping test spec cases to planned Rust test names
- Batching strategy if the work item is large
- "Status: Step 1 complete, awaiting review"

⛔ GATE: Present your findings and plan. Stop and wait for review.
