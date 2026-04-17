# Development Workflow

Procedural steps for implementing features, fixing bugs, and maintaining
quality in deadline-cloud-rs. This file is purely the loop — design
philosophy lives in `specs/patterns.md`, cross-cutting audit methodology
lives in `specs/audit.md`.

## Session Start

1. First, you MUST READ the following files to understand coding conventions for ALL future development (in order, skip none):
- [ ] `AGENTS.md` — repo conventions, build/test commands
- [ ] `specs/architecture.md` — crate dependency graph, data flows, shared conventions
- [ ] `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow
- [ ] `specs/patterns.md` — design principles, AWS SDK usage patterns, coding conventions
- [ ] `specs/workflow.md` — this file (the development loop you're following)
2. Then, read `specs/HANDOFF.md`. If it has active work, resume from where it left off.
3. If no active work, check the Work Items table in `specs/progress.md`
   and identify all "Not started" items whose dependencies are all done.

**⛔ GATE:** Present the candidate work items to the human and wait for
them to choose which one to work on. Do not pick autonomously.

4. Update `specs/HANDOFF.md` with the chosen work item.

## Rules

- **Update HANDOFF.md after every step.** This is the running log — if the
  session ends mid-work, the next session resumes exactly where this one
  left off.
- **Cross-reference Python for every feature.** The Python code often does
  things you wouldn't expect from the spec alone. Read it.
- **Improvements before new code.** Before adding new features, audit
  existing implementation against the Python source. Flag behavior gaps
  and cleanups. Implement improvements first.
- **Commit per batch.** A batch is the complete loop (Steps 1-7) for a
  work item. Do not commit partway through — complete all steps including
  spec updates, then commit. The commit message covers the entire batch.
- **Specs stay in sync with code.** Before committing, the relevant
  `specs/{crate}/` files must reflect what was built. Specs that drift
  from code are worse than no specs.
- **Defer honestly.** If a feature is blocked on an unimplemented crate,
  say so and document it in the Progress table rather than building
  throwaway scaffolding.
- **Full audits use the audit prompt.** For cross-cutting behavioral
  audits spanning multiple work items, follow `specs/audit.md`.

---

## The Loop

For each work item, cycle through these steps. The agent drives the loop
autonomously — human review happens at step boundaries where noted.

### Step 1: Study Python

Read deeply — not just the happy path, but edge cases, surprising
behaviors, error messages, and how the feature interacts with config,
credentials, and other subsystems.

**Reading checklist** (in order, skip none):
- [ ] `specs/{crate}/README.md` for the target crate — architecture, gotchas, status
- [ ] `specs/{crate}/` topic files relevant to the feature
- [ ] `specs/test_specs/` — find the section for this work item's test cases
- [ ] `specs/cli/{command}.md` — when the work item involves CLI commands
- [ ] `specs/python-observations.md` — behavioral notes and ambiguities
- [ ] Python source for the feature being ported
- [ ] Python tests for the feature being ported

For CLI commands that call AWS APIs, also check:
- [ ] The SDK operation docs on docs.rs for input/output types
- [ ] Whether a paginator exists
- [ ] What fields the Python CLI selects for list output and in what order
- [ ] Run the Python CLI command and capture its exact output — this is
  the target you must match

**⛔ GATE:** Update `specs/HANDOFF.md` with findings before proceeding:
- A high-level plan explaining how you intend to write the tests and
  implement the feature (which crates/modules change, what new types
  are needed, how the CLI command wires to the library).
- A cross-reference table mapping test spec cases to planned test names.
- Any batching strategy if the work item is large.

### Step 2: Write exhaustive tests

Write tests that define the behavioral contract before writing any
implementation. This may mean writing new tests, or updating existing
tests that have gaps (e.g., tests that accepted placeholder output,
tests that don't assert on the full behavior, or tests that need
additional mock infrastructure to exercise the real code path).

Derive test cases from:

1. The test spec section for this work item (`specs/test_specs/`)
2. The Python unit tests for the feature (port them to Rust equivalents)
3. The Python CLI output (run it, capture it, assert on it)
4. Edge cases discovered during Step 1
5. Error paths — every error the Python code can produce

Prefer Level 2 (CLI subprocess) tests when the behavior is CLI-reachable.
Use Level 1 (library unit) tests for behavior too low-level to assert
through CLI output.

**CLI output tests use `insta-cmd` snapshots.** For each snapshot,
actually run the Python CLI and capture its output — this is the
reference. Do not reason about what the output "would be."

Run the tests and confirm they fail. If a test passes before
implementation, it's not testing anything new.

**⛔ GATE:** Present the test suite and your implementation plan. Wait
for human approval before writing implementation code.

**⛔ GATE:** Update `specs/HANDOFF.md` with current step status.

### Step 3: Implement

Write the minimum code to make the tests pass.

For CLI commands that call AWS APIs, follow the patterns in
`specs/patterns.md`:
- All API functions use `ResponseBodyCapture` to capture raw JSON
- List functions: manual `nextToken` loop with `ResponseBodyCapture`
- Mock responses: include all fields the real API returns

After tests pass:
1. Run `cargo build` (full workspace)
2. Run `cargo test` (full test suite)

**⛔ GATE: Compare both CLIs.** Do not skip this step.
- Run `deadline auth status` (Python CLI) to check authentication.
  If not authenticated, ask the human to log in.
- Discover real resources: run `deadline farm list`, `deadline queue
  list`, etc. to find IDs for comparison.
- For each key case, run both CLIs with the same arguments and diff:
  ```bash
  diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
  ```
- Fix any output differences (field order, formatting, missing
  fields) before proceeding.
- If the command has no API calls (pure argument validation / URL
  parsing), compare error messages and exit codes for all error paths.

3. If snapshots were created, run `cargo insta review` after verifying
   each against Python output. **Do not use `INSTA_UPDATE=always`
   without reviewing each snapshot.**

Refactor for clarity once green — but don't over-abstract.

**⛔ GATE:** Update `specs/HANDOFF.md` with current step status and
any differences found during CLI comparison.

### Step 4: Write spec

Write or update the spec files in `specs/{crate}/` to reflect what was
built. The spec describes the code as it exists — not aspirational design.

**What goes where:**
- If the feature fits an existing topic file → update that file
- If the feature is a new subsystem → create a new topic file
- If a new file was created → update `specs/{crate}/README.md` index table
- Cross-cutting behavior → update `specs/architecture.md` or `specs/patterns.md`

**What to include:**
- What the feature does (behavioral contract)
- What data flows through it (inputs, outputs, key data structures)
- Behavioral edge cases and error handling outcomes
- Design decisions with rationale (why, not how)
- Gotchas and constraints
- Differences from Python (table format when there are multiple)

**What to exclude:**
- Implementation mechanics (no code snippets unless they aid comprehension)
- Internal helper function APIs
- Module-internal wiring details

### Step 5: Audit (spec ↔ code ↔ tests)

This is the quality step. Compare three artifacts:

1. **Specs** — `specs/{crate}/` files for the feature
2. **Implementation** — `crates/{crate}/src/` source code
3. **Tests** — unit tests and CLI subprocess tests

Check alignment:
- Does the spec accurately describe what the code does?
- Does the code do what the spec says?
- Do the tests cover what the spec documents?
- Are there behaviors in the code that the spec doesn't mention?
- Are there spec claims that no test verifies?

**⛔ GATE:** Produce a written findings list. For each finding, categorize:
- **Bug** — code doesn't match intended behavior
- **Spec drift** — spec says one thing, code does another
- **Missing coverage** — behavior exists but no test covers it
- **Improvement** — code works but could be cleaner/faster/safer

If there are no findings, state "Audit clean — no findings." Do not
skip producing the list.

### Step 6: Fix

Address findings from Step 5:
- Bugs → fix code, verify tests catch the fix
- Spec drift → update spec or code (whichever is wrong)
- Missing coverage → add tests
- Improvements → implement if low-risk, defer if not

If fixes were significant, re-run Step 5 on the changed areas. Loop
until the audit produces no actionable findings.

### Step 7: Commit

1. Verify all tests pass: `cargo test`
2. Verify specs are updated and accurate
3. Update `specs/progress.md` work items table
4. Clear `specs/HANDOFF.md` active work item
5. Commit with a conventional commit message covering the full batch
