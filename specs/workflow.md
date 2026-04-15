# Development Workflow

How to implement features, fix bugs, and maintain quality in deadline-cloud-rs.
Adapted from the [openjd-rs porting methodology](archive/rust-rewrite-workflow.md)
with front-loaded testing and spec-as-quality-tool.

## Session Start

1. Read `specs/HANDOFF.md`. If it has active work, resume from where it left off.
2. If no active work, check the Work Items table in `specs/progress.md`
   and pick the first "Not started" item whose dependencies are all done.
3. Update `specs/HANDOFF.md` with the chosen work item.

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

## Design Principles

The goal is identical observable behavior, not identical internal structure.

1. **Don't replicate Python's module structure.** Python uses module-level
   functions with global caches because that's idiomatic Python. Rust
   should use structs that own their state. If Python has five free
   functions sharing a module-level dict, Rust probably wants one struct
   with five methods.

2. **Globals must earn their keep.** Prefer owned state on a struct
   when the state needs different configurations per consumer or must
   be isolated for testing. However, process-wide singletons (caches,
   connection pools, configuration) are legitimate when the alternative
   is threading a parameter through every function call. Use
   `std::sync::Mutex<T>` with `std::sync::LazyLock` (Rust 1.80+) for
   mutable globals. The underlying struct should still be usable
   independently for cases that need a separate instance.

3. **Every abstraction must earn its keep.** Before adding a type,
   wrapper, conversion layer, or any indirection that the Python
   doesn't have, ask: "what does this prevent or enable?" If the
   answer is a concrete benefit (compile-time error catching, safety,
   testability), add it. If the answer is "it's more Rust-like" or
   "it feels cleaner," don't — that's complexity without value. The
   simplest correct implementation wins.

4. **Preserve observable behavior exactly.** Same output, same errors,
   same exit codes, same caching semantics. The test specs define the
   contract — internal structure is free to diverge.

5. **Don't over-abstract.** If the Python is a simple function that
   doesn't need to become a trait, don't make it one. Only introduce
   abstractions that solve a real problem.

6. **Ask "what would I design if the Python didn't exist?"** Read the
   test spec and the Python source, then close the Python file and
   design the Rust API from the behavioral requirements. Open the
   Python again only to verify you haven't missed edge cases.

7. **Implement behavior, don't mirror code.** The Python source is a
   reference for *what the system does*, not *how to build it*. Read
   Python to understand the behavioral contract, then implement that
   contract in idiomatic Rust.

## The Loop

For each work item, cycle through these steps. The agent drives the loop
autonomously — human review happens at step boundaries where noted.

### Step 1: Study Python

Read deeply — not just the happy path, but edge cases, surprising
behaviors, error messages, and how the feature interacts with config,
credentials, and other subsystems.

Before studying, read these (in order, skip none):

- [ ] `AGENTS.md` — repo conventions, build/test commands
- [ ] `specs/architecture.md` — crate dependency graph, data flows, shared conventions
- [ ] `specs/testing.md` — test philosophy, levels, no-mocking policy, snapshot workflow
- [ ] `specs/patterns.md` — AWS SDK usage patterns, coding conventions
- [ ] `specs/workflow.md` — this file (the development loop you're following)
- [ ] `specs/progress.md` — work items table, status, dependencies
- [ ] `specs/{crate}/README.md` for the target crate — architecture, gotchas, status
- [ ] `specs/{crate}/` topic files relevant to the feature
- [ ] `specs/cli/{command}.md` — when the work item involves CLI commands
- [ ] `specs/python-observations.md` — behavioral notes and ambiguities
- [ ] Python source for the feature being ported

For CLI commands that call AWS APIs, also check:
- [ ] The SDK operation docs on docs.rs for input/output types
- [ ] Whether a paginator exists
- [ ] What fields the Python CLI selects for list output and in what order
- [ ] Run the Python CLI command and capture its exact output — this is
  the target you must match

Update `specs/HANDOFF.md` with findings.

### Step 2: Write exhaustive tests

Write tests that define the behavioral contract before writing any
implementation. Derive test cases from:

1. The Python unit tests for the feature (port them to Rust equivalents)
2. The Python CLI output (run it, capture it, assert on it)
3. Edge cases discovered during Step 1
4. Error paths — every error the Python code can produce

Prefer Level 2 (CLI subprocess) tests when the behavior is CLI-reachable.
Use Level 1 (library unit) tests for behavior too low-level to assert
through CLI output.

**CLI output tests use `insta-cmd` snapshots.** For each snapshot,
actually run the Python CLI and capture its output — this is the
reference. Do not reason about what the output "would be."

Run the tests and confirm they fail. If a test passes before
implementation, it's not testing anything new.

**Gate:** Present the test suite and your implementation plan. Wait for
human approval before writing implementation code.

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
3. Run both CLIs and compare output for key cases:
   ```bash
   diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
   ```
4. If snapshots were created, run `cargo insta review` after verifying
   each against Python output. **Do not use `INSTA_UPDATE=always`
   without reviewing each snapshot.**

Refactor for clarity once green — but don't over-abstract.

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

This is the quality step adapted from openjd-rs. Compare three artifacts:

1. **Specs** — `specs/{crate}/` files for the feature
2. **Implementation** — `crates/{crate}/src/` source code
3. **Tests** — unit tests and CLI subprocess tests

Check alignment:
- Does the spec accurately describe what the code does?
- Does the code do what the spec says?
- Do the tests cover what the spec documents?
- Are there behaviors in the code that the spec doesn't mention?
- Are there spec claims that no test verifies?

Produce a findings list. For each finding, categorize:
- **Bug** — code doesn't match intended behavior
- **Spec drift** — spec says one thing, code does another
- **Missing coverage** — behavior exists but no test covers it
- **Improvement** — code works but could be cleaner/faster/safer

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

## Audit Methodology

For cross-cutting behavioral parity audits spanning multiple work items,
see `specs/audit.md`. Use when:
- After completing a batch of new feature work
- Before a release
- After a major refactor touching output formatting or error handling

The per-crate quality evaluation prompt (from openjd-rs):

> Read the specs in `specs/{crate}/`. Read the implementation in
> `crates/{crate}/src/`. Read the tests. Check that all three are
> aligned: specs describe what the code does, code does what the specs
> say, tests confirm the specs are correct. Flag any misalignment.
> Write findings into a report.

## AWS SDK for Rust Patterns

See `specs/patterns.md` for SDK usage conventions: ResponseBodyCapture,
credential scoping, telemetry wrapping, error formatting.
