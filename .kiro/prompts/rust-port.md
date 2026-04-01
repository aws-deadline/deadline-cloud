You are porting AWS Deadline Cloud client software from Python to Rust. Before doing anything, read these files in order — skip none:

1. `AGENTS.md` — repo conventions, build/test commands
2. `docs/designs/rust-rewrite/README.md` — progress table with work items
3. `docs/designs/rust-rewrite/workflow.md` — the development workflow (rules, design principles, AWS SDK patterns, steps 0–7)
4. `docs/ARCHITECTURE.md` — crate dependency graph
5. `docs/TESTING.md` — test philosophy (Level 1 vs Level 2, no mocking, insta snapshots)
6. `docs/designs/rust-rewrite/migration_strategy.md` — goals, GUI strategy, phased rollout
7. `docs/designs/rust-rewrite/data_flow.md` — persistent data formats
8. `docs/designs/rust-rewrite/observations.md` — behavioral notes and ambiguities

After reading, follow this workflow:

## Pick the next work item

Open `docs/designs/rust-rewrite/README.md` and find the Progress table. Pick the first row with status "Not started" whose dependencies are all "✅ Done". Announce which work item you're picking up.

## Step 0: Plan and get approval

Read the `docs/specs/<crate>.md` for the target crate and the relevant `docs/designs/rust-rewrite/test_specs/` sections listed in the work item row. Then read the Python source for the feature being ported (the Python repo is at `../deadline-cloud-python`).

Present:
- What you're implementing and which test spec cases it covers
- How you'll batch the work into commits
- The Rust API design (structs/modules, not a line-by-line port — reference the Design Principles in workflow.md)
- Edge cases and surprising Python behaviors
- What's blocked or deferred

**Wait for my approval before writing any code.**

## Step 1: Audit existing code

Before new code, check what's already implemented against the Python source. Flag behavior gaps, missing edge cases, idiomatic Rust improvements. Implement improvements first, commit, then proceed.

## Step 2: Update the crate spec

Write up the feature's behavior in `docs/specs/<crate>.md`.

## Step 3: Red — write failing tests

Write tests asserting on observable behavior (stdout, stderr, exit code, file contents). Prefer Level 2 (CLI subprocess + stub server + insta snapshots). Run them — they must fail.

## Step 4: Green — implement

Write minimum code to make tests pass. Follow the AWS SDK patterns from workflow.md (ResponseBodyCapture, manual nextToken loops, etc.).

## Step 5: Verify snapshots against Python

Compare every new `.snap` against the Python CLI output. Fix mismatches before accepting. Run `cargo insta review`.

## Step 6: Refactor

Clean up. Tests must still pass.

## Step 7: Update docs

Update `docs/specs/<crate>.md`, `docs/ARCHITECTURE.md` if crate relationships changed, and the Progress table in `docs/designs/rust-rewrite/README.md`. Commit.

## Key rules

- No code before approval (steps 0–2 are planning)
- Cross-reference Python for every feature — it does unexpected things
- No mocking — use real temp dirs, wiremock stub servers
- CLI-reachable behavior → Level 2 tests (subprocess + snapshot)
- Commit per logical batch, red-green TDD
- Docs stay in sync with code
- `cargo build && cargo test` must pass before every commit
