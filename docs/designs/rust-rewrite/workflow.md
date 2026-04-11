# Development Workflow

The workflow for porting each feature from the Python CLI to Rust.

## Rules

- **Check HANDOFF.md first.** At the start of every session, read
  `HANDOFF.md`. If it contains an active work item, resume from where
  it left off — it has the current workflow step, what's done, and
  what's next. If there is no active work item, consult the Work Items
  table in `README.md` and pick the first row with status "Not started"
  whose dependencies are all "✅ Done". When a work item is complete,
  clear `HANDOFF.md` so the next session picks up fresh work.
- **Update HANDOFF.md after every step.** After completing each
  workflow step (0, 1, 2, 3, 4, 5, 6, 7), update `HANDOFF.md` with
  the current step, what was done, and what's next. This is the
  running log — if the session ends mid-work, the next session can
  resume exactly where this one left off.
- **Risk spikes before bulk implementation.** Before starting any new
  batch of work, check the Risk Spikes table in `README.md`. If any
  spike is "Not started" or "In progress", that spike takes priority
  over all other implementation work. Do not proceed with bulk feature
  porting until all spikes have passed. See `migration_strategy.md`
  § "Fail-Fast Strategy" for spike definitions and pass/fail gates.
- **No code before approval.** Steps 0-2 are planning. Present the plan
  and wait for the human to approve before writing any code.
- **Cross-reference Python for every feature.** The Python code often does
  things you wouldn't expect from the spec alone. Read it.
- **Improvements before new code.** Before adding new features, audit
  existing implementation against the Python source. Flag behavior gaps
  and cleanups. Implement improvements first.
- **Commit per batch.** A batch is the complete workflow (Steps 1-7)
  for a work item or group of work items being done together. Do not
  commit partway through — complete all steps including doc updates,
  then commit. Within a batch, the working state lives in HANDOFF.md
  (updated after each step) and uncommitted files. The commit message
  covers the entire batch. Keep batches small and focused.
- **Docs stay in sync.** Before committing, update
  `docs/specs/<crate>.md` with Rust-specific design decisions, update
  the Progress table in `README.md`, and update `HANDOFF.md`. All doc
  updates are part of the batch and included in the same commit. Do
  not let docs drift from code.
- **Defer honestly.** If a feature is blocked on an unimplemented crate,
  say so and document it in the Progress table rather than building
  throwaway scaffolding.

## Design Principles

These apply at Step 0 when designing the Rust equivalent. The goal is
identical observable behavior, not identical internal structure.

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
   same exit codes, same caching semantics (calls are cached, force
   refresh clears cache). The test specs define the contract — internal
   structure is free to diverge.

5. **Don't over-abstract.** If the Python is a simple function that
   doesn't need to become a trait, don't make it one. Only introduce
   abstractions that solve a real problem (testability, extensibility
   the code actually needs).

6. **Ask "what would I design if the Python didn't exist?"** Read the
   test spec and the Python source, then close the Python file and
   design the Rust API from the behavioral requirements. Open the
   Python again only to verify you haven't missed edge cases.

7. **Implement behavior, don't mirror code.** The Python source is a
   reference for *what the system does*, not *how to build it*. Read
   Python to understand the behavioral contract (inputs, outputs, error
   conditions, edge cases), then implement that contract in idiomatic
   Rust. Mirroring Python structure is only justified when the
   structure itself is the simplest way to achieve the behavior. If
   you find yourself translating Python line-by-line, stop and ask
   what behavior you're trying to produce — then write the Rust that
   produces it.

## Steps

### 0. Plan, study Python, and get approval

Before planning, read these (in order). Skip none.

- [ ] `../../AGENTS.md` — repo conventions, build/test commands
- [ ] `migration_strategy.md` — goals and constraints
- [ ] `data_flow.md` — persistent data formats
- [ ] `observations.md` — behavioral notes and ambiguities
- [ ] `../../ARCHITECTURE.md` — crate relationships
- [ ] `../../TESTING.md` — test philosophy and levels
- [ ] `../../specs/<crate>.md` for the target crate
- [ ] `../../PATTERNS.md` — when the work item involves API calls
- [ ] Relevant `test_specs/` sections
- [ ] Python source for the feature being ported

Read the Python source deeply — not just the happy path, but edge cases,
surprising behaviors, error messages, and how it interacts with config,
credentials, and other subsystems. The Python code often does things you
wouldn't think to do from the spec alone (quirky defaults, silent
fallbacks, format-specific output details). You need this understanding
to design the Rust API correctly.

For CLI commands that call AWS APIs, also check:
- [ ] The SDK operation docs on docs.rs (e.g.
  `aws_sdk_deadline::operation::get_farm`) for input/output types
- [ ] The output struct fields — these are the fields you must extract
- [ ] Whether a paginator exists (e.g. `ListFarmsPaginator`)
- [ ] What fields the Python CLI selects for list output (e.g.
  `["farmId", "displayName"]`) and what order
- [ ] Run the Python CLI command and capture its exact output — this is
  the target you must match

Then apply the Design Principles above and present:

- What you're implementing and which test spec cases it covers
- How you'll batch the work
- The Rust API design — not a function-by-function port, but the
  struct/trait/module layout that satisfies the test spec behaviors.
  Reference the Design Principles to justify structural divergences
  from Python.
- Edge cases and surprising Python behaviors that affect the design
- What's blocked or deferred and why

**Wait for approval before proceeding to step 1.**

### 1. Review existing implementation for improvements

Before writing new code, audit what's already implemented. The goal is
to verify the Rust code produces correct behavior — not to check whether
it mirrors the Python. For each existing function in scope:

1. **State the behavioral contract:** What inputs does it accept? What
   does it return? What errors can it produce? What side effects does
   it have? Derive this from the test spec and by running the Python
   CLI, not by reading Python source line-by-line.
2. **Verify the Rust code satisfies the contract.** Does it handle all
   the input cases? Does it produce the right output? Does it surface
   errors instead of swallowing them?
3. **Flag gaps:** Missing edge cases, silent failures, wrong error
   messages, unnecessary Python-isms that don't serve the behavior.
4. **Flag improvements:** Idiomatic Rust opportunities (ValueEnum
   instead of manual FromStr, concrete error types instead of
   Box<dyn Error>, etc.) that make the code simpler without changing
   behavior.

Implement improvements first, update docs, commit, then proceed to new
features. Skip this step if the crate is a stub with no existing
implementation.

### 2. Update the crate spec

Write up the feature's behavior and implementation approach in the relevant
`docs/specs/<crate>.md`. Describe what it does and how it should work in
Rust, but keep it at the design level — no code blocks unless they're
needed to show a non-obvious interface or data format. This becomes the
reference for both the tests and the implementation.

### 3. Red — Write failing tests

Write tests that assert on observable behavior: stdout, stderr, exit code,
file contents. Prefer Level 2 (CLI subprocess) tests. Run them and confirm
they fail. If a test passes before implementation, it's not testing anything
new.

**CLI output tests use `insta-cmd` snapshots** (see `../../TESTING.md` for
the generic framework).

Read the relevant section in `docs/designs/rust-rewrite/test_specs/` for
test case inspiration. Do **not** reference section or case numbers in test
names or comments — the test specs are migration-era scaffolding, not
maintained after tests are written.

### 4. Green — Implement

Write the minimum code to make the tests pass.

For CLI commands that call AWS APIs, follow the patterns in
`../../PATTERNS.md`:
- All API functions use `ResponseBodyCapture` to capture the raw JSON
  response. This applies to `get_*`, `list_*`, and `search_*` alike.
- List functions: manual `nextToken` loop with `ResponseBodyCapture` on
  each page (SDK paginators don't support `.customize().interceptor()`).
- Search functions: single `ResponseBodyCapture` call (no pagination token).
- Mock responses: include all fields the real API returns, not just the
  minimum. Check the SDK output struct on docs.rs for the complete list.

### 5. Verify snapshots against Python CLI

Before accepting any snapshot, compare it against the Python CLI output.
The Python CLI is the reference implementation — the Rust output must match.

**This step is mandatory, not optional.** Do not reason about what the
Python output "would be" — actually run the Python CLI and capture the
output. Skipping this step has caused behavior gaps in past work items.

1. Run `cargo test` — new snapshots are written as `.snap.new` files
2. For each snapshot, **actually run** the equivalent Python CLI command
   against the local stub server or real API and capture its exact output.
   Use the Python source at `../deadline-cloud-python`. If the command
   requires a stub server, use the same mock data the Rust test uses.
   If the command can be tested against the real API with credentials,
   prefer that:
   ```bash
   diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
   ```
3. Compare the snapshot content against the Python output. Watch for:
   - YAML key ordering (Python's dict insertion order vs Rust's serde)
   - Field completeness in `get` commands (all API response fields present)
   - Header lines on `list` commands (count/offset)
   - Boolean capitalization (`True`/`False` vs `true`/`false`)
   - Error message format
   - Known accepted differences (see `../../PATTERNS.md`
     § "Known differences from Python/boto3")
4. If the Rust output differs from Python, fix the implementation first —
   do not accept a snapshot that doesn't match (unless it's a documented
   accepted difference)
5. Once verified, run `cargo insta review` to accept

When possible, also verify against the real API. Obtain AWS credentials
for a test account via your credential management tool (e.g. AWS SSO,
credential process, environment variables), then diff the outputs:
```bash
diff <(deadline <command> 2>&1) <(./target/debug/deadline <command> 2>&1)
```
This catches issues that mock-based tests miss (e.g. fields the real API
returns that the mock omits, datetime precision differences, extra fields
that boto3 strips). Any differences should be either fixed or documented
in the "Known differences" section.

**Do not use `INSTA_UPDATE=always` without reviewing each snapshot.**

### 6. Refactor

Clean up the implementation by ensuring it follows proper code quality and code standards with minimal redundancy or code smells. Tests must still pas once refactors are complete.

### 7. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed. Update the Progress table in `README.md`. Commit all changes after
getting developer approval.
