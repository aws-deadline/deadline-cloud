# Development Workflow

The workflow for porting each feature from the Python CLI to Rust.

## Rules

- **No code before approval.** Steps 0-3 are planning. Present the plan
  and wait for the human to approve before writing any code.
- **Cross-reference Python for every feature.** The Python code often does
  things you wouldn't expect from the spec alone. Read it.
- **Improvements before new code.** Before adding new features, audit
  existing implementation against the Python source. Flag behavior gaps
  and cleanups. Implement improvements first.
- **Commit per batch.** Each logical batch gets its own commit following
  red-green TDD. Keep commits small and focused.
- **Docs stay in sync.** After implementation, update `docs/specs/<crate>.md`
  with Rust-specific design decisions. Update the Progress table in
  `README.md`. Do not let docs drift from code.
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

2. **Globals are a code smell.** If the Python uses `@lru_cache` on
   module-level functions or mutable module globals, the Rust design
   should use owned state on a struct. Ask: "who owns this data?" If
   the answer is "nobody, it's global" — that's the thing to fix.

3. **Type the boundaries.** If Python passes a string like `"deadline"`
   to select a service client, Rust should use an enum or generic. If
   Python returns `dict[str, Any]`, Rust should return a concrete type.
   Push validation to compile time wherever possible.

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

## Steps

### 0. Plan and get approval

Before planning, read these (in order). Skip none.

- [ ] `migration_strategy.md` — goals and constraints
- [ ] `../../ARCHITECTURE.md` — crate relationships
- [ ] `../../TESTING.md` — test philosophy and levels
- [ ] `../../specs/<crate>.md` for the target crate
- [ ] Relevant `test_specs/` sections
- [ ] Python source for the feature being ported

Then apply the Design Principles above and present:

- What you're implementing and which test spec cases it covers
- How you'll batch the work
- The Rust API design — not a function-by-function port, but the
  struct/trait/module layout that satisfies the test spec behaviors.
  Reference the Design Principles to justify structural divergences
  from Python.
- What's blocked or deferred and why

**Wait for approval before proceeding to step 1.**

### 1. Study the Python implementation

Read the Python source for the feature being ported. Understand:
- What the function/command actually does (not just what you'd assume)
- Edge cases and surprising behaviors
- How it interacts with config, credentials, and other subsystems
- Error messages and exit codes

This step exists because the Python code often does things you wouldn't
think to do from the spec alone — quirky defaults, silent fallbacks,
format-specific output details.

### 2. Review existing implementation for improvements

Before writing new code, audit what's already implemented against the
Python source. Look for:
- Behavior gaps (e.g., stdout vs stderr for errors)
- Missing edge cases
- Opportunities to use idiomatic Rust (ValueEnum instead of manual
  FromStr, concrete error types instead of Box<dyn Error>)

Implement improvements first, commit, then proceed to new features.

### 3. Update the crate spec

Write up the feature's behavior and implementation approach in the relevant
`docs/specs/<crate>.md`. Describe what it does and how it should work in
Rust, but keep it at the design level — no code blocks unless they're
needed to show a non-obvious interface or data format. This becomes the
reference for both the tests and the implementation.

### 4. Red — Write failing tests

Write tests that assert on observable behavior: stdout, stderr, exit code,
file contents. Prefer Level 2 (CLI subprocess) tests. Run them and confirm
they fail. If a test passes before implementation, it's not testing anything
new.

Read the relevant section in `docs/designs/rust-rewrite/test_specs/` for
test case inspiration. Do **not** reference section or case numbers in test
names or comments — the test specs are migration-era scaffolding, not
maintained after tests are written.

### 5. Green — Implement

Write the minimum code to make the tests pass.

### 6. Refactor

Clean up the implementation. Tests must still pass. Commit.

### 7. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed. Update the Progress table in `README.md`.
