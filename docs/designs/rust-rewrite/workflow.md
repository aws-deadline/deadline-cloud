# Development Workflow

The workflow for porting each feature from the Python CLI to Rust.

## Collaboration rules

When starting a new batch of work:

1. **Present the plan before executing.** Explain what you're implementing,
   which test spec cases it covers, what batches you'll group them into, and
   how each batch will be implemented (Rust-specific design choices). Wait
   for approval before writing code.
2. **Cross-reference Python before implementing.** Read the Python source for
   every feature being ported. Identify where Rust can do things better
   (type-level guarantees, idiomatic patterns) while preserving identical
   observable behavior.
3. **Review existing implementation for improvements.** Before adding new
   code, audit what's already written against the Python source. Flag
   behavior gaps (e.g., stdout vs stderr for errors) and suggest cleanups.
   Implement improvements first, then new features.
4. **Commit-driven development.** Each logical batch gets its own commit
   following red-green TDD: write failing tests → implement → full suite
   check → commit. Keep commits small and focused.
5. **Keep docs in sync.** After implementation, update `docs/specs/<crate>.md`
   to reflect Rust-specific design decisions (not just "what" but "how" and
   "why it differs from Python"). Update the Progress table in `README.md`.
6. **Defer honestly.** If a feature is blocked on an unimplemented crate,
   say so and document it in the Progress table rather than building
   throwaway scaffolding.

## Steps

### 1. Study the Python implementation

Read the Python source for the feature being ported. Understand:
- What the function/command actually does (not just what you'd assume)
- Edge cases and surprising behaviors
- How it interacts with config, credentials, and other subsystems
- Error messages and exit codes

This step exists because the Python code often does things you wouldn't
think to do from the spec alone — quirky defaults, silent fallbacks,
format-specific output details.

### 2. Update the crate spec

Write up the feature's behavior and implementation approach in the relevant
`docs/specs/<crate>.md`. Describe what it does and how it should work in
Rust, but keep it at the design level — no code blocks unless they're
needed to show a non-obvious interface or data format. This becomes the
reference for both the tests and the implementation.

### 3. Review test specs

Read the relevant section in `docs/designs/rust-rewrite/test_specs/` for
the feature being ported. These files document behavioral test cases
organized by section number (matching the Progress table in `README.md`).
Use them as inspiration for what scenarios to cover — happy paths, error
handling, boundary values, output formats, etc. Not every case needs a
1:1 test, but the specs ensure you don't miss important scenarios.

### 4. Red — Write failing tests

Write tests that assert on observable behavior: stdout, stderr, exit code,
file contents. Prefer Level 2 (CLI subprocess) tests. Run them and confirm
they fail. If a test passes before implementation, it's not testing anything
new.

Do **not** reference section or case numbers from the test specs in test
names, comments, or file headers. The test specs are migration-era
scaffolding for porting from Python to Rust — they are not maintained
after the tests are written. Tests should be self-describing through
their names and assertions alone.

### 5. Green — Implement

Write the minimum code to make the tests pass.

### 6. Refactor

Clean up the implementation. Tests must still pass. Commit.

### 7. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed.
