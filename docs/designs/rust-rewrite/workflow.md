# Development Workflow

The workflow for porting each feature from the Python CLI to Rust.

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

### 3. Red — Write failing tests

Write tests that assert on observable behavior: stdout, stderr, exit code,
file contents. Prefer Level 2 (CLI subprocess) tests. Run them and confirm
they fail. If a test passes before implementation, it's not testing anything
new.

### 4. Green — Implement

Write the minimum code to make the tests pass.

### 5. Refactor

Clean up the implementation. Tests must still pass. Commit.

### 6. Update docs

Update `docs/specs/<crate>.md` if the implementation diverged from the
initial spec. Update `docs/ARCHITECTURE.md` if cross-crate relationships
changed.
