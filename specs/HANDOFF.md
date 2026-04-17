# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

## Active Work Item

None. Pick the next "Not started" item from `specs/progress.md`.

## Critical Context

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
2. **All tests pass.** 1036 tests across all crates.
3. **Testing rules:** Level 2 CLI tests must exercise the full stack through
   the CLI binary. No stubs at the application layer — only HTTP stub server.
4. **Serialization patterns.** See `specs/patterns.md`.

## Notes from #15d

**Behavioral gap found:** `queue export-credentials` doesn't validate
empty/missing credential responses — outputs null JSON instead of
erroring. Python raises `KeyError`. Documented in queue_resources.rs
tests with `⚠️ BEHAVIORAL GAP` comment. Should be addressed in #15e.
