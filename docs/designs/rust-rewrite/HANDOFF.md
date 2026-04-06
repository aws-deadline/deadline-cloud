# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Pick the next "Not started" item from the Work Items table in
`README.md` whose dependencies are all "✅ Done".

## Recently Completed

**#4 — Queue parameters** (marked ✅ Done)

Implemented:
- `list_queue_environments` and `get_queue_environment` in `api.rs`
- `get_queue_parameter_definitions` in `queue_parameters.rs`
  (deadline-client) with inline validation, UI control detection,
  and definition comparison helpers
- `deadline queue paramdefs` CLI command
- 7 Level 2 tests covering §8 cases 1-10 and §42 cases 6-7

Design note: parameter helpers are private in `queue_parameters.rs`.
They will move to `deadline-job-bundle::parameters` when work item #7
is implemented. No new inter-crate dependencies were added; only
`serde_yaml` and `indexmap` external deps on `deadline-client`.

**#6 — Job monitoring & logs** (marked ✅ Done)

See previous handoff for details.
