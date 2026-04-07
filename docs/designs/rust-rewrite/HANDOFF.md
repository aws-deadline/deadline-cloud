# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Consult the Work Items table in `README.md` for the next item.

## Recently Completed

**#12b — Job search**, **#15b — Job get search term**,
**#15c — Job logs auto-selection messages** (batched, marked ✅ Done)

- `deadline job search` with `--filter-expressions`, `--sort-expressions`,
  `--page-size`, `--item-offset`
- `deadline job get [SEARCH_TERM]` — positional arg, job ID pattern
  detection, single/multiple match handling, `--job-id` precedence
- `job logs` auto-selection messages: "Using the only available session"
  / "Using the latest session" (non-JSON only)
- `search_jobs_with_filters` API with SDK type builders for filter/sort
- `SessionAutoSelect` enum in `log_retrieval.rs`
- 14 Level 2 tests, verified against Python CLI on real API
- Fixes during verification: local timestamps in search summary,
  output order (auto-select message before header), resolved session ID
  in "Retrieving logs" line

**#7 — Job bundle** (marked ✅ Done)

- `deadline-job-bundle` crate: loader, parameters, submission, history
- 179 Level 1 tests (174 spec cases from §15-18 + 5 extras)

**#12 — Job cancel** and **#15 — Job requeue-tasks** (marked ✅ Done)

**#4 — Queue parameters** (marked ✅ Done)

**#6 — Job monitoring & logs** (marked ✅ Done)
