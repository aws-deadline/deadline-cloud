# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Pick the next "Not started" item from the Work Items table in
`README.md` whose dependencies are all "✅ Done".

## Recently Completed

**#6 — Job monitoring & logs** (marked ✅ Done)

Implemented:
- `wait_for_job_completion` + `deadline job wait` (14 Level 2 tests)
- `get_session_logs` + `deadline job logs` (10 Level 2 tests)
- `get_worker_logs` library function (2 Level 1 tests)
- `assume_fleet_role_for_read`, `session::get_sdk_config`
- CloudWatch mock helpers, test harness CloudWatch endpoint
- TESTING.md snapshot guidance, mock response data rules

Deferred within #6:
- `deadline job trace-schedule` — EXPERIMENTAL, low priority
- `--session-action-id` for `job logs`
- `--timezone` deprecated flag
- DCM credential paths (queue-role, fleet-role) not tested
