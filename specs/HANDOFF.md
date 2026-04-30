# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — D5 next. Eliminate `ResponseBodyCapture` entirely.

### Design intent (FINAL — no exceptions)

**No raw HTTP interception.** `ResponseBodyCapture` must be removed.
Every API call uses SDK fluent builders (input) and returns SDK output
structs. Callers extract the fields they need from the typed output.

Rules:

1. **No wrapper functions** for simple API calls. Callers use the SDK
   client directly: `dl.get_job().farm_id(f).queue_id(q).job_id(j).send().await`

2. **Helper functions exist only for:** pagination logic, error handling
   with telemetry, and cases where multiple callers share complex setup.

3. **Business logic callers** access typed fields directly:
   `output.name()`, `output.started_at()`, `output.job_id()`

4. **Full-dump display callers** (CLI `get` commands) manually extract
   every field from the SDK output into a serializable response struct.
   If a new field is added to the SDK, we add it to the struct — that's
   fine.

5. **Complex nested SDK types** (e.g. `Attachments`, `JobParameter`,
   `LogConfiguration`, `HostPropertiesResponse`, `FleetConfiguration`)
   don't implement `Serialize`. For display, manually convert them to
   `serde_json::Value` by walking their typed accessors. No raw capture.

6. **Telemetry** is injected into API calls via the existing
   `telemetry_interceptor` on the SDK client (already wired in D1–D3).

7. **Error handling** uses `sdk_err()` to format SDK errors with code
   and message (already established pattern).

### What's done (D1–D4)

- `client.rs`, `telemetry_interceptor.rs` — SDK client with telemetry
- `FarmResponse`, `QueueResponse`, `FleetResponse` — fully typed (no raw)
- `JobResponse`, `StepResponse`, `TaskResponse`, `SessionResponse`,
  `WorkerResponse` — currently use `from_output_and_raw` (D5 will fix)
- `get_job`, `get_step`, `get_task`, `get_session`, `get_worker` return
  SDK output types (typed variants done)

### What D5 must do

1. **Remove `ResponseBodyCapture`** — delete `response_capture.rs`,
   remove all `_with_raw` variants, remove `capture_send` helper.

2. **Fix response structs** — change `from_output_and_raw(output, &raw)`
   to `from(output)`. Manually convert complex nested types by walking
   SDK accessors (e.g. `output.attachments()` → build JSON from its
   typed fields).

3. **Convert remaining raw functions** — every function in `api.rs` that
   still uses `capture_send` must return SDK output types or be inlined
   at call sites:
   - `get_queue` → `GetQueueOutput`
   - `list_sessions` → `Vec<SessionSummary>` (paginated)
   - `list_steps` → `Vec<StepSummary>` (paginated)
   - `list_tasks` → `Vec<TaskSummary>` (paginated)
   - `search_jobs` / `search_jobs_with_filters` → search output
   - `search_workers` → search output
   - `batch_get_steps_page` / `batch_get_tasks_page` → batch output
   - `assume_queue_role_for_user/read` → credentials output
   - `assume_fleet_role_for_read` → credentials output
   - `get_session_action` → `GetSessionActionOutput`
   - `get_storage_profile_for_queue` → output
   - `list_storage_profiles_for_queue` → paginated output
   - `list_queue_environments` → paginated output
   - `get_queue_environment` → output
   - `list_queue_fleet_associations` → paginated output
   - `list_session_actions` → paginated output
   - `update_job` → `()`
   - `update_task` → `()`
   - `create_job` → `CreateJobOutput`

4. **Migrate all callers** to use typed SDK output fields instead of
   `value["fieldName"]` access.

### Batching strategy

- **D5a:** Remove `_with_raw` from D4 structs. Convert `from_output_and_raw`
  to `from(output)` with manual nested type conversion. Delete
  `ResponseBodyCapture` usage from get_job/step/task/session/worker.
- **D5b:** Convert `get_queue`, credential APIs (`assume_*`), and
  `get_session_action`. These have few callers.
- **D5c:** Convert list/search APIs (`list_sessions`, `list_steps`,
  `list_tasks`, `search_jobs`, `search_workers`, `batch_get_*`).
  These are paginated — keep helper functions for pagination logic.
- **D5d:** Convert remaining (`list_queue_environments`,
  `get_queue_environment`, `list_queue_fleet_associations`,
  `list_storage_profiles_for_queue`, `get_storage_profile_for_queue`,
  `list_session_actions`, `update_job`, `update_task`, `create_job`).
- **D5e:** Delete `response_capture.rs`, `capture_send`, and any
  remaining `ResponseBodyCapture` imports. Clean sweep.

---

## Batch D4 — ✅ Complete (will be revised in D5a)

Job/Step/Task/Session/Worker response structs added, API functions return
SDK output types. Uses `_with_raw` pattern that D5a will eliminate.

---

## Queued small items (from #21b Bucket 3)

### #21d — CLI backward-compat flags (~30 lines)

- `job logs --timezone` — deprecated flag mapping to `--timestamp-format`
- `queue export-credentials --output-format` — accept `credentials_process`
- `manifest snapshot/diff -ie` — short alias for `--include-exclude-config`
- `auth status --output` — validate `verbose`/`json`

### #21e — `deadlinew` windowless launcher (~7 lines)

`#![windows_subsystem = "windows"]` binary target for GUI commands on Windows.

### #21f — Windows config path normalization (~50 lines)

Normalize `\`↔`/` for path-type config settings on Windows.

### #21g — Telemetry parity: success/fail events (~50 lines)

`asset_upload`, `asset_snapshot`, `queue_sync_output`, `download_job_output`
success/fail telemetry events.
