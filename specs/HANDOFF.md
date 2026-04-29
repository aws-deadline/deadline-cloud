# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — starting Batch A

### Design

Every API function gets two variants:

- **Typed (default name):** Typed request + typed response (SDK output type).
  Use for all business logic, FFI, and anywhere specific fields are needed.
- **Raw (`_raw` suffix):** Typed request + raw `Value` response via interceptor.
  Use ONLY for CLI `get` commands that print the full API response verbatim.

```rust
pub async fn get_farm(...) -> Result<GetFarmOutput, DeadlineError>      // typed
pub async fn get_farm_raw(...) -> Result<Value, DeadlineError>          // raw
```

**Rule:** No function returns `Value` without the `_raw` suffix.

### Module split

`api.rs` (1,258 lines, 35 functions) is split into resource modules:

```
src/api/
├── mod.rs       — re-exports, shared helpers (format_sdk_error, sdk_err, capture_send, paginated_list)
├── farm.rs      — get_farm, list_farms
├── queue.rs     — get_queue, list_queues, assume_queue_role_*, storage profiles, queue envs, queue-fleet associations
├── fleet.rs     — get_fleet, list_fleets, assume_fleet_role_for_read
├── job.rs       — get_job, list_jobs, search_jobs*, create_job, update_job, wait_for_create_job_to_complete
├── step.rs      — get_step, list_steps, batch_get_steps_page
├── task.rs      — get_task, list_tasks, batch_get_tasks_page, update_task
├── worker.rs    — get_worker, search_workers
└── session.rs   — get_session, list_sessions, list_session_actions, get_session_action
```

### Batches

| Batch | Scope | Changes |
|-------|-------|---------|
| A | Split `api.rs` into modules + create all typed/raw pairs | No caller changes. Purely additive. Existing callers use `_raw` names. |
| B | Migrate callers: Farm + Fleet + Queue | Switch logic callers to typed, print callers to `_raw` |
| C | Migrate callers: Job + Step + Task + Worker + Session | Switch remaining callers |
| D | FFI migration | Serializable structs for Python bindings |

**Batch A** is the foundation — one large batch that establishes the
complete API surface. All 35 existing functions become `_raw`, and 35
new typed functions are added alongside them. Tests prove both variants
work. No existing behavior changes.

**Batches B-C** are mechanical caller migrations. Each caller is
switched from `_raw` (Value indexing) to typed (accessor methods) where
appropriate. Print paths stay `_raw`.

**Batch D** replaces `pythonize(Value)` in FFI with typed →
`#[derive(Serialize)]` structs → pythonize.

---

## Queued small items (from #21b Bucket 3)

### #21d — CLI backward-compat flags (~30 lines)

Add deprecated/missing CLI flags for migration compatibility:

- `job logs --timezone` — deprecated flag that maps to `--timestamp-format`.
  Accept `utc`/`local`, print deprecation warning to stderr, error if
  both `--timezone` and `--timestamp-format` provided. (~15 lines in `job.rs`)
- `queue export-credentials --output-format` — accept `credentials_process`
  (only valid value), validate, record in telemetry. (~10 lines in `queue.rs`)
- `manifest snapshot/diff -ie` — add short alias for `--include-exclude-config`.
  (~2 lines in `manifest.rs`)
- `auth status --output` — validate value is `verbose` or `json`, error on
  invalid instead of silent fallthrough to verbose. (~3 lines in `auth.rs`)

### #21e — `deadlinew` windowless launcher (~7 lines)

Add a second binary target that suppresses the console window on Windows:

- New file `src/main_windowless.rs` with `#![windows_subsystem = "windows"]`
  that includes the same main function. (~4 lines)
- New `[[bin]]` entry in `Cargo.toml`. (~3 lines)
- On macOS/Linux the attribute is ignored; both binaries are identical.
- Prerequisite for #16f (DCC switchover) and #24 (production distribution).

### #21f — Windows config path normalization (~50 lines)

Normalize backslash↔forward-slash for path-type config settings on Windows:

- Add `is_path: bool` and `is_path_list: bool` to `SettingDef`. Mark
  `deadline-cloud-monitor.path` and `settings.job_history_dir` as `is_path`,
  `settings.known_asset_paths` as `is_path_list`.
- Add `#[cfg(windows)]` normalization: `\` → `/` on write, `/` → `\` on read.
- Wire into `get_setting()` and `set_setting()` in `config_file.rs`.
- No-op on macOS/Linux. Prevents INI corruption from backslash escaping
  and ensures cross-client config compatibility with the Python CLI.

### #21g — Telemetry parity: success/fail events (~50 lines)

Add success/fail telemetry events matching Python's
`record_success_fail_telemetry_event` decorator:

- `asset_upload` success/fail — wrap upload section in `submission.rs`
- `asset_snapshot` success/fail — wrap snapshot section in `submission.rs`
- `queue_sync_output` success/fail — in `queue.rs` sync-output handler
- `download_job_output` success/fail — in `job.rs` download-output handler
- Each emits `is_success: bool` and `exception_type` on failure.
