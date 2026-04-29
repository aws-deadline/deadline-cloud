# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** Not started

### Problem Statement

All `deadline-api` functions currently return `serde_json::Value` via the
`ResponseBodyCapture` interceptor. This provides zero compile-time safety
for code that accesses response fields — typos in string keys
(`job["lifecycleStatus"]`) are only caught at runtime. The interceptor is
necessary for CLI `get` commands that must print every field (including
fields the SDK doesn't know about yet), but most callers only need a few
typed fields for business logic or FFI.

### Design

Split API functions into two variants:

1. **Typed (default):** Returns the SDK's native output type (e.g.
   `GetFarmOutput`, `GetJobOutput`). Callers access fields via typed
   accessors (`.farm_id()`, `.lifecycle_status()`). Compile-time safe.
   Use for business logic, FFI, and list commands that cherry-pick fields.

2. **Raw (`_raw` suffix):** Returns `serde_json::Value` via the
   interceptor. Forward-compatible — includes all fields the API sends,
   even those the SDK doesn't model yet. Use ONLY for CLI `get` commands
   that dump the entire response to the user.

```rust
// Typed — compile-time safe field access
pub async fn get_farm(...) -> Result<GetFarmOutput, DeadlineError>

// Raw — full wire JSON for print paths
pub async fn get_farm_raw(...) -> Result<Value, DeadlineError>
```

Both use typed requests (the SDK builder pattern with typed input params).

### Caller migration

| Caller pattern | Before | After |
|----------------|--------|-------|
| `deadline farm get` (print all) | `get_farm()` → `Value` | `get_farm_raw()` → `Value` |
| `deadline farm list` (print subset) | `list_farms()` → `Value`, index `["farmId"]` | `list_farms()` → `Vec<FarmSummary>`, `.farm_id()` |
| `job download-output` (logic) | `get_job()` → `Value`, index `["storageProfileId"]` | `get_job()` → `GetJobOutput`, `.storage_profile_id()` |
| GUI FFI (dropdown data) | `list_farms()` → `Value` → pythonize all | `list_farms()` → typed → build `#[derive(Serialize)]` subset → pythonize |

### Pagination

Typed list functions can use the SDK's built-in paginator (`.into_paginator()`)
since they don't need the interceptor. This simplifies pagination code.

### FFI implications

- GUI FFI uses typed variants → extracts needed fields → builds small
  `#[derive(Serialize)]` structs → pythonize to Python dict
- Python receives a dict with known fields; adding a new field requires
  updating the Rust struct (compile error on typo)
- Python side never breaks from new fields — it just gets a dict

### What this does NOT change

- `_raw` functions keep the interceptor — zero maintenance for print paths
- No DTOs or wrapper crates needed — uses SDK types directly
- No build scripts or proc macros
- DateTime formatting in `_raw` paths unchanged

### Migration strategy

Incremental — not a big-bang rewrite:
1. Add typed variants alongside existing functions (rename current → `_raw`)
2. Switch logic/FFI callers one at a time to typed variants
3. Switch list command callers to typed + paginator
4. `_raw` variants remain permanently for print paths

### Scope estimate

- ~35 API functions in `api.rs` to split into typed + raw pairs
- ~15 call sites in `deadline-cli` to migrate from Value indexing to typed
- ~5 call sites in `deadline-python-bindings` to migrate
- Pagination simplification for list functions
- New small `#[derive(Serialize)]` structs for FFI boundary (~5 structs)

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
