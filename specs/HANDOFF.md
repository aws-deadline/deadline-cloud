# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — Step 1 (Study + design) complete, Step 2 next.

### Design intent

Leverage the AWS SDK for Deadline Cloud in Rust strongly — typed inputs
and typed outputs, driven by the caller. Eliminate redundant wrapper
functions that re-declare parameters the SDK fluent builder already
expresses. Keep raw response extraction (`ResponseBodyCapture` →
`serde_json::Value`) only where we need it: CLI `get`-style commands
that print the full API response verbatim so new API fields surface to
customers automatically.

### Current state

`src/api.rs` has 35 wrapper functions (~1,258 lines). Each function
re-declares SDK inputs as `&str` parameters, optionally loops for
pagination, attaches `ResponseBodyCapture`, records telemetry, and maps
`SdkError`. Only 3–5 functions encode non-trivial logic. The remaining
~30 are shape-for-shape wrappers — abstraction without value per
`patterns.md` §3 and §5.

### New design — client-level telemetry interceptor + two helpers

Callers use the SDK fluent builder directly. A telemetry interceptor
installed on the client automatically emits one latency event per
operation, reading the operation name from the SDK's own `Metadata`
config-bag entry (`aws_smithy_runtime_api::client::orchestrator::Metadata`).
No hand-maintained operation-name table, no marker structs, no macro.

Consumer crates (`deadline-cli`, `deadline-python-bindings`) import SDK
types directly from `aws-sdk-deadline` — no re-exports from `deadline-api`.
The real architectural rule is: **`deadline-api` owns session construction,
credential scoping, telemetry, and error mapping** — not type re-exporting.

**The entire helper surface:**

```rust
// src/client.rs

/// Unchanged from today.
pub fn format_sdk_error<E, R>(err: &SdkError<E, R>) -> String;
pub fn deadline_error<E>(e: SdkError<E>) -> DeadlineError;

/// Drain a native SDK paginator into Vec of page outputs. The client's
/// telemetry interceptor emits ONE event covering the whole iteration
/// (matches Python's @record_function_latency_telemetry_event decorator).
pub async fn collect_paginated<O, E>(
    stream: PaginationStream<Result<O, SdkError<E, HttpResponse>>>,
) -> Result<Vec<O>, DeadlineError>
where
    E: ProvideErrorMetadata + Error + 'static;

/// Raw-paginated: caller provides a closure that sends one page with an
/// optional nextToken and returns the raw JSON. Helper loops, aggregates
/// items under `items_key`, returns {items_key: [...]} as one Value.
pub async fn collect_paginated_raw<F, Fut>(
    items_key: &str,
    send_page: F,
) -> Result<Value, DeadlineError>
where
    F: Fn(Option<String>) -> Fut,
    Fut: Future<Output = Result<Value, DeadlineError>>;

/// Apply the DCM user's principal_id to a list builder. Implemented as a
/// tiny trait (WithPrincipalId) impl'd for the three builders that need
/// it: ListFarmsFluentBuilder, ListQueuesFluentBuilder, ListJobsFluentBuilder.
pub fn apply_dcm_principal<B: WithPrincipalId>(
    builder: B,
    config: Option<&IniConfig>,
) -> B;
```

### Telemetry interceptor (the key enabler)

```rust
// src/telemetry_interceptor.rs

/// Installed on the session's DeadlineClient at construction time.
/// Reads the SDK's own Metadata entry for the operation name — no
/// plaintext in our code, no mapping table.
pub(crate) struct TelemetryInterceptor {
    telemetry: Option<TelemetryClient>,
    start: Arc<Mutex<Option<Instant>>>,
}

impl Intercept for TelemetryInterceptor {
    fn name(&self) -> &'static str { "DeadlineTelemetry" }

    fn read_before_execution(&self, _ctx, cfg: &mut ConfigBag) -> Result<(), BoxError> {
        *self.start.lock().unwrap() = Some(Instant::now());
        Ok(())
    }

    fn read_after_execution(&self, _ctx, _rc, cfg: &mut ConfigBag) -> Result<(), BoxError> {
        let elapsed = self.start.lock().unwrap().take()
            .map(|s| s.elapsed()).unwrap_or_default();
        // SDK-provided operation name (e.g. "GetFarm")
        let sdk_name = cfg.load::<Metadata>().map(|m| m.name()).unwrap_or("unknown");
        // Python emits snake_case ("get_farm") — convert to match.
        let metric = pascal_to_snake(sdk_name);
        if let Some(ref tel) = self.telemetry {
            tel.record_latency(&metric, elapsed);
        }
        Ok(())
    }
}
```

Confirmed viable: the generated SDK stores
`Metadata::new("GetFarm", "deadline")` in the config bag per operation,
and the SDK's own `aws-smithy-runtime/src/client/metrics.rs` reads it
the same way (`cfg.load::<Metadata>()`).

### Pagination semantics

`collect_paginated` internally calls `stream.try_collect().await`:

```rust
pub async fn collect_paginated<O, E>(
    stream: PaginationStream<Result<O, SdkError<E, HttpResponse>>>,
) -> Result<Vec<O>, DeadlineError> {
    stream.try_collect().await.map_err(deadline_error)
}
```

The native paginator calls the SDK orchestrator per page, so the
interceptor fires per page. To preserve Python's one-event-per-outer-call
model: a ConfigBag flag (`PaginationGroup`) suppresses per-page events
during `collect_paginated` / `collect_paginated_raw`, and the helper
emits one aggregate event at the end with total elapsed time. Adds ~20
lines to the interceptor + helper.

### Raw path

Stays per-call with `.customize().interceptor(cap).send()`, unchanged
from today. Safer than client-level raw capture (no cross-call mixup in
concurrent code). Telemetry still fires via the client-level interceptor.

### File structure after all batches

```
crates/deadline-api/src/
├── lib.rs
├── client.rs                     # collect_paginated, collect_paginated_raw,
│                                 #   apply_dcm_principal, format_sdk_error,
│                                 #   deadline_error, WithPrincipalId, pascal_to_snake
├── telemetry_interceptor.rs      # TelemetryInterceptor + pagination grouping
├── job_api.rs                    # Domain logic survivors (see below)
├── response_capture.rs           # ResponseBodyCapture interceptor (already renamed)
├── session.rs                    # Session cache, deadline_client(), credentials
├── auth.rs                       # DCM detection, login/logout, auth status
├── job_monitoring.rs             # wait_for_job_completion, failed task collection
├── log_retrieval.rs              # CloudWatch Logs, session auto-selection
├── queue_parameters.rs           # Queue environment parameter extraction
├── telemetry.rs                  # TelemetryClient, background event sender
├── update_checker.rs             # Remote version check
├── submitter_info.rs             # Submitter metadata
├── path_utils.rs                 # File size formatting, path summarization
└── errors.rs                     # DeadlineError enum
```

Gone: `api.rs` (deleted at end of Batch C).
New: `client.rs`, `telemetry_interceptor.rs`, `job_api.rs` (~300 LOC total).

### Domain functions that survive

Move to `src/job_api.rs`:

- `list_jobs_by_filter_expression` — createdAt-thresholding pagination,
  dedup, identical-timestamp guard.
- `wait_for_create_job_to_complete` — polling loop with backoff,
  timeout, cancellation callback.
- `build_filter_expressions` / `build_sort_expressions` — JSON → SDK
  typed filter struct converters for search.
- `build_sdk_attachments` + `create_job` input decoder from
  `Map<String, Value>`.

### Example caller sites (after migration)

**`farm list` — typed + paginated:**
```rust
use aws_sdk_deadline::operation::list_farms::ListFarmsOutput;
use deadline_api::client;

let client = session::deadline_client(Some(&config)).await;
let mut builder = client.list_farms();
builder = client::apply_dcm_principal(builder, Some(&config));
let pages: Vec<ListFarmsOutput> = client::collect_paginated(
    builder.into_paginator().send()
).await.map_err(|e| CliError::Operation(format!("Failed to get Farms: {e}")))?;
let structured: Vec<_> = pages.iter()
    .flat_map(|p| p.farms())
    .map(|f| json!({"farmId": f.farm_id(), "displayName": f.display_name()}))
    .collect();
// Telemetry: one "list_farms" event emitted automatically by client interceptor.
```

**`farm get` — raw for full-response print:**
```rust
use deadline_api::client;
use deadline_api::response_capture::ResponseBodyCapture;

let client = session::deadline_client(Some(&config)).await;
let cap = ResponseBodyCapture::new();
client.get_farm().farm_id(&farm)
    .customize().interceptor(cap.clone())
    .send().await.map_err(client::deadline_error)?;
let resp = cap.json().map_err(|e| CliError::Operation(e.to_string()))?;
println!("{}", common::cli_object_repr(&resp));
// Telemetry: one "get_farm" event emitted automatically.
```

**`job cancel` — typed mutating:**
```rust
use aws_sdk_deadline::types::JobTargetTaskRunStatus;
use deadline_api::client;

let client = session::deadline_client(Some(&config)).await;
client.update_job()
    .farm_id(&farm).queue_id(&queue).job_id(&job)
    .target_task_run_status(JobTargetTaskRunStatus::Canceled)
    .send().await.map_err(client::deadline_error)?;
// Telemetry: one "update_job" event emitted automatically.
```

**`mcp list_jobs` — raw paginated:**
```rust
use deadline_api::client;
use deadline_api::response_capture::ResponseBodyCapture;

let client = session::deadline_client(Some(&config)).await;
let all = client::collect_paginated_raw("jobs", |token| {
    let client = client.clone();
    let farm = farm.clone();
    let queue = queue.clone();
    async move {
        let cap = ResponseBodyCapture::new();
        let mut req = client.list_jobs().farm_id(&farm).queue_id(&queue);
        if let Some(t) = token { req = req.next_token(t); }
        req.customize().interceptor(cap.clone())
            .send().await.map_err(client::deadline_error)?;
        cap.json().map_err(|e| DeadlineError::OperationError(e.to_string()))
    }
}).await?;
```

### Batches

| Batch | Scope |
|-------|-------|
| **A** | Create `src/client.rs` with `collect_paginated`, `collect_paginated_raw`, `apply_dcm_principal`, `format_sdk_error`, `deadline_error`, `WithPrincipalId` trait, `pascal_to_snake`. Create `src/telemetry_interceptor.rs` with `TelemetryInterceptor` + pagination group support. Install interceptor on every client built by `session::deadline_client()`. Add `aws-sdk-deadline = { workspace = true }` to `deadline-cli/Cargo.toml` and `deadline-python-bindings/Cargo.toml`. Remove dead `aws-sdk-deadline` deps from `deadline-job-attachments/Cargo.toml` and `deadline-job-bundle/Cargo.toml`. Helper tests (wiremock). |
| **B** | Migrate Farm + Queue + Fleet callers (CLI + FFI) to use SDK fluent builders through the helpers. Delete corresponding wrappers from `api.rs`. |
| **C** | Migrate Job + Step + Task + Worker + Session callers. Move surviving domain functions to `src/job_api.rs`. Delete rest of `api.rs`. Remove `pub mod api` from `lib.rs`. |
| **D** | FFI DTO conversion: create `deadline-python-bindings/src/dtos.rs`, rewrite `resources.rs`, add DTO round-trip tests. |
| **E** | Spec updates (`deadline-api/README.md`, `architecture.md`, `response-capture.md`, `patterns.md` §"AWS SDK for Rust Usage", `deadline-python-bindings/architecture.md`). |

Each batch is a complete migration for its slice — no transitional
re-exports, no dead code. `api.rs` shrinks visibly per batch and is
deleted at the end of Batch C.

### Caller migration pattern summary

| Caller | Pattern |
|--------|---------|
| `farm/queue/fleet list` | `collect_paginated` + typed `*Summary` accessors |
| `farm/queue/fleet get` | Raw capture (full response print) |
| `fleet get --queue-id` | Typed for queue + associations; raw for each printed fleet |
| `job list`, `job get` (no --search-term) | Raw capture |
| `job get --search-term` | Typed (read field subsets) |
| `job cancel/requeue/update` | Typed fluent builder direct |
| `job_monitoring`, `log_retrieval` | Typed accessors |
| `queue_parameters` | Typed accessors |
| `mcp.rs` list tools | `collect_paginated_raw` |
| `mcp.rs` get tools | Raw capture |
| FFI (`resources.rs`) | Typed + `From<&X>` → DTO → `pythonize(&dto)` |

### Helper tests (Batch A, wiremock)

| Behavior | Rust test name |
|----------|---------------|
| Telemetry interceptor reads SDK operation name | `telemetry_interceptor_reads_sdk_metadata_as_snake_case` |
| Telemetry interceptor emits one event per non-paginated call | `telemetry_interceptor_emits_one_event_per_call` |
| `collect_paginated` drains all pages | `collect_paginated_drains_all_pages_via_native_paginator` |
| `collect_paginated` emits ONE telemetry event for all pages | `collect_paginated_records_one_event_across_pages` |
| `collect_paginated` maps SdkError from any page | `collect_paginated_maps_sdk_error_from_any_page` |
| `collect_paginated_raw` aggregates under items_key | `collect_paginated_raw_aggregates_items_under_key` |
| `collect_paginated_raw` emits ONE telemetry event for all pages | `collect_paginated_raw_records_one_event_across_pages` |
| `apply_dcm_principal` sets id for DCM user | `apply_dcm_principal_sets_id_when_user_and_store_present` |
| `apply_dcm_principal` no-op for non-DCM user | `apply_dcm_principal_noop_when_dcm_markers_absent` |
| `pascal_to_snake` conversions | `pascal_to_snake_converts_standard_sdk_op_names` |

Existing tests preserved (move with their functions to `job_api.rs`
in Batch C):
- `wait_for_create_job_cancels_when_callback_returns_false`
- `build_filter_string_list_filter_produces_filter`
- `build_filter_date_time_filter_produces_filter`
- `build_filter_unknown_filter_type_produces_empty`

Regression coverage for caller migrations: existing CLI Level-2 snapshot
tests. Must pass unchanged after each batch.

### Key design decisions (locked 2026-04-29)

1. **SDK-provided operation name via `Metadata`.** Our
   `TelemetryInterceptor` reads `cfg.load::<Metadata>()` in
   `read_after_execution` — the SDK itself records it per operation.
   Zero plaintext operation names in our code; zero mapping table.

2. **One telemetry event per logical operation, all pages.** Helper
   suppresses per-page events during `collect_paginated` /
   `collect_paginated_raw` via a ConfigBag flag, emits one aggregate at
   the end. Matches Python's decorator behavior.

3. **Caller drives the SDK fluent builder.** No wrappers that
   re-declare `&str` parameters. Caller writes
   `client.get_farm().farm_id(id).send()` directly.

4. **Typed by default; raw only for full-response print.** Raw capture
   stays per-call via `.customize().interceptor(cap).send()`. No
   client-level raw interceptor — avoids cross-call mixup in concurrent
   code.

5. **Native SDK paginators for typed list.** Every `list_*` operation
   in `aws-sdk-deadline` has `.into_paginator()`. `search_*` has none
   (offset/pageSize) — single call is enough.

6. **FFI gets owned DTO structs.** Only fields the GUI consumes cross
   the boundary. No `pythonize(Value)`, no `Serialize` on SDK types.

7. **Consumer crates import SDK types directly.** `deadline-cli` and
   `deadline-python-bindings` add `aws-sdk-deadline = { workspace = true }`
   to their Cargo.toml and import types like `FarmSummary`, `GetFarmInput`
   directly. No re-exports from `deadline-api`. The real architectural
   rule is: **`deadline-api` owns session construction, credential
   scoping, telemetry, and error mapping** — not type re-exporting.

8. **Error mapping unchanged.** `SdkError → DeadlineError::OperationError
   (format_sdk_error(&e))`. Same message, same exit codes.

9. **Module renames:** `api.rs` → `client.rs` + `telemetry_interceptor.rs` + `job_api.rs`;
   `raw_response.rs` → `response_capture.rs` (already done).

### Out of scope

- Typed filter struct input for `search_jobs_with_filters`.
- Typed `CreateJobInput` at `create_job` callsites.
- S3 / STS / CloudWatch client patterns.

### Step status

- [x] Step 1 — Study + design locked.
- [x] Step 2 — Batch A helper tests written (13 tests).
- [x] Step 3 — Batch A implemented (client.rs + telemetry_interceptor.rs).
- [x] Step 5 — Audit clean (stale doc comment fixed, parameter name fixed).
- [x] **Batch B complete** — TelemetryInterceptor wired into session, WithPrincipalId impls, Farm/Queue/Fleet callers migrated (CLI + MCP + FFI + helpers), 5 wrappers deleted from api.rs. All 1,280 tests pass.
- [x] **Batch C complete** — Stripped `with_telemetry_latency_async` from all 30 api.rs functions, removed `telemetry` parameter from all signatures, updated 69 call sites across 13 files. Double-telemetry fixed. All 1,280 tests pass.
- [ ] **Next: Batch D** — FFI DTO conversion (optional, low priority).
- [ ] **Next: Batch E** — Final spec updates.
- [ ] Step 6 — Spec updates (Batch E).
- [ ] Step 7 — Commit per batch.

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
