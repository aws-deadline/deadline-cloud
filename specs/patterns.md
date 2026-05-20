# Coding Patterns

Patterns and conventions for writing code in this workspace. Read this
before implementing API calls or touching `deadline-lib::api`.

## Design Principles

The goal is identical observable behavior, not identical internal structure.

1. **Don't replicate Python's module structure.** Python uses module-level
   functions with global caches because that's idiomatic Python. Rust
   should use structs that own their state. If Python has five free
   functions sharing a module-level dict, Rust probably wants one struct
   with five methods.

2. **Globals must earn their keep.** Prefer owned state on a struct
   when the state needs different configurations per consumer or must
   be isolated for testing. However, process-wide singletons (caches,
   connection pools, configuration) are legitimate when the alternative
   is threading a parameter through every function call. Use
   `std::sync::Mutex<T>` with `std::sync::LazyLock` (Rust 1.80+) for
   mutable globals. The underlying struct should still be usable
   independently for cases that need a separate instance.

3. **Every abstraction must earn its keep.** Before adding a type,
   wrapper, conversion layer, or any indirection that the Python
   doesn't have, ask: "what does this prevent or enable?" If the
   answer is a concrete benefit (compile-time error catching, safety,
   testability), add it. If the answer is "it's more Rust-like" or
   "it feels cleaner," don't — that's complexity without value. The
   simplest correct implementation wins.

4. **Preserve observable behavior exactly.** Same output, same errors,
   same exit codes, same caching semantics. The test specs define the
   contract — internal structure is free to diverge.

5. **Don't over-abstract.** If the Python is a simple function that
   doesn't need to become a trait, don't make it one. Only introduce
   abstractions that solve a real problem or optimize a process.

6. **Ask "what would I design if the Python didn't exist?"** Read the
   test spec and the Python source, then close the Python file and
   design the Rust API from the behavioral requirements. Open the
   Python again only to verify you haven't missed edge cases.

7. **Implement behavior, don't mirror code.** The Python source is a
   reference for *what the system does*, not *how to build it*. Read
   Python to understand the behavioral contract, then implement that
   contract in idiomatic Rust.

## Type Annotations

Use explicit type annotations in tests to document the expected return
type — they serve as an extra compile-time assertion:

```rust
let output: GetFarmOutput = api::get_farm("farm-abc", None).await.unwrap();
let pages: Vec<ListStepsOutput> = api::list_steps(farm, queue, job, config).await.unwrap();
```

In production code, let inference handle local bindings. Function
signatures already declare types at the API boundary; annotating every
`let` inside a function body is noise.

## AWS SDK for Rust Usage

The Rust SDK (`aws-sdk-deadline`) differs from Python's boto3 in ways
that affect how we build API functions. Know these before writing code.

### SDK types don't implement `Serialize`

The SDK output types don't implement `serde::Serialize`
([awslabs/aws-sdk-rust#269](https://github.com/awslabs/aws-sdk-rust/issues/269)).
This means you cannot `serde_json::to_value(&output)` on an SDK response.
For display paths, manually extract fields into a serializable struct or
`serde_json::Map`.

### Library/CLI separation of concerns

**The test:** "Is this useful as a library function to call within scripts?"
If the answer is "it calls one SDK endpoint" — it stays in CLI. If the
answer involves orchestration logic that any consumer would need — it
belongs in the library.

**The library provides:**

1. **Infrastructure** — `deadline_client(profile)` returns a correctly-
   configured client with session caching, user-agent, telemetry
   interceptor, and DCM support. Plus utilities: `apply_dcm_principal`,
   `collect_paginated`, `deadline_error`, response types (`FarmResponse`,
   etc. with `From<SdkOutput>` impls).

2. **Complex operations** — Multi-step orchestration where the value is
   in the logic: `create_job_from_job_bundle` (hooks, hashing, upload,
   create, wait), `ManifestDownloader`, `IncrementalDownloadJob`,
   `login`/`logout`, `job_monitoring`, `log_retrieval`.

3. **Config utilities** — `read_config()`, `get_setting()`, etc.
   Available to any caller that uses config files. NOT wired into
   operations — just a utility.

**The CLI owns:**

1. **Simple API calls** — `list_farms`, `get_queue`, `search_workers`,
   etc. These are 2-3 lines using the client the library gave you.
   Don't wrap single SDK calls in library functions.

2. **Config reading and extraction** — reads INI, extracts fields,
   passes explicit values into library functions.

3. **Output formatting and error presentation** — `cli_object_repr`,
   progress bars, suggestions, exit codes.

**Library operations take explicit params, not `&IniConfig`:**

```rust
// Library — takes what it needs, no config awareness
session::deadline_client(profile: Option<&str>)
create_job_from_job_bundle(params: SubmitJobParams)  // explicit fields, no config

// CLI — reads config, extracts values, passes them in
let config = config_file::read_config()?;
let profile = config_file::get_setting("defaults.aws_profile_name", &config).ok();
let dl = session::deadline_client(profile.as_deref()).await;
```

**A programmatic consumer (no config file) uses the same library:**

```rust
// No config, no disk — just pass values directly
let dl = session::deadline_client(Some("my-profile")).await;
let builder = client::apply_dcm_principal(dl.list_farms(), Some("my-profile"));
let pages = client::collect_paginated(builder.into_paginator().send()).await?;
```

### What is a thin wrapper? (avoid these)

A function is a **thin wrapper** if it does nothing beyond forwarding
arguments — literally 1-2 lines that a caller could write inline with
no loss of clarity:

```rust
// DON'T — wraps a single SDK call, adds no value
pub async fn get_farm(farm_id: &str, profile: Option<&str>) -> Result<FarmDetails, DeadlineError> {
    let client = session::deadline_client(profile).await;
    Ok(FarmDetails::from(client.get_farm().farm_id(farm_id).send().await?))
}
```

The CLI can write those 2 lines inline. A library function is justified
only when it contains real logic: orchestration, multi-step flows,
algorithmic pagination, polling, or complex input construction.

### What belongs in `api.rs` (complex operations)

Functions with **real logic beyond a single SDK call**:
- Algorithmic pagination (e.g. `list_jobs_by_filter_expression` with
  createdAt thresholding and dedup)
- Input construction from untyped data (e.g. `create_job` mapping a
  JSON map to typed SDK builders with conditional fields)
- Identifier construction (e.g. `batch_get_steps_page` building
  `BatchGetStepIdentifier` from Value arrays)
- Polling loops (e.g. `wait_for_create_job_to_complete`)
- SDK type construction utilities (e.g. `build_filter_expressions`)

### Shared infrastructure (not wrappers — utilities)

| Utility | Purpose |
|---------|---------|
| `session::deadline_client(profile)` | Session caching, user-agent, telemetry interceptor |
| `client::collect_paginated(stream)` | Drains SDK paginator into `Vec<PageOutput>` |
| `client::deadline_error(e)` | Maps `SdkError` → `DeadlineError` with code+message |
| `client::format_sdk_error(&e)` | Formats `SdkError` as `"Code: message"` string |
| `client::apply_dcm_principal(builder, profile)` | DCM principal injection for list APIs |
| `api::build_filter_expressions(json)` | Constructs SDK filter types from JSON |
| `api::build_sort_expressions(json)` | Constructs SDK sort types from JSON |
| `responses::FarmResponse`, etc. | Serializable types with `From<SdkOutput>` impls |

### DateTime formatting for display

SDK `DateTime` values must be formatted to match Python CLI output:

```rust
fn format_datetime(dt: &aws_sdk_deadline::primitives::DateTime) -> String {
    dt.fmt(aws_sdk_deadline::primitives::DateTimeFormat::DateTimeWithOffset)
        .unwrap_or_default()
        .replace('T', " ")
        .replace('Z', "+00:00")
}
// Input:  2024-12-18T00:00:00Z
// Output: 2024-12-18 00:00:00+00:00
```

### HashMap iteration and deterministic output

SDK types use `HashMap` for maps (e.g. `task_run_status_counts`,
`parameters`). HashMap iteration order is non-deterministic. **Always
sort keys** when serializing map types to display output:

```rust
let mut pairs: Vec<_> = params.iter()
    .map(|(k, v)| format!("{k}={v}"))
    .collect();
pairs.sort();
```

### Error formatting

The SDK's `SdkError` `Display` impl just says `"service error"` — it
never includes the actual error code or message. Always use a helper
that extracts the error code and message via `ProvideErrorMetadata`:

- `api::format_sdk_error` — generic, works for any AWS SDK error.
- `api::sdk_err` — wraps `format_sdk_error` into `DeadlineError`
  for Deadline API calls.
- `log_retrieval.rs::cw_sdk_err` — same pattern for CloudWatch Logs.
- `s3.rs::format_sts_sdk_err` — same pattern for STS.

Output format: `"AccessDeniedException: User is not authorized..."`.

**Never use `format!("{e}")` on an `SdkError`.** It produces `"service
error"` which is useless to the user and breaks error-type detection.


## Credential Scoping for Non-Deadline AWS Services

Operations that access non-Deadline AWS services (CloudWatch Logs, S3)
may require queue-scoped or fleet-scoped credentials. When the user is
logged in via Deadline Cloud Monitor (DCM), the base credentials only
have Deadline API permissions — the queue or fleet role grants access
to other services.

**Rule:** When building a non-Deadline AWS client, check whether the
user is a DCM user. If so, use scoped credentials. If not, use the
base session.

For the full pattern including queue-scoped credentials, fleet-scoped
credentials, caching, and endpoint propagation, see
[`deadline-lib/api/credential-scoping.md`](deadline-lib/api/credential-scoping.md).

## Serialization: API Responses vs Owned File Formats

Two distinct patterns exist for JSON serialization in this codebase.
Choose based on who controls the schema.

### API responses → typed SDK output + serializable response structs

Use the SDK output type directly. Access fields via typed accessors.
For display paths that need to print the full response, use a response
struct with `#[derive(Serialize)]` and `From<Output>`:

```rust
// Business logic — typed accessors
let output = client.get_job()...send().await?;
let name = output.name();
let status = output.lifecycle_status();

// Display path — response struct
let resp = JobResponse::from(output);
println!("{}", cli_object_repr(&serde_json::to_value(&resp)?));
```

Response structs live in `responses.rs`. Nested SDK types that lack
`Serialize` are converted to `serde_json::Value` via helpers in
`type_conversions.rs` that walk typed SDK accessors.

### Owned file formats → `#[derive(Serialize, Deserialize)]`

For file formats we control (checkpoint files, state persistence,
config structures we write and read back), use serde derives:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MyState {
    pub required_field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optional_field: Option<String>,
    #[serde(default)]
    pub defaulted_field: Vec<Item>,
}
```

This gives compile-time field validation, automatic error messages for
missing/malformed fields, and eliminates manual JSON object construction.
The schema is under our control so there's no SDK compatibility concern.

**Use for:** checkpoint files, incremental download state, any
persistent data format owned by this codebase.

## GUI Commands: Rust → Python Subprocess

GUI commands (`bundle gui-submit`, `config gui`) spawn a Python
subprocess because the Qt GUI code (PySide6/qtpy) must run in Python.
All business logic stays in Rust — Python only handles widget rendering
and calls back into Rust via `deadline._native` (PyO3).

### Pattern: launch a GUI command

```rust
// 1. Validate args in Rust (submitter-info fields, parameter format, etc.)
let params = serde_json::json!({ "job_bundle_dir": dir, "output": "json" });

// 2. Find Python
let python = gui::find_python()?;  // DEADLINE_PYTHON → _internal/Python → PATH

// 3. Spawn and capture output
let stdout = gui::launch_gui(&python, "gui-submit", &params.to_string(), install_gui)?;
```

### Rules

- **Rust validates, Python renders.** Arg parsing and validation
  (submitter-info fields, parameter format) happen in Rust before
  Python is spawned. Python receives pre-validated JSON.
- **JSON is the contract.** Rust sends params as `--params-json '{...}'`.
  Python prints result JSON to stdout. No other IPC mechanism.
- **Single entry point.** All GUI commands go through
  `python -m deadline.client.ui._gui_entry <command>`. Don't add
  separate Python scripts per command.
- **No `click` in `gui/`.** The Python GUI code must not depend on
  `click`. Use stdlib (`print`, `input`, `argparse`) only.
- **Python discovery is deterministic.** `DEADLINE_PYTHON` env var →
  `_internal/Python` relative to binary → `python3` on PATH → `python`
  on PATH. This order supports installer, pip install, and dev workflows.

### What's NOT yet patterned

The Rust→Python data boundary for widget state (how `read_config()`
returns should look, how widgets consume API responses from `_native`)
is still being resolved. Patterns for that will be added after the
GUI widget rendering fixes (#16d3).


## Porting a CLI Command: Parity Checklist

When porting a Python CLI command to Rust, verify these categories to
avoid the most common parity gaps (sourced from 4 behavioral audits):

1. **All flags in `--help`** — compare Python's `--help` output flag-by-flag.
   Missing flags (`--ignore-storage-profiles`, `--include`,
   `--match-paths-by`) were the #1 source of parity bugs.
2. **Config settings it reads** — check which `get_setting()` calls the
   Python command makes. Missing config fallbacks (`max_retries_per_task`,
   `max_failed_tasks_count`) caused silent behavior differences.
3. **Which AWS APIs it calls** — verify the exact API operations and their
   parameters. Different API strategies (STS vs ListFarms for auth check)
   caused permission failures in restricted environments.
4. **Platform-conditional code** — search for `sys.platform`, `os.name`,
   `os.path.normcase` in the Python source. Case sensitivity, Windows UNC
   paths, and `~user/` expansion are recurring platform gaps.

## Accepted Cosmetic Differences from Python

These output differences have been explicitly evaluated and accepted
across multiple audits. Do not re-raise them as bugs:

- **Error format:** `Code: message` (Rust) vs
  `An error occurred (Code) when calling Op: message` (Python/boto3).
  Rust's format is intentionally shorter. Both convey the same information.
- **DateTime trailing zeros:** Rust may format `2024-01-01 00:00:00+00:00`
  where Python emits `2024-01-01 00:00:00.000000+00:00`. Sub-second
  precision differences are acceptable.
- **HashMap key ordering:** Rust sorts map keys alphabetically for
  deterministic output. Python preserves wire order from the API response.
  Both are valid; Rust's approach is more predictable.
- **macOS `/tmp` → `/private/tmp`:** Rust resolves the symlink via
  `canonicalize()`. Python does not. Both are correct paths to the same
  location.
- **Default field inclusion:** Rust includes `maxFailedTasksCount` and
  `maxRetriesPerTask` in CreateJob requests (from config defaults).
  Python omits them when unset, letting the server apply its own defaults.
  The server behavior is identical either way.
