# Coding Patterns

Patterns and conventions for writing code in this workspace. Read this
before implementing API calls or touching `deadline-api`.

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
let output: GetFarmOutput = get_farm("farm-abc", None, None).await.unwrap();
let value: Value = get_farm_raw("farm-abc", None, None).await.unwrap();
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

### Dual API pattern: typed + raw

API functions come in two variants:

1. **Typed (default name, e.g. `get_farm`):** Returns the SDK's native
   output type (`GetFarmOutput`). Callers access fields via typed
   accessors (`.farm_id()`, `.lifecycle_status()`). Use for business
   logic, FFI, and list commands that cherry-pick fields.

2. **Raw (`_raw` suffix, e.g. `get_farm_raw`):** Returns
   `serde_json::Value` via the `ResponseBodyCapture` interceptor.
   Includes all fields the API sends, even those the SDK doesn't model
   yet. Use ONLY for CLI commands that dump the entire response to the
   user (e.g. `deadline farm get`).

```rust
// Typed — compile-time safe field access, use by default
pub async fn get_farm(...) -> Result<GetFarmOutput, DeadlineError>

// Raw — full wire JSON for print paths only
pub async fn get_farm_raw(...) -> Result<Value, DeadlineError>
```

**When to use which:**
- Accessing specific fields for logic → typed
- Passing a subset to FFI/Python → typed, then build a serializable struct
- Printing the full API response verbatim → raw

### `ResponseBodyCapture` pattern (raw variant only)

The `_raw` functions use an SDK interceptor that captures the raw HTTP
response body as `serde_json::Value`. The interceptor also converts
datetimes to Python format and strips nulls. New API fields appear
automatically without code changes.

For the full pattern including pagination, search APIs, datetime
handling, and known differences from boto3, see
[`deadline-api/response-capture.md`](deadline-api/response-capture.md).

### Typed variant pattern

The typed functions call the SDK directly without the interceptor:

```rust
pub async fn get_farm(farm_id: &str, config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>)
    -> Result<GetFarmOutput, DeadlineError>
{
    let client = session::deadline_client(config).await;
    client.get_farm().farm_id(farm_id).send().await.map_err(sdk_err)
}
```

For list functions, the typed variant uses the SDK's built-in paginator:

```rust
pub async fn list_farms(config: Option<&IniConfig>, telemetry: Option<&TelemetryClient>)
    -> Result<Vec<FarmSummary>, DeadlineError>
{
    let client = session::deadline_client(config).await;
    let mut farms = Vec::new();
    let mut paginator = client.list_farms().into_paginator().send();
    while let Some(page) = paginator.next().await {
        let page = page.map_err(sdk_err)?;
        farms.extend(page.farms());
    }
    Ok(farms)
}
```

### Error formatting

The SDK's `SdkError` `Display` impl just says `"service error"` — it
never includes the actual error code or message. Always use a helper
that extracts the error code and message via `ProvideErrorMetadata`:

- `api.rs::format_sdk_error` — generic, works for any AWS SDK error.
- `api.rs::sdk_err` — wraps `format_sdk_error` into `DeadlineError`
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
[`deadline-api/credential-scoping.md`](deadline-api/credential-scoping.md).

## Serialization: API Responses vs Owned File Formats

Two distinct patterns exist for JSON serialization in this codebase.
Choose based on who controls the schema.

### API responses → typed SDK output OR `serde_json::Value`

- **Typed (default):** Use the SDK output type directly. Access fields
  via typed accessors. Use for business logic and FFI.
- **Raw (`_raw` suffix):** Use `ResponseBodyCapture` to capture raw
  JSON as `serde_json::Value`. Use for CLI print paths that must show
  all fields. Model types that parse API responses use manual
  `from_json(&Value)` methods (e.g. `StorageProfile::from_json`).

**Use typed for:** anything that accesses specific fields for logic or FFI.
**Use raw for:** CLI commands that dump the full response to the user.

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
