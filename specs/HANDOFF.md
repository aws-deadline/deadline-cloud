# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — Batch D1–D4 complete. Remaining: migrate `get_queue` and search/list APIs to typed pattern.

### Design intent

Base API functions return the SDK output struct directly. Callers decide
what to do with it:

- **Internal business logic:** Use typed fields from the SDK output
  (e.g. `output.name()`, `output.started_at()`). Access only what's needed.
- **External print/display (full dump):** Build a response struct from
  the SDK output that serializes ALL fields to JSON for display. Complex
  nested SDK types that lack `Serialize` are extracted from raw HTTP body.
- **External print/display (partial):** Access typed fields directly,
  no response struct needed.

**No `_raw` / `_typed` split.** One base function per API returns SDK output.
A `_with_raw` variant exists only for resources with complex nested types
that need full-dump display (returns `(SdkOutput, Value)`).

### What's done

- Batches A–C: `client.rs`, `telemetry_interceptor.rs`, telemetry wired
- **Batch D1–D3:** `responses.rs` with `FarmResponse`, `QueueResponse`,
  `FleetResponse` + `format_datetime`. All farm/queue/fleet callers
  migrated (CLI list+get, helpers, mcp, FFI resources). Suggest on list
  failure restored. Fleet get `--queue-id` suggest gap fixed.
- **Batch D4:** `JobResponse`, `StepResponse`, `TaskResponse`,
  `SessionResponse`, `WorkerResponse` added to `responses.rs`. API
  functions return SDK output types. `_with_raw` variants for full-dump
  paths. All callers in `job.rs`, `worker.rs`, `mcp.rs`, `queue.rs`,
  `manifest.rs`, `job_monitoring.rs` migrated. Deterministic output
  ordering for HashMap-derived fields.

### Pattern established in D1–D3

**List:** Typed paginator → field extraction inline → `json!({...})`.

**Get (simple types like Farm):** `dl.get_farm().send().await` →
`FarmResponse::from(output)` → `serde_json::to_value(&resp)`. No raw capture.

**Get (complex nested types like Fleet/Queue):** Use `ResponseBodyCapture`
alongside `.send()` to get both typed output AND raw JSON. Build response
struct from typed fields + `raw.get("nestedField").cloned()` for SDK types
that lack `Serialize` (FleetConfiguration, JobRunAsUser, etc.).

**Rule:** Response structs exist ONLY for the "print every field" display
path. If a caller only needs a few fields, it uses the SDK output directly.

---

## Batch D4 — ✅ Complete

Job/Step/Task/Session/Worker response structs added, API functions return
SDK output types, all callers migrated. See commit `deee43a`.

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
