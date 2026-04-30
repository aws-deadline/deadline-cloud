# Handoff

Current in-flight work. Read this at the start of every session before
consulting the Work Items table in `specs/progress.md`.

---

## #27 — Typed SDK API layer

**Status:** In progress — Batch D1–D3 complete, Batch D4 next.

### Design intent

Fully typed SDK usage end-to-end:
- **Input:** Callers use SDK fluent builders directly.
- **Output:** Typed SDK output → response struct (field extraction) →
  serializable JSON for CLI print and FFI boundary.
- **No raw HTTP extraction.** The SDK controls which fields are visible.

### What's done

- Batches A–C: `client.rs`, `telemetry_interceptor.rs`, telemetry wired
- **Batch D1–D3:** `responses.rs` with `FarmResponse`, `QueueResponse`,
  `FleetResponse` + `format_datetime`. All farm/queue/fleet callers
  migrated (CLI list+get, helpers, mcp, FFI resources). Suggest on list
  failure restored. Fleet get `--queue-id` suggest gap fixed.

### Pattern established in D1–D3

**List:** Typed paginator → field extraction inline → `json!({...})`.

**Get (simple types like Farm):** `dl.get_farm().send().await` →
`FarmResponse::from(output)` → `serde_json::to_value(&resp)`. No raw capture.

**Get (complex nested types like Fleet/Queue):** Use `ResponseBodyCapture`
alongside `.send()` to get both typed output AND raw JSON. Build response
struct from typed fields + `raw.get("nestedField").cloned()` for SDK types
that lack `Serialize` (FleetConfiguration, JobRunAsUser, etc.).

---

## Next: Batch D4 — Job/Step/Task/Session/Worker response structs

**Scope:** Add `JobResponse`, `StepResponse`, `TaskResponse`,
`SessionResponse`, `WorkerResponse` to `responses.rs`. Migrate `job.rs`,
`worker.rs`, and remaining `mcp.rs` get tools.

### Guidance for implementation

1. **Check SDK output types** in `~/.cargo/registry/src/*/aws-sdk-deadline-1.99.0/src/operation/get_{resource}/_get_{resource}_output.rs` for exact fields and required/optional status.

2. **Complex nested types** that need raw JSON extraction (same pattern as Fleet/Queue):
   - `GetJob`: `taskRunStatusCounts` (HashMap<TaskRunStatus, i32>), `parameters` (HashMap<String, JobParameter>), `attachments` (Attachments)
   - `GetStep`: `taskRunStatusCounts`, `dependencyCounts`, `requiredCapabilities`, `parameterSpace`
   - `GetTask`: `parameters` (HashMap<String, TaskParameterValue>)
   - `GetSession`: `log` (LogConfiguration), `hostProperties`, `workerLog`
   - `GetWorker`: `hostProperties`, `log`

3. **Enum types** that need `.as_str()` conversion (same as Fleet/Queue):
   - `JobLifecycleStatus`, `TaskRunStatus`, `JobTargetTaskRunStatus`
   - `StepLifecycleStatus`, `StepTargetTaskRunStatus`
   - `TaskTargetRunStatus`, `WorkerStatus`, `SessionLifecycleStatus`

4. **`job.rs` callers** — most use `api::get_job()` which returns `Value`.
   After D4, `api::get_job()` should return typed output. But `job.rs`
   also has `job get` (print full response) and `job get --search-term`
   (print subset). Both need response structs.

5. **`mcp.rs` remaining** — `get_job`, `get_queue` (already done),
   `get_fleet` (already done). Check which MCP tools still use raw capture.

6. **Existing Level 2 snapshot tests** cover `job.rs` and `worker.rs`
   extensively. They'll catch any output format changes.

### Step status

- [ ] Step 1 — Study (verify SDK types for Job/Step/Task/Session/Worker)
- [ ] Step 2 — Write tests
- [ ] Step 3 — Implement
- [ ] Step 4 — CLI comparison
- [ ] Step 5 — Audit
- [ ] Step 6 — Spec
- [ ] Step 7 — Commit

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
