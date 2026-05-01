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
   remove all `_with_raw` variants, remove `capture_send` helper,
   remove `paginated_list` helper.

2. **Fix response structs** — change `from_output_and_raw(output, &raw)`
   to `From<Output> for Response`. Manually convert complex nested types
   by walking SDK accessors.

3. **Convert remaining raw functions** — every function in `api.rs` that
   still uses `capture_send` must return SDK output types or be inlined
   at call sites.

4. **Migrate all callers** to use typed SDK output fields instead of
   `value["fieldName"]` access.

### Step 1 — Study findings

#### Complex nested types requiring manual conversion functions

Each of these SDK types lacks `Serialize`. We need `fn foo_to_value(&T) -> Value`:

| SDK Type | Fields | Depth | Used by |
|----------|--------|-------|---------|
| `Attachments` | manifests: Vec<ManifestProperties>, file_system: enum | 2 | JobResponse |
| `ManifestProperties` | file_system_location_name?, root_path, root_path_format: enum, output_relative_directories?, input_manifest_path?, input_manifest_hash? | 1 | Attachments |
| `JobParameter` | enum: Float(String), Int(String), Path(String), String(String) | 1 | JobResponse |
| `TaskParameterValue` | enum: ChunkInt(String), Float(String), Int(String), Path(String), String(String) | 1 | TaskResponse |
| `LogConfiguration` | log_driver, options?: HashMap<S,S>, parameters?: HashMap<S,S>, error? | 1 | SessionResponse, WorkerResponse |
| `HostPropertiesResponse` | ip_addresses?: IpAddresses, host_name?, ec2_instance_arn?, ec2_instance_type? | 2 | SessionResponse, WorkerResponse |
| `IpAddresses` | ipv4_addresses?: Vec<String>, ipv6_addresses?: Vec<String> | 1 | HostPropertiesResponse |
| `FleetConfiguration` | enum: CustomerManaged(CMFC), ServiceManagedEc2(SMEFC) | 3+ | FleetResponse |
| `CustomerManagedFleetConfiguration` | mode: enum, auto_scaling_configuration?, worker_capabilities?, storage_profile_id?, tag_propagation_mode? | 3+ | FleetConfiguration |
| `ServiceManagedEc2FleetConfiguration` | instance_capabilities?, instance_market_options?, vpc_configuration?, storage_profile_id?, auto_scaling_configuration? | 3+ | FleetConfiguration |
| `JobRunAsUser` | posix?: PosixUser, windows?: WindowsUser, run_as: enum | 2 | QueueResponse |
| `PosixUser` | user, group | 1 | JobRunAsUser |
| `WindowsUser` | user, passwordArn | 1 | JobRunAsUser |
| `JobAttachmentSettings` | s3_bucket_name, root_prefix | 1 | QueueResponse |
| `ParameterSpace` | parameters: Vec<StepParameter>, combination? | 2 | StepResponse |
| `StepParameter` | name, type: enum, chunks?: StepParameterChunks | 2 | ParameterSpace |
| `StepRequiredCapabilities` | attributes: Vec<...>, amounts: Vec<...> | 2+ | StepResponse |
| `DependencyCounts` | dependencies_resolved, dependencies_unresolved, consumers_resolved, consumers_unresolved | 1 | StepResponse |

#### Callers of remaining raw functions

| Function | Callers | What they access |
|----------|---------|-----------------|
| `get_queue` (→ Value) | job.rs, queue.rs, manifest.rs, attachment.rs, submission.rs | `jobAttachmentSettings.{s3BucketName, rootPrefix}`, `displayName`, `roleArn` |
| `list_jobs` (→ Value) | job.rs | iterates `jobs[]`, accesses `jobId`, `name`, `lifecycleStatus`, etc. |
| `search_jobs` (→ Value) | job.rs | iterates `jobs[]`, accesses `jobId`, `name` |
| `search_jobs_with_filters` (→ Value) | job.rs (list-jobs-by-filter) | iterates `jobs[]` |
| `search_workers` (→ Value) | worker.rs | iterates `workers[]`, accesses `workerId`, `status`, etc. |
| `list_sessions` (→ Value) | job.rs | iterates `sessions[]`, accesses `sessionId`, `startedAt`, `endedAt` |
| `list_steps` (→ Value) | job.rs | iterates `steps[]`, accesses `stepId`, `name`, `taskRunStatusCounts` |
| `list_tasks` (→ Value) | job.rs | iterates `tasks[]`, accesses `taskId`, `runStatus`, `parameters` |
| `batch_get_steps_page` (→ Value) | job.rs (download-output) | accesses `steps[].name` |
| `batch_get_tasks_page` (→ Value) | job.rs (download-output) | accesses `tasks[].parameters`, `latestSessionActionId` |
| `assume_queue_role_for_user` (→ Value) | session.rs | accesses `credentials.{accessKeyId, secretAccessKey, sessionToken, expiration}` |
| `assume_queue_role_for_read` (→ Value) | session.rs | same credential fields |
| `assume_fleet_role_for_read` (→ Value) | session.rs | same credential fields |
| `get_storage_profile_for_queue` (→ Value) | submission.rs, queue.rs | accesses `displayName`, `fileSystemLocations[]` |
| `list_storage_profiles_for_queue` (→ Value) | queue.rs | iterates `storageProfiles[]` |
| `get_session_action` (→ Value) | job.rs (download-output) | accesses `startedAt`, `endedAt` |
| `list_session_actions` (→ Value) | job.rs (trace-schedule) | iterates `sessionActions[]` |
| `list_queue_environments` (→ Value) | queue.rs, queue_parameters.rs | iterates `environments[]` |
| `get_queue_environment` (→ Value) | queue_parameters.rs | accesses `template`, `templateType` |
| `list_queue_fleet_associations` (→ Value) | queue.rs | iterates `queueFleetAssociations[]` |
| `update_job` (→ Value) | job.rs | ignores response (just checks success) |
| `update_task` (→ Value) | job.rs | ignores response |
| `create_job` (→ Value) | submission.rs | accesses `jobId` |

#### Conversion function complexity estimate

- **Shallow types** (1 level, ~5 lines each): `JobParameter`, `TaskParameterValue`, `JobAttachmentSettings`, `PosixUser`, `WindowsUser`, `IpAddresses`, `LogConfiguration`, `DependencyCounts`
- **Medium types** (2 levels, ~15 lines each): `Attachments`, `ManifestProperties`, `HostPropertiesResponse`, `JobRunAsUser`, `ParameterSpace`, `StepParameter`
- **Deep types** (3+ levels, ~50+ lines each): `FleetConfiguration` (CustomerManaged + ServiceManagedEc2 with nested capabilities, market options, VPC config, etc.)

### Implementation plan

#### Module: `crates/deadline-api/src/type_conversions.rs` (NEW)

All `fn sdk_type_to_value(&T) -> Value` converters live here. Keeps
`responses.rs` clean. Organized by complexity:

```rust
// Simple enum → {"variant": "value"}
pub fn job_parameter_to_value(p: &JobParameter) -> Value
pub fn task_parameter_value_to_value(p: &TaskParameterValue) -> Value

// Flat structs → json object
pub fn job_attachment_settings_to_value(s: &JobAttachmentSettings) -> Value
pub fn log_configuration_to_value(l: &LogConfiguration) -> Value
pub fn ip_addresses_to_value(a: &IpAddresses) -> Value
pub fn host_properties_to_value(h: &HostPropertiesResponse) -> Value
pub fn posix_user_to_value(u: &PosixUser) -> Value
pub fn windows_user_to_value(u: &WindowsUser) -> Value
pub fn job_run_as_user_to_value(j: &JobRunAsUser) -> Value
pub fn dependency_counts_to_value(d: &DependencyCounts) -> Value

// Nested structs
pub fn manifest_properties_to_value(m: &ManifestProperties) -> Value
pub fn attachments_to_value(a: &Attachments) -> Value
pub fn parameter_space_to_value(p: &ParameterSpace) -> Value
pub fn fleet_configuration_to_value(c: &FleetConfiguration) -> Value
```

#### Changes to `responses.rs`

- `QueueResponse`: `from_output_and_raw` → `From<GetQueueOutput>`, uses
  `job_attachment_settings_to_value`, `job_run_as_user_to_value`
- `FleetResponse`: `from_output_and_raw` → `From<GetFleetOutput>`, uses
  `fleet_configuration_to_value`, `host_properties_to_value`
- `JobResponse`: `from_output_and_raw` → `From<GetJobOutput>`, uses
  `job_parameter_to_value`, `attachments_to_value`
- `StepResponse`: `from_output_and_raw` → `From<GetStepOutput>`, uses
  `parameter_space_to_value`, `dependency_counts_to_value`
- `TaskResponse`: `from_output_and_raw` → `From<GetTaskOutput>`, uses
  `task_parameter_value_to_value`
- `SessionResponse`: `from_output_and_raw` → `From<GetSessionOutput>`, uses
  `log_configuration_to_value`, `host_properties_to_value`
- `WorkerResponse`: `from_output_and_raw` → `From<GetWorkerOutput>`, uses
  `log_configuration_to_value`, `host_properties_to_value`

#### Changes to `api.rs`

- Delete `capture_send`, `paginated_list`, `capture_err`
- Delete all `_with_raw` variants
- Convert each remaining function to return SDK output type
- For paginated functions: use SDK paginator or manual typed pagination
- `update_job`/`update_task` → return `()`
- `create_job` → return `CreateJobOutput`

#### Caller migration

- `queue["jobAttachmentSettings"]["s3BucketName"]` → `output.job_attachment_settings().map(|s| s.s3_bucket_name())`
- `value["jobs"].as_array()` → `output.jobs()` (paginator)
- `value["credentials"]["accessKeyId"]` → `output.credentials().access_key_id()`

### Batching strategy (revised)

- **D5a:** Create `type_conversions.rs`. Convert all response structs to
  `From<Output>`. Remove `_with_raw` variants. Fix all callers of
  `get_job/step/task/session/worker` and `get_queue/fleet` display paths.
  (~200 lines new converters, ~100 lines caller changes)

- **D5b:** Convert `get_queue` to return `GetQueueOutput`. Migrate 6
  callers from `value["field"]` to typed accessors. Convert credential
  APIs (`assume_*`) to return typed output. Migrate session.rs callers.
  (~80 lines)

- **D5c:** Convert list/search APIs to return typed SDK output.
  `list_sessions` → paginator → `Vec<SessionSummary>`, etc.
  Migrate all callers in job.rs, worker.rs. (~150 lines)

- **D5d:** Convert remaining: `list_queue_environments`,
  `get_queue_environment`, `list_queue_fleet_associations`,
  `list_storage_profiles_for_queue`, `get_storage_profile_for_queue`,
  `list_session_actions`, `update_job`, `update_task`, `create_job`.
  (~100 lines)

- **D5e:** Delete `response_capture.rs`. Remove all `ResponseBodyCapture`
  imports. Update `specs/patterns.md` to remove raw pattern docs.
  Clean sweep. (~50 lines deleted)

### Step status

- [x] Step 1 — Study
- [ ] Step 2 — Write tests
- [ ] Step 3 — Implement
- [ ] Step 4 — CLI comparison
- [ ] Step 5 — Audit
- [ ] Step 6 — Spec
- [ ] Step 7 — Commit

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
