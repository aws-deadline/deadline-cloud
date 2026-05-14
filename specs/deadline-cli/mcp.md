# MCP Server Architecture

## Location

MCP server lives inside `deadline-cli` at `src/commands/mcp.rs` — no
separate crate. Nothing else depends on the MCP server, and a separate
crate adds overhead without benefit.

## Design

Single `DeadlineServer` struct with `#[tool_router]` (rmcp macro) exposes
16 tools over stdio transport. The CLI gets a `deadline mcp-server`
subcommand that creates a tokio runtime and runs the server until
interrupted.

### Tool categories

- **Pass-through (13):** Call `deadline-lib::api` functions, return
  `serde_json::Value` directly. Thin async methods.
- **Custom (2):** `submit_job` validates inputs (directory exists, params
  are JSON array, farm_id/queue_id required) then calls
  `create_job_from_job_bundle`. `download_job_output` validates inputs
  (task_id requires step_id, job_id required, conflict_resolution valid)
  then calls `download_output_impl`.
- **Composite (1):** `get_session_and_worker_logs` calls `get_session` +
  `get_session_logs` + `get_worker_logs` with graceful error handling
  on worker logs.

### Send+Sync workaround

`submit_job` and `download_job_output` use `std::thread::spawn` +
`Handle::block_on` to run the actual submission/download. This is
because `SubmitJobParams` contains `Box<dyn Fn + Send>` callbacks
that are `Send` but not `Sync`, and rmcp's `#[tool]` macro requires
the handler future to be `Send`. Moving the work to a separate thread
avoids the `Sync` requirement.

### Error handling

Tool errors return `{"error": "...", "type": "..."}` as text content
(not MCP protocol errors). Missing required params cause protocol-level
errors via rmcp's deserialization. Validation errors (bad directory,
invalid JSON, missing IDs) return error dicts with type `"ValueError"`.

### Parameter filtering

Optional params use `Option<T>`. rmcp handles null deserialization.
List tools auto-paginate internally — `next_token` is accepted but
ignored (all pages returned).

### Instructions

INSTRUCTIONS string matches Python exactly — same debugging workflow,
key concepts, and configuration guidance.

## Dependencies

`rmcp` v1.5 (server, transport-io, macros), `schemars` for JSON Schema
generation, `deadline-lib::api` for all AWS API calls, `deadline-lib::bundle`
for submission, `deadline-lib::attachments` for download.

## Known differences from Python

- Parameter names use snake_case (Python list tools use camelCase for
  boto3 pass-through). MCP schemas are self-describing so clients adapt.
- `principalId` not exposed as a parameter — auto-injected internally.
- `get_session_logs` missing `start_time`/`end_time` params (low priority).
- `download_job_output` return omits `output` text field (Rust download
  functions return structured data, not stdout text).
- Worker logs `error` field omitted when null (Rust convention).

## Telemetry

Three events emitted matching Python's `_mcp/utils.py`:

- **`com.amazon.rum.deadline.mcp.server_startup`** — emitted once on
  server start with `{usage_mode: "MCP", startup_method: "cli"}`.
- **`com.amazon.rum.deadline.mcp.latency`** — emitted per tool call
  with `{latency: nanos, tool_name, usage_mode: "MCP"}`.
- **`com.amazon.rum.deadline.mcp.usage`** — emitted per tool call
  with `{tool_name, is_success, error_type, usage_mode: "MCP"}`.

All tools are wrapped via `with_mcp_telemetry!` macro. Telemetry is
fire-and-forget — errors never affect tool results.
