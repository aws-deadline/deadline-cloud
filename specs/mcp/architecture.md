# deadline-mcp Architecture

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

- **Pass-through (13):** Call `deadline-api` functions, return
  `serde_json::Value` directly. Thin async methods.
- **Custom (2):** `submit_job`, `download_job_output` — validation logic
  + library calls. Not yet implemented (Batch 2-3).
- **Composite (1):** `get_session_and_worker_logs` — calls get_session +
  get_session_logs + get_worker_logs. Not yet implemented (Batch 3).

### Error handling

Tool errors return `{"error": "...", "type": "..."}` as text content
(not MCP protocol errors). Missing required params cause protocol-level
errors via rmcp's deserialization.

### Parameter filtering

Optional params use `Option<T>`. rmcp handles null deserialization.
List tools auto-paginate internally — `next_token` is accepted but
ignored (all pages returned).

### Instructions

INSTRUCTIONS string matches Python exactly — same debugging workflow,
key concepts, and configuration guidance.

## Dependencies

`rmcp` v1.5 (server, transport-io, macros), `schemars` for JSON Schema
generation, `deadline-api` for all AWS API calls.

## Known differences from Python

- Parameter names use snake_case (Python list tools use camelCase for
  boto3 pass-through). MCP schemas are self-describing so clients adapt.
- `principalId` not exposed as a parameter — auto-injected internally.
- `get_session_logs` missing `start_time`/`end_time` params (low priority).
- No telemetry recording yet (deferred).
