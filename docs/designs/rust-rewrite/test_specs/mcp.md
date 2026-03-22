# MCP Server

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 50: MCP server

> **Rust crate:** (new) `deadline-mcp` or deferred
> **⚠️ DEFERRED:** Python-specific MCP integration. Consider deferring for initial Rust port.
>
> **Logic under test:** The MCP (Model Context Protocol) server that exposes
> Deadline Cloud operations as tools for LLM clients. Includes tool registry,
> tool wrapper generation (parameter filtering, serialization, error handling,
> telemetry), and the concrete tools: submit_job, download_job_output, and
> get_session_and_worker_logs.

### Tool registry

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Get definition for a registered tool name | Returns the tool definition with function and parameter names | |
| 2 | Error handling | Get definition for an unregistered tool name | Returns error (tool not found) | |
| 3 | Happy path | Get all tool names | Returns list of all registered tool names | |
| 4 | Happy path | Registry contains expected tools | Includes list_farms, list_queues, list_jobs, list_fleets, list_storage_profiles_for_queue, check_authentication_status, get_session_logs, submit_job, download_job_output, get_session_and_worker_logs, get_job, get_session, list_sessions, list_steps, list_tasks, search_jobs | |

### Tool wrapper generation

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Wrapper called with valid parameters | Underlying function called; result serialized to JSON-compatible dict | |
| 6 | Happy path | Wrapper receives null/empty/`"null"` parameter values | Those parameters filtered out before calling underlying function | |
| 7 | Error handling | Underlying function raises an error | Error handler returns dict with error message and type name | |
| 8 | Happy path | Successful call | Telemetry recorded with latency, tool_name, usage_mode=MCP, is_success=true | |
| 9 | Error handling | Failed call | Telemetry recorded with is_success=false and error_type | |
| 10 | Happy path | Telemetry recording itself fails | Tool still returns result (telemetry errors suppressed) | |
| 11 | Happy path | Result contains objects serialized via their field map | Serialized using the object's field-to-value mapping | |
| 12 | Happy path | Result contains objects with explicit serialization method | Serialized via the object's conversion function | |

### Tool registration

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 13 | Happy path | Register all tools (no filter) | All registry tools registered with prefix applied to names | |
| 14 | Happy path | Register specific tools by function reference | Only those tools registered | |
| 15 | Happy path | Tool already registered (idempotent registration) | Skipped without error | |
| 16 | Error handling | Function not found in registry | Returns error (function not found in tool registry) | |
| 17 | Error handling | Non-callable passed as tool | Returns error (not callable) | |

### `submit_job(job_bundle_dir, job_parameters?, name?, farm_id?, queue_id?, ...)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | Valid bundle directory with defaults configured | Job submitted; returns dict with status=success, job_id, message, total_time_seconds | |
| 19 | Happy path | job_parameters provided as JSON array string | Parameters parsed and passed to submission | |
| 20 | Error handling | job_bundle_dir does not exist | Returns error (directory does not exist) | |
| 21 | Error handling | job_bundle_dir is a file, not a directory | Returns error (not a directory) | |
| 22 | Error handling | job_parameters is not a JSON array | Returns error (must be a JSON array) | |
| 23 | Error handling | farm_id not provided and not in config | Returns error (farm_id is required) | |
| 24 | Error handling | queue_id not provided and not in config | Returns error (queue_id is required) | |
| 25 | Happy path | submitter_name not provided | Defaults to "MCP" | |

### `download_job_output(farm_id?, queue_id?, job_id?, step_id?, task_id?, conflict_resolution?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 26 | Happy path | Valid job_id with defaults configured | Output downloaded; returns dict with status=success, job_id, output text, total_time_seconds | |
| 27 | Happy path | step_id and task_id provided | Downloads output for specific step/task | |
| 28 | Error handling | task_id provided without step_id | Returns error (step_id required when task_id provided) | |
| 29 | Error handling | job_id not provided | Returns error (job_id is required) | |
| 30 | Error handling | Invalid conflict_resolution value | Returns error (must be SKIP, OVERWRITE, or CREATE_COPY) | |
| 31 | Happy path | auto_accept always set to true | No interactive prompts during download | |

### `get_session_and_worker_logs(farm_id, queue_id, job_id, session_id, limit?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 32 | Happy path | Session has worker and fleet IDs | Returns session details, session logs, and worker logs | |
| 33 | Happy path | Session has no worker ID | Worker logs section has empty events and count=0 | |
| 34 | Error handling | Worker log retrieval fails (permissions) | Worker logs section has error field with message; session logs still returned | |
| 35 | Happy path | limit parameter provided | Both session and worker logs limited to specified count | |
| 36 | Happy path | Default limit | 100 log events per stream | |
| 37 | Happy path | Log events formatted | Each event has timestamp (as string) and message fields | |

### MCP server startup

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 38 | Happy path | Server started | All tools registered with `deadline_` prefix; server runs until interrupted | |
| 39 | Happy path | Server instructions | Contains debugging workflow, key concepts, and configuration guidance | |

> ✅ Complete (39 cases)

---
