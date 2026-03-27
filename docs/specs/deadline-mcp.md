# deadline-mcp

Binary crate. MCP (Model Context Protocol) server exposing Deadline Cloud
operations as tools for AI assistants.

## Status: Not started (Phase 5)

## Overview

Uses the official Rust MCP SDK
([rmcp](https://rust.sdk.modelcontextprotocol.io/)) to implement a stdio-based
MCP server. Started via `deadline mcp-server` CLI command.

The server exposes the same tools as the current Python MCP implementation,
calling directly into `deadline-client` for all API operations.

## Tools

| Tool | Underlying API | Description |
|------|---------------|-------------|
| `list_farms` | `ListFarms` | List farms accessible to the user |
| `list_queues` | `ListQueues` | List queues in a farm |
| `list_jobs` | `ListJobs` | List jobs in a queue |
| `list_fleets` | `ListFleets` | List fleets in a farm |
| `list_storage_profiles_for_queue` | `ListStorageProfilesForQueue` | List storage profiles |
| `list_sessions` | `ListSessions` | List sessions for a job |
| `list_steps` | `ListSteps` | List steps for a job |
| `list_tasks` | `ListTasks` | List tasks for a step |
| `get_job` | `GetJob` | Get job details |
| `get_session` | `GetSession` | Get session details |
| `search_jobs` | `SearchJobs` | Search jobs with filters |
| `check_authentication_status` | STS + ListFarms | Check credential validity |
| `get_session_logs` | CloudWatch Logs | Fetch session log events |
| `get_session_and_worker_logs` | CloudWatch Logs | Fetch session + worker logs |
| `submit_job` | `CreateJob` + attachments | Submit a job bundle |
| `download_job_output` | S3 + manifests | Download job output files |

## Telemetry

Each tool invocation records two telemetry events via `deadline-client`:
- `com.amazon.rum.deadline.mcp.latency` — execution time
- `com.amazon.rum.deadline.mcp.usage` — tool name, success/failure, error type

## Dependencies

| Crate | Purpose |
|-------|---------|
| `rmcp` | Official Rust MCP SDK |
| `deadline-config` | Config file operations |
| `deadline-client` | AWS API calls |
| `deadline-job-bundle` | Bundle loading for submit_job |
| `deadline-job-attachments` | Attachment handling for submit/download |
| `deadline-models` | Shared types |
| `tokio` | Async runtime |
| `serde_json` | JSON serialization |
