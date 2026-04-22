# deadline-mcp Crate Specifications

MCP (Model Context Protocol) server for AI agent integration. Library crate
invoked by `deadline-cli` via `deadline mcp-server`.

Consumers: `deadline-cli`.

Dependencies: `rmcp` (MCP SDK), `deadline-config`, `deadline-api`,
`deadline-job-bundle`, `deadline-job-attachments`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, tool surface, design decisions |

## Status

Placeholder — crate exists with a doc comment only. No tool definitions,
no server implementation, no `mcp-server` CLI subcommand. Python MCP has
13+ tools including `submit_job`, `download_job_output`,
`get_session_and_worker_logs`, and diagnostic APIs. See test spec
`mcp.md` (section 50) for 39 planned test cases.
