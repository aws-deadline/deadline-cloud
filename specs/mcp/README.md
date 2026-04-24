# deadline-mcp Crate Specifications

MCP (Model Context Protocol) server for AI agent integration. Lives in
`deadline-cli` at `src/commands/mcp.rs`, invoked via `deadline mcp-server`.

Consumers: `deadline-cli`.

Dependencies: `rmcp` (MCP SDK), `deadline-config`, `deadline-api`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, tool surface, design decisions, Python differences |

## Status

Batch 1 complete: server skeleton, 13 pass-through tools, CLI subcommand.
13 Level 2 tests passing. Batches 2-3 (submit_job, download_job_output,
get_session_and_worker_logs) not yet implemented.
