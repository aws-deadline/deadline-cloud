# deadline-mcp Crate Specifications

MCP (Model Context Protocol) server for AI agent integration. Lives in
`deadline-cli` at `src/commands/mcp.rs`, invoked via `deadline mcp-server`.

Consumers: `deadline-cli`.

Dependencies: `rmcp` (MCP SDK), `deadline-config`, `deadline-api`,
`deadline-job-bundle`, `deadline-job-attachments`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, tool surface, design decisions, Python differences |

## Status

All 16 tools implemented. 28 Level 2 tests passing. Telemetry deferred.
