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

Implemented — MCP server with tool registration, telemetry, and error handling.
