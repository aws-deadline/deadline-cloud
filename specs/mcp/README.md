# deadline-mcp Crate Specifications

MCP (Model Context Protocol) server for AI agent integration. Library crate
invoked by `deadline-cli` via `deadline mcp-server`.

Consumers: `deadline-cli`.

Dependencies: `rmcp` (MCP SDK), `deadline-config`, `deadline-client`,
`deadline-job-bundle`, `deadline-job-attachments`, `deadline-common`,
`deadline-models`.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, tool surface, design decisions |

## Status

Deferred — ships after core CLI commands are complete.
