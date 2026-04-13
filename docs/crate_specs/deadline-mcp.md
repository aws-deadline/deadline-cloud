# deadline-mcp

MCP (Model Context Protocol) server for AI agent integration with
Deadline Cloud.

## Role in the System

Library crate invoked by `deadline-cli` via the `deadline mcp-server`
subcommand. Implements a stdio-based MCP server that exposes Deadline
Cloud operations as tools for AI agents. Uses the official Rust MCP SDK
(`rmcp`).

Consumers: `deadline-cli` (subcommand dispatch), AI agents (via MCP
protocol).

## Key Concepts

**MCP is a standard protocol for AI tool use.** The server communicates
over stdin/stdout using JSON-RPC. AI agents discover available tools,
call them with parameters, and receive structured results. The server is
stateless between tool calls — each invocation is independent.

**Tools map directly to Deadline Cloud operations.** Each tool wraps a
`deadline-client` API call or a composition of calls. The tool interface
is designed for AI consumption — clear parameter names, structured JSON
responses, and error messages that help the agent recover.

**Telemetry tracks tool usage.** Each invocation records latency and
success/failure events, enabling monitoring of how agents interact with
Deadline Cloud.

## Behavior & Contracts

**Transport:** stdio (stdin for requests, stdout for responses). The CLI
subcommand connects the MCP server to the process's stdio streams.

**Tool discovery:** Agents call `tools/list` to get available tools with
their parameter schemas. Tools are registered at server startup.

**Error handling:** Tool failures return structured error responses (not
process crashes). The agent receives an error message and can retry or
try a different approach.

## Design Decisions

**Library crate, not a separate binary.** The MCP server shares
credentials, config, and API client infrastructure with the CLI. Making
it a library called by the CLI binary avoids duplicating setup logic and
keeps the deployment simple (one binary).

**Uses `rmcp` (official Rust MCP SDK).** Rather than implementing the
JSON-RPC protocol manually, we use the maintained SDK. This ensures
protocol compliance and reduces maintenance burden as the MCP spec
evolves.

## Gotchas & Constraints

- The server runs as a long-lived process (unlike CLI commands that exit
  after one operation). Credential refresh and config changes during a
  session need consideration.

- stdout is reserved for MCP protocol messages. Any logging or diagnostic
  output must go to stderr to avoid corrupting the protocol stream.

- The tool set should be stable — AI agents may cache tool schemas.
  Adding tools is safe; removing or changing parameter schemas is a
  breaking change for agents that depend on them.

## Status & Gaps

Not yet implemented. This is deferred until core CLI commands are complete
(work item #17). The crate exists as a placeholder with the planned tool
set documented above.
