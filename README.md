# deadline-cloud-rs

Rust implementation of the [AWS Deadline Cloud](https://aws.amazon.com/deadline-cloud/)
client software: CLI, GUI FFI layer, and shared library crates.

## Prerequisites

- [Rust](https://rustup.rs/) (stable, edition 2024)
- `cargo-insta` for snapshot test review: `cargo install cargo-insta`

## Build

```bash
cargo build                    # full workspace
cargo build -p deadline-cli    # just the CLI binary
```

The CLI binary is at `target/debug/deadline`.

## Test

```bash
cargo test                     # full test suite
cargo test -p deadline-cli     # CLI subprocess tests
cargo test -p deadline-config  # single crate
cargo insta review             # review new/changed output snapshots
```

See [specs/testing.md](specs/testing.md) for the test philosophy (no mocking, Level 1
vs Level 2, snapshot workflow).

## Crates

| Crate | What it does |
|-------|-------------|
| `deadline-cli` | CLI binary — argument parsing, subcommand dispatch, output formatting |
| `deadline-gui-ffi` | C ABI shared library for Python GUI and DCC plugins |
| `deadline-mcp` | MCP server for AI agent integration |
| `deadline-config` | INI config file read/write, setting resolution |
| `deadline-api` | AWS Deadline Cloud API, session, auth, telemetry |
| `deadline-job-bundle` | Job bundle parsing, template loading, parameter validation |
| `deadline-job-attachments` | Asset manifests, S3 upload/download, hash cache |
| `deadline-test-server` | Test-only wiremock stub server |

See [specs/architecture.md](specs/architecture.md) for the crate dependency graph and
data flows.

## Documentation

| Path | Contents |
|------|----------|
| [specs/](specs/) | All design specifications — per-crate architecture, CLI command docs, cross-cutting design docs |
| [specs/architecture.md](specs/architecture.md) | Crate dependency graph, data flows, shared conventions |
| [specs/testing.md](specs/testing.md) | Test philosophy, no-mocking policy, snapshot workflow |
| [specs/patterns.md](specs/patterns.md) | AWS SDK usage patterns, coding conventions |
| [specs/cli/](specs/cli/) | Per-command CLI feature documentation |
| [specs/cli/reference.md](specs/cli/reference.md) | Complete CLI command reference |

## License

Apache-2.0
