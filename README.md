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

See [TESTING.md](TESTING.md) for the test philosophy (no mocking, Level 1
vs Level 2, snapshot workflow).

## Crates

| Crate | What it does |
|-------|-------------|
| `deadline-cli` | CLI binary — argument parsing, subcommand dispatch, output formatting |
| `deadline-gui-ffi` | C ABI shared library for Python GUI and DCC plugins |
| `deadline-mcp` | MCP server for AI agent integration |
| `deadline-config` | INI config file read/write, setting resolution |
| `deadline-client` | AWS Deadline Cloud API calls |
| `deadline-job-bundle` | Job bundle parsing, template loading, parameter validation |
| `deadline-job-attachments` | Asset manifests, S3 upload/download, hash cache |
| `deadline-models` | Shared data types and error types |
| `deadline-common` | Shared utilities (path helpers, formatting) |
| `deadline-test-server` | Test-only wiremock stub server |

See [ARCHITECTURE.md](ARCHITECTURE.md) for the crate dependency graph and
data flows.

## Documentation

| Path | Contents |
|------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | Crate dependency graph, data flows, shared conventions |
| [TESTING.md](TESTING.md) | Test philosophy, no-mocking policy, snapshot workflow |
| [PATTERNS.md](PATTERNS.md) | AWS SDK usage patterns, coding conventions |
| [docs/crate_specs/](docs/crate_specs/) | One file per crate — describes current behavior and design |
| [docs/design_docs/](docs/design_docs/) | Technical design documents, feature designs, decision records |

## License

Apache-2.0
