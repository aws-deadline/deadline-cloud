# deadline-test-server Crate Specifications

Test-only crate. Wiremock-based fake AWS server and `TestHarness` for CLI
subprocess tests. Dev-dependency of `deadline-cli`.

Each `TestHarness::new()` starts a fresh wiremock server and creates an
isolated temp directory for config files. Environment variables point the
CLI at the stub server. No shared state between tests — every test gets
its own server instance and config directory.

## Document Index

| Document | Description |
|----------|-------------|
| [architecture.md](architecture.md) | Module layout, test harness design, mock API surface |
| [python-ffi-testing.md](python-ffi-testing.md) | How Python GUI tests use the same stub server as Rust tests |

## Status

Fully implemented. Grows as new CLI commands are added.
