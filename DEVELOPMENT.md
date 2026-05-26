# Development

Developer guide for the AWS Deadline Cloud Rust client.

## Table of Contents

- [Getting Started](#getting-started)
- [Common Commands](#common-commands)
- [Workspace Structure](#workspace-structure)
- [How To...](#how-to)
  - [Add a new CLI command](#add-a-new-cli-command)
  - [Add a new API call](#add-a-new-api-call)
  - [Work on the Python GUI](#work-on-the-python-gui)
- [Coding Conventions](#coding-conventions)
- [Further Reading](#further-reading)

## Getting Started

Install prerequisites and build:

```bash
# Rust toolchain (stable channel, edition 2024)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dev tools (cargo-insta, cargo-deny, cargo-outdated, cargo-bloat)
make setup-tools

# Python venv (needed for GUI and wheel builds)
python3 -m venv .venv && source .venv/bin/activate
pip install maturin PySide6-essentials qtpy pyyaml pytest-qt

# Build everything and install into venv
make
```

After `make`, the `deadline` CLI is on PATH (in the venv) and
`deadline._native` is importable from Python.

For Rust-only work (no GUI), you can skip the venv and use
`cargo build` / `cargo test` directly.

## Common Commands

| Task | Command |
|------|---------|
| Build all + install into venv | `make` |
| Build Rust only | `cargo build` |
| Build release wheel | `make wheel` |
| Run all tests (Rust + Python) | `make test` (auto-detects `.venv/`) |
| Run Rust tests only | `make test-rust` (no venv needed) |
| Run xa11y UI tests | `make test-ui` |
| Run PyO3 binding tests | `make test-bindings` |
| Run single crate tests | `cargo test -p deadline-lib` |
| Review snapshot changes | `cargo insta review` |
| Lint | `make lint` |
| Format code | `cargo fmt` |
| Check formatting | `cargo fmt --check` |
| Audit dependencies | `cargo deny check` |
| Find outdated deps | `cargo outdated -R` |
| Analyze binary size | `cargo bloat --release -p deadline-cli --crates` |
| Clean all artifacts | `make clean` |

The CLI binary is at `target/debug/deadline` (or on PATH after `make`).

## Workspace Structure

```
deadline-cloud-rs/
├── crates/
│   ├── deadline-cli/                # Binary — CLI commands, output formatting
│   ├── deadline-lib/                # Unified library: config, API, bundles, attachments
│   │   └── src/
│   │       ├── api/                 # AWS API calls, auth, session, telemetry
│   │       ├── attachments/         # S3 transfer, manifests, download/upload orchestration
│   │       ├── bundle/              # Job bundle parsing, validation, submission
│   │       └── config/              # INI config read/write, setting resolution
│   ├── deadline-python-bindings/    # PyO3 module (deadline._native)
│   └── deadline-test-server/        # Test-only wiremock stub server
├── gui/                             # Python Qt GUI (PySide6/qtpy)
├── deadline.data/                   # Build artifact: CLI binary for wheel (gitignored)
├── specs/                           # Design specifications
├── test_fixtures/                   # Sample job bundles for testing
├── Makefile                         # Build orchestration (make, make wheel, make test)
├── pyproject.toml                   # Maturin config for Python wheel builds
├── AGENTS.md                        # AI agent instructions
├── CONTRIBUTING.md                  # Contribution guidelines
└── DEVELOPMENT.md                   # This file
```

### Crate responsibilities

| Crate | What it does |
|-------|-------------|
| `deadline-cli` | Binary. Clap argument parsing, subcommand dispatch, output formatting. No business logic. |
| `deadline-lib` | Unified library: config (INI I/O), API (AWS SDK calls, auth, session), bundles (parsing, submission), attachments (S3 transfer, manifests). |
| `deadline-python-bindings` | PyO3 extension module exposing Rust functions to Python for the GUI. |
| `deadline-test-server` | Test-only. Wiremock stub server and `TestHarness` for CLI subprocess tests. |

### Dependency graph

```
deadline-cli
├── deadline-lib
├── openjd-snapshots (for types in queue sync-output)
└── deadline-test-server (dev-dependency)

deadline-python-bindings
├── deadline-lib
└── pyo3, pythonize

deadline-lib
├── openjd-snapshots (hashing, manifests, upload/download engine)
├── openjd-expr (path mapping)
├── aws-sdk-deadline, aws-sdk-s3, aws-sdk-sts
└── (no dependency on CLI or bindings)
```

## How To...

### Add a new CLI command

1. Create a new file in `crates/deadline-cli/src/commands/` (or add to
   an existing command group).
2. Define the clap structs for arguments and subcommands.
3. Implement the handler by calling functions from `deadline-lib`
   modules (`deadline_lib::api`, `deadline_lib::bundle`, etc.). The CLI
   crate contains no business logic — only argument parsing and output
   formatting.
4. Register the command in `crates/deadline-cli/src/main.rs`.
5. Write Level 2 tests in `crates/deadline-cli/tests/cli/`. Use
   `TestHarness` to start a stub server and run the binary as a
   subprocess. Use `assert_cmd_snapshot!` for output assertions.
6. Run tests and review snapshots.

Look at `crates/deadline-cli/src/commands/farm.rs` for the pattern.

### Add a new API call

Callers own their SDK calls — there are no thin wrapper functions.
Call the SDK fluent builder directly at the call site:

```rust
let client = deadline_lib::api::session::deadline_client(config).await;
let output = client.get_farm().farm_id(id).send().await
    .map_err(|e| deadline_lib::api::api::format_sdk_error(&e))?;
```

For paginated list operations, use the SDK paginator with
`collect_paginated`:

```rust
let client = deadline_lib::api::session::deadline_client(config).await;
let pages = deadline_lib::api::client::collect_paginated(
    client.list_steps().farm_id(f).queue_id(q).job_id(j)
        .into_paginator().send()
).await?;
for page in &pages {
    for step in page.steps() { /* typed StepSummary access */ }
}
```

For error handling, always use `format_sdk_error` or `sdk_err` — never
`format!("{e}")` on an `SdkError`, which produces the useless string
`"service error"`.

Add mock responses in `crates/deadline-test-server/src/deadline_api/`.

### Work on the Python GUI

The architecture is: Rust binary → spawns Python subprocess → Python
shows Qt dialog → Python calls back into Rust via `deadline._native`
(PyO3).

Key directories:

- `gui/deadline/client/ui/` — Qt widgets and dialogs
- `gui/deadline/client/config/` — config shim routing through `_native`
- `crates/deadline-python-bindings/` — the PyO3 module source

Python discovery order: `DEADLINE_PYTHON` env var → `_internal/Python`
relative to binary → `python3` on PATH → `python` on PATH.

## Testing

### Philosophy

Tests follow three rules:

1. **If the CLI can exercise it, test it through the CLI.** Level 2
   tests run the compiled binary as a subprocess against a local
   wiremock stub server and assert on stdout/stderr/exit code.
2. **If the CLI can't reach it, test the public function directly.**
   Level 1 unit tests for library code or precision edge cases.
3. **No traditional mocking.** No `mockall` or hand-rolled mocks. Use
   wiremock for API calls, real temp directories for filesystem
   operations, and real config files for config tests.

### UI accessibility tests (`test/ui/`)

End-to-end GUI tests using [xa11y](https://lib.rs/crates/xa11y-macos).
Launches the real binary against `MockDeadlineBackend` and drives it
via the OS accessibility tree.

```bash
pip install botocore xa11y
cargo build && pytest test/ui/ -v --tb=short
```

Requires macOS Accessibility permission for your terminal app
(System Settings → Privacy & Security → Accessibility).
On Linux: `apt install at-spi2-core xvfb && xvfb-run pytest test/ui/ -v`.

### Snapshots

CLI output tests use `insta` snapshots. When you change CLI output, new
snapshots appear as `.snap.new` files after running tests. Use
`cargo insta review` to inspect each change, then accept or fix.

### Test naming

```
{command_or_function}_{scenario}_{expected_outcome}
```

See [specs/testing.md](specs/testing.md) for the full testing guide.

## Coding Conventions

- **Comments explain *what* and *why***, not Rust language concepts.
- **Error formatting:** Always use `format_sdk_error` / `sdk_err` for
  AWS SDK errors. Never `format!("{e}")` on `SdkError`.
- **API calls use typed SDK output.** Callers call the SDK fluent
  builder directly and access fields via typed accessors. No wrapper
  functions, no `serde_json::Value` from API responses. See
  [specs/patterns.md](specs/patterns.md) § "AWS SDK for Rust Usage".
- **Config is threaded, not global.** Functions take `&IniConfig` for
  reads or `&mut IniConfig` for writes.
- **No mocking.** Use wiremock, real temp dirs, real config files.
- **Snapshot tests for CLI output.** Use `assert_cmd_snapshot!` for
  happy-path CLI tests. Use `assert_eq!` for JSON, config side effects,
  and unit test return values.
- **Credential scoping:** Non-Deadline AWS clients (CloudWatch, S3)
  must use queue-scoped or fleet-scoped credentials when the user is
  logged in via Deadline Cloud Monitor. See
  [specs/patterns.md](specs/patterns.md) § "Credential Scoping".

## Further Reading

| Document | What it covers |
|----------|---------------|
| [specs/architecture.md](specs/architecture.md) | Crate dependency graph, data flows, packaging |
| [specs/patterns.md](specs/patterns.md) | AWS SDK patterns, error formatting, serialization conventions |
| [specs/testing.md](specs/testing.md) | Full testing guide: levels, snapshots, mock response rules |
| [specs/deadline-cli/reference.md](specs/deadline-cli/reference.md) | Complete CLI command reference |
| [specs/progress.md](specs/progress.md) | Work item tracking |
