# Python FFI Test Infrastructure

How the Python FFI tests (`pytests/bindings/`) and GUI accessibility tests
(`pytests/ui_accessibility/`) use stub server infrastructure for isolation.

## Context

The GUI is Python Qt (PySide6) in `gui/`. The Rust CLI spawns it as a
subprocess for `bundle gui-submit` and `config gui`. Python is involved in:
1. **DCC plugins** — Maya/Blender/etc. import `deadline.client.ui` which
   uses `deadline._native` (PyO3) for backend operations.
2. **CLI GUI commands** — The Rust binary spawns Python to show the Qt dialog.
3. **L2 accessibility tests** — Python `xa11y` library drives the GUI
   through the OS accessibility tree.

The `pytests/bindings/test_native.py` tests verify the PyO3 boundary still works
correctly for DCC plugin consumers.

## Problem

The `deadline._native` PyO3 module calls Rust functions that
make real AWS API calls (STS, Deadline, S3). Without isolation:

- Tests hit real AWS endpoints (slow, flaky, requires credentials)
- `deadline_login()` launches Deadline Cloud Monitor browser flow
- `deadline_logout()` kills the developer's active session
- Tests can't assert on specific data — only "returns something or errors"

The Rust CLI tests solved this with `TestHarness` + wiremock. The Python
tests need the same isolation.

## Architecture

```
                    ┌─────────────────────────────────┐
                    │  ffi-test-server (Rust binary)   │
                    │                                  │
                    │  1. Starts wiremock on random port│
                    │  2. Mounts canned API responses   │
                    │  3. Writes temp config file        │
                    │  4. Prints JSON to stdout:         │
                    │     {"endpoint": "http://...",     │
                    │      "config_path": "/tmp/..."}    │
                    │  5. Waits for SIGTERM              │
                    └──────────┬──────────────────────┘
                               │ stdout (JSON)
                    ┌──────────▼──────────────────────┐
                    │  conftest.py (pytest fixture)     │
                    │                                  │
                    │  1. Starts ffi-test-server binary │
                    │  2. Reads endpoint + config_path  │
                    │  3. Sets env vars:                │
                    │     AWS_ENDPOINT_URL_DEADLINE     │
                    │     AWS_ENDPOINT_URL_STS          │
                    │     AWS_ACCESS_KEY_ID             │
                    │     DEADLINE_CONFIG_FILE_PATH     │
                    │  4. Yields deadline._native instance   │
                    │  5. Kills binary on teardown      │
                    └──────────┬──────────────────────┘
                               │
                    ┌──────────▼──────────────────────┐
                    │  testdeadline._native (pytest tests)       │
                    │                                  │
                    │  ffi.list_farms()                 │
                    │    → PyO3 → Rust FFI            │
                    │      → AWS SDK reads env vars     │
                    │        → HTTP to localhost:PORT   │
                    │          → wiremock returns canned │
                    │            response               │
                    │  assert result["farms"][0]["farmId"]│
                    │    == "farm-abc123"               │
                    └──────────────────────────────────┘
```

## Why This Works

The FFI functions call the same Rust code as the CLI (`deadline-lib::api`,
`deadline-lib::config`, etc.). That code uses the AWS SDK, which reads
`AWS_ENDPOINT_URL_*` environment variables to determine where to send
requests. Setting those env vars before calling the FFI redirects all
API traffic to the local stub server.

This is the exact same mechanism `TestHarness` uses for CLI subprocess
tests — just applied across the Python/Rust boundary.

## Components

### 1. `ffi-test-server` binary

A `[[bin]]` target in the `deadline-test-server` crate. Reuses the
existing mock helpers (`farms::mock_list_farms`, `sts::mock_get_caller_identity`,
etc.) — one source of truth for canned responses.

```
crates/deadline-test-server/
├── Cargo.toml              # Add [[bin]] target
└── src/
    ├── lib.rs              # Existing library (unchanged)
    ├── harness.rs          # Existing TestHarness (unchanged)
    ├── bin/
    │   └── ffi_test_server.rs  # NEW: standalone server binary
    └── deadline_api/       # Existing mock helpers (unchanged)
```

The binary:
- Creates a `TestHarness` (wiremock + temp config)
- Mounts canned responses for all FFI-exercised endpoints
- Prints connection info as a single JSON line to stdout
- Blocks until terminated

### 2. Python `conftest.py` session fixture

A `scope="session"` fixture that:
- Builds and starts the `ffi-test-server` binary
- Reads the JSON connection info from stdout
- Sets environment variables for the process
- Provides the endpoint URL and config path to tests
- Kills the server on session teardown

Session scope means one server instance for the entire pytest run —
fast startup, no per-test overhead.

### 3. Test assertions on specific data

With a stub server, tests assert on exact canned data:

```python
# Before (hitting real AWS — vague assertions)
def test_list_farms(self, ffi):
    try:
        result = ffi.list_farms()
        assert "farms" in result
    except DeadlineOperationError:
        pass  # shrug

# After (hitting stub — precise assertions)
def test_list_farms(self, ffi):
    result = ffi.list_farms()
    assert result["farms"][0]["farmId"] == "farm-abc123"
    assert result["farms"][0]["displayName"] == "Test Farm"
```

## Rust Tests

The Rust unit and integration tests use `TestHarness` (wiremock) for
all API interactions. No special setup needed — `cargo test` runs them.

## Running All Tests

```bash
# Rust tests (CLI + library)
cargo test

# Python binding tests
python3 -m pytest pytests/bindings/

# Python GUI accessibility tests
python3 -m pytest pytests/ui_accessibility/

# Everything
make test
```

## Env Vars Set by the Test Fixture

| Variable | Value | Purpose |
|----------|-------|---------|
| `AWS_ENDPOINT_URL_DEADLINE` | `http://localhost:{port}` | Redirect Deadline API calls |
| `AWS_ENDPOINT_URL_STS` | `http://localhost:{port}` | Redirect STS calls |
| `AWS_ENDPOINT_URL_S3` | `http://localhost:{port}` | Redirect S3 calls |
| `AWS_ENDPOINT_URL_CLOUDWATCHLOGS` | `http://localhost:{port}` | Redirect CloudWatch calls |
| `AWS_ACCESS_KEY_ID` | `AKIAIOSFODNN7EXAMPLE` | Fake credentials |
| `AWS_SECRET_ACCESS_KEY` | `wJalrXUtnFEMI/...` | Fake credentials |
| `AWS_DEFAULT_REGION` | `us-west-2` | Consistent region |
| `DEADLINE_CONFIG_FILE_PATH` | `/tmp/.../config` | Isolated config |
| `HOME` | `/tmp/...` | Prevent reading `~/.aws` |

These match exactly what `TestHarness::cli()` and `TestHarness::cmd()`
set for CLI subprocess tests.

## Implementation Status

1. ~~Add `[[bin]]` target `ffi-test-server` to `deadline-test-server`~~ (eliminated — PyO3 tests use MockDeadlineBackend directly)
2. ✅ `pytests/bindings/conftest.py` starts MockDeadlineBackend in-process
3. ✅ `pytests/bindings/test_native.py` asserts on canned data
4. ✅ Rust tests use `TestHarness` (wiremock)
5. ✅ `make test` runs all suites
