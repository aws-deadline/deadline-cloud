# Python FFI Test Infrastructure

How the Python GUI tests (`gui/tests/`) use the same stub server
infrastructure as the Rust CLI tests.

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

The FFI functions call the same Rust code as the CLI (`deadline-api`,
`deadline-config`, etc.). That code uses the AWS SDK, which reads
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

## Rust gui-ffi Tests

The existing Rust unit tests in `crates/deadline-python-bindings/src/lib.rs`
have the same problem — they call `extern "C"` functions that hit real
AWS. These should also be upgraded to use `TestHarness` in-process:

```rust
#[tokio::test]
async fn list_farms_returns_farms_array() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        json!({"farmId": "farm-abc", "displayName": "My Farm"}),
    ]).await;
    // env vars redirect FFI calls to stub
    std::env::set_var("AWS_ENDPOINT_URL_DEADLINE", harness.server_uri());
    // ...
    let json = call_ffi_json(deadline_list_farms(harness.config_path));
    assert_eq!(json["farms"][0]["farmId"], "farm-abc");
}
```

## Running All Tests

```bash
# Rust tests (CLI + library + gui-ffi)
cargo test

# Python FFI tests
PYTHONPATH=gui python3 -m pytest gui/tests/

# Everything
make test  # or: cargo test && PYTHONPATH=gui pytest gui/tests/
```

The Python tests require `cargo build -p deadline-test-server` first
(to build the `ffi-test-server` binary). The pytest fixture handles
this automatically if the binary is missing.

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

## Implementation Order

1. Add `[[bin]]` target `ffi-test-server` to `deadline-test-server`
2. Rewrite `gui/tests/conftest.py` to start the server binary
3. Rewrite `gui/tests/testdeadline._native` to assert on canned data
4. Upgrade Rust gui-ffi tests to use `TestHarness`
5. Add `make test` or equivalent to run both suites
