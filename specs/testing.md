# Testing Framework

> Guidelines for writing tests in the `deadline-cloud-rs` workspace.
## Workflow: Red → Green → Refactor

1. **Red.** Write tests that assert on observable behavior. Run them. They
   must fail — if they pass, the test isn't testing anything new.
2. **Green.** Write the minimum implementation to make the tests pass.
3. **Refactor.** Clean up the implementation. Tests must still pass.

### Red phase: stub requirements

Implementation stubs must use `todo!()` so tests fail immediately with
a clear signal that the code hasn't been written yet. Tests themselves
must be complete — full setup, real assertions, real expected values.
When the real implementation replaces the `todo!()`, tests pass or fail
based on whether the behavior is correct.

**Bug-driven updates:** Never fix a bug without a failing test first. If an
existing test should have caught it, strengthen that test. If no test covers
the code path, write a new one.
## No Mocking

No mocking libraries (`mockall`, etc.) or hand-rolled mock objects.

| Instead of... | We use... |
|---|---|
| Mocking AWS API calls | Local `wiremock` stub server returning real response JSON |
| Mocking the filesystem | Real temp directories (`tempfile::TempDir`) |
| Mocking config state | Real config files written to a temp directory |
| Mocking S3 | Local stub server speaking the S3 protocol subset we need |

If code under test writes outside a `TempDir` (e.g. to cwd), use an RAII
`CleanupDir` guard — see existing tests for the pattern.
## Test Levels

### Level 1: Direct Unit Tests

Call a public function in-process, assert on return value or error.

**When to use:**
- Behavior not reachable through any CLI command
- CLI-level testing would be imprecise for the specific assertion
- Internal optimization invisible to the CLI (caching, atomicity)
- Many edge cases where Level 1 gives faster, more precise feedback

**Pattern:**
```rust
#[test_case(0, "0 B" ; "zero bytes")]
#[test_case(1_500_000_000, "1.5 GB" ; "fractional GB")]
fn human_readable_file_size(input: u64, expected: &str) {
    assert_eq!(human_readable_file_size(input), expected);
}
```

### Level 2: CLI Subprocess + Stub Server

The compiled `deadline` binary runs as a child process against a `wiremock`
stub server. Tests assert on stdout, stderr, and exit code.

**When to use:** Everything the CLI can exercise. This is the default.

**Pattern:**
```rust
#[tokio::test]
async fn farm_list_shows_farms_in_yaml() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        json!({"farmId": "farm-aaa", "displayName": "My Farm"}),
    ]).await;
    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}
```

### Level 2: GUI Accessibility Tests (xa11y)

The Python Qt GUI subprocess runs against a `MockDeadlineBackend` HTTP
server. Tests drive the real GUI through the accessibility tree using
`xa11y` and assert on widget state, process output, and side effects.

Same principle as CLI Level 2: invoke the top-level interface (GUI
instead of CLI), mock the backend, validate behavior from the outside.

**When to use:** GUI behavior that can't be tested through the CLI —
dialog layout, widget interactions, submission flows, JSON output.

**Location:** `pytests/ui_accessibility/`

**Infrastructure:**
- Session-scoped `MockDeadlineBackend` HTTP server (same mock as CLI tests)
- `sitecustomize.py` shim injected via `PYTHONPATH`:
  - SIGTERM handler → `QApplication.quit()` for clean process shutdown
  - QTimer pulse (100ms) so Python signal handlers fire inside Qt's C++ loop
- `PYTHONUNBUFFERED=1` ensures stdout flushes before exit
- `AWS_ENDPOINT_URL_DEADLINE` points at mock (not `AWS_ENDPOINT_URL` —
  STS calls use the cached account ID, not the mock)

**Constraints:**
- Requires real display (no headless mode on macOS)
- Don't interact with the screen during test runs
- Tests run sequentially (one GUI process at a time)

**Pattern:**
```python
with SubmitterDialog.open(bundle_dir, env=submitter_env) as app:
    app.wait_farm_resolved()
    app.submit_and_ok()
    # close() sends SIGTERM → QApplication.quit() → exec() returns → print(result)
```

### Choosing the Right Level

Test at the highest interface that reliably exercises the behavior.

1. CLI can exercise it → Level 2 CLI. (output, exit code, file side effects)
2. GUI behavior (dialogs, widgets, signals) → Level 2 GUI. (accessibility-driven)
3. Neither CLI nor GUI can reach it → Level 1, at the highest consumer-facing entry point. (PyO3 bindings, library-only APIs)
4. Higher entry point can't reliably assert it → test the function directly. (internal caching, atomicity, error variants swallowed by callers)

Repeat rule 4 recursively: only drop to a lower function when its
behavior can't be verified through a caller above it.

### When Both Levels Add Value

Keep Level 1 alongside Level 2 when:
- Level 1 asserts on precision the CLI can't expose (exact error message)
- Level 1 is significantly faster for combinatorial edge cases
- The function is a shared foundation used by 3+ modules

**Remove** a Level 1 test only when a Level 2 snapshot asserts on the
exact same output with the same precision.
## Snapshots (`insta` + `insta-cmd`)

Level 2 tests use `assert_cmd_snapshot!` to assert on **exact** stdout and
stderr. Any output change — missing field, reordered lines, changed
wording — is caught automatically.

**`harness.cmd()`** → `std::process::Command` for snapshot tests.
**`harness.cli()`** → `assert_cmd::Command` for `.assert()` chains and
file side-effect checks.

**Workflow:** write test → `cargo test` (fails, writes `.snap.new`) →
`cargo insta review` (review output) → accept → commit `.snap` file.

**Filters** for non-deterministic content:
```rust
let mut settings = insta::Settings::clone_current();
settings.add_filter(r"createdAt: .*", "createdAt: [TIMESTAMP]");
let _guard = settings.bind_to_scope();
assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-abc"]));
```

### When NOT to use snapshots

| Test type | Tool |
|-----------|------|
| CLI JSON output | `serde_json::from_str` + field assertions |
| Telemetry verification | `.assert().success()` |
| Config file side effects | `assert_eq!` on file content |
| API request validation | wiremock request matchers |
| Unit test return values | `assert_eq!` |

## GUI Testing

The GUI (`gui/`) uses Python Qt (PySide6). Testing uses accessibility
tree tests that drive the GUI through the OS accessibility API.

### Level 2: Accessibility Tree Tests (`xa11y` + subprocess)

Launch the Python Qt GUI as a subprocess, then drive it through the OS
accessibility API using `xa11y` (Python). Qt exposes all widgets to the
platform accessibility layer (AT-SPI on Linux, Accessibility API on
macOS, UIA on Windows). `xa11y` queries this tree by role and name.

**What to test at L2:**
- Dialog opens with correct title
- All settings groups present
- Settings round-trip: `deadline config set` → open GUI → verify value
- Ok/Cancel/Apply button semantics
- Host requirements form controls
- Job submission flow

**Infrastructure:** `pytests/ui_accessibility/conftest.py` (mock backend + isolated env),
`pytests/ui_accessibility/helpers.py` (page objects wrapping `xa11y.App`).

### Choosing the Right Level

| What | Level | Location |
|------|-------|----------|
| Dialog behavior (full stack) | L2 | `pytests/ui_accessibility/` (Python + xa11y) |
| PyO3 bindings (DCC compat) | L1 | `pytests/bindings/test_native.py` |


## Manual CLI Comparison Testing

Stub-server tests verify behavior against canned responses. Manual
comparison against the real API catches differences that stubs miss.

`test_fixtures/job_bundles/` contains sample job bundles for comparing
the Python (`deadline`) and Rust (`./target/debug/deadline`) CLIs:

| Bundle | Use Case |
|--------|----------|
| `simple_job` | `bundle submit --dry-run`, parameter validation |
| `cli_job` | Attachment upload/download with INOUT PATH parameter |
| `job_attachments_devguide_output` | Output download, `queue sync-output` |

```bash
diff <(deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1) \
     <(./target/debug/deadline bundle submit test_fixtures/job_bundles/simple_job --dry-run --yes 2>&1)
```

See `test_fixtures/README.md` for more examples.


## Lint Conventions in Tests

The workspace enables strict clippy lints including `unwrap_used` and
`expect_used` (see `clippy.toml` and `Cargo.toml`). The `clippy.toml`
setting `allow-unwrap-in-tests = true` covers `#[test]` functions and
`#[cfg(test)]` modules, but **not** helper functions in integration test
crates (`tests/` directories). Clippy treats those helpers as regular
functions since they lack a `#[test]` attribute.

To avoid false positives, each integration test entry file has a
crate-level allow:

```rust
#![allow(clippy::unwrap_used, clippy::expect_used)]
```

This applies to:
- `crates/deadline-cli/tests/cli.rs`
- `crates/deadline-lib/tests/attachments.rs`
- `crates/deadline-lib/tests/bundle.rs`

When adding a new integration test crate, include the same allow at the
top of its entry file.


## Continuous Integration

Four GitHub Actions workflows enforce quality. All live in
`.github/workflows/`.

### `ci.yml` — Core Rust gate (PR + mainline)

**Triggers:** Every push to `mainline`, every PR targeting `mainline`,
`release`, `patch_*`, `feature_*`.

| Job | Runs on | What it does |
|-----|---------|-------------|
| Rustfmt | ubuntu | `cargo fmt --all -- --check` |
| cargo-deny | ubuntu | License, advisory, ban, source checks |
| Build & Test | ubuntu, macOS, Windows (matrix) | Build, clippy `-D warnings`, `cargo test --workspace` |
| Documentation | ubuntu | `cargo doc --no-deps --workspace -D warnings` |

**Key infrastructure:**
- No `rust-toolchain.toml` — CI uses latest stable Rust
- `*.localhost` host entries on macOS/Windows (AWS SDK host prefix resolution)
- Cargo cache keyed on `(os, rustc-hash, Cargo.lock-hash)` with stale eviction
- `concurrency` cancels in-progress PR runs; never cancels mainline
- `fail-fast: false` — all 3 OSes complete even if one fails

### `conformance.yml` — Python CLI parity (nightly + PR)

**Triggers:** Nightly 07:00 UTC, PRs touching `crates/`, `conformance/`,
`Cargo.toml`, `Cargo.lock`, and manual dispatch.

Replays `deadline-cloud-python`'s `test/cli_e2e/` test suite against the
Rust `deadline` binary. Catches parity drift — behavior Python
added/changed that Rust hasn't ported.

**How it works:**
1. Checks out both repos (Rust + latest Python mainline)
2. Builds the Rust binary
3. Installs Python test deps from the Python repo
4. Runs `pytest test/cli_e2e/` with a conftest plugin that:
   - Rewrites `AWS_ENDPOINT_URL_DEADLINE` → localhost stub
   - Points `DEADLINE_CLI_PATH` at the Rust binary
5. Uses an xfail allowlist (`conformance/xfail.txt`) for known gaps

### `python.yml` — Bindings + GUI accessibility (PR)

**Triggers:** PRs touching `crates/deadline-python-bindings/`, `gui/`,
`pytests/`, `pyproject.toml`, `Cargo.toml`, `Cargo.lock`.

Two job groups:

**Bindings tests** (all 3 OSes): Builds the PyO3 extension module via
maturin, runs `pytests/bindings/` — verifies `deadline._native` still
works for DCC plugin consumers.

**GUI xa11y tests** (all 3 OSes): Drives the real Qt GUI through the OS
accessibility tree using `xa11y`. Platform-specific setup:

| OS | Setup | Notes |
|----|-------|-------|
| Linux | Xvfb + dbus + AT-SPI2 | Virtual display, accessibility bus |
| macOS | TCC accessibility permission grant | `tccutil` before test run |
| Windows | No extra setup | UIA works natively |

**xa11y test infrastructure (`pytests/ui_accessibility/`):**

- `conftest.py` starts `MockDeadlineBackend` (Python HTTP server, 1,273
  lines in `pytests/_common/mock_deadline_backend.py`) — a stateful
  in-memory Deadline Cloud simulator that validates requests against the
  botocore service model
- `helpers.py` provides `SubmitterDialog` page object wrapping `xa11y.App`
- `sitecustomize.py` (injected via `PYTHONPATH`):
  - Installs SIGTERM handler → `QApplication.quit()` for clean shutdown
  - QTimer pulse (100ms) so Python signal handlers fire inside Qt's C++ loop
- `PYTHONUNBUFFERED=1` ensures stdout flushes before exit
- `AWS_ENDPOINT_URL_DEADLINE` points at the mock (not `AWS_ENDPOINT_URL` —
  STS calls use the cached account ID, not the mock)

### `gui-drift.yml` — GUI test parity tracker (nightly)

**Triggers:** Nightly 07:30 UTC (after conformance), manual dispatch.

Clones `deadline-cloud-python`, extracts `test/ui/` test function names,
compares against our `pytests/ui_accessibility/` test function names.
Fails if the Python repo has tests we haven't ported — surfaces new GUI
tests without executing them.

### Mock Backends

The repo uses two separate mock backends:

| Backend | Language | Used by | Location |
|---------|----------|---------|----------|
| `deadline-test-server` | Rust (wiremock) | Rust CLI L2 tests, PyO3 binding tests | `crates/deadline-test-server/` |
| `MockDeadlineBackend` | Python (http.server) | xa11y GUI tests | `pytests/_common/mock_deadline_backend.py` |

**Why two?** The Rust wiremock server is static (canned responses, no
state). The Python mock is stateful — it supports create/list/get
workflows that the GUI exercises across multiple API calls in a single
test (e.g., create farm → list farms → select → submit). It also
validates request shapes against the botocore service model.

Both mock the same Deadline Cloud API surface. The Python mock additionally
mocks STS `GetCallerIdentity` and the `AssumeQueueRoleForUser` credential
exchange. See `specs/deadline-test-server/` for the Rust server docs.


## Build & Test Performance

Benchmarked 2026-05-21 on macOS arm64 (M-series).

### Build times

| Scenario | Time | Notes |
|----------|------|-------|
| Fresh (`cargo clean && cargo build`) | ~3.5 min | AWS SDK crates dominate (~90s) |
| Incremental (touch deadline-lib) | ~2s | Fast — pure Rust |
| Incremental (touch deadline-cli) | ~5s | Relinking large binary |
| No changes | ~1s | Fully cached |

### Test times

| Scenario | Time | Notes |
|----------|------|-------|
| Full suite (`cargo test`) | ~75s | 1,372 tests |
| CLI tests only (`-p deadline-cli`) | ~53s | 441 subprocess tests |
| Lib tests only (`-p deadline-lib`) | ~7s | 140 integration tests |

### Why `cargo-nextest` doesn't help here

Tested: `cargo nextest run` (62s for CLI) vs `cargo test` (53s for CLI).
Nextest is slower because:

- Our CLI tests use the **single-binary pattern** (one `cli.rs` entry
  point with `mod` submodules) — no redundant linking.
- Each test spawns a subprocess + wiremock server. Nextest adds per-test
  process isolation overhead on top of that.
- The tests are I/O-bound (port binding, process spawn), not CPU-bound.
  Parallelism doesn't help when the bottleneck is I/O contention.

### Recommended workflow

- `cargo check` — fastest feedback for type errors (~2s)
- `cargo test -p deadline-cli -- config` — targeted CLI tests (~5s)
- `cargo test` — full suite before committing (~75s)
- Avoid `cargo clean` — incremental builds are 100x faster than fresh
