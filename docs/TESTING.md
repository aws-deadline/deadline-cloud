# Testing Framework

> Guidelines for writing tests in the `deadline-cloud-rs` workspace.

---

## Philosophy

The default test approach is to run the compiled CLI binary as a subprocess,
pointed at a local stub server, and assert on its observable behavior (stdout,
stderr, exit code). Direct unit tests exist only for code that the CLI cannot
reach or where CLI-level testing would be imprecise.

**Three rules:**
1. If the CLI can exercise it, test it through the CLI.
2. If the CLI can't reach it, test the public function directly.
3. No traditional mocking. Ever.

The goal is to maximize coverage through the highest-level interface available.
Tests that go through the CLI survive internal refactors — if you rename a
helper function, restructure modules, or change how data flows internally,
CLI tests keep passing as long as the observable behavior is the same. Tests
that call internal helpers break on every refactor, even when nothing the user
sees has changed. Prefer one CLI test that exercises a code path end-to-end
over five unit tests that each test a helper in isolation.

---

## Workflow: Red → Green → Refactor

All new features follow test-driven development:

1. **Red.** Write tests that assert on observable behavior (stdout, stderr,
   exit code, file contents). Run them. They must fail — if they pass, the
   test isn't testing anything new.
2. **Green.** Write the minimum implementation to make the tests pass.
3. **Refactor.** Clean up the implementation. Tests must still pass.

Tests describe *what the system does*, never *how it does it*. A test that
breaks when you rename an internal function was written at the wrong level.

---

## Scenario Coverage

For each interface (CLI command or public function), systematically consider
these categories. Skip any that don't apply.

| Category | What to test |
|----------|-------------|
| Happy path | Valid inputs producing expected output |
| Missing/invalid args | Omit required args, wrong types, malformed values |
| Boundary values | Empty strings, zero, max lengths, empty collections |
| Error handling | Expected errors, error messages, exit codes |
| Auth/credential states | No creds, expired creds, wrong permissions |
| Config interaction | How configuration settings alter behavior |
| Pagination/batching | Large result sets, partial pages, empty pages |
| Interactive vs scripted | Prompts, `--yes` flags, piped input |
| Output formats | JSON, table, human-readable; stdout vs stderr |
| Cross-resource references | Referencing nonexistent or mismatched resources |
| Concurrency/cancellation | Parallel operations, interrupted transfers, timeouts |

This is a checklist, not a quota. A simple getter might only need happy path
and error handling. A complex submission command might hit all eleven.

---

## Behavioral Ambiguity

When the reference implementation does something that seems wrong, inconsistent,
or surprising, still write the test case but flag it:

```rust
// clear_setting writes default back rather than removing key
// ⚠️ Intentional per data_flow.md observation #6
```

This keeps the decision visible so future readers know it was deliberate
rather than accidental.

---

## No Mocking

This codebase does not use mocking libraries (`mockall`, `mock_derive`, etc.)
or hand-rolled mock objects that record calls and return canned values in-process.

**Why:** Mocks test interaction ("was this function called with these args?").
We test behavior ("given this input, does the system produce this output?").
Mocks couple tests to implementation details. When you refactor internals,
mock-based tests break even though behavior is unchanged.

**What we use instead:**

| Instead of... | We use... |
|---|---|
| Mocking AWS API calls in-process | A local HTTP stub server (`wiremock`) returning real response JSON |
| Mocking the filesystem | Real temp directories (`tempfile::TempDir`) |
| Mocking config state | Real config files written to a temp directory |
| Mocking S3 | A local HTTP stub server that speaks the S3 protocol subset we need |

A stub server is a lightweight, working alternative implementation that returns
canned responses. A mock is a recording device that asserts on how it was
called. Stubs test that the system works. Mocks test that the system calls
things in the expected order. We want the former.

**One exception:** If a future dependency is genuinely impossible to stub
(e.g., hardware interaction, OS kernel behavior), document why mocking is
necessary in the test file and keep the mock surface as small as possible.

---

## Test Levels

### Level 1: Direct Unit Tests

Call a public function in-process, assert on return value or error.

**When to use — ALL of these must be true:**
- The behavior is not reachable through any CLI command, OR
- CLI-level testing would be imprecise (e.g., testing that a function returns
  exactly `"1.5 GB"` vs. parsing it out of a formatted table), OR
- The behavior is an internal optimization invisible to the CLI (caching,
  atomicity, file permissions)
- AND the function is a high-use helper whose reliability must be independently
  confirmed

**Examples:**
- `human_readable_file_size(1_500_000_000)` → `"1.5 GB"`
- Error type construction and display messages
- `str2bool("yes")` → `true`
- Atomic file write + permission checks
- Manifest format parsing/decoding
- Hash computation correctness
- Parameter validation edge cases where the exact error message matters

**Pattern:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    #[test_case(0, "0 B" ; "zero bytes")]
    #[test_case(999, "999 B" ; "sub-kilobyte")]
    #[test_case(1000, "1.0 KB" ; "exactly one KB")]
    #[test_case(1_500_000_000, "1.5 GB" ; "fractional GB")]
    fn human_readable_file_size(input: u64, expected: &str) {
        assert_eq!(human_readable_file_size(input), expected);
    }
}
```

### Level 2: CLI Subprocess + Stub Server

The compiled `deadline` binary runs as a real child process. A `wiremock`
HTTP stub server runs in the test process, simulating the Deadline Cloud
and S3 APIs. The CLI is configured via environment variables to use the
stub server. Tests assert on stdout, stderr, and exit code.

**When to use:** Everything that the CLI can exercise. This is the default.

**What this tests end-to-end:**
- Argument parsing (clap)
- Config file reading/writing (real files in a temp directory)
- AWS SDK request serialization and signing
- HTTP request/response handling
- Response deserialization and business logic
- Output formatting (table, JSON, YAML)
- Error messages and exit codes
- Environment variable handling
- Interactive vs. non-interactive behavior (`--yes` flags)

**Pattern:**
```rust
use assert_cmd::Command;
use predicates::prelude::*;
use wiremock::{MockServer, Mock, ResponseTemplate};
use wiremock::matchers::{method, header};
use tempfile::TempDir;

#[tokio::test]
async fn farm_list_shows_farms_in_table() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(header("x-amz-target", "Deadline.ListFarms"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "farms": [{
                "farmId": "farm-0123456789abcdef0123456789abcdef",
                "displayName": "My Farm"
            }]
        })))
        .mount(&server)
        .await;

    let config_dir = TempDir::new().unwrap();
    let config_path = config_dir.path().join("config");
    std::fs::write(&config_path, "").unwrap();

    Command::cargo_bin("deadline").unwrap()
        .env("AWS_ENDPOINT_URL_DEADLINE", server.uri())
        .env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
        .env("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        .env("AWS_DEFAULT_REGION", "us-west-2")
        .env("DEADLINE_CONFIG_FILE_PATH", config_path.to_str().unwrap())
        .args(["farm", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("My Farm"));
}
```

---

## Stub Server Architecture

### `deadline-test-server` crate

A workspace member used only as a `[dev-dependency]`. Provides reusable
response builders for Deadline Cloud and S3 API stubs.

The server starts stateless (canned request → canned response). It can evolve
to stateful (in-memory farm/queue/job store) when multi-step test scenarios
require it.

### `TestHarness` helper

Each CLI test gets a harness that reduces boilerplate:

```rust
use deadline_test_server::TestHarness;

#[tokio::test]
async fn config_set_persists_value() {
    let harness = TestHarness::new().await;

    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    harness.cli(&["config", "get", "defaults.farm_id"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-abc"));
}
```

`TestHarness` encapsulates:
- Starting the `wiremock` stub server
- Creating an isolated temp directory for config files
- Building `assert_cmd::Command` with all env vars pre-configured
- Providing methods to mount API response stubs

---

## Decision Tree

```
Can the behavior be exercised by running `deadline <subcommand>`?
├─ YES → Level 2 (CLI subprocess + stub server)
│        Mount the appropriate API stubs, run the command, assert on output.
└─ NO
   Is it a pure function or data type with no I/O?
   ├─ YES → Level 1 (direct unit test)
   └─ NO
       Is it an internal optimization (caching, atomicity, permissions)?
       ├─ YES → Level 1 with real tempdir where filesystem behavior matters
       └─ NO
           Can you add a CLI subcommand or flag to expose it?
           ├─ YES → Do that, then Level 2
           └─ NO → Level 1 (and document why CLI can't reach it)
```

---

## Naming Convention

```
{command_or_function}_{scenario}_{expected_outcome}
```

```rust
// Level 2
fn farm_list_with_two_farms_prints_both_names() { ... }
fn config_set_invalid_setting_name_exits_with_error() { ... }

// Level 1
fn human_readable_file_size_zero_returns_zero_b() { ... }
fn str2bool_yes_returns_true() { ... }
```

---

## Test Dependencies

Workspace `Cargo.toml`:

```toml
[workspace.dependencies]
# Test dependencies
test-case = "3"
assert_cmd = "2"
predicates = "3"
tempfile = "3"
wiremock = "0.6"
tokio = { version = "1", features = ["full"] }
serde_json = "1"

# Internal test crate
deadline-test-server = { path = "crates/deadline-test-server" }
```

---

## Execution

```bash
# Full suite
cargo test

# Just CLI tests (the bulk of the suite)
cargo test -p deadline-cli

# Just unit tests for a specific crate
cargo test -p deadline-common

# Specific test
cargo test -p deadline-cli farm_list_with_valid_creds

# With output visible
cargo test -p deadline-cli -- --nocapture
```

---

## Checklist: Before Marking Tests Complete

- [ ] CLI-reachable behavior is tested through the CLI subprocess (Level 2)
- [ ] Level 1 tests exist only for code the CLI cannot reach
- [ ] Parametric tests cover all boundary values
- [ ] Error cases assert on error message content, not just exit code
- [ ] No test depends on execution order
- [ ] `cargo test -p <crate>` passes with no warnings
- [ ] Stub server responses match the real API response shape
