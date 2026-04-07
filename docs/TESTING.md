# Testing Framework

> Guidelines for writing tests in the `deadline-cloud-rs` workspace.

---

## Philosophy

The default test approach is to run the compiled CLI binary as a subprocess,
pointed at a local stub server, and assert on its observable behavior (stdout,
stderr, exit code). Direct unit tests supplement CLI tests for precision on
edge cases, and are the primary approach for library code not yet reachable
through a CLI command.

**Three rules:**
1. If the CLI can exercise it, test it through the CLI.
2. If the CLI can't reach it, or CLI-level testing lacks precision, test the
   public function directly.
3. No traditional mocking. Ever.

The goal is to maximize coverage through the highest-level interface available.
Tests that go through the CLI survive internal refactors — if you rename a
helper function, restructure modules, or change how data flows internally,
CLI tests keep passing as long as the observable behavior is the same.

Level 1 and Level 2 tests can coexist on the same code path when they test
different things. A Level 2 test catches integration issues (argument parsing,
config loading, output formatting). A Level 1 test on the same function
catches precise error messages or edge cases that the CLI output doesn't
distinguish. See "When Both Levels Add Value" below for guidance.

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

**When to use:**
- The behavior is not reachable through any CLI command (e.g., library crate
  whose CLI consumer doesn't exist yet), OR
- CLI-level testing would be imprecise (e.g., testing that a function returns
  exactly `"1.5 GB"` vs. parsing it out of a formatted table), OR
- The behavior is an internal optimization invisible to the CLI (caching,
  atomicity, file permissions), OR
- The function has many edge cases where Level 1 gives faster, more precise
  feedback than Level 2 (e.g., 48 parameter validation variants)

Level 1 tests may coexist with Level 2 tests on the same code path.
See "When Both Levels Add Value" below.

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
use deadline_test_server::TestHarness;
use insta_cmd::assert_cmd_snapshot;

#[tokio::test]
async fn farm_list_shows_farms_in_yaml() {
    let harness = TestHarness::new().await;

    farms::mock_list_farms(&harness.server, &[
        serde_json::json!({
            "farmId": "farm-0123456789abcdef0123456789abcdef",
            "displayName": "My Farm",
        })
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
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

    let output = harness.cli(&["config", "get", "defaults.farm_id"])
        .output()
        .expect("failed to run");

    assert!(output.status.success());
    assert_eq!(String::from_utf8_lossy(&output.stdout), "farm-abc\n");
}
```

`TestHarness` encapsulates:
- Starting the `wiremock` stub server
- Creating an isolated temp directory for config files
- `cli()` → `assert_cmd::Command` for `.assert()` chains and file side effects
- `cmd()` → `std::process::Command` for `insta_cmd::assert_cmd_snapshot!`
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

## When Both Testing Levels Add Value

The decision tree above picks the *default* level for a new test. But some
behaviors benefit from tests at both levels. Keep a Level 1 test alongside
a Level 2 test when:

- **The Level 1 test asserts on precision the CLI can't expose.** Example:
  `validate_job_parameter_value` with INT param and float 3.7 — the Level 1
  test asserts the error contains "not an integer". A Level 2 `bundle submit`
  test only sees a generic error exit. If the error message regresses, only
  the Level 1 test catches it.

- **The Level 1 test is significantly faster for a hot development loop.**
  Level 1 tests run in-process with no subprocess spawn. For validation
  functions with many edge cases (48 parameter validation variants), Level 1
  gives sub-millisecond feedback. The Level 2 test covers the integration
  path; the Level 1 tests cover the combinatorial space.

**Remove** a Level 1 test only when the Level 2 test asserts on the exact
same observable output with the same precision. If the CLI snapshot captures
the full error message verbatim, the Level 1 test for that error is
redundant.

**Library-first development:** When implementing library crates before their
CLI consumers exist (e.g., `deadline-job-bundle` before `bundle submit`),
write Level 1 tests first. When the CLI command lands, add Level 2 tests
for CLI-reachable paths. Audit the Level 1 tests and remove only those that
are fully subsumed.

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
insta = { version = "1", features = ["filters"] }
insta-cmd = "0.6"

# Internal test crate
deadline-test-server = { path = "crates/deadline-test-server" }
```

Install `cargo-insta` for the snapshot review TUI: `cargo install cargo-insta`

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

# Review new/changed CLI output snapshots
cargo insta review

# Auto-accept all new snapshots (use with caution)
INSTA_UPDATE=always cargo test -p deadline-cli
```

---

## Checklist: Before Marking Tests Complete

- [ ] CLI-reachable behavior is tested through the CLI subprocess (Level 2)
- [ ] Level 1 tests cover precision or edge cases that Level 2 can't distinguish
- [ ] Level 1 tests that are fully subsumed by a Level 2 snapshot are removed
- [ ] Happy-path CLI tests assert on **exact stdout** (not `contains`)
- [ ] Error CLI tests assert on **exact stdout** including suggestion text
- [ ] Mock responses include **all fields** the real API returns
- [ ] Parametric tests cover all boundary values
- [ ] Error cases assert on error message content, not just exit code
- [ ] No test depends on execution order
- [ ] `cargo test -p <crate>` passes with no warnings
- [ ] Stub server responses match the real API response shape

---

## CLI Output Assertions: Exact Match, Not Substring

Level 2 CLI tests must assert on the **exact stdout and stderr content**, not
just that a substring is present. Substring checks (`contains`) miss:

- Missing fields (test passes even if half the output is gone)
- Wrong field ordering (YAML key order matters for readability parity)
- Extra or missing whitespace, newlines, headers
- Wrong capitalization (`true` vs `True`)
- Missing count/offset headers on list commands

We use **`insta` + `insta-cmd`** for snapshot testing CLI output. This is the
industry standard for Rust CLI testing, used by `rye`, `uv`, `minijinja-cli`,
and other major projects.

### What uses snapshots vs what doesn't

Use `assert_cmd_snapshot!` when the test cares about the **exact formatted
output** the user sees. This is the default for Level 2 tests. Any change
to output format — missing field, reordered lines, changed wording, extra
whitespace — is caught automatically.

For non-deterministic content (elapsed time, timestamps), use insta filters
to redact the varying parts before the snapshot comparison.

| Test type | Tool | Why |
|-----------|------|-----|
| CLI verbose/YAML output (Level 2) | `insta-cmd` snapshot | Full output captured; any format regression caught |
| CLI JSON output | `serde_json::from_str` + field assertions | Need typed comparison; snapshots are brittle to field ordering |
| CLI error messages | `insta-cmd` snapshot | Error wording matters to users |
| Telemetry verification | `.assert().success()` | Only checks command doesn't break; no user-visible output to snapshot |
| Config file side effects | `assert_eq!` on file content | Run command, read file, verify contents |
| API request validation | wiremock request matchers | Verify CLI sends correct params (e.g. `principalId`) |
| Unit test return values (Level 1) | `assert_eq!` | Simple value-in/value-out; snapshots are overkill |
| Behavioral checks | `assert!` | Exit code, file existence |

**Rule of thumb:** if a human would notice the output changed, use a snapshot.

### How `insta-cmd` works

The `assert_cmd_snapshot!` macro runs a `Command`, captures stdout, stderr,
and exit code, then compares against a stored `.snap` file:

```rust
use insta_cmd::assert_cmd_snapshot;

#[tokio::test]
async fn farm_list_prints_farms_in_yaml() {
    let harness = TestHarness::new().await;
    farms::mock_list_farms(&harness.server, &[
        json!({"farmId": "farm-aaa", "displayName": "Alpha Farm"}),
    ]).await;

    assert_cmd_snapshot!(harness.cmd(&["farm", "list"]));
}
```

The snapshot file (`snapshots/cli_farm__farm_list_prints_farms_in_yaml.snap`) stores:

```
---
source: crates/deadline-cli/tests/cli_farm.rs
expression: "harness.cmd(&[\"farm\", \"list\"])"
---
success: true
exit_code: 0
----- stdout -----
- farmId: farm-aaa
  displayName: Alpha Farm

----- stderr -----
```

### Workflow

1. Write the test with `assert_cmd_snapshot!` — just setup + one macro call
2. Run `cargo test` — test fails (no snapshot yet), writes `.snap.new` file
3. Run `cargo insta review` — interactive TUI shows the captured output
4. Review the output for correctness before accepting. insta doesn't know
   what's correct; you do.
5. Accept the snapshot → `.snap.new` becomes `.snap`
6. Commit the `.snap` file to git alongside the test code
7. Future runs diff against the snapshot — any output change is a test failure
8. After intentional changes: `cargo insta review` to accept new output

**Important:** `harness.cmd()` returns `std::process::Command` (for insta-cmd).
`harness.cli()` returns `assert_cmd::Command` (for `assert!`/`assert_eq!`
checks on file side effects, config round-trips, etc.). Use `cmd()` for
snapshot tests, `cli()` for everything else.

### Filters for non-deterministic content

When output contains temp paths, timestamps, or generated IDs, use insta
filters to redact them:

```rust
let mut settings = insta::Settings::clone_current();
settings.add_filter(r"createdAt: .*", "createdAt: [TIMESTAMP]");
let _guard = settings.bind_to_scope();

assert_cmd_snapshot!(harness.cmd(&["farm", "get", "--farm-id", "farm-abc"]));
```

### Mock Response Data Rules

Our `api.rs` functions use a `ResponseBodyCapture` interceptor to extract
raw JSON from AWS SDK responses. However, the SDK deserializes the HTTP
response into its typed output struct *before* the interceptor runs. If
deserialization fails, the SDK returns `SdkError::ServiceError` with
"Unknown: No message" and our code never sees the raw bytes. This means
mock response bodies must be valid enough for the SDK's deserializer.

**Safe to omit** — the SDK auto-fills defaults for missing required scalars:
- `String` → `""`, `i32` → `0`, `DateTime` → epoch, enums → `Unknown`
- Any `Option<T>` field is always safe to omit

**Must be structurally correct** — wrong shapes break deserialization:
- **Union types** must use the tagged object format. For example,
  `TaskParameterValue` is a union with variants `int`, `float`, `string`,
  `path`. Write `{ "Frame": { "int": "1" } }`, NOT `{ "Frame": "1" }`.
  A bare string where the SDK expects a union object causes deserialization
  to fail silently with "Unknown: No message".
- **Struct-typed fields** must be JSON objects, not scalars.
- **Primitive types** must match (don't pass a string where a number is expected).

Extra/unknown fields are silently skipped — adding new fields to the real
API won't break existing mocks.

When in doubt, look up the operation in the
[AWS Deadline Cloud API Reference](https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/Welcome.html)
and check whether a response field is a "structure" or "union" type — those
need the correct nested object shape. You can also check the generated SDK
types in `~/.cargo/registry/src/*/aws-sdk-deadline-*/src/types/` to see
which fields are `Option<T>` (safe to omit) vs bare `T` (required but
auto-defaulted).

See also: `crates/deadline-test-server/src/deadline_api/mod.rs` doc comment.

### Why Snapshots Over Substring Checks

A test that only checks `contains("farm-abc")` will pass even if:
- Half the fields are missing from the output
- The YAML key order changed
- A header line disappeared
- Boolean values changed capitalization

Snapshot tests catch all of these automatically because they assert on the
full output.
