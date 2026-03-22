# Testing Framework

> Guidelines for writing tests in the `deadline-cloud-rs` workspace.
> All test cases are derived from the behavioral test specification in
> `../documentation/test_specs/`.

---

## Philosophy

This is an AI-assisted codebase. Generated code must be verified through tests
that exercise the real built artifact — not abstractions of it. The default test
approach is to run the compiled CLI binary as a subprocess, pointed at a fake
AWS server, and assert on its observable behavior (stdout, stderr, exit code).

Direct unit tests exist only for code that the CLI cannot reach or where
CLI-level testing would be imprecise.

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

When the reference implementation does something that seems wrong, inconsistent, or
surprising, still write the test case but flag it:

```rust
// §1 case 53: clear_setting writes default back rather than removing key
// ⚠️ Intentional per data_flow.md observation #6
```

This keeps the decision visible. The implementation can choose to
replicate or fix the behavior, but the flag ensures it's a deliberate choice
rather than an accidental copy.

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
| Mocking AWS API calls in-process | A real HTTP server (`wiremock`) that returns real response JSON |
| Mocking the filesystem | Real temp directories (`tempfile::TempDir`) |
| Mocking the clock for cache expiry | Real files with modified timestamps (via `filetime` crate) |
| Mocking config state | Real config files written to a temp directory |
| Mocking S3 | A real HTTP server that speaks the S3 protocol subset we need |

**The distinction:** A *fake* is a working alternative implementation (fake
HTTP server, temp directory). A *mock* is a recording device that asserts on
how it was called. Fakes test that the system works. Mocks test that the
system calls things in the expected order. We want the former.

**One exception:** If a future dependency is genuinely impossible to fake
(e.g., hardware interaction, OS kernel behavior), document why mocking is
necessary in the test file and keep the mock surface as small as possible.
This should be rare to nonexistent for a CLI tool.

---

## Test Levels

### Level 1: Direct Unit Tests

**What:** Call a public function in-process, assert on return value or error.

**When to use — ALL of these must be true:**
- The behavior is not reachable through any CLI command, OR
- CLI-level testing would be imprecise (e.g., testing that a function returns
  exactly `"1.5 GB"` vs. parsing it out of a formatted table), OR
- The behavior is an internal optimization invisible to the CLI (caching,
  atomicity, file permissions)
- AND the function is a high-use helper whose reliability must be independently
  confirmed

**Examples:**
- `human_readable_file_size(1_500_000_000)` → `"1.5 GB"` (§36)
- Error type construction and display messages (§51)
- `str2bool("yes")` → `true` (§1)
- Config mtime-based cache invalidation (§1 cases 11-13)
- Atomic file write + permission checks (§1 cases 16-22)
- Manifest format parsing/decoding (§25)
- Hash computation correctness (§20)
- Parameter validation edge cases where the exact error message matters (§16)

**Pattern:**
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // §36 case 1
    #[test_case(0, "0 B" ; "zero bytes")]
    // §36 case 2
    #[test_case(999, "999 B" ; "sub-kilobyte")]
    // §36 case 3
    #[test_case(1000, "1.0 KB" ; "exactly one KB")]
    // §36 case 6
    #[test_case(1_500_000_000, "1.5 GB" ; "fractional GB")]
    fn human_readable_file_size(input: u64, expected: &str) {
        assert_eq!(human_readable_file_size(input), expected);
    }
}
```

### Level 3: CLI Subprocess + Fake HTTP Server

**What:** The compiled `deadline` binary runs as a real child process. A
`wiremock` HTTP server runs in the test process, simulating the Deadline Cloud
and S3 APIs. The CLI is configured via environment variables to use the fake
server. Tests assert on stdout, stderr, and exit code.

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
    // §40 case 1: List farms with valid credentials
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

#[tokio::test]
async fn farm_list_no_credentials_exits_with_error() {
    // §40 case 3: No credentials configured
    let config_dir = TempDir::new().unwrap();
    let config_path = config_dir.path().join("config");
    std::fs::write(&config_path, "").unwrap();

    Command::cargo_bin("deadline").unwrap()
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_PROFILE")
        .env("DEADLINE_CONFIG_FILE_PATH", config_path.to_str().unwrap())
        .args(["farm", "list"])
        .assert()
        .failure();
}
```

---

## Fake Server Architecture

### `deadline-test-server` crate

A workspace member used only as a `[dev-dependency]`. Provides reusable mock
builders for Deadline Cloud and S3 API responses.

```
crates/deadline-test-server/
├── Cargo.toml
└── src/
    ├── lib.rs              # MockDeadlineServer (wraps wiremock::MockServer)
    ├── deadline_api/
    │   ├── mod.rs
    │   ├── farms.rs        # ListFarms, GetFarm, CreateFarm mocks
    │   ├── queues.rs       # ListQueues, GetQueue mocks
    │   ├── jobs.rs         # CreateJob, GetJob, ListJobs mocks
    │   ├── sessions.rs     # Session and credential mocks
    │   └── errors.rs       # Throttling, 404, 403, 500 responses
    └── s3/
        ├── mod.rs
        ├── objects.rs      # GetObject, PutObject, HeadObject mocks
        └── buckets.rs      # ListObjects, bucket operations
```

The server starts stateless (canned request → canned response). It can evolve
to stateful (in-memory farm/queue/job store) when multi-step test scenarios
require it.

### `TestHarness` helper

Each CLI test file gets a harness that reduces boilerplate:

```rust
use deadline_test_server::TestHarness;

#[tokio::test]
async fn config_set_persists_value() {
    let harness = TestHarness::new().await;

    // Set a value
    harness.cli(&["config", "set", "defaults.farm_id", "farm-abc"])
        .assert()
        .success();

    // Read it back
    harness.cli(&["config", "get", "defaults.farm_id"])
        .assert()
        .success()
        .stdout(predicate::str::contains("farm-abc"));
}
```

`TestHarness` encapsulates:
- Starting the `wiremock` server
- Creating a temp directory for config files
- Building `assert_cmd::Command` with all env vars pre-configured
- Providing methods to mount API mocks

---

## Section-to-Level Mapping

| Sections | Domain | Primary Level | Level 1 Carve-outs |
|----------|--------|---------------|-------------------|
| §1-2 | Config | 3 (via `deadline config` commands) | Mtime caching (cases 11-13), atomic writes (16-22), `str2bool` (58-63), `get_config_file_path` env var logic (1-5) |
| §3-5 | Session | 3 (via `deadline auth` commands) | None — all reachable through CLI |
| §6-10 | API resource mgmt | 3 (via `deadline farm/queue/fleet` commands) | None |
| §11-14 | API job lifecycle | 3 (via `deadline job/bundle` commands) | Telemetry internals (§14) if not observable via CLI |
| §15-18 | Job bundle | 3 (via `deadline bundle submit`) | Template parsing edge cases if error messages are ambiguous through CLI |
| §19-25 | Job attachments data | 3 (via `deadline attachment/manifest` commands) | Hash computation (§20), manifest decode (§25), model construction (§19) |
| §26-29 | Job attachments orchestration | 3 (via CLI commands) | Path mapping logic (§26), glob matching (§27), file permissions (§29) |
| §30-31 | Public attachment API | 3 (via CLI commands that call these) | None |
| §32-35 | Progress, errors, AWS helpers | 3 where CLI-reachable | Progress tracking internals (§32), cache mechanics (§24) |
| §36 | path_utils | 1 | All — pure functions, CLI output would be imprecise |
| §37-49 | CLI commands | 3 | None — this IS the CLI |
| §50 | MCP server | Deferred | — |
| §51-52 | Models/errors | 1 | All — pure data types |

---

## Decision Tree

```
Can the behavior be exercised by running `deadline <subcommand>`?
├─ YES → Level 3 (CLI subprocess + fake server)
│        Mount the appropriate API mocks, run the command, assert on output.
└─ NO
   Is it a pure function or data type with no I/O?
   ├─ YES → Level 1 (direct unit test)
   └─ NO
       Is it an internal optimization (caching, atomicity, permissions)?
       ├─ YES → Level 1 with real tempdir where filesystem behavior matters
       └─ NO
           Can you add a CLI subcommand or flag to expose it?
           ├─ YES → Do that, then Level 3
           └─ NO → Level 1 (and document why CLI can't reach it)
```

---

## Test Organization

### CLI tests (Level 3) — in `deadline-cli`

```
crates/deadline-cli/
└── tests/
    ├── common/
    │   └── mod.rs           # TestHarness, shared helpers
    ├── cli_config.rs        # §38 config commands
    ├── cli_auth.rs          # §39 auth commands
    ├── cli_farm.rs          # §40 farm commands
    ├── cli_fleet.rs         # §41 fleet commands
    ├── cli_queue.rs         # §42 queue commands
    ├── cli_worker.rs        # §43 worker commands
    ├── cli_job.rs           # §44 job commands
    ├── cli_bundle.rs        # §45 bundle commands
    ├── cli_attachment.rs    # §46 attachment commands
    ├── cli_manifest.rs      # §47 manifest commands
    ├── cli_handle_web_url.rs # §48
    └── cli_common.rs        # §37 root group, help, version
```

### Unit tests (Level 1) — inline in source files

```rust
// crates/deadline-common/src/path_utils.rs

pub fn human_readable_file_size(bytes: u64) -> String {
    // ...
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_case::test_case;

    // §36 cases 1-7
    #[test_case(0, "0 B")]
    #[test_case(999, "999 B")]
    // ...
    fn human_readable_file_size_formats_correctly(input: u64, expected: &str) {
        assert_eq!(human_readable_file_size(input), expected);
    }
}
```

For Level 1 tests that need filesystem access (config atomicity, permissions):

```
crates/deadline-config/
└── tests/
    ├── config_file_io.rs    # §1 cases 16-22 (atomic writes, permissions)
    └── config_caching.rs    # §1 cases 11-13 (mtime cache behavior)
```

### Naming Convention

```
{command_or_function}_{scenario}_{expected_outcome}
```

```rust
// Level 3
fn farm_list_with_two_farms_prints_both_names() { ... }
fn config_set_invalid_setting_name_exits_with_error() { ... }
fn bundle_submit_missing_template_file_exits_with_error() { ... }

// Level 1
fn human_readable_file_size_zero_returns_zero_b() { ... }
fn str2bool_yes_returns_true() { ... }
```

### Traceability

Every test maps to a spec row:

```rust
// §40 case 1: List farms with valid credentials → prints farm table
#[tokio::test]
async fn farm_list_with_valid_creds_prints_table() { ... }
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

Per-crate usage:

```toml
# crates/deadline-cli/Cargo.toml
[dev-dependencies]
deadline-test-server = { workspace = true }
assert_cmd = { workspace = true }
predicates = { workspace = true }
tempfile = { workspace = true }
tokio = { workspace = true }
serde_json = { workspace = true }

# crates/deadline-common/Cargo.toml
[dev-dependencies]
test-case = { workspace = true }
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

## Checklist: Before Marking a Test Spec Section Complete

- [ ] Every row in the test spec table has a corresponding test
- [ ] Each test has a `// §N case M` traceability comment
- [ ] CLI-reachable behavior is tested through the CLI subprocess (Level 3)
- [ ] Level 1 tests exist only for code the CLI cannot reach
- [ ] Parametric tests cover all boundary values in the spec
- [ ] Error cases assert on error message content, not just exit code
- [ ] No test depends on execution order
- [ ] `cargo test -p <crate>` passes with no warnings
- [ ] Fake server mocks match the real API response shape
