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

### Decision Tree

```
Can the CLI exercise it?
├─ YES → Level 2
└─ NO → Level 1
```

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
- `crates/deadline-job-attachments/tests/suite.rs`
- `crates/deadline-job-bundle/tests/suite.rs`

When adding a new integration test crate, include the same allow at the
top of its entry file.
