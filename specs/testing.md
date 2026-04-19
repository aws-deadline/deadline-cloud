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

Level 1 and Level 2 tests can coexist on the same code path when they test
different things — see "When Both Levels Add Value" below.

---

## Workflow: Red → Green → Refactor

1. **Red.** Write tests that assert on observable behavior. Run them. They
   must fail — if they pass, the test isn't testing anything new.
2. **Green.** Write the minimum implementation to make the tests pass.
3. **Refactor.** Clean up the implementation. Tests must still pass.

**Bug-driven updates:** Never fix a bug without a failing test first. If an
existing test should have caught it, strengthen that test. If no test covers
the code path, write a new one.

---

## Scenario Coverage

For each interface, systematically consider these categories (skip any that
don't apply):

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

---

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

---

## Test Infrastructure

### `deadline-test-server` crate

A workspace member (`crates/deadline-test-server/`) used only as a
`[dev-dependency]`. Provides:

- **`TestHarness`** — per-test isolation. Each `TestHarness::new()` starts
  a fresh `wiremock` server and creates an isolated temp directory for
  config files. Environment variables point the CLI at the stub server.
  No shared state between tests.
- **Mock helpers** — organized by API domain under
  `deadline_test_server::deadline_api::`:
  `farms`, `jobs`, `queues`, `queue_resources`, `sessions`, `s3`, `sts`,
  `telemetry`, `errors`. Each module has `mock_*` async functions that
  mount canned responses on `harness.server`.

### `TestHarness` methods

- `harness.cmd(&["farm", "list"])` → `std::process::Command` for
  `assert_cmd_snapshot!` (snapshot tests)
- `harness.cli(&["config", "set", ...])` → `assert_cmd::Command` for
  `.assert().success()` and file side-effect checks
- `harness.server` → the `wiremock::MockServer` to mount stubs on

---

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

---

## Naming Convention

```
{command_or_function}_{scenario}_{expected_outcome}
```

---

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

---

## Mock Response Rules

Mock responses must be valid enough for the AWS SDK deserializer.

**Safe to omit:** `String` → `""`, `i32` → `0`, any `Option<T>` field.

**Must be structurally correct:**
- **Union types** must use tagged object format:
  `{ "Frame": { "int": "1" } }`, NOT `{ "Frame": "1" }`
- **Struct-typed fields** must be JSON objects, not scalars

**Error mocks:**
- MUST include `__type` with the correct AWS error code
- MUST NOT include a `message` field (prevents coupling to AWS message text)
- Prefer non-retryable errors (`AccessDeniedException` 403,
  `ResourceNotFoundException` 404) for fast tests (~300-500ms vs ~2-3s
  for retryable errors)

---

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

---

## Checklist: Before Marking Tests Complete

- [ ] CLI-reachable behavior tested through Level 2
- [ ] Level 1 covers precision/edge cases Level 2 can't distinguish
- [ ] Redundant Level 1 tests removed when Level 2 snapshot covers them
- [ ] Happy-path CLI tests use snapshots (exact match, not `contains`)
- [ ] Error tests assert on message content, not just exit code
- [ ] Mock responses match the real API response shape
- [ ] No test depends on execution order
