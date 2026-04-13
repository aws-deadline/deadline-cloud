# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#15e-F1 — Fix `job logs` against real API (audit finding F-1)**

Remaining item from the behavioral parity audit (#15e). All other
audit findings are fixed (commit `9e8e4ef`). Full audit report:
`audit_reports/2026-04-10-behavioral-parity.md`.

### Current Step: 3 (Red — write failing tests)

Steps 0-2 are complete. The SDK retry hang investigation is resolved
(see "What's Done" below). Ready to write failing tests per §53.

### What's Done

- **Root cause identified:** `logs_client()` in `log_retrieval.rs`
  builds a CloudWatch client from base credentials. For DCM users,
  base credentials lack CloudWatch Logs permissions — the queue role
  (via `AssumeQueueRoleForUser`) grants that access. Python checks
  `get_user_and_identity_store_id()` and assumes the queue role when
  DCM is detected. Rust skips this entirely.

- **Full scope identified:** Same bug affects `get_worker_logs` (fleet
  credentials), `attachment download`, and `attachment upload`. All use
  base credentials where Python uses scoped credentials for DCM users.

- **SDK retry investigation resolved:** The "hang" noted in the
  previous session was a false alarm. PoC testing confirmed:
  - `AccessDeniedException` (403) is NOT retried — completes in ~500ms
  - `ThrottlingException` (403) IS retried — ~2.5s (3 attempts)
  - `500` transient errors ARE retried — ~2.2s (3 attempts)
  - The SDK classifies errors using the `__type` field in the JSON body.
    As long as error mocks include `__type`, retry behavior is correct.

- **SDK error formatting fixed (separate commit):** All non-Deadline
  AWS SDK errors (CloudWatch, STS) now surface the actual error code
  and message instead of the opaque `"service error"` from
  `SdkError::Display`. Uses `aws_smithy_types::ProvideErrorMetadata`.

- **Mock error infrastructure cleaned up (separate commit):** Removed
  `message` parameter from all mock error helpers. All error mocks use
  a fixed `"mock error"` placeholder via `MOCK_ERROR_MESSAGE` constant.
  Tests cannot accidentally assert on fake AWS message text.

- **Docs updated:**
  - `PATTERNS.md` — updated error formatting section
  - `ARCHITECTURE.md` — added credential scoping to shared conventions
  - `TESTING.md` — added mock error response rules and retry awareness
  - `docs/crate_specs/deadline-client.md` — added `get_queue_scoped_config`
  - `docs/crate_specs/deadline-job-attachments.md` — documented CLI
    layer credential responsibility

- **Test spec created:** `test_specs/credential_scoping.md` (§53, 22
  cases) covering queue-scoped, fleet-scoped, error formatting, CLI
  end-to-end, and future operations.

- **Work item #15f created** in README for DCM credential scoping tests
  (depends on #15e-F1 implementation).

### What's Next

1. **Step 3 (Red)** — Write failing tests per §53 cases 1-10 (Level 1)
   and cases 11-16 (Level 2). Level 1 tests need `serial` attribute
   (env var mutation). Level 2 tests use `write_dcm_aws_config` helper
   from `cli_dcm.rs`.

2. **Step 4 (Green)** — Implement:
   - `get_queue_scoped_config()` in `session.rs`
   - Fix `get_session_logs` to use queue-scoped credentials
   - Fix `get_worker_logs` to use fleet-scoped credentials
   - Fix `attachment download/upload` CLI to use queue-scoped credentials

3. **Step 5 (Verify)** — Run both CLIs against real API with DCM profile.

4. **Steps 6-7** — Refactor, update docs, commit.

### Approach

- Add `get_queue_scoped_config(farm_id, queue_id, config)` to
  `session.rs` — checks DCM, assumes queue role if DCM, falls back to
  base config on failure.
- `logs_client()` becomes `logs_client(config, farm_id, queue_id)` and
  calls `get_queue_scoped_config`.
- `get_worker_logs` calls `assume_fleet_role_for_read`, builds a
  temporary `SdkConfig` from the returned credentials.
- `attachment.rs` download/upload paths call `get_queue_scoped_config`
  instead of `aws_config::defaults().load()`.

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
   The Python CLI binary is at `/Users/viknith/DeadlineCloudSubmitter/DeadlineClient/deadline`.
   Always run the Python CLI to verify Rust output before accepting
   snapshots (workflow Step 5).

2. **Queue role assumption is not wired in the CLI.** Commands that
   access S3 without `--profile` need to: read farm/queue from config →
   call GetQueue to get attachment settings → assume queue role via
   `get_queue_user_credentials` → use queue-scoped credentials for S3.
   The library functions accept pre-built S3 clients. The CLI layer
   needs to build those clients with queue-scoped credentials. Affects:
   - `deadline attachment download/upload` without `--profile`
   - `deadline manifest download` (fully stubbed — returns error)
   - `deadline manifest upload` without `--s3-cas-uri`

3. **All tests pass.** The full workspace test suite is green as of
   this commit. If a test fails, it's a real regression.

4. **VFS (§28, 89 cases) is deferred** per migration strategy. Skip it
   when working on #10.

5. **DCM test infrastructure exists.** `cli_dcm.rs` has
   `write_dcm_aws_config()` helper and `cli_auth.rs` has
   `setup_dcm_env()`/`dcm_cmd()`. Use these patterns for new DCM tests.

6. **Mock error helpers omit message text.** All error mocks in
   `errors.rs` omit the `message` field from the JSON body — only
   `__type` is included. Do not add a `message` parameter — tests must
   not assert on AWS service error message text. See TESTING.md
   § "Mock Error Response Rules".
