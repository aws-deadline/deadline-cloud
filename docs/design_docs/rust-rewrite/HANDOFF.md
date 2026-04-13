# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#15e-F1 — Fix `job logs` against real API (audit finding F-1)**

Remaining item from the behavioral parity audit (#15e). All other
audit findings are fixed (commit `9e8e4ef`). Full audit report:
`audit_reports/2026-04-10-behavioral-parity.md`.

### Problem

`deadline job logs` fails with `"Failed to retrieve logs: service error"`
against the real Deadline Cloud API. The Python CLI works correctly for
the same job/session. Two issues to investigate:

1. **Root cause:** Why the CloudWatch Logs call fails. Likely
   credentials or region not propagating correctly to the CloudWatch
   client built in `log_retrieval.rs::logs_client()`. The function
   uses `session::get_sdk_config(config)` but may not be picking up
   the profile's region or credentials. Compare with how Python's
   `boto3.Session` builds its CloudWatch client.

2. **Error formatting:** The SDK's `Display` for `SdkError` just says
   "service error". Need a `cw_sdk_err` helper (like `api.rs::sdk_err`)
   that extracts the actual error code and message via
   `ProvideErrorMetadata`. This was prototyped but reverted with the
   deferral — see the approach in `api.rs::format_sdk_error`.

### Approach

- Run both CLIs side by side against the real API with the same
  credentials and compare what happens at the HTTP level.
- Add `RUST_LOG=debug` or SDK-level tracing to see the actual request
  the Rust CLI sends to CloudWatch.
- Check whether the CloudWatch endpoint, region, and credentials match
  what Python sends.
- Once root cause is found, fix it and apply the error formatting fix.
- Follow TDD: write a failing test, fix, verify against Python CLI.

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
