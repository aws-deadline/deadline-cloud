# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#15e — Fix behavioral parity gaps**

The audit is complete. All findings are in
`audit_reports/2026-04-10-behavioral-parity.md` (ranked summary table
at the bottom). Fix all issues before picking up new feature work
(#10, #11, etc.). Work in priority order:

1. **C-3 (Critical):** YAML output doesn't quote YAML 1.1 boolean-like
   strings (`ON`/`OFF`/`YES`/`NO`). Data corruption when parsed.
2. **A-1 (High):** Setting descriptions truncated in `config show`.
3. **B-1 (High):** `get_credentials_source` returns `HOST_PROVIDED`
   instead of `NOT_VALID` for non-existent profiles.
4. **C-1 (High):** Missing required option exits code 1 instead of 2.
5. **F-1 (High):** `job logs` broken against real API ("service error").
6. **A-2, A-3, A-4 (Medium):** Error quoting, wording, JSON spacing.

For each fix: follow TDD (failing test → fix → verify). Commit after
all fixes are applied. Update the audit report's resolution fields.

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
