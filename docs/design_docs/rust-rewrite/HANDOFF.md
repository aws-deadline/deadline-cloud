# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

None. Consult the Work Items table in `README.md` and pick the first
row with status "Not started" whose dependencies are all "✅ Done".

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
   The Python CLI binary is at `/Users/viknith/DeadlineCloudSubmitter/DeadlineClient/deadline`.
   Always run the Python CLI to verify Rust output before accepting
   snapshots (workflow Step 5).

2. **All tests pass.** The full workspace test suite is green (841 tests).
   If a test fails, it's a real regression.

3. **VFS (§28, 89 cases) is deferred** per migration strategy. Skip it
   when working on #10.

4. **DCM test infrastructure exists.** `cli_dcm.rs` has
   `write_dcm_aws_config()` helper and `cli_auth.rs` has
   `setup_dcm_env()`/`dcm_cmd()`. `cli_credential_scoping.rs` has
   the pattern for testing DCM vs non-DCM credential paths.

5. **Mock error helpers omit message text.** All error mocks in
   `errors.rs` omit the `message` field from the JSON body — only
   `__type` is included. See TESTING.md for rules.

6. **`deadline-job-bundle` now owns submission orchestration.** It depends
   on `deadline-client` (API calls) and `deadline-job-attachments` (S3
   upload). This was an architecture change made during work item #11.
   See `ARCHITECTURE.md` for the updated dependency graph.

7. **Queue-scoped SdkConfig propagates endpoint URLs.** The
   `get_queue_user_config` function in `session.rs` now propagates
   endpoint URL overrides to the queue-scoped config. This was a bug
   fix discovered during #11 — without it, S3/STS clients built from
   queue credentials would hit real AWS endpoints instead of the stub
   server in tests.

8. **`bundle submit` remaining gaps:** Asset path summary message,
   unknown path confirmation prompt, `--json` output, and
   `--save-debug-snapshot` are not yet implemented.
