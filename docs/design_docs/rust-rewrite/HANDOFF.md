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

2. **All tests pass.** The full workspace test suite is green (804 tests).
   If a test fails, it's a real regression.

3. **VFS (§28, 89 cases) is deferred** per migration strategy. Skip it
   when working on #10.

4. **DCM test infrastructure exists.** `cli_dcm.rs` has
   `write_dcm_aws_config()` helper and `cli_auth.rs` has
   `setup_dcm_env()`/`dcm_cmd()`. `cli_credential_scoping.rs` has
   the pattern for testing DCM vs non-DCM credential paths.

5. **Mock error helpers omit message text.** All error mocks in
   `errors.rs` omit the `message` field from the JSON body — only
   `__type` is included. Do not add a `message` parameter — tests must
   not assert on AWS service error message text. See TESTING.md
   § "Mock Error Response Rules".
