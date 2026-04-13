# Handoff

Current in-flight work for the rust-rewrite. Read this at the start of
every session before consulting the Work Items table in `README.md`.

## Active Work Item

**#15f — Wire queue/fleet assume role for all existing CLI commands**

All existing CLI commands that access non-Deadline AWS services (S3,
CloudWatch) must use the correct credential scoping to match Python.

### Current Step: 0 (Plan)

Previous work item #15e-F1 is complete (committed). That fixed
`get_session_logs` and `get_worker_logs` to use DCM-gated credential
scoping. This work item covers the remaining commands.

### What's Done (from #15e-F1)

- `get_queue_scoped_config()` in `session.rs` — DCM-gated: checks
  `get_user_and_identity_store_id`, assumes queue role if DCM,
  propagates error on failure, returns base config for non-DCM.
- `get_fleet_scoped_config()` in `log_retrieval.rs` — DCM-gated:
  same pattern with `AssumeFleetRoleForRead`.
- `get_session_logs` and `get_worker_logs` use scoped credentials. ✅
- 3 Level 2 tests (§53 cases 3, 11, 12). ✅
- Fleet role mock helpers in test server. ✅
- Test spec §53 corrected: error propagation (not fallback), two
  distinct credential patterns documented (DCM-gated for logs,
  unconditional for S3).

### What's Next

Fix `attachment download` and `attachment upload` CLI commands to call
`get_queue_user_config` unconditionally when no `--profile` is provided,
matching Python's `get_queue_user_boto3_session` pattern. Then validate
all other existing commands for credential parity.

**Two distinct patterns in Python (must match both):**

1. **CloudWatch Logs (DCM-gated):** `get_session_logs` and
   `get_worker_logs` check for DCM before assuming roles. ✅ Done.

2. **S3 operations (unconditional):** `attachment download/upload`
   call `get_queue_user_boto3_session` always when no `--profile`.
   The `QueueUserCredentialProvider` is inserted into the botocore
   session regardless of DCM status. ❌ Not done — Rust currently
   uses `aws_config::defaults().load()` (base creds only).

**Commands to audit and fix:**

| Command | Current Rust | Python behavior | Action |
|---------|-------------|-----------------|--------|
| `attachment download` (no `--profile`) | Base creds | `get_queue_user_boto3_session` (unconditional) | Fix |
| `attachment upload` (no `--profile`) | Base creds | `get_queue_user_boto3_session` (unconditional) | Fix |
| `attachment download` (`--profile`) | Profile creds | Profile creds (skips queue role) | ✅ Already correct |
| `attachment upload` (`--profile`) | Profile creds | Profile creds (skips queue role) | ✅ Already correct |
| `job logs` | Queue-scoped (DCM) / base (non-DCM) | Same | ✅ Fixed in #15e-F1 |
| `get_worker_logs` (library) | Fleet-scoped (DCM) / base (non-DCM) | Same | ✅ Fixed in #15e-F1 |
| `queue export-credentials` | Direct assume role call | Same | ✅ Already correct |
| `farm/queue/fleet/worker/job list/get` | Base creds (Deadline API) | Same | ✅ No scoping needed |
| `config show/get/set/clear` | No AWS calls | Same | ✅ N/A |
| `auth login/logout/status` | Base creds / STS | Same | ✅ N/A |

### Approach

- In `attachment.rs`, replace `aws_config::defaults().load().await`
  with `session::get_queue_user_config(farm_id, queue_id, ...)` when
  no `--profile` is provided.
- Write Level 2 tests (§53 cases 13-16).
- Verify against Python CLI with real API.

## Critical Context for New Sessions

1. **Python source is at `../deadline-cloud-python`** (sibling directory).
   The Python CLI binary is at `/Users/viknith/DeadlineCloudSubmitter/DeadlineClient/deadline`.
   Always run the Python CLI to verify Rust output before accepting
   snapshots (workflow Step 5).

2. **Queue role assumption is not wired in the CLI.** Commands that
   access S3 without `--profile` need to: read farm/queue from config →
   call `get_queue_user_config` → use queue-scoped credentials for S3.
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
   `setup_dcm_env()`/`dcm_cmd()`. `cli_credential_scoping.rs` has
   the pattern for testing DCM vs non-DCM credential paths.

6. **Mock error helpers omit message text.** All error mocks in
   `errors.rs` omit the `message` field from the JSON body — only
   `__type` is included. Do not add a `message` parameter — tests must
   not assert on AWS service error message text. See TESTING.md
   § "Mock Error Response Rules".
