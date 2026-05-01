# Audit: CLI Feature Parity — Rust CLI vs Python CLI v0.56.0

**Date:** 2026-05-01
**Scope:** Full three-tier audit: live CLI comparison, code review of new Python changes, schema comparison
**Status:** Complete
**Python version:** 0.56.0.post24+g3dfd14b9c (commit 3dfd14b)
**Rust version:** 0.1.0

## Summary

| Priority | Count | Fixed | No Issue / Accepted | Remaining |
|----------|-------|-------|---------------------|-----------|
| Critical | 1     | 0     | 0                   | 1         |
| High     | 2     | 0     | 0                   | 2         |
| Medium   | 4     | 0     | 0                   | 4         |
| Low      | 5     | 0     | 3                   | 2         |
| Info     | 2     | 0     | 2                   | 0         |

**Remaining open findings: 9**

## Methodology

Three audit tiers per `specs/audit.md`:

- **Tier 1 — Live CLI comparison:** Ran both CLIs and diffed output for config,
  version, manifest snapshot/diff, bundle submit error paths, and resource
  command error paths. Auth status compared structurally.
- **Tier 2 — Code review:** Reviewed 5 recent Python changes (commits 78b10da,
  848cdf0, a6fbd91, d50f261, telemetry changes) against Rust source.
- **Tier 3 — Schema comparison:** Compared hash cache SQLite, config INI,
  checkpoint JSON, manifest JSON, and debug snapshot formats.

Additionally, a full help-text comparison was performed across all commands
and subcommands to identify Rust-only additions and missing flags.

## Findings

### AUDIT-101: Auth status uses STS instead of ListFarms

- **Category:** Behavioral gap
- **Priority:** Critical
- **Command/Function:** `auth status` → `auth.rs::check_authentication_status()`
- **Python behavior:** Calls `deadline.list_farms(maxResults=1)` to validate
  credentials. No STS dependency. Validates both credential validity AND
  Deadline API reachability in one call. (`_session.py:290-306`)
- **Rust behavior:** Calls `sts.get_caller_identity().send().await` via
  `session::sts_client()`. Separately calls `check_deadline_api_available()`
  (ListFarms) for API availability. (`auth.rs:159-173`)
- **Impact:** STS call requires `sts:GetCallerIdentity` IAM permission, which
  may not be available in restricted environments (scoped-down queue role
  sessions, VPC endpoints without STS). Python removed this dependency
  explicitly (commit 78b10da) because it caused slow timeouts and hard
  failures on otherwise-working configurations.
- **Resolution:** Pending — replace STS call with ListFarms in
  `check_authentication_status()`, consolidate with existing
  `check_deadline_api_available()`

### AUDIT-102: Missing config settings for submission defaults

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `config show`, `bundle submit`
- **Python behavior:** Defines `settings.max_retries_per_task` (default `"5"`)
  and `settings.max_failed_tasks_count` (default `"20"`) in SETTINGS dict.
  `bundle submit` falls back to these when CLI flags are omitted.
  (`config_file.py:183-190`, `bundle_group.py:267-274`)
- **Rust behavior:** Neither setting exists in `SETTINGS` array
  (`settings.rs`). `bundle submit` passes `None` to CreateJob when flags
  are omitted — server defaults apply instead of user-configured defaults.
  (`bundle.rs:240-250`)
- **Impact:** Users who configure `max_retries_per_task=10` via
  `deadline config set` will have it honored by Python CLI but silently
  ignored by Rust CLI. `config show` lists 20 settings (Rust) vs 22 (Python).
- **Resolution:** Pending — add both settings to `SETTINGS` in
  `deadline-config/src/settings.rs`, add config fallback in `bundle.rs`

### AUDIT-103: Missing `--ignore-storage-profiles` on `job download-output`

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline job download-output`
- **Python behavior:** Has `--ignore-storage-profiles` flag (boolean, default
  False). When True, skips storage profile path mapping — downloads to
  original unmapped paths. (`job_group.py:970-978`, `_job_download_helpers.py:53-69`)
- **Rust behavior:** No `--ignore-storage-profiles` flag on `download-output`.
  The flag exists on `queue sync-output` (`queue.rs:73-75`) but not on
  `download-output`. (`job.rs:204-219`)
- **Impact:** Users who submit and download on the same machine cannot bypass
  storage profile path mapping in Rust CLI.
- **Resolution:** Pending — add flag to `DownloadOutput` struct, thread
  through `download_output_impl()`, follow `sync-output` pattern

### AUDIT-104: Manifest snapshot includes `.manifest` files

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `deadline manifest snapshot`
- **Python behavior:** Excludes `.manifest` files from snapshots.
- **Rust behavior:** Includes `.manifest` files. When snapshotting a directory
  that already contains manifest files, they get included, inflating the
  manifest and causing cascading issues on re-snapshot.
- **Impact:** Manifest inflation, potential cascading issues. Manifests grow
  on each re-snapshot of the same directory.
- **Resolution:** Pending — add `.manifest` extension to exclusion filter

### AUDIT-105: `--json` flag doesn't suppress human-readable output

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `deadline manifest snapshot --json`
- **Python behavior:** Suppresses human-readable messages when `--json` is
  specified. Only JSON goes to stdout.
- **Rust behavior:** Prints human-readable messages ("Manifest creation path
  defaulted to...", "Manifest generated at...") AND JSON to stdout. Breaks
  piping to `jq` or other JSON parsers.
- **Impact:** Scripts that parse `--json` output will fail.
- **Resolution:** Pending — suppress non-JSON output when `--json` is active,
  or redirect human messages to stderr

### AUDIT-106: Telemetry `account_id` not resolved

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** Telemetry system
- **Python behavior:** Resolves `account_id` best-effort on background
  telemetry thread. Prefers `credentials.account_id` (free for SSO/DCM),
  falls back to STS with 2s timeout. Events get `accountId` at send time.
  (`_telemetry.py:281-288, 381-409`)
- **Rust behavior:** `account_id` passed as parameter to
  `initialize_with_metadata()`. Most callers pass `None` (e.g.,
  `bundle.rs:224`). Background thread does not resolve it.
  (`telemetry.rs:96-122`)
- **Impact:** Rust telemetry events missing `accountId` in most cases.
  Reduces telemetry data quality for usage analytics. Not user-facing.
- **Resolution:** Pending — add credential fast-path for account_id
  resolution (SSO/DCM case), optionally add background STS fallback

### AUDIT-107: Debug snapshot has reduced content

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline bundle submit --save-debug-snapshot`
- **Python behavior:** `queue.json` contains full queue resource (all API
  fields). Writes `storage_profile.json` when storage profile is configured.
  Shell scripts use `shlex.join()` for proper quoting.
- **Rust behavior:** `queue.json` contains only `jobAttachmentSettings`
  subset. No `storage_profile.json` written. Shell scripts don't quote
  values (broken if values contain spaces/special chars).
- **Impact:** Reduced debugging utility. Generated shell scripts may be
  broken for values with spaces.
- **Resolution:** Pending — expand `queue.json`, add `storage_profile.json`,
  add shell quoting

### AUDIT-108: Rust-only subcommands not in Python CLI

- **Category:** Extra Rust behavior
- **Priority:** Low
- **Command/Function:** `job get-session`, `job list-sessions`,
  `job list-steps`, `job list-tasks`, `job search`,
  `queue get-storage-profile`
- **Python behavior:** None of these subcommands exist.
- **Rust behavior:** All 6 subcommands are implemented and functional.
- **Impact:** Feature creep beyond Python CLI scope. May confuse users
  expecting identical command sets. Some are used internally by
  `trace-schedule`.
- **Resolution:** Pending — decision needed: remove, keep, or document
  as Rust-only additions

### AUDIT-109: Rust-only `--json` flag on `bundle submit`

- **Category:** Extra Rust behavior
- **Priority:** Low
- **Command/Function:** `deadline bundle submit --json`
- **Python behavior:** No `--json` flag on `bundle submit` (only on
  `gui-submit`).
- **Rust behavior:** Has `--json` flag for JSON output.
- **Impact:** Minor feature addition beyond Python scope.
- **Resolution:** Pending — decision needed: remove or keep

### AUDIT-110: Missing backward-compat flags

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** Multiple commands
- **Python behavior:** `job logs --timezone` (deprecated, maps to
  `--timestamp-format`). `queue export-credentials --output-format`
  (accepts `credentials_process`). `manifest snapshot/diff -ie` (short
  alias for `--include-exclude-config`).
- **Rust behavior:** None of these flags exist.
- **Impact:** Scripts using deprecated flags will break when switching
  to Rust CLI.
- **Resolution:** Pending — tracked as work item #21d

### AUDIT-111: `manifest diff --root` is required in Rust, optional in Python

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline manifest diff`
- **Python behavior:** `--root` is optional. When omitted, Python derives
  the root from the manifest file's directory.
- **Rust behavior:** `--root` is required (clap enforces it).
- **Impact:** Minor UX difference. Users must always specify `--root` in Rust.
- **Resolution:** Pending — make `--root` optional, derive from manifest path

### AUDIT-112: `config show` description text differs

- **Category:** Nice-to-have
- **Priority:** Info
- **Command/Function:** `deadline config show`
- **Python behavior:** `settings.submitter_update_notification` description:
  "Controls whether the submitter shows a dialog when a newer version is
  available..."
- **Rust behavior:** Different wording: "Enable update notification checks
  for DCC submitter integrations..."
- **Impact:** Cosmetic only.
- **Resolution:** Accepted difference

### AUDIT-113: Help text framework differences

- **Category:** Nice-to-have
- **Priority:** Info
- **Command/Function:** All `--help` output
- **Python behavior:** Click-style help with "Common workflows" examples
  section, inline choice display for `--log-level`, verbose option
  descriptions.
- **Rust behavior:** Clap-style help without examples section, `--log-level`
  doesn't show valid choices inline, terser descriptions.
- **Impact:** Cosmetic. Clap vs Click framework conventions.
- **Resolution:** Accepted difference — consider adding "Common workflows"
  section and inline choices as polish items

### AUDIT-114: Hash cache surrogatepass encoding

- **Category:** Behavioral gap
- **Priority:** Info
- **Command/Function:** `deadline-job-attachments` hash cache
- **Python behavior:** Uses `errors="surrogatepass"` for file paths with
  lone surrogates (rare edge case on some filesystems).
- **Rust behavior:** Uses plain UTF-8 `as_bytes()`. Cannot handle lone
  surrogates (inherent Rust limitation).
- **Impact:** Extremely rare edge case. Only affects file paths with
  invalid UTF-8 sequences.
- **Resolution:** Accepted — inherent Rust limitation, documented

## Tier 3: Schema Compatibility Summary

| Schema | Compatible? | Notes |
|--------|------------|-------|
| Hash cache (SQLite) | ✅ Yes | Same table, columns, format. surrogatepass edge case only. |
| Config INI | ⚠️ Mostly | 2 missing settings (AUDIT-102). All other settings match. |
| Checkpoint files | ✅ Yes | Same JSON structure. Timestamp Z vs +00:00 handled by both parsers. |
| Manifest format | ✅ Yes | Byte-identical canonical JSON. Same sort order, encoding, fields. |
| Debug snapshot | ⚠️ Mostly | Reduced content (AUDIT-107). Not a data interchange format. |

## Ranked Summary

| ID | Title | Priority | Category | Status |
|----|-------|----------|----------|--------|
| AUDIT-101 | Auth status uses STS instead of ListFarms | Critical | Behavioral gap | Pending |
| AUDIT-102 | Missing config settings for submission defaults | High | Behavioral gap | Pending |
| AUDIT-103 | Missing `--ignore-storage-profiles` on download-output | High | Behavioral gap | Pending |
| AUDIT-104 | Manifest snapshot includes `.manifest` files | Medium | Bug | Pending |
| AUDIT-105 | `--json` doesn't suppress human-readable output | Medium | Bug | Pending |
| AUDIT-106 | Telemetry `account_id` not resolved | Medium | Behavioral gap | Pending |
| AUDIT-107 | Debug snapshot has reduced content | Medium | Behavioral gap | Pending |
| AUDIT-108 | Rust-only subcommands not in Python CLI | Low | Extra Rust behavior | Pending |
| AUDIT-109 | Rust-only `--json` on `bundle submit` | Low | Extra Rust behavior | Pending |
| AUDIT-110 | Missing backward-compat flags | Low | Behavioral gap | Pending (#21d) |
| AUDIT-111 | `manifest diff --root` required vs optional | Low | Behavioral gap | Pending |
| AUDIT-112 | Config description text differs | Info | Nice-to-have | Accepted |
| AUDIT-113 | Help text framework differences | Info | Nice-to-have | Accepted |
| AUDIT-114 | Hash cache surrogatepass encoding | Info | Behavioral gap | Accepted |
