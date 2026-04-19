# Audit: Behavioral Parity (all implemented work items 0a–9, 12, 15)

**Date:** 2026-04-10
**Scope:** All implemented CLI commands and library code compared against Python CLI
**Status:** Complete

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 1     | 1     | 0         |
| High     | 4     | 3     | 1         |
| Medium   | 8     | 3     | 5         |
| Low      | 4     | 0     | 4         |

F-1 (High) deferred — requires real-API investigation.
Medium items C-4, C-5, G-1, K-1, C-2 accepted as known differences.

---

## Findings

### Batch A — Config Commands (`config show`, `config get`, `config set`, `config clear`)

**Audited:** 2026-04-10. Compared Python CLI at `/Users/viknith/DeadlineCloudSubmitter/DeadlineClient/deadline`
against Rust CLI at `./target/debug/deadline`. Both run with `DEADLINE_CONFIG_FILE_PATH` pointing to
a temp file (empty config).

#### A-1: Setting descriptions are truncated in Rust

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline config show` (verbose)
- **Python behavior:** Displays the full description text for each setting, word-wrapped to 77 chars.
  For example, `defaults.aws_profile_name` shows:
  ```
  The AWS profile name to use by default. Set to '' to use the default
  credentials. Other settings are saved with the profile.
  ```
- **Rust behavior:** Displays a shortened description. For the same setting:
  ```
  The AWS profile name to use by default.
  ```
- **Impact:** Users see less information about what each setting does. Affects 10 of 18 settings
  where the Rust description is a truncated version of the Python description. The affected settings
  and their missing text:
  - `defaults.aws_profile_name` — missing "Set to '' to use the default credentials. Other settings are saved with the profile."
  - `settings.storage_profile_id` — missing "It specifies where shared file systems are mounted, and where named job attachments should go."
  - `defaults.job_id` — missing "This gets updated by job submission, so is normally the most recently submitted job."
  - `settings.conflict_resolution` — Python has no trailing period ("...already exists"), Rust adds one ("...already exists.")
  - `telemetry.identifier` — missing "for this configuration."
  - `defaults.job_attachments_file_system` — missing "COPIED means to download a copy of the attachment data, VIRTUAL means to use a virtual file system for lazy loading."
  - `settings.s3_max_pool_connections` — missing the full explanation about default value and recommendation
  - `settings.small_file_threshold_multiplier` — missing the full explanation about large/small file separation
  - `settings.known_asset_paths` — missing "separated by the OS path list separator (semicolon on Windows, colon on Linux/macOS)."
  - `settings.force_s3_check` — missing the detailed explanation of true/false behavior
- **Resolution:** Update `crates/deadline-config/src/settings.rs` to use the full Python description
  strings. This is a straightforward text change in the `SETTINGS` static array.

#### A-2: Error message quoting style differs (single quotes vs double quotes)

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline config get/set/clear` (error paths)
- **Python behavior:** Uses single quotes around setting names in error messages:
  ```
  The setting name 'bad_name' is not valid.
  AWS Deadline Cloud configuration has no setting named 'settings.nonexistent'.
  ```
- **Rust behavior:** Uses double quotes (Rust's `{:?}` Debug formatting):
  ```
  The setting name "bad_name" is not valid.
  AWS Deadline Cloud configuration has no setting named "settings.nonexistent".
  ```
- **Impact:** Scripts that parse error messages by matching exact strings will break.
  Users see cosmetically different output. The quoting difference comes from Rust using
  `format!("{setting_name:?}")` which produces `"value"` vs Python's `f"{setting_name!r}"`
  which produces `'value'`.
- **Resolution:** Change `validate_setting()` in `config_file.rs` to use `'{setting_name}'`
  instead of `{setting_name:?}`.

#### A-3: Error message wording differs for known-section unknown-setting

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline config get/set/clear` (error paths)
- **Python behavior:** For `settings.nonexistent` (known section "settings", unknown key):
  ```
  AWS Deadline Cloud configuration has no setting named 'settings.nonexistent'.
  ```
  Python does not distinguish between known and unknown sections — it always says
  "has no setting named".
- **Rust behavior:** For the same input:
  ```
  AWS Deadline Cloud configuration section "settings" has no setting named "settings.nonexistent".
  ```
  Rust adds extra context about the section being known. This is Rust-only behavior.
- **Impact:** Different error message wording. The Rust message is arguably more helpful
  (tells you the section exists but the setting doesn't), but it doesn't match Python.
  Scripts matching exact error text will break.
- **Resolution:** Change `validate_setting()` to always use the Python message format:
  `"AWS Deadline Cloud configuration has no setting named '{name}'."` regardless of
  whether the section is known. The extra section info is nice-to-have but not worth
  the behavioral divergence.

#### A-4: JSON output spacing differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline config show --output json`
- **Python behavior:** Uses Python's default `json.dumps()` which produces spaces after
  colons and commas: `{"key": "value", "key2": "value2"}`
- **Rust behavior:** Uses `serde_json` compact format with no spaces:
  `{"key":"value","key2":"value2"}`
- **Impact:** Both are valid JSON and parse identically. However, scripts doing exact
  string comparison of the JSON output will see differences. The Python output is more
  human-readable.
- **Resolution:** Use `serde_json::to_string_pretty` is wrong (that adds newlines).
  Instead, manually serialize with `", "` and `": "` separators to match Python's
  `json.dumps()` default. Or accept this as a known cosmetic difference — both are
  valid JSON.

#### A-5: No mtime-based caching in Rust config reader

- **Category:** Nice-to-have
- **Priority:** Low
- **Command/Function:** `config_file::read_config()` / `read_config_from()`
- **Python behavior:** Uses module-level globals (`__config`, `__config_mtime`,
  `__config_file_path`) to cache the parsed config. On subsequent calls, checks if
  the file's mtime has changed before re-reading. This avoids redundant disk I/O
  in long-running processes (GUI, DCC plugins).
- **Rust behavior:** `read_config_from()` always reads from disk. No caching.
  The primary API (`get_setting_with_config`, `set_setting_in_config`) takes an
  explicit `&IniConfig` parameter, so callers control caching by holding the config
  in memory. The convenience wrappers (`get_setting`, `set_setting`) read from disk
  every time.
- **Impact:** No impact for CLI commands (they read once and exit). Could matter for
  the GUI FFI layer or MCP server where the process is long-lived. The Rust design
  is arguably better (explicit state, no globals) but the convenience wrappers are
  slower for repeated calls.
- **Resolution:** Defer. The Rust design is intentionally different (Design Principle #2:
  "Globals must earn their keep"). The CLI reads config once at startup. The GUI FFI
  layer will hold its own `IniConfig` instance. No behavioral gap for users.

### Batch A — Rust-only behavior check

- **Rust has `read_config_from(path)` as a separate function from `read_config()`.**
  Python only has `read_config()` which always uses the global path. The Rust split
  is an improvement (testability, explicit paths) — keep it.
- **Rust `validate_setting()` distinguishes known vs unknown sections in error messages.**
  See A-3 above — this is extra behavior that should be removed for parity.
- **Rust `write_config_to()` sets 0o600 permissions explicitly after writing.**
  Python's `mkstemp` creates with 0o600 by default. Same end result, different mechanism.
  No behavioral difference.

### Batch A — Python-only behavior check

- **Python has mtime-based caching.** See A-5 above — deferred, not a gap for CLI.
- **Python `write_config` has Windows DACL handling.** Rust does not implement
  `_reset_directory_permissions_windows`. This is a gap but only affects Windows
  first-time directory creation. Deferred until Windows support is prioritized.
- **Python `config show` verbose output has full descriptions.** See A-1 — high priority fix.

---

### Batch B — Auth Commands (`auth login`, `auth logout`, `auth status`)

**Audited:** 2026-04-10. Compared Python CLI against Rust CLI. Both run with
`DEADLINE_CONFIG_FILE_PATH` pointing to a temp file (empty config). Auth commands
tested without real AWS credentials (tests credential detection and error paths).

#### B-1: `get_credentials_source` returns `HOST_PROVIDED` instead of `NOT_VALID` for non-existent profiles

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline auth status --profile <non-existent-profile>`
- **Python behavior:** When the specified profile doesn't exist in the AWS config,
  `boto3.Session` raises `ProfileNotFound`. Python catches this and returns
  `AwsCredentialsSource.NOT_VALID`. Output:
  ```
      Profile Name: test-profile
            Source: NOT_VALID
            Status: CONFIGURATION_ERROR
  API Availability: False
  ```
- **Rust behavior:** `get_credentials_source` reads the AWS config file directly via
  `read_aws_profile_key`. If the profile section doesn't exist, `monitor_id` is `None`,
  so it returns `HostProvided`. Output:
  ```
      Profile Name: test-profile
            Source: HOST_PROVIDED
            Status: CONFIGURATION_ERROR
  API Availability: False
  ```
- **Impact:** Users and scripts checking credential source get wrong information.
  `NOT_VALID` tells the user "this profile doesn't exist" — `HOST_PROVIDED` implies
  credentials exist but aren't from DCM, which is misleading.
- **Resolution:** In `get_credentials_source`, check whether the profile section exists
  in the AWS config file. If the profile is not found, return `NotValid`. The
  `read_aws_profile_key` function already reads the file — add a check for whether the
  `[profile X]` section exists at all before checking for `monitor_id`.

#### B-2: JSON output spacing (same as A-4)

- **Category:** Behavioral gap
- **Priority:** Medium (same root cause as A-4, single fix addresses both)
- **Command/Function:** `deadline auth status --output json`
- **Python behavior:** `{"profile_name": "(default)", "source": "HOST_PROVIDED", ...}`
- **Rust behavior:** `{"profile_name":"(default)","source":"HOST_PROVIDED",...}`
- **Impact:** Same as A-4 — valid JSON but cosmetically different.
- **Resolution:** Same fix as A-4 — use spaced JSON serialization.

### Batch B — Rust-only behavior check

- No Rust-only behavior found. The verbose output format, field alignment, and
  capitalization (`True`/`False` for API availability) all match Python exactly.

### Batch B — Python-only behavior check

- **Python `auth status` suppresses logging during credential checks** via
  `_modified_logging_level(logging.getLogger("deadline.client.api"), logging.CRITICAL)`.
  Rust doesn't have this because it doesn't use a logging framework that would emit
  noise during credential detection. No behavioral gap for users.
- **Python `auth login` prints the profile name with single quotes** (`'(default)'`).
  Rust also uses single quotes. ✅ Match.
- **Python `auth login` uses `config_file.get_setting('defaults.aws_profile_name')` with
  `!r` repr formatting.** Rust uses `session::display_profile_name` with manual single
  quotes. Both produce `'(default)'`. ✅ Match.

---

### Batch C — Resource Commands (`farm list/get`, `fleet list/get`, `queue list/get`, `worker list/get`)

**Audited:** 2026-04-10. Compared Python CLI against Rust CLI for error paths and
output format. Real API comparison deferred (requires auth).

#### C-1: Missing required option exits with code 1 instead of code 2

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline farm get` (no `--farm-id`), `deadline queue list` (no `--farm-id`),
  `deadline queue get` (no `--farm-id`), `deadline fleet list` (no `--farm-id`), etc.
- **Python behavior:** Uses Click's `UsageError` which exits with code 2 and includes
  usage information:
  ```
  Usage: deadline farm get [OPTIONS]
  Try 'deadline farm get -h' for help.

  Error: Missing '--farm-id' or default Farm ID configuration
  ```
- **Rust behavior:** Prints just the error message and exits with code 1:
  ```
  Missing '--farm-id' or default Farm ID configuration
  ```
- **Impact:** Scripts that check exit code 2 for usage errors will not detect these.
  The missing usage hint makes it harder for users to discover the correct syntax.
  This affects all commands that use `apply_cli_options_to_config` with required options:
  `farm get`, `fleet list`, `fleet get`, `queue list`, `queue get`, `queue paramdefs`,
  `queue export-credentials`, `job get`, `job list`, `job search`, etc.
- **Resolution:** Change the error handling in `common::apply_cli_options_to_config` to
  return a distinct error variant (e.g., `CliError::Usage`) that the top-level handler
  maps to exit code 2. Optionally include the usage line, though this is less critical
  than the exit code.

#### C-2: Clap required arg error format differs from Click

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline worker list` (no `--fleet-id`), `deadline worker get`
  (no `--fleet-id` or `--worker-id`)
- **Python behavior:** Click's required option error:
  ```
  Usage: deadline worker list [OPTIONS]
  Try 'deadline worker list -h' for help.

  Error: Missing option '--fleet-id'.
  ```
- **Rust behavior:** Clap's required argument error:
  ```
  error: the following required arguments were not provided:
    --fleet-id <FLEET_ID>

  Usage: deadline worker list --fleet-id <FLEET_ID>

  For more information, try '--help'.
  ```
- **Impact:** Both exit with code 2 (correct). The message format differs but both
  clearly indicate what's missing. This is a cosmetic difference inherent to using
  Clap vs Click — not worth changing unless we want to override Clap's error formatting.
- **Resolution:** Accept as known difference. Both frameworks communicate the same
  information (missing `--fleet-id`). Overriding Clap's error formatting would be
  complex and fragile.

### Batch C — Deferred verification (requires auth)

The following checks require running both CLIs against the real Deadline Cloud API
to compare field completeness and output format:

- `farm list` — verify field selection matches (farmId, displayName only)
- `farm get` — verify all API response fields are present in Rust output
- `fleet get --queue-id` — verify association listing format
- `queue get` — verify all fields including nested `jobAttachmentSettings`
- `worker list` — verify count/offset header format, field selection
- `worker get` — verify all fields

**Action needed:** Developer to authenticate, then re-run this batch with real API calls.

### Batch C — Rust-only behavior check

- **Rust `worker list` has `--page-size` default of 5, Python has 10.** Need to verify
  this against the Python source.
- **Rust `worker list` header says "Displaying X of Y workers starting at Z".**
  Need to verify Python's header format matches.

### Batch C — Python-only behavior check

- **Python `farm get` pops `ResponseMetadata` from the response.** Rust uses
  `ResponseBodyCapture` which never includes `ResponseMetadata` (it captures the
  raw HTTP body, not the SDK wrapper). No behavioral gap — same end result.

---

### Batch C/D/E — Real API Comparison (authenticated, 2026-04-10)

Ran both CLIs against the real Deadline Cloud API with profile `viknith-us-west-2`.

#### C-3: YAML output does not quote YAML 1.1 boolean-like strings (ON, OFF, YES, NO)

- **Category:** Bug
- **Priority:** Critical
- **Command/Function:** `deadline job get`, any `get` command that returns string values
  matching YAML 1.1 boolean literals
- **Python behavior:** Quotes strings like `ON`, `OFF`, `YES`, `NO` in YAML output:
  ```yaml
  _2_comp_mainTitle__MultiFrameRendering:
    string: 'OFF'
  _2_comp_mainTitle__IgnoreMissingDependencies:
    string: 'ON'
  ```
- **Rust behavior:** Does not quote these strings:
  ```yaml
  _2_comp_mainTitle__MultiFrameRendering:
    string: OFF
  _2_comp_mainTitle__IgnoreMissingDependencies:
    string: ON
  ```
- **Impact:** When the Rust YAML output is parsed by a YAML 1.1 parser (like PyYAML's
  `safe_load`, which is the most common Python YAML parser), unquoted `ON` is interpreted
  as boolean `True` and `OFF` as `False`. This corrupts data for any downstream consumer
  that parses the CLI output. Verified:
  ```python
  >>> yaml.safe_load("string: ON")
  {'string': True}
  ```
  This affects any string value that matches YAML 1.1 boolean literals: `y`, `Y`, `yes`,
  `Yes`, `YES`, `n`, `N`, `no`, `No`, `NO`, `true`, `True`, `TRUE`, `false`, `False`,
  `FALSE`, `on`, `On`, `ON`, `off`, `Off`, `OFF`.
- **Resolution:** The `cli_object_repr` function uses `serde_yaml::to_string` which
  follows YAML 1.2 (where only `true`/`false` are booleans). But consumers use YAML 1.1
  parsers. Fix by either:
  1. Post-processing the YAML output to quote values matching YAML 1.1 booleans, OR
  2. Using a custom YAML serializer that quotes these values, OR
  3. Switching to a YAML library that supports YAML 1.1 quoting rules

  Option 1 is simplest: after `serde_yaml::to_string`, scan for unquoted values matching
  the YAML 1.1 boolean set and wrap them in single quotes.

#### C-4: Field order in `get` commands differs from Python

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `farm get`, `fleet get`, `queue get`, `job get` — all `get` commands
- **Python behavior:** Fields appear in the order defined by the boto3 service model
  (Smithy model order). For `farm get`: `farmId, displayName, description, costScaleFactor,
  createdAt, createdBy`.
- **Rust behavior:** Fields appear in the raw HTTP response order from the API, which
  differs from the Smithy model order. For `farm get`: `farmId, displayName, createdAt,
  createdBy, description, costScaleFactor, arn`.
- **Impact:** Users who visually scan output or scripts that rely on field position will
  see different ordering. The data is correct but presented differently. This is a
  documented known difference in PATTERNS.md ("Field order: Raw API response order may
  differ from boto3").
- **Resolution:** Accept as known difference per PATTERNS.md. A future Smithy model
  filtering step could reorder fields to match boto3, but this is significant work
  for cosmetic benefit. Document in the "Known differences" section.

#### C-5: Extra `arn` field in Rust `get` commands

- **Category:** Extra Rust behavior
- **Priority:** Medium (accepted)
- **Command/Function:** `farm get`, `fleet get`, `queue get`, `job get`
- **Python behavior:** Does not include `arn` field (boto3 strips it based on service model).
- **Rust behavior:** Includes `arn` field (raw API response includes it).
- **Impact:** Extra information shown to users. Not harmful but different from Python.
  Already documented in PATTERNS.md as a known difference.
- **Resolution:** Accept as known difference. The `arn` is useful information. A future
  Smithy model filtering step would remove it if exact parity is required.

#### D-1: `queue export-credentials` output matches ✅

- **Category:** N/A
- **Command/Function:** `deadline queue export-credentials`
- Both produce identical JSON format with pretty-printing (2-space indent), RFC 3339
  `Expiration` field, and `Version: 1`. No issues found.

#### D-2: `queue paramdefs` output matches ✅

- **Category:** N/A
- **Command/Function:** `deadline queue paramdefs`
- Both produce identical YAML output including multi-line string block scalars. No issues.

#### E-1: Fractional second precision differs (documented)

- **Category:** Behavioral gap (accepted)
- **Priority:** Low (documented known difference)
- **Command/Function:** `job list`, `job get` — any datetime with fractional seconds
- **Python behavior:** Always displays 6 fractional digits: `16:59:24.968000+00:00`
- **Rust behavior:** Preserves API precision (3 digits): `16:59:24.968+00:00`
- **Impact:** Cosmetic. Both represent the same instant in time. The trailing zeros in
  Python are a display artifact of `str(datetime)`.
- **Resolution:** Accept as documented known difference per PATTERNS.md.

#### E-2: Multi-line string wrapping in YAML differs

- **Category:** Behavioral gap
- **Priority:** Low (cosmetic)
- **Command/Function:** `job get` — `lifecycleStatusMessage` field
- **Python behavior:** Wraps long strings using YAML block scalar with line breaks:
  ```yaml
  lifecycleStatusMessage: 'Tasks updated to SUSPENDED: 1. Tasks failed to update: 0.
    Tasks skipped: 0.'
  ```
- **Rust behavior:** Keeps the string on one line:
  ```yaml
  lifecycleStatusMessage: 'Tasks updated to SUSPENDED: 1. Tasks failed to update: 0. Tasks skipped: 0.'
  ```
- **Impact:** Cosmetic. Both parse to the same string value. Python's `DeadlineDumper`
  wraps at a certain width; `serde_yaml` does not.
- **Resolution:** Accept as cosmetic difference. Both are valid YAML producing the same
  parsed value.

---

### Batch F — Job Commands (`job cancel`, `job requeue-tasks`, `job logs`)

**Audited:** 2026-04-10. Compared against real API with auth.

#### F-1: `job logs` fails with unhelpful "service error" against real API

- **Category:** Bug
- **Priority:** High
- **Command/Function:** `deadline job logs --job-id <id>`
- **Python behavior:** Successfully connects to CloudWatch Logs, auto-selects the session,
  and either prints logs or reports "No logs found for the specified session."
  ```
  Using the only available session: session-c67f42c3a1ab4604935a6b1895b64a27
  Retrieving logs for session session-c67f42c3a1ab4604935a6b1895b64a27 from log group ...
  Job ID: job-cb829dd2c16f48988459ad15bca4c3d2
  Job Name: titleFlip (converted).aep

  No logs found for the specified session.
  ```
- **Rust behavior:** Fails with exit code 1 and an unhelpful error:
  ```
  Failed to retrieve logs: service error
  ```
- **Impact:** `job logs` is completely broken against the real API. The error message
  gives no indication of what went wrong (permissions? endpoint? log stream not found?).
  The generic "service error" comes from the SDK's default `Display` for `SdkError` when
  the error details aren't extracted.
- **Resolution:** Two issues to fix:
  1. The CloudWatch Logs call is failing — investigate whether it's a credential/endpoint
     issue or a log stream naming issue. The `logs_client` function checks
     `AWS_ENDPOINT_URL_CLOUDWATCHLOGS` but may not be picking up the correct region or
     credentials from the profile.
  2. The error message should extract the actual error code and message from the SDK error
     (like `api.rs`'s `sdk_err` helper does for Deadline API calls).

#### F-2: `job cancel` and `job requeue-tasks` output matches ✅

- **Category:** N/A
- Both commands produce matching output format, field selection, and messaging.
  Only difference is the documented fractional seconds precision.

#### F-2b: `job wait` output matches ✅

- **Category:** N/A
- **Verified live** against job-dbf135a349bf4bb1840b1b004a7a9fb6 (SUCCEEDED).
- Both produce identical output: header, `\r`-based progress updates with
  task/worker counts and elapsed time, completion message, exit code 0.
- Format: `"Current status: {STATUS} ({n}/{total} tasks succeeded, {w} workers running). [{t}s elapsed]"`

#### F-3: `job search` is Rust-only (Python uses `job get [SEARCH_TERM]`)

- **Category:** Extra Rust behavior (intentional)
- **Priority:** Low (design decision)
- **Command/Function:** `deadline job search`
- **Python behavior:** Search is integrated into `job get` — passing a search term
  argument triggers search mode. `deadline job get titleFlip` searches for jobs.
- **Rust behavior:** Search is a separate `job search` command with `--filter-expressions`.
- **Impact:** Different UX but both provide the same functionality. The Rust approach
  is more explicit (separate command for separate behavior). This was a deliberate
  design decision in work item #12b.
- **Resolution:** Accept as intentional design divergence. Document in CLI help that
  `job search` replaces Python's `job get [SEARCH_TERM]` pattern.

---

### Batch G — Attachment & Manifest Commands

**Audited:** 2026-04-10. Compared local manifest operations against real filesystem.

#### G-1: `manifest diff` output format differs significantly

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline manifest diff`
- **Python behavior:** Prints verbose "Found difference" lines, then a tree-style display
  with `├──`/`└──` characters and ANSI color codes (green for new, yellow for modified):
  ```
  Found size difference at: file1.txt, Status: FileStatus.MODIFIED
  Manifest Diff of root directory: /tmp/test
  ├──\033[93mfile1.txt M\033[0m
  └──\033[92mnew_file.txt +\033[0m
  ```
- **Rust behavior:** Prints a simpler grouped format without colors or tree characters:
  ```
  Manifest Diff of root directory: /tmp/test
  New files:
    + new_file.txt
  Modified files:
    M file1.txt
  ```
- **Impact:** Visual difference. The information content is the same (which files are
  new/modified/deleted) but the presentation differs. Python's tree view is more visually
  appealing but harder to parse programmatically. Rust's grouped format is cleaner for
  scripting.
- **Resolution:** Accept as intentional design improvement. The Rust format is more
  readable and parseable. The tree-style output with ANSI codes is a Python-specific
  presentation choice that doesn't add functional value.

#### G-2: `manifest snapshot` missing hashing progress output

- **Category:** Behavioral gap
- **Priority:** Low (known deferred item)
- **Command/Function:** `deadline manifest snapshot`
- **Python behavior:** Prints hashing progress:
  ```
  Hashing Attachments
  Hashing Summary:
      Processed 3 files totaling 472 B.
      Skipped re-processing 0 files totaling 0 B.
      Total processing time of 0.01902 seconds at 24.82 KB/s.
  ```
- **Rust behavior:** No progress output — just the default path message and manifest path.
- **Impact:** Users don't see progress during hashing of large directories. For small
  directories this is fine; for large ones it looks like the CLI is hanging.
- **Resolution:** Deferred. Progress callback wiring is documented as a known limitation
  in HANDOFF.md. Will be addressed when the progress reporting infrastructure is complete.

#### G-3: `manifest snapshot` default path message matches ✅

- Both print: `"Manifest creation path defaulted to /path \n"` (with trailing space+newline).

#### G-4: `attachment download/upload` interface matches ✅

- Same options (`--manifests`, `--s3-root-uri`, `--path-mapping-rules`, `--farm-id`,
  `--queue-id`, `--profile`, `--conflict-resolution`, `--json`). Help text is more
  terse in Rust but functionally equivalent.

---

### Batch H — Bundle Submit (stub verification)

**Audited:** 2026-04-10.

#### H-1: `bundle submit` not implemented (expected)

- **Category:** N/A (expected — work item #11 "Not started")
- **Command/Function:** `deadline bundle submit`
- **Rust behavior:** `"error: unrecognized subcommand 'bundle'"` — the command doesn't
  exist yet.
- **Impact:** None — this is tracked as work item #11 and blocked on #7 and #9 (both done).
  It's the next major feature to implement after the audit.

---

### Batch I — Session, User-Agent, Telemetry (Library Sweep)

**Audited:** 2026-04-10. Code review of `deadline-api/src/session.rs` and
`deadline-common/src/telemetry.rs` against Python equivalents.

#### I-1: Session caching matches Python behavior ✅

- Rust uses `LazyLock<Mutex<SessionCache>>` with profile-keyed invalidation.
  Python uses `@lru_cache` on `_get_boto3_session_for_profile`.
- Both cache by profile name and invalidate when profile changes.
- Queue user configs cached by `(farm_id, queue_id)` in both.
- `invalidate()` clears all caches — matches Python's cache clearing on logout.

#### I-2: User-agent format matches Python ✅

- Both produce: `app/deadline-api#<version> submitter/<name>#<ver> cli-command/<cmd>`
- Rust uses `AppName` on the SDK config builder; Python uses `user_agent_extra` on
  botocore Config. Different mechanism, same wire format.

#### I-3: Telemetry opt-out logic matches Python ✅

- Priority: env var `DEADLINE_CLOUD_TELEMETRY_OPT_OUT` → config `telemetry.opt_out`
- Identifier: validates existing UUID or generates new UUID4, persists to config
- Both use background thread for async event sending

#### I-4: Telemetry event structure needs verification

- **Category:** Needs investigation
- **Priority:** Deferred
- The telemetry event payload structure (field names, types, enrichment) should be
  verified against the Python telemetry module to ensure events are compatible with
  the telemetry backend. This requires deeper comparison of the event schemas which
  is lower priority than the CLI-facing issues found in Pass 1.

---

### Batch J — Job Bundle Library

**Audited:** 2026-04-10. Code review of `deadline-job-bundle/src/`.

#### J-1: Job bundle loader matches Python behavior ✅

- Symlink containment validation: walks directory, canonicalizes paths, checks
  `starts_with(resolved_root)`. Matches Python's `_validate_directory_symlink_containment`.
- Dual file discovery (`read_yaml_or_json`): checks both `.json` and `.yaml`, errors if
  both exist, errors if required and neither exists. Matches Python.
- Template loading and parameter resolution: covered by 174 test cases (work item #7).

---

### Batch K — Hash Cache, S3 Check Cache, Hashing

**Audited:** 2026-04-10. Code review of `deadline-job-attachments/src/caches.rs`.

#### K-1: Hash cache uses `hashesV5` instead of Python's `hashesV4`

- **Category:** Behavioral gap (intentional)
- **Priority:** Medium
- **Command/Function:** `HashCache` in `caches.rs`
- **Python behavior:** Uses `hashesV4` table with string timestamps (`last_modified_time`
  stored as a timestamp string).
- **Rust behavior:** Uses `hashesV5` table with integer nanosecond timestamps
  (`last_modified_time` stored as INTEGER).
- **Impact:** The hash caches are incompatible between Python and Rust CLIs. Users
  switching between them will re-hash all files on first use of the other CLI. This is
  a one-time cost per file but could be significant for large asset directories.
  During the migration period (Phase 1 rollout), users may switch between CLIs.
- **Resolution:** This is documented as intentional ("improvement over Python's hashesV4
  string timestamps"). The integer nanosecond format is more correct and avoids
  float-to-string precision issues. Accept as intentional schema migration. Consider
  documenting in release notes that the Rust CLI creates a new cache table and does not
  read the Python cache.

#### K-2: S3 check cache matches Python (`s3checkV1`) ✅

- Same table name, same schema, same 30-day expiry logic, same float timestamp parsing.
  Caches are compatible between Python and Rust CLIs.

#### K-3: xxh128 hashing matches Python ✅

- Both use xxh3_128 with chunked reading. Verified by 80+ Level 1 tests in batch 8a
  that produce identical hashes to Python's reference values.

---

### Batch L — Upload/Download Engine (Library Sweep)

**Audited:** 2026-04-10. High-level review based on HANDOFF.md batch 9 notes.
Detailed code was audited during batch 9 implementation (see HANDOFF.md § "Batch 9e-1
— Step 1 Review" and "Batch 9b — Steps 5-6").

#### L-1: Upload/download engine previously audited ✅

- Batch 9b Step 5 found and fixed 3 behavioral gaps (upload order, missing integrity
  check, missing error guidance). All fixed.
- Batch 9d Step 6 found and fixed 1 bug (`attachment_upload` using `source_path` instead
  of `destination_path`). Fixed.
- 228 tests pass across the crate.

#### L-2: Manifest ops previously audited ✅

- Batch 9e-3 Step 5 verified against Python CLI output. `glob_files` macOS
  `canonicalize()` bug found and fixed.

---

## Ranked Issue Summary (All Findings)

Sorted from highest to lowest priority:

| # | ID | Priority | Category | Issue |
|---|-----|----------|----------|-------|
| 1 | C-3 | Critical | Bug | YAML output doesn't quote YAML 1.1 boolean-like strings (ON/OFF/YES/NO) — corrupts data when parsed |
| 2 | A-1 | High | Gap | Setting descriptions truncated in `config show` (10 of 18 settings) |
| 3 | B-1 | High | Gap | `get_credentials_source` returns HOST_PROVIDED instead of NOT_VALID for non-existent profiles |
| 4 | C-1 | High | Gap | Missing required option exits code 1 instead of code 2 (all commands with required options) |
| 5 | F-1 | High | Bug | `job logs` fails with unhelpful "service error" against real API |
| 6 | A-2 | Medium | Gap | Error message quoting: double quotes vs single quotes |
| 7 | A-3 | Medium | Gap | Error message wording differs for known-section unknown-setting |
| 8 | A-4 | Medium | Gap | JSON output spacing (no spaces after colons/commas) — affects all JSON output |
| 9 | C-4 | Medium | Gap | Field order in `get` commands differs (raw API order vs boto3 model order) |
| 10 | C-5 | Medium | Extra | Extra `arn` field in `get` commands (raw API includes it, boto3 strips it) |
| 11 | G-1 | Medium | Gap | `manifest diff` output format differs (tree vs grouped) |
| 12 | K-1 | Medium | Gap | Hash cache `hashesV5` incompatible with Python's `hashesV4` |
| 13 | C-2 | Medium | Gap | Clap vs Click required arg error format (accepted difference) |
| 14 | A-5 | Low | Nice-to-have | No mtime-based caching in config reader (intentional design) |
| 15 | E-1 | Low | Gap | Fractional second precision (3 vs 6 digits — documented) |
| 16 | E-2 | Low | Gap | Multi-line string wrapping in YAML (cosmetic) |
| 17 | F-3 | Low | Extra | `job search` is separate command (intentional design) |
| 18 | G-2 | Low | Gap | `manifest snapshot` missing hashing progress (deferred) |

### Accepted Differences (not counted as issues)

- Field order in `get` commands (C-4) — documented in PATTERNS.md
- Extra `arn` field (C-5) — documented in PATTERNS.md
- Fractional second precision (E-1) — documented in PATTERNS.md
- `job search` as separate command (F-3) — intentional design decision
- Hash cache schema change (K-1) — intentional improvement
- Clap vs Click error formatting (C-2) — inherent to framework choice
