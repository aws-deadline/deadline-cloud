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
