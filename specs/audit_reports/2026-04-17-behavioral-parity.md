# Audit: Behavioral Parity — Rust CLI vs Python CLI

**Date:** 2026-04-17
**Scope:** Full codebase comparison across all implemented features
**Status:** Complete
**Work Item:** #15e

## Summary

| Priority | Count | Fixed | Remaining |
|----------|-------|-------|-----------|
| Critical | 4     | 3     | 1         |
| High     | 10    | 1     | 9         |
| Medium   | 22    | 4     | 18        |
| Low      | 20    | 0     | 20        |

## Methodology

Five parallel audit tracks compared Rust and Python source code:
1. Auth & Session
2. Config & CLI Infrastructure
3. Resource Commands & Job Monitoring
4. Job Bundle & Submission
5. Job Attachments & Transfer

Findings already documented in `specs/python-observations.md` (observations
1–39) are excluded. Only NEW undocumented gaps are listed below.

---

## Critical Findings

### AUDIT-001: `queue sync-output` does not download files

- **Category:** Bug
- **Priority:** Critical
- **Command/Function:** `deadline queue sync-output`
- **Python behavior:** Calls `_incremental_output_download` which performs actual S3 downloads of job output files. (`queue_group.py:484-490`)
- **Rust behavior:** Collects session actions but never downloads files. Summary always shows "Downloaded files: 0". (`queue.rs:460-480`)
- **Impact:** `queue sync-output` is non-functional in Rust — it categorizes jobs but downloads nothing.
- **Resolution:** Pending

### AUDIT-002: `known_asset_paths` config separator uses wrong character

- **Category:** Bug
- **Priority:** Critical
- **Command/Function:** `deadline bundle submit` (asset path discovery)
- **Python behavior:** Splits `settings.known_asset_paths` using `os.pathsep` (`:` on Unix, `;` on Windows). (`_submit_job_bundle.py:595`)
- **Rust behavior:** Splits using `std::path::MAIN_SEPARATOR` (`/` on Unix, `\` on Windows) — the directory separator, not the path-list separator. (`submission.rs:346`)
- **Impact:** On Unix, splits on `/`, destroying every absolute path. The known-asset-paths feature is completely broken in Rust.
- **Resolution:** Fixed — changed to path-list separator (`:` / `;`)

### AUDIT-003: `auto_accept` + unknown paths proceeds instead of canceling

- **Category:** Bug
- **Priority:** Critical
- **Command/Function:** `deadline bundle submit --yes`
- **Python behavior:** When `auto_accept=True` AND files exist outside known asset paths, submission is **canceled** with safety message. (`_submit_job_bundle.py:700-707`)
- **Rust behavior:** When `auto_accept=True`, the unknown-path check is skipped entirely and submission **proceeds**. (`submission.rs:392`)
- **Impact:** Rust silently uploads files from unexpected locations when `--yes` is used — opposite of Python's safety behavior.
- **Resolution:** Fixed

### AUDIT-004: INI key case sensitivity mismatch

- **Category:** Bug
- **Priority:** Critical
- **Command/Function:** `deadline-config` crate (all config operations)
- **Python behavior:** `ConfigParser` lowercases all keys. `Farm_Id = val` is stored as `farm_id`. (Python stdlib)
- **Rust behavior:** `IniConfig` is case-sensitive. `Farm_Id` would not match `farm_id`. (`ini.rs:9`)
- **Impact:** Config files with mixed-case keys (manual edits, external tools) silently lose values in Rust. Python-written configs work fine (already lowercase), but round-tripping through external editors could break.
- **Resolution:** Fixed

---

## High Findings

### AUDIT-005: Default profile DCM detection skipped

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `auth.rs::get_credentials_source()`
- **Python behavior:** Creates a session even for default profile (profile_name=None), checks `get_scoped_config()` for `monitor_id`. (`_session.py:170-181`)
- **Rust behavior:** When `resolve_profile_name()` returns `None`, immediately returns `HostProvided` without checking for `monitor_id`. (`auth.rs:82-96`)
- **Impact:** DCM credentials in `[default]` AWS profile are not detected. Queue role assumption, auth status source, and login/logout all break for default-profile DCM users.
- **Resolution:** Fixed

### AUDIT-006: `fleet get --queue-id` mode missing

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline fleet get`
- **Python behavior:** Accepts `--queue-id` as alternative to `--fleet-id`. Lists queue-fleet associations then gets each fleet. (`fleet_group.py:68-130`)
- **Rust behavior:** Only accepts `--fleet-id` (required). No `--queue-id` option. (`fleet.rs:20-22`)
- **Impact:** Common discovery workflow (`fleet get --queue-id`) unavailable in Rust.
- **Resolution:** Pending

### AUDIT-007: `queue sync-output` job discovery limited to 100 results

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline queue sync-output`
- **Python behavior:** Uses `_list_jobs_by_filter_expression` with `createdAt` thresholding to paginate through ALL matching jobs. (`_list_jobs_by_filter_expression.py:40-140`)
- **Rust behavior:** Two `search_jobs_with_filters` calls each limited to 100 results with no pagination. (`queue.rs:310-350`)
- **Impact:** Silently misses jobs for queues with >100 active or recently-ended jobs. Correctness bug for high-volume queues.
- **Resolution:** Pending

### AUDIT-008: `job download-output` no interactive root path editing

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline job download-output`
- **Python behavior:** Interactive loop where users view output roots, select by index to edit, confirm or cancel. (`job_group.py:555-600`)
- **Rust behavior:** Prints path summary but proceeds directly to download with no editing. (`job.rs:1145-1155`)
- **Impact:** Users cannot redirect output to different directories, especially important for cross-OS downloads.
- **Resolution:** Pending

### AUDIT-009: Upload confirmation prompt only for unknown paths

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline bundle submit`
- **Python behavior:** Confirmation prompt shown for ALL asset uploads, displaying total file count, size, and path summary. (`_submit_job_bundle.py:680-714`)
- **Rust behavior:** Prompt ONLY shown when files exist outside known asset paths. No summary for known-path uploads. (`submission.rs:368-397`)
- **Impact:** Users don't see upload summary (file count, total size) before submission proceeds.
- **Resolution:** Pending

### AUDIT-010: No telemetry events during submission

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline bundle submit`
- **Python behavior:** Emits 6 telemetry events: function latency, asset upload success/fail, upload summary, submission event, create_job event, error recording. (`_submit_job_bundle.py`, `bundle_group.py`)
- **Rust behavior:** No telemetry events emitted anywhere in submission flow. (`submission.rs`, `bundle.rs`)
- **Impact:** No operational visibility into Rust CLI submission success/failure rates or performance.
- **Resolution:** Pending

### AUDIT-011: Upload is sequential (no parallelism)

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline-job-attachments` upload
- **Python behavior:** Uploads small files in parallel via `ThreadPoolExecutor(max_workers=num_upload_workers)`. (`upload.py:258-290`)
- **Rust behavior:** Uploads ALL files sequentially in a single loop. (`upload.rs:395-440`)
- **Impact:** Significantly slower uploads for jobs with many small files.
- **Resolution:** Pending

### AUDIT-012: Download is sequential (no parallelism)

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline-job-attachments` download
- **Python behavior:** Downloads files in parallel via `ThreadPoolExecutor(max_workers=num_download_workers)`. (`download.py:350-380`)
- **Rust behavior:** Downloads ALL files sequentially. (`download.rs:244-280`)
- **Impact:** Significantly slower downloads for jobs with many files.
- **Resolution:** Pending

### AUDIT-013: Hash cache schema incompatible between Python and Rust

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline-job-attachments` caches
- **Python behavior:** `hashesV4` table, `last_modified_time` as text timestamp string. (`caches/hash_cache.py:72-82`)
- **Rust behavior:** `hashesV5` table, `last_modified_time` as integer nanoseconds. (`caches.rs:100-108`)
- **Impact:** Python and Rust CLIs cannot share hash caches. Users switching between tools get full re-hashing of all files.
- **Resolution:** Pending — intentional improvement but interop gap

### AUDIT-014: No multipart upload (5GB PutObject limit)

- **Category:** Behavioral gap
- **Priority:** High
- **Command/Function:** `deadline-job-attachments` upload
- **Python behavior:** Uses boto3 TransferManager with automatic multipart for files >8MB threshold. (`upload.py:340`)
- **Rust behavior:** Uses single `PutObject` regardless of file size. (`upload.rs:310-330`)
- **Impact:** Files >5GB will fail to upload (S3 PutObject limit). Critical for large file workflows.
- **Resolution:** Pending

---

## Medium Findings

### AUDIT-015: `auth status --output` is case-sensitive

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline auth status --output`
- **Python behavior:** `click.Choice(case_sensitive=False)` — `--output JSON` works. (`auth_group.py:80-84`)
- **Rust behavior:** Case-sensitive comparison `output == "json"`. `--output JSON` silently falls through to verbose. (`commands/auth.rs:67`)
- **Impact:** Scripts using uppercase `JSON` get wrong output format.
- **Resolution:** Fixed

### AUDIT-016: `auth status` JSON key order differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline auth status --output json`
- **Python behavior:** Dict insertion order: `profile_name`, `source`, `status`, `api_availability`. (`auth_group.py:113-118`)
- **Rust behavior:** `serde_json::json!()` uses `BTreeMap` — alphabetical order. (`commands/auth.rs:69-74`)
- **Impact:** Scripts depending on key order break.
- **Resolution:** Pending

### AUDIT-017: Login polling DCM exit code 0 handling differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline auth login`
- **Python behavior:** `p.poll()` is falsy for exit code 0 — continues polling. (`_loginout.py:73`)
- **Rust behavior:** `child.try_wait()` returns `Some` for any exit — immediately errors. (`auth.rs:222`)
- **Impact:** Edge case where DCM exits cleanly before credentials propagate. Rust errors; Python eventually succeeds.
- **Resolution:** Pending

### AUDIT-018: INI colon delimiter not supported

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline-config` INI parsing
- **Python behavior:** `ConfigParser` accepts both `=` and `:` as delimiters. (Python stdlib)
- **Rust behavior:** Only recognizes `=`. Lines with `:` delimiter silently ignored. (`ini.rs:48`)
- **Impact:** Config files using `:` delimiter lose settings in Rust.
- **Resolution:** Pending

### AUDIT-019: INI section/key ordering changes on write

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline config set` (any config write)
- **Python behavior:** `ConfigParser` preserves insertion order. (`config_file.py:268-283`)
- **Rust behavior:** `BTreeMap` produces alphabetically sorted output. (`ini.rs:14-15`)
- **Impact:** Round-tripping config through Rust reorders sections/keys, creating noisy diffs.
- **Resolution:** Pending

### AUDIT-020: `suggest_resources` dispatch strategy differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** Error suggestion on API failures
- **Python behavior:** Dispatches based on `exc.operation_name` — precise per-operation chains. (`_suggest_resources.py:111-255`)
- **Rust behavior:** Dispatches based on available resource IDs — greedy approach trying all types. (`commands/helpers.rs:47-100`)
- **Impact:** May show irrelevant suggestions (e.g., listing queues for a fleet error).
- **Resolution:** Pending

### AUDIT-021: Unexpected error messages lack context

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** CLI error handling (all commands)
- **Python behavior:** Prints "The AWS Deadline Cloud CLI encountered the following exception:" + full traceback. (`_common.py:80-83`)
- **Rust behavior:** Prints only the error message via `println!("{e}")`. No prefix, no backtrace. (`main.rs:253-257`)
- **Impact:** Users debugging unexpected errors get significantly less information.
- **Resolution:** Pending

### AUDIT-022: `apply_cli_options_to_config` missing `conflict_resolution` and `storage_profile_id`

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** CLI common options
- **Python behavior:** Handles `storage_profile_id` and `conflict_resolution` CLI options. (`_common.py:119-127`)
- **Rust behavior:** Only handles `profile`, `farm_id`, `queue_id`, `job_id`, `yes`. (`common.rs:82-113`)
- **Impact:** Commands accepting these flags can't override via common options path.
- **Resolution:** Pending

### AUDIT-023: Negative timedelta formatting differs

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `job logs --timestamp-format relative`
- **Python behavior:** `str(timedelta)` for negative: `-1 day, 23:30:00`. (Python stdlib)
- **Rust behavior:** `format_timedelta` for negative: `0:-30:00`. (`common.rs:260-269`)
- **Impact:** Confusing output when log timestamps precede reference start time.
- **Resolution:** Pending

### AUDIT-024: YAML output key ordering differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** All `get` commands (farm, queue, fleet, job, worker)
- **Python behavior:** `sort_keys=False` — preserves insertion order. (`_yaml.py:78`)
- **Rust behavior:** `serde_yaml::to_string` on `BTreeMap` — alphabetical order. (`common.rs:148`)
- **Impact:** YAML output has different key order between CLIs.
- **Resolution:** Pending — accepted difference per `specs/patterns.md`

### AUDIT-025: `worker list/get` missing resource suggestion on error

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline worker list`, `deadline worker get`
- **Python behavior:** Calls `_suggest_resources_on_client_error` on `ClientError`. (`worker_group.py:56-60`)
- **Rust behavior:** Returns bare error message. (`worker.rs:46-49`)
- **Impact:** No helpful suggestions for mistyped fleet/worker IDs.
- **Resolution:** Pending

### AUDIT-026: `job logs --session-action-id` option missing

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline job logs`
- **Python behavior:** Accepts `--session-action-id` to scope logs to a specific action's time window. (`job_group.py:580-583`)
- **Rust behavior:** No `--session-action-id` option. (`job.rs:120-133`)
- **Impact:** Users cannot scope logs to a specific session action.
- **Resolution:** Pending — noted as deferred in progress.md

### AUDIT-027: `job cancel --mark-as` has no value validation

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `deadline job cancel --mark-as`
- **Python behavior:** `click.Choice(["SUSPENDED", "CANCELED", "FAILED", "SUCCEEDED"])` rejects invalid values. (`job_group.py:239`)
- **Rust behavior:** Free-form `String` — invalid values like `"BANANA"` passed to API. (`job.rs:134`)
- **Impact:** Confusing API errors instead of clean CLI usage errors.
- **Resolution:** Fixed

### AUDIT-028: `job requeue-tasks --run-status` has no value validation

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `deadline job requeue-tasks --run-status`
- **Python behavior:** `click.Choice` rejects invalid values. (`job_group.py:334-338`)
- **Rust behavior:** `Vec<String>` with no validation. (`job.rs:142`)
- **Impact:** Typos silently requeue zero tasks with no warning.
- **Resolution:** Fixed

### AUDIT-029: `job logs` relative timestamp bug for auto-selected sessions

- **Category:** Bug
- **Priority:** Medium
- **Command/Function:** `deadline job logs --timestamp-format relative`
- **Python behavior:** Fetches session `startedAt` for auto-selected sessions. (`job_group.py:714-722`)
- **Rust behavior:** `reference_start` is `None` for auto-selected sessions — falls back to `Utc::now()`. (`job.rs:399-406`)
- **Impact:** Relative timestamps are wrong (relative to "now" instead of session start) in the common auto-select case.
- **Resolution:** Fixed

### AUDIT-030: `handle-web-url` macOS support missing

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline handle-web-url install/uninstall`
- **Python behavior:** Supports macOS via `lsregister` and plist files. (`_deadline_web_url.py`)
- **Rust behavior:** Only Windows and Linux. macOS returns error. (`handle_web_url.rs:186-189`)
- **Impact:** macOS users cannot install `deadline://` URL handler.
- **Resolution:** Pending

### AUDIT-031: `--save-debug-snapshot` not implemented

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline bundle submit`
- **Python behavior:** Creates debug directory with `create_job_args.json`, scripts, queue info. (`_submit_job_bundle.py:296-370`)
- **Rust behavior:** No `--save-debug-snapshot` option. (`bundle.rs`)
- **Impact:** Cannot create reproducible debug snapshots for troubleshooting.
- **Resolution:** Pending

### AUDIT-032: `suggest_resources_on_client_error` not used in bundle submit

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline bundle submit`
- **Python behavior:** Calls suggestion helper on `ClientError`. (`bundle_group.py:344-349`)
- **Rust behavior:** No suggestion helper called. (`bundle.rs`)
- **Impact:** Raw API errors without helpful suggestions on submission failure.
- **Resolution:** Pending

### AUDIT-033: `defaults.job_id` not set by library function

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `create_job_from_job_bundle` (library)
- **Python behavior:** Sets `defaults.job_id` when using default config. (`_submit_job_bundle.py:812-814`)
- **Rust behavior:** Only CLI command sets it, not library function. (`submission.rs` vs `bundle.rs:195-201`)
- **Impact:** Non-CLI callers (GUI FFI, MCP) won't have `defaults.job_id` auto-updated.
- **Resolution:** Pending

### AUDIT-034: `--submitter-info` not implemented

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline bundle gui-submit`
- **Python behavior:** Supports `--submitter-info` with multi-format input. (`bundle_group.py:131-145`)
- **Rust behavior:** No `--submitter-info` option. Only `--submitter-name`. (`bundle.rs:62-63`)
- **Impact:** Cannot pass structured submitter metadata.
- **Resolution:** Pending

### AUDIT-035: Download path traversal not validated

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline-job-attachments` download
- **Python behavior:** Calls `_ensure_paths_within_directory` before downloading. (`download.py:600-605`)
- **Rust behavior:** No path traversal validation. (`download.rs:244-280`)
- **Impact:** Malicious manifest with `../../etc/passwd` could write outside download directory.
- **Resolution:** Pending

### AUDIT-036: Download output manifest merge order differs

- **Category:** Behavioral gap
- **Priority:** Medium
- **Command/Function:** `deadline-job-attachments` download
- **Python behavior:** Sorts manifests by S3 `LastModified` timestamp (oldest first, newer wins). (`download.py:290-320`)
- **Rust behavior:** Merges in S3 listing order (lexicographic by key). (`download.rs:340-360`)
- **Impact:** Different merge results when multiple session actions produce output for the same task.
- **Resolution:** Pending

---

## Low Findings

### AUDIT-037: `job cancel`/`requeue-tasks` confirmation prompt behavior differs

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline job cancel`, `deadline job requeue-tasks`
- **Python behavior:** `click.confirm(default=None)` re-prompts on invalid input. (`job_group.py:283-289`)
- **Rust behavior:** Raw `stdin().read_line()` — Enter without input treated as "no". (`job.rs:537-541`)
- **Impact:** Minor UX difference.
- **Resolution:** Pending

### AUDIT-038: `job get --search-term` task summary missing statuses

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline job get --search-term`
- **Python behavior:** Includes `INTERRUPTING` and `NOT_COMPATIBLE` status counts. (`_job_helpers.py:48-49`)
- **Rust behavior:** Omits these statuses. (`job.rs:870-882`)
- **Impact:** Incomplete task summaries for jobs with these statuses.
- **Resolution:** Pending

### AUDIT-039: `job wait` verbose output goes to stderr in Rust

- **Category:** Extra Rust behavior
- **Priority:** Low
- **Command/Function:** `deadline job wait`
- **Python behavior:** Status updates to stdout via `click.echo()`. (`job_group.py:476-487`)
- **Rust behavior:** Status updates to stderr, final results to stdout. (`job.rs:316-326`)
- **Impact:** Different stream separation. Rust's approach is arguably better.
- **Resolution:** Pending — accepted difference

### AUDIT-040: `job requeue-tasks` no adaptive retry strategy

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline job requeue-tasks`
- **Python behavior:** Creates client with `retries=dict(mode="adaptive", total_max_attempts=5)`. (`job_group.py:381-385`)
- **Rust behavior:** Uses default retry behavior. (`job.rs:640`)
- **Impact:** More susceptible to throttling for jobs with many tasks.
- **Resolution:** Pending

### AUDIT-041: `job trace-schedule` command missing

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline job trace-schedule`
- **Python behavior:** Full command for Chrome trace format data and summary statistics. (`job_group.py:730-1020`)
- **Rust behavior:** No `trace-schedule` subcommand. (`job.rs`)
- **Impact:** Cannot generate performance trace data. Marked EXPERIMENTAL in Python.
- **Resolution:** Pending — noted as deferred in progress.md

### AUDIT-042: Windows stdin handling for login subprocess

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline auth login`
- **Python behavior:** Uses `stdin=subprocess.PIPE` on Windows. (`_loginout.py:47-55`)
- **Rust behavior:** Always uses `stdin(Stdio::null())`. (`auth.rs:203`)
- **Impact:** Could cause issues on Windows when spawning DCM.
- **Resolution:** Pending

### AUDIT-043: User-agent string mechanism differs

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** Session creation
- **Python behavior:** Uses `user_agent_extra` appending to existing UA. (`_session.py:96-106`)
- **Rust behavior:** Uses `app_name()` which sets the `app` component. (`session.rs:115-118`)
- **Impact:** Server-side UA parsing may not recognize Rust CLI requests.
- **Resolution:** Pending

### AUDIT-044: INI multiline values not supported

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-config` INI parsing
- **Python behavior:** `ConfigParser` supports continuation lines. (Python stdlib)
- **Rust behavior:** Each line processed independently. (`ini.rs:26-57`)
- **Impact:** Low — no known Deadline settings use multiline values.
- **Resolution:** Pending

### AUDIT-045: `--version` output format differs

- **Category:** Nice-to-have
- **Priority:** Low
- **Command/Function:** `deadline --version`
- **Python behavior:** `deadline, version X.Y.Z`. (`_main.py:95`)
- **Rust behavior:** `deadline X.Y.Z`. (`main.rs:18`)
- **Impact:** Scripts parsing version output may break.
- **Resolution:** Pending

### AUDIT-046: `require_setting` uses exit code 1 instead of 2

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `helpers.rs::require_setting` (currently unused)
- **Python behavior:** Missing required options exit with code 2. (`_common.py:146-156`)
- **Rust behavior:** Returns `CliError::Operation` → exit code 1. (`helpers.rs:36`)
- **Impact:** Latent — function is defined but not called.
- **Resolution:** Pending

### AUDIT-047: `manifest download` CLI is a stub

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline manifest download`
- **Python behavior:** Fully functional — downloads input/output manifests. (`manifest_group.py:140-180`)
- **Rust behavior:** Returns error: "not yet wired in CLI". (`manifest.rs:130-140`)
- **Impact:** Feature not available in Rust CLI.
- **Resolution:** Pending

### AUDIT-048: `manifest upload` missing farm/queue credential derivation

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline manifest upload`
- **Python behavior:** Derives bucket/prefix from queue's `jobAttachmentSettings`. (`manifest_group.py:230-270`)
- **Rust behavior:** Requires explicit `--s3-cas-uri`. (`manifest.rs:165-170`)
- **Impact:** Users must always provide S3 URI manually.
- **Resolution:** Pending

### AUDIT-049: No multipart download for large files

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-job-attachments` download
- **Python behavior:** Uses boto3 TransferManager with multipart downloads. (`download.py:280-290`)
- **Rust behavior:** Single `GetObject` — entire file in memory. (`download.rs:170-210`)
- **Impact:** Slower for large files; potential OOM for very large files.
- **Resolution:** Pending

### AUDIT-050: `force_s3_check` skips cache update

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-job-attachments` upload
- **Python behavior:** Always updates S3 check cache after confirming file exists. (`upload.py:310-315`)
- **Rust behavior:** When `force_s3_check=true`, skips cache entirely without updating. (`upload.rs:410-440`)
- **Impact:** Subsequent uploads won't benefit from cache.
- **Resolution:** Pending

### AUDIT-051: Download duplicate path collision not handled

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-job-attachments` download
- **Python behavior:** Prefixes duplicate file paths with original root to avoid collisions. (`download.py:530-555`)
- **Rust behavior:** Simply moves manifests without collision check. (`download.rs:430-440`)
- **Impact:** Cross-OS downloads with same relative paths silently overwrite.
- **Resolution:** Pending

### AUDIT-052: `decode_manifest` rejects empty paths array

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-job-attachments` manifest parsing
- **Python behavior:** Allows empty paths array. (`asset_manifests/decode.py`)
- **Rust behavior:** Rejects with "paths must have at least one item". (`asset_manifests.rs:230`)
- **Impact:** Edge case — empty manifests rejected in Rust.
- **Resolution:** Pending

### AUDIT-053: Windows long path (UNC) handling missing

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline-job-attachments` upload/download
- **Python behavior:** Prepends `\\?\` for paths >260 chars on Windows. (`_utils.py:80-110`)
- **Rust behavior:** No equivalent handling. (No equivalent file)
- **Impact:** Windows files with long paths fail in Rust.
- **Resolution:** Pending

### AUDIT-054: `--redirect-output` is Unix-only in Rust

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline --redirect-output`
- **Python behavior:** Cross-platform via `sys.stdout` reassignment. (`_main.py:136-139`)
- **Rust behavior:** Uses `libc::dup2` — Unix-only. (`main.rs:274-280`)
- **Impact:** Won't compile on Windows.
- **Resolution:** Pending

### AUDIT-055: `job download-output` conflict resolution prompt missing

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** `deadline job download-output`
- **Python behavior:** Interactive Skip/Overwrite/CreateCopy menu. (`job_group.py:610-630`)
- **Rust behavior:** Uses `--conflict-resolution` arg or defaults to CreateCopy. (`job.rs:1158-1166`)
- **Impact:** No interactive choice for conflicting files.
- **Resolution:** Pending

### AUDIT-056: `suggest_resources` missing storage profile chain

- **Category:** Behavioral gap
- **Priority:** Low
- **Command/Function:** Error suggestion helper
- **Python behavior:** Handles `GetStorageProfileForQueue` operations. (`_suggest_resources.py:238-253`)
- **Rust behavior:** No storage profile suggestion chain. (`commands/helpers.rs`)
- **Impact:** Storage profile errors get no suggestions.
- **Resolution:** Pending

---

## Ranked Summary

| ID | Title | Priority | Category |
|----|-------|----------|----------|
| AUDIT-001 | `queue sync-output` doesn't download files | Critical | Bug |
| AUDIT-002 | `known_asset_paths` separator wrong | Critical | Bug |
| AUDIT-003 | `auto_accept` + unknown paths proceeds | Critical | Bug |
| AUDIT-004 | INI key case sensitivity mismatch | Critical | Bug |
| AUDIT-005 | Default profile DCM detection skipped | High | Behavioral gap |
| AUDIT-006 | `fleet get --queue-id` missing | High | Behavioral gap |
| AUDIT-007 | `queue sync-output` job discovery limited | High | Behavioral gap |
| AUDIT-008 | `job download-output` no root editing | High | Behavioral gap |
| AUDIT-009 | Upload confirmation prompt scope | High | Behavioral gap |
| AUDIT-010 | No telemetry in submission | High | Behavioral gap |
| AUDIT-011 | Upload sequential (no parallelism) | High | Behavioral gap |
| AUDIT-012 | Download sequential (no parallelism) | High | Behavioral gap |
| AUDIT-013 | Hash cache schema incompatible | High | Behavioral gap |
| AUDIT-014 | No multipart upload (5GB limit) | High | Behavioral gap |
| AUDIT-015 | `auth status --output` case-sensitive | Medium | Behavioral gap |
| AUDIT-016 | `auth status` JSON key order | Medium | Behavioral gap |
| AUDIT-017 | Login polling exit code 0 | Medium | Behavioral gap |
| AUDIT-018 | INI colon delimiter unsupported | Medium | Behavioral gap |
| AUDIT-019 | INI section ordering on write | Medium | Behavioral gap |
| AUDIT-020 | `suggest_resources` dispatch strategy | Medium | Behavioral gap |
| AUDIT-021 | Error messages lack context | Medium | Behavioral gap |
| AUDIT-022 | Missing CLI options in config apply | Medium | Behavioral gap |
| AUDIT-023 | Negative timedelta formatting | Medium | Bug |
| AUDIT-024 | YAML key ordering | Medium | Behavioral gap |
| AUDIT-025 | Worker suggest_resources missing | Medium | Behavioral gap |
| AUDIT-026 | `job logs --session-action-id` missing | Medium | Behavioral gap |
| AUDIT-027 | `job cancel --mark-as` no validation | Medium | Bug |
| AUDIT-028 | `job requeue-tasks --run-status` no validation | Medium | Bug |
| AUDIT-029 | `job logs` relative timestamp bug | Medium | Bug |
| AUDIT-030 | `handle-web-url` macOS missing | Medium | Behavioral gap |
| AUDIT-031 | `--save-debug-snapshot` missing | Medium | Behavioral gap |
| AUDIT-032 | `suggest_resources` not in bundle submit | Medium | Behavioral gap |
| AUDIT-033 | `defaults.job_id` not set by library | Medium | Behavioral gap |
| AUDIT-034 | `--submitter-info` missing | Medium | Behavioral gap |
| AUDIT-035 | Download path traversal not validated | Medium | Behavioral gap |
| AUDIT-036 | Download manifest merge order | Medium | Behavioral gap |
| AUDIT-037 | Confirmation prompt behavior | Low | Behavioral gap |
| AUDIT-038 | Task summary missing statuses | Low | Behavioral gap |
| AUDIT-039 | `job wait` verbose to stderr | Low | Extra Rust behavior |
| AUDIT-040 | No adaptive retry for requeue | Low | Behavioral gap |
| AUDIT-041 | `job trace-schedule` missing | Low | Behavioral gap |
| AUDIT-042 | Windows stdin for login | Low | Behavioral gap |
| AUDIT-043 | User-agent mechanism | Low | Behavioral gap |
| AUDIT-044 | INI multiline values | Low | Behavioral gap |
| AUDIT-045 | `--version` format | Low | Nice-to-have |
| AUDIT-046 | `require_setting` exit code | Low | Behavioral gap |
| AUDIT-047 | `manifest download` stub | Low | Behavioral gap |
| AUDIT-048 | `manifest upload` no queue derivation | Low | Behavioral gap |
| AUDIT-049 | No multipart download | Low | Behavioral gap |
| AUDIT-050 | `force_s3_check` cache skip | Low | Behavioral gap |
| AUDIT-051 | Download path collision | Low | Behavioral gap |
| AUDIT-052 | Empty manifest paths rejected | Low | Behavioral gap |
| AUDIT-053 | Windows long path handling | Low | Behavioral gap |
| AUDIT-054 | `--redirect-output` Unix-only | Low | Behavioral gap |
| AUDIT-055 | Download conflict resolution prompt | Low | Behavioral gap |
| AUDIT-056 | Storage profile suggestion chain | Low | Behavioral gap |
