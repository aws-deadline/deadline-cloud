---
status: complete
last_verified: 0a4b3215e9cb6322403a728ef50bff6a11ff5125
release_tag: "0.54.2"
scope: "Bugs, gaps, inconsistencies, and design notes discovered by code review. These are things an agent would not spot by casually reading the Python source."
---

# Observations

Extracted from code review of release 0.54.2. Grouped by feature area.

## Tag Reference

| Tag | Meaning |
|-----|---------|
| `[GAP]` | Missing behavior that seems like it should exist |
| `[INCONSISTENCY]` | Behavior differs from sibling commands |
| `[SILENT FAILURE]` | Error caught but not surfaced |
| `[ASSUMPTION]` | Based on code reading, not fully verified |
| `[DESIGN]` | Deliberate choice with tradeoffs worth noting |
| `[QUESTION]` | Needs human input to resolve |

---

## Authentication (`deadline auth login/logout/status`)

1. `[INCONSISTENCY]` The `login` command does not accept a `--profile` option, while `status` does. To log in with a non-default profile, the user must first change the `defaults.aws_profile_name` configuration setting.

2. `[INCONSISTENCY]` The `logout` command's error message for a failed subprocess has a missing space: `"...profile {profile_name}.Return code {e.returncode}: ..."`.

3. `[SILENT FAILURE]` In `auth status`, if the ListFarms API availability check fails, the exception is logged but the user only sees `False` for API availability. The underlying cause (permissions, network, throttling) is not surfaced in either verbose or JSON output.

4. `[ASSUMPTION]` The `login` polling loop uses an LRU-cached session factory. If the session is created before login completes (when credentials are not yet valid), the internal refresh timeout patching may silently fail (caught by a bare except). The cached session then uses default refresh timeouts (15 min advisory, 10 min mandatory) instead of the intended shorter ones (5 min advisory, 2.5 min mandatory). Since Deadline Cloud monitor credentials expire after 15 minutes and the default advisory refresh is also 15 minutes, credentials may never be reused between API calls in that process lifetime.

## Configuration (`deadline config show/get/set/clear/gui`)

5. `[DESIGN]` The `clear` command writes the default value explicitly rather than removing the key. If the default changes in a future version, a "cleared" setting retains the old default.

6. `[GAP]` The `set` command performs no value validation beyond checking the setting name is recognized. Users can set `settings.log_level` to `"BANANA"` or `defaults.farm_id` to a non-matching pattern. Invalid values are written to disk and only fail when a downstream feature reads them.

7. `[INCONSISTENCY]` The `show` command in verbose mode marks values as `(default)` by string equality comparison. If a user explicitly sets a value to match the resolved default, the marker still appears — there is no way to distinguish "not configured" from "explicitly set to the default."

8. `[INCONSISTENCY]` The `show` command in JSON mode includes a `settings.config_file_path` key that is not a recognized setting and is absent from verbose output. JSON output also omits the `(default)` indicator entirely.

9. `[ASSUMPTION]` Config file writes use `os.replace` with a temp file created by `mkstemp` (default `0o600` permissions). This silently changes file permissions if the original had different permissions (e.g., `0o644`). Likely intentional for security but undocumented.

10. `[QUESTION]` The GUI dialog's Apply button always writes `settings.storage_profile_id` from the dropdown's current selection, even if unchanged. Unclear if intentional (sync guarantee) or an oversight causing unnecessary writes.

## Resource Discovery (`deadline farm/fleet/queue/worker list/get`)

11. `[INCONSISTENCY]` None of the resource discovery commands accept `--output json`. Other command groups (`auth status`, `config show`, `job wait`, `job logs`) support it. Scripts must parse YAML output.

12. `[DESIGN]` `worker list` uses SearchWorkers with user-controlled pagination, while `farm/fleet/queue list` accumulate all pages in memory. This is appropriate given the expected cardinality (few farms/queues, many workers).

13. `[DESIGN]` List commands filter to two fields (`id` + `displayName`), except `worker list` which also includes `status` and `createdAt` — workers are more transient so status is immediately relevant.

14. `[DESIGN]` `fleet get` in Queue ID mode calls ListQueueFleetAssociations, which does not accept `principalId` for membership scoping. The association list may include fleets the user cannot access. A single inaccessible fleet causes the entire command to fail with `AccessDeniedException`.

## Job Submission (`deadline bundle submit/gui-submit`)

15. `[GAP]` The CLI `bundle submit` does not save a job history bundle. Only the GUI path creates a job history directory. CLI users have no built-in audit trail.

16. `[GAP]` The MCP `submit_job` tool does not expose `target_task_run_status`, `force_s3_check`, or `debug_snapshot_dir`, limiting its functionality compared to the CLI.

17. `[ASSUMPTION]` `wait_for_create_job_to_complete` checks `lifecycleStatus` first and falls back to `state` (a legacy field name). The 300-second timeout is hardcoded and not configurable.

18. `[GAP]` The `--parameter` help text claims support for inline JSON strings and `file://` paths, but the Click callback (`validate_parameters`) only handles `Name=Value` format. The multi-format parser is wired to `--submitter-info` but not `--parameter`.

## Job Attachments (file transfer engine)

19. `[ASSUMPTION]` Cache integrity sampling selects up to 30 entries randomly. The sample size is hardcoded.

20. `[DESIGN]` The hash cache has no eviction policy — entries persist indefinitely and the database grows unboundedly. The S3 check cache has a 30-day expiry on lookup but does not proactively delete expired entries.

21. `[DESIGN]` `_get_tasks_manifests_keys_from_s3` selects the "latest" session action per task by sorting `{timestamp}_{sessionActionId}` folder names alphabetically. A TODO notes the timestamp comes from the WorkerAgent and should not be relied upon.

22. `[GAP]` `verify_hash_cache_integrity` samples the S3 check cache but does not verify the hash cache itself. If the hash cache contains stale entries (file modified without changing mtime), the tool uploads wrong content.

23. `[INCONSISTENCY]` The upload engine opens files with `O_NOFOLLOW` to reject symlinks, but the hashing phase calls `path.stat()` and `path.resolve()` which follow symlinks. A file could be hashed via a symlink target but rejected at upload, causing a manifest/upload mismatch.

24. `[DESIGN]` Manifest `paths` are sorted in reverse lexicographic order during creation, but canonical JSON serialization sorts by UTF-16 big-endian byte ordering. These produce the same result for ASCII paths but could diverge for non-ASCII paths.

25. `[SILENT FAILURE]` When `_open_non_symlink_file_binary` fails, it yields `None` and the upload silently skips the file with a warning log. The file remains in the manifest with its hash, but the CAS object is never uploaded. A downstream download would 404.

26. `[DESIGN]` S3 client and transfer manager instances are `@lru_cache`d by session object. All operations within a process share the same connection pool.

## Job Monitoring (`deadline job list/get/cancel/requeue-tasks/wait/logs/trace-schedule/download-output`)

27. `[INCONSISTENCY]` `job list` and `job get` do not support `--output json`, unlike `job wait`, `job logs`, and `job download-output`.

28. `[GAP]` `job cancel` and `job requeue-tasks` do not support `--output json`. No machine-readable confirmation.

29. `[DESIGN]` `job wait` uses exit code 4 for both `SUSPENDED` and `ARCHIVED`. The help text lists `SUSPENDED` but not `ARCHIVED`.

30. `[ASSUMPTION]` `job logs` session auto-selection paginates through all sessions to find the latest. For jobs with many sessions, this could be many API calls. No server-side sorting or filtering by session status exists.

31. `[DESIGN]` `job download-output` cross-OS path remapping prompts regardless of `settings.auto_accept` — no safe default exists. But the MCP `download_job_output` tool sets `auto_accept=true` with verbose mode, so cross-OS downloads via MCP will hang waiting for interactive input.

32. `[GAP]` `job trace-schedule` does not support `--output json`. Summary statistics are plain text only.

33. `[INCONSISTENCY]` `job logs` does not declare `job_id` in `required_options` — it reads `defaults.job_id` directly. If unset, `None` is passed to GetJob, producing a service error instead of a clean usage error.

34. `[DESIGN]` `job wait` verbose output uses `\r` to update in place. Clean in terminals, garbled when piped.

35. `[SILENT FAILURE]` `job wait` extracts session IDs from `latestSessionActionId` by splitting on `-` and assuming `sessionaction-<uuid>-<number>`. If the format changes, extraction silently produces `None`.

36. `[GAP]` `job logs` does not support following/tailing logs in real time. Users must re-run with `--next-token`.
