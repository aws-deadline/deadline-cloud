# Job Attachments — Orchestration & Utilities

> Part of [AWS Deadline Cloud Client — Behavioral Test Specification](index.md)
>
> This document describes observable behavior through public interfaces.
> Error descriptions use category labels (e.g., "returns error") rather than
> language-specific exception types. The Rust implementation should produce
> equivalent observable outcomes, not necessarily identical internal mechanics.

---

## Section 23: Job attachments — asset sync

> **Rust crate:** `deadline-job-attachments` · **Module:** `download`, `upload`
>
> **Logic under test:** High-level sync operations that orchestrate download (sync_inputs)
> and upload (sync_outputs) of job attachments. Handles COPIED vs VIRTUAL file system
> modes, storage profile path mapping, step dependencies, and VFS cleanup.

### `AssetSync.get_s3_settings(farm_id, queue_id) -> optional S3 settings`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Queue has `jobAttachmentSettings` | Returns `JobAttachmentS3Settings` | |
| 2 | Happy path | Queue has no `jobAttachmentSettings` | Returns none | |

### `AssetSync.get_attachments(farm_id, queue_id, job_id) -> optional attachments`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Job has attachments | Returns `Attachments` object | |
| 4 | Happy path | Job has no attachments | Returns none | |

### `AssetSync.sync_inputs(s3_settings, attachments, queue_id, job_id, session_dir, ...) -> (summary stats, list of path mapping rules)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Valid settings and attachments with COPIED file system | Downloads files; returns summary stats and path mapping rules | |
| 6 | Happy path | `s3_settings` is None | Returns empty `SummaryStatistics` and empty list | |
| 7 | Happy path | `attachments` is None | Returns empty `SummaryStatistics` and empty list | |
| 8 | Happy path | Attachments use VIRTUAL file system on Linux with VFS available | Mounts VFS; returns empty summary stats and path mapping rules | |
| 9 | Happy path | Attachments use VIRTUAL but VFS executable not found | Falls back to COPIED download flow | |
| 10 | Happy path | Attachments use VIRTUAL on Windows | Falls back to COPIED download flow | VFS not supported on Windows |
| 11 | Happy path | `storage_profiles_path_mapping_rules` provided | Local root is mapped from source path to destination path | |
| 12 | Error handling | Storage profile mapping missing for a manifest's root path | Returns error "No path mapping rule found" | |
| 13 | Happy path | `step_dependencies` provided | Output manifests from dependent steps are downloaded over inputs | |
| 14 | Happy path | Multiple manifests for same root | Merged into single manifest before download | |
| 15 | Error handling | Download returns 404 | Re-raises with guidance about S3 check cache being out of date | |
| 16 | Happy path | Disk capacity check passes | Download proceeds normally | |
| 17 | Error handling | Insufficient disk capacity | Raises error before download starts | |

### `AssetSync.sync_outputs(s3_settings, attachments, queue_id, job_id, step_id, task_id, session_action_id, ...) -> SummaryStatistics`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | Output files exist and have been modified since session start | Files are uploaded to S3 CAS; manifest uploaded to S3 | |
| 19 | Happy path | `s3_settings` is None | Returns empty `SummaryStatistics` | |
| 20 | Happy path | `attachments` is None | Returns empty `SummaryStatistics` | |
| 21 | Happy path | No output files found | Returns empty `SummaryStatistics` | |
| 22 | Happy path | `storage_profiles_path_mapping_rules` provided | Local root is mapped from source to destination | |
| 23 | Error handling | Storage profile mapping missing for output root path | Returns error (asset sync) | |
| 24 | Happy path | Output manifest S3 key includes session_action_id with timestamp | Correct hierarchical S3 prefix used | |

### `AssetSync.cleanup_session(session_dir, file_system, os_user=None)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | `file_system` is COPIED | Returns immediately (no cleanup needed) | |
| 26 | Happy path | `file_system` is VIRTUAL and VFS found | Kills all VFS processes for the session | |
| 27 | Error handling | `file_system` is VIRTUAL and `os_user` is None | Returns error (VFS OS user not set) | |
| 28 | Happy path | `file_system` is VIRTUAL but VFS executable not found | Logs error; no processes to kill | |

---

## Section 26: Job attachments — path mapping

> **Rust crate:** `deadline-job-attachments` · **Module:** (new) `path_mapping`
>
> **Logic under test:** Generating path mapping rules from source/destination storage
> profiles (matched by location name), and applying them via a TRIE-based transformer
> that selects the most specific (longest) matching rule. Windows paths matched
> case-insensitively.

### `generate_path_mapping_rules(source_storage_profile, destination_storage_profile) -> list[PathMappingRule]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Source and destination profiles have matching location names | Returns one `PathMappingRule` per matching name | |
| 2 | Happy path | Source and destination are the same profile (same storageProfileId) | Returns empty list (no mapping needed) | |
| 3 | Happy path | Source has locations not in destination | Those locations produce no rules | |
| 4 | Happy path | Destination has locations not in source | Those locations produce no rules | |
| 5 | Happy path | Source profile is Windows OS family | Rules use `WINDOWS` source path format | |
| 6 | Happy path | Source profile is Linux/macOS OS family | Rules use `POSIX` source path format | |
| 7 | Boundary values | Both profiles have empty `fileSystemLocations` | Returns empty list | |

### `PathMappingRuleApplier(path_mapping_rules)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Error handling | Rules have mixed source path formats | Returns error "multiple source path formats" | |
| 9 | Error handling | Rule has unexpected source path format | Returns error "Unexpected source path format" | |

### `PathMappingRuleApplier.transform(source_path) -> Union[str, Path]`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | Path matches a rule exactly | Returns the destination path | |
| 11 | Happy path | Path is a child of a rule's source path | Returns destination path joined with remaining path components | |
| 12 | Happy path | Path does not match any rule | Returns the original path unchanged | |
| 13 | Happy path | Two rules: `/mnt/Projects` and `/mnt/Projects/Special` | `/mnt/Projects/Special/data.txt` uses the more specific rule | Most specific (longest) match wins |
| 14 | Happy path | Windows source path with different case | Matches case-insensitively | |
| 15 | Happy path | No rules configured | Returns the original path unchanged | |

### `PathMappingRuleApplier.strict_transform(source_path) -> Path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Happy path | Path matches a rule | Returns the transformed Path | |
| 17 | Error handling | Path does not match any rule | Returns error "No path mapping rule could be applied" | |
| 18 | Error handling | No rules configured (source_path_format is None) | Returns error (invalid value) | |
| 19 | Happy path | Path matches the shorter of two overlapping rules | Returns the longer (more specific) rule's result | |

---

## Section 27: Job attachments — glob & diff

> **Rust crate:** `deadline-job-attachments` · **Module:** (new) `glob`
>
> **Logic under test:** Glob-based file discovery with include/exclude patterns (JSON
> config or file path input), and manifest comparison (NEW/MODIFIED/UNCHANGED/DELETED
> status per file path, compared by hash).

### `process_glob_inputs(glob_arg_input) -> GlobConfig`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Valid JSON string `{"include": ["*.exr"], "exclude": ["*.tmp"]}` | Returns `GlobConfig` with include and exclude patterns | |
| 2 | Happy path | Input is a path to a JSON file | File is read and parsed as JSON | |
| 3 | Boundary values | Input is `None` | Returns default `GlobConfig` (include `["**/*"]`, no exclude) | |
| 4 | Boundary values | Input is empty string | Returns default `GlobConfig` | |
| 5 | Error handling | Input is not valid JSON and not a file path | Returns error (invalid input) | |
| 6 | Happy path | JSON missing `include` key | Uses default include pattern `["**/*"]` | |
| 7 | Happy path | JSON missing `exclude` key | Uses default exclude (None) | |

### `glob_paths(path, include=["**/*"], exclude=None) -> list of strings`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 8 | Happy path | Directory with files matching `**/*` | Returns all files recursively | |
| 9 | Happy path | Include pattern `*.exr` | Returns only `.exr` files | |
| 10 | Happy path | Include `**/*` with exclude `*.tmp` | Returns all files except `.tmp` files | |
| 11 | Boundary values | Empty directory | Returns empty list | |
| 12 | Happy path | Returned paths are normalized | Paths use OS-native separators | |
| 13 | Happy path | Directories are excluded from results | Only files are returned, not directories | |

### `compare_manifest(reference_manifest, compare_manifest) -> list of (status, manifest path)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | File exists in both manifests with same hash | Returns `(UNCHANGED, path)` | |
| 15 | Happy path | File exists in both manifests with different hash | Returns `(MODIFIED, path)` | |
| 16 | Happy path | File exists only in compare manifest | Returns `(NEW, path)` | |
| 17 | Happy path | File exists only in reference manifest | Returns `(DELETED, path)` | |
| 18 | Boundary values | Both manifests are empty | Returns empty list | |
| 19 | Happy path | Multiple files with mixed statuses | Returns correct status for each file | |

### `diff_manifest(asset_manager, asset_root_manifest, manifest, update) -> list of (status, manifest path)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 20 | Happy path | Files have changed since last snapshot | Returns list of `(NEW/MODIFIED, path)` tuples | |
| 21 | Happy path | Files within the manifest directory itself are skipped | Manifest folder contents excluded from diff | |
| 22 | Error handling | `asset_root_manifest.asset_manifest` is None | Returns error (invalid input) | |

---

## Section 32: Job attachments — progress tracking

> **Rust crate:** `deadline-job-attachments` · **Module:** `progress_tracker`
>
> **Logic under test:** Progress reporting during upload/download operations. Tracks
> processed and skipped files/bytes, reports at time intervals or chunk boundaries,
> supports cancellation via callback return value, and computes transfer rate.

### `ProgressTracker(status, total_files, total_bytes, on_progress_callback=None)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Create with callback | Callback is stored; `continue_reporting` is True | |
| 2 | Happy path | Create without callback | Default no-op callback is used; always returns True | |

### `ProgressTracker.increase_processed(num_files, file_bytes)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Increase by 1 file and 1000 bytes | `processed_files` and `processed_bytes` are incremented | |

### `ProgressTracker.increase_skipped(num_files, file_bytes)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Increase by 1 file and 500 bytes | `skipped_files` and `skipped_bytes` are incremented | |

### `ProgressTracker.report_progress() -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Time interval has elapsed since last report | Callback is invoked with progress metadata | |
| 6 | Happy path | Chunk of files completed | Callback is invoked | |
| 7 | Happy path | All files processed (100%) | Callback is invoked | |
| 8 | Concurrency/cancellation | Callback returns False | `continue_reporting` set to False; subsequent calls return False | |
| 9 | Happy path | `continue_reporting` already False | Returns False without invoking callback | |

### `ProgressTracker.get_summary_statistics() -> SummaryStatistics`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | After processing some files | Returns stats with correct totals, processed, skipped, and transfer rate | |
| 11 | Boundary values | `total_time` is 0 | `transfer_rate` is 0.0 | |

### `SummaryStatistics`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 12 | Happy path | `aggregate(other)` with another SummaryStatistics | All fields are summed; transfer_rate is recalculated | |
| 13 | Error handling | `aggregate(other)` with wrong type | Returns error (wrong type) | |
| 14 | Happy path | `str()` representation | Includes processed files, skipped files, total time, and transfer rate | |
| 15 | Happy path | 1 processed file | String uses singular "file" not "files" | |

### `ProgressTracker.track_progress_callback(bytes_amount, current_file_done=False) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 16 | Happy path | Called with bytes and `current_file_done=True` | `processed_bytes` incremented; `processed_files` incremented by 1 | |
| 17 | Happy path | Called with bytes and `current_file_done=False` | `processed_bytes` incremented; `processed_files` unchanged | |

---

## Section 33: Job attachments — exceptions

> **Rust crate:** `deadline-models` · **Module:** `errors`
>
> **Logic under test:** Exception hierarchy and message formatting for S3 client errors,
> transport errors, and sync cancellation. These define the error contract that the Rust
> implementation should replicate in terms of error categories and message content.

### `JobAttachmentsS3ClientError`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Construct with action, status_code, bucket, key, and message | Message includes all fields formatted as "Error {action} in bucket '{bucket}', Target key or prefix: '{key}', HTTP Status Code: {code}, {message}" | |
| 2 | Happy path | Construct without optional message | Message omits the custom message part | |

### `JobAttachmentS3BotoCoreError`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Construct with action and error_details | Message includes guidance about credentials and network | |

### `AssetSyncCancelledError`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Construct with message and summary_statistics | `summary_statistics` attribute is accessible on the error | |
| 5 | Happy path | Construct with message only | `summary_statistics` is none | |

### Error hierarchy

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 6 | Happy path | `JobAttachmentsS3ClientError` is a subtype of `AssetSyncError` | Type hierarchy check passes | |
| 7 | Happy path | `AssetSyncCancelledError` is a subtype of `JobAttachmentsError` | Type hierarchy check passes | |
| 8 | Happy path | All specific errors inherit from either `AssetSyncError` or `JobAttachmentsError` | Correct hierarchy for error matching | |

---

## Section 28: Job attachments: VFS

> **Rust crate:** `deadline-job-attachments` · **Module:** `vfs`
> **⚠️ DEFERRED:** Linux-only, complex subprocess management. Consider deferring for initial Rust port.
>
> **Logic under test:** Virtual File System process management — locating the VFS
> executable and launch script, building launch commands, starting/stopping VFS
> mount processes, PID file tracking, manifest ownership, environment setup,
> mount point creation, log access, and the orchestrator that writes manifests,
> sets permissions, and launches VFS per asset root. Also covers merging manifests
> when re-mounting at an existing mount point.

### `VFSProcessManager(asset_bucket, region, manifest_path, mount_point, os_user, os_env_vars, os_group?, cas_prefix?, asset_cache_path?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Construct with all required parameters | All fields stored; no active process or thread | |
| 2 | Happy path | Construct with optional `cas_prefix` and `asset_cache_path` | Optional fields stored correctly | |
| 3 | Boundary values | Construct without optional parameters | `os_group`, `cas_prefix`, `asset_cache_path` are absent/none | |

### `find_vfs() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Executable found on system PATH | Returns the discovered path; caches result for subsequent calls | |
| 5 | Happy path | Result already cached from prior call | Returns cached path without repeating PATH lookup | |
| 6 | Happy path | Not on PATH but `DEADLINE_VFS_PATH` env var set and `$DEADLINE_VFS_PATH/bin/deadline_vfs` exists | Returns env-var-derived path | |
| 7 | Happy path | Not on PATH, env var not set, but default install location `/opt/deadline_vfs/bin/deadline_vfs` exists | Returns default install path | |
| 8 | Happy path | Not on PATH, env var not set, default missing, but `./bin/deadline_vfs` exists relative to cwd | Returns cwd-relative path | |
| 9 | Error handling | Executable not found by any method | Returns error (executable not found) | Python: `VFSExecutableMissingError` |

### `find_vfs_launch_script() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 10 | Happy path | `DEADLINE_VFS_PATH` env var set and launch script exists at expected relative path | Returns env-var-derived script path; caches result | Script path: `$DEADLINE_VFS_PATH/scripts/production/al2/run_deadline_vfs_al2.sh` |
| 11 | Happy path | Result already cached from prior call | Returns cached path without filesystem check | |
| 12 | Happy path | Env var not set; script exists at default install location | Returns default-install-derived script path | |
| 13 | Error handling | Script not found at env var path or default path | Returns error (launch script not found) | Python: `VFSLaunchScriptMissingError` |

### `build_launch_command(mount_point) -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | No `cas_prefix` or `asset_cache_path` | Command is `sudo -E -u <os_user> <script> <mount_point> -f --clienttype=deadline --bucket=<bucket> --manifest=<path> --region=<region> -oallow_other` | |
| 15 | Happy path | `cas_prefix` provided | Command includes `--casprefix=<value>` appended | |
| 16 | Happy path | `asset_cache_path` provided | Command includes `--cachedir=<value>` appended | |
| 17 | Happy path | Both `cas_prefix` and `asset_cache_path` provided | Command includes both flags | |

### `get_library_path() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 18 | Happy path | Executable at `/opt/deadline_vfs/bin/deadline_vfs` | Returns `/opt/deadline_vfs/lib` | `../lib` relative to executable |
| 19 | Happy path | Result already cached | Returns cached path without calling `find_vfs` again | |

### `get_launch_environ() -> environment map`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 20 | Happy path | Standard construction | Returns map containing provided `os_env_vars`, with `PATH` prepended with VFS link directory and `LD_LIBRARY_PATH` set to library path | |
| 21 | Config interaction | `DEADLINE_VFS_CACHE` env var set in host environment | Launch environment includes `DEADLINE_VFS_CACHE` value from host | |
| 22 | Config interaction | `DEADLINE_VFS_CACHE` env var not set in host environment | Launch environment does not contain `DEADLINE_VFS_CACHE` key | |
| 23 | Happy path | Host environment variables not in provided `os_env_vars` | Those host variables are not passed through to launch environment | Only explicitly provided vars plus VFS-specific vars |

### `create_mount_point(mount_point)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 24 | Happy path | Mount point directory does not exist | Directory created recursively with permissions `0o777` (world-readable/writable/executable) | |
| 25 | Happy path | Mount point directory already exists | No action taken | |

### `set_manifest_owner()`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 26 | Happy path | `os_group` is set and manifest file exists | File group ownership set to `os_group`; file permissions set to `0o640` | |
| 27 | Happy path | `os_group` is not set | No ownership change attempted | |
| 28 | Error handling | Manifest file does not exist on disk | Logs error; returns without raising | |
| 29 | Error handling | Setting group ownership fails (OS error) | Error is re-raised | |

### `start(session_dir)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 30 | Happy path | VFS mounts successfully | Child process spawned via shell; mount point created; PID file written with `mount_point:pid:manifest_path` format; telemetry records success | |
| 31 | Happy path | Child process produces stdout output | Output is captured asynchronously and logged line by line | |
| 32 | Happy path | PID file already exists with entries for other mount points | New entry added; existing entries for different mount points preserved | |
| 33 | Happy path | PID file already has entry for same mount point | Old entry for that mount point replaced with new PID | |
| 34 | Happy path | PID file does not exist yet | File created with single entry | |
| 35 | Error handling | Mount does not appear within timeout | Telemetry records failure; returns error (failed to mount) | Python: `VFSFailedToMountError` |
| 36 | Error handling | Spawning child process fails | Error is re-raised | |
| 37 | Happy path | Working directory of spawned process | Process working directory is `session_dir` | |

### `wait_for_mount(mount_path, session_dir, mount_wait_seconds=60, expected=true) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 38 | Happy path | Mount appears immediately | Returns true | |
| 39 | Happy path | Mount appears after several seconds of polling | Polls once per second; returns true when mount detected | |
| 40 | Error handling | Mount never appears within timeout | Returns false; prints end of VFS log file | |
| 41 | Happy path | `expected=false` and mount disappears within timeout | Returns true | Used during shutdown |
| 42 | Error handling | `expected=false` and mount persists past timeout | Returns false | |

### `is_mount(path) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 43 | Happy path | Path is an active FUSE mount | Returns true | Uses `findmnt` system command |
| 44 | Happy path | Path is not a mount | Returns false | |

### `kill_all_processes(session_dir, os_user)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 45 | Happy path | PID file has two entries | Both mounts are unmounted via `fusermount3 -u`; PID file deleted | |
| 46 | Error handling | PID file does not exist | Logs warning; no error raised | |

### `kill_process_at_mount(session_dir, mount_point, os_user) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 47 | Happy path | Mount point is active and listed in PID file | Mount unmounted; entry removed from PID file; returns true | |
| 48 | Happy path | Mount point not found in PID file | Returns false | |
| 49 | Happy path | Path is not currently a mount | Returns false immediately without reading PID file | |
| 50 | Error handling | PID file does not exist | Logs warning; returns false | |

### `get_manifest_path_for_mount(session_dir, mount_point) -> optional path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 51 | Happy path | Mount point found in PID file and manifest file exists on disk | Returns path to manifest | |
| 52 | Happy path | Mount point found but manifest file does not exist on disk | Logs warning; returns none | |
| 53 | Happy path | Mount point not found in PID file | Logs warning; returns none | |
| 54 | Error handling | PID file does not exist | Logs warning; returns none | |

### `shutdown_libfuse_mount(mount_path, os_user, session_dir) -> bool`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 55 | Happy path | Unmount command succeeds and mount disappears | Returns true | |
| 56 | Error handling | Unmount command fails | Logs warning; still checks if mount is gone via `wait_for_mount` | |
| 57 | Error handling | `fusermount3` binary not found at expected location | Returns false | ⚠️ Code may reference uninitialized result variable if `get_shutdown_args` returns none. Verify intent. |

### `get_shutdown_args(mount_path, os_user) -> optional list of strings`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 58 | Happy path | `fusermount3` exists in VFS link directory | Returns `["sudo", "-u", <os_user>, <fusermount3_path>, "-u", <mount_path>]` | |
| 59 | Error handling | `fusermount3` not found in VFS link directory | Returns none | |

### `get_cwd() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 60 | Happy path | Executable at `/opt/deadline_vfs/bin/deadline_vfs` | Returns `/opt/deadline_vfs` (parent of `bin/`) | |
| 61 | Happy path | Result already cached | Returns cached path | |

### `find_vfs_link_dir() -> string`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 62 | Happy path | Executable at `/opt/deadline_vfs/bin/deadline_vfs` | Returns `/opt/deadline_vfs/link` | `../link` relative to executable |

### `logs_folder_path(session_dir) -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 63 | Happy path | Any session directory | Returns `<session_dir>/.vfs_logs` | |

### `get_logs_folder() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 64 | Happy path | Called after `start()` has set the run path | Returns `<run_path>/.vfs_logs` | |
| 65 | Error handling | Called before `start()` (run path not set) | Returns error (run path not set) | Python: `VFSRunPathNotSetError` |

### `print_log_end(session_dir, log_file_name="vfs_log.txt", lines=100, log_level=WARNING)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 66 | Happy path | Log file exists with more than 100 lines | Last 100 lines logged at specified level | |
| 67 | Boundary values | Log file exists with fewer than 100 lines | All lines logged | |
| 68 | Error handling | Log file does not exist | Logs warning; returns without error | |

### `get_file_path(relative_file_name) -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 69 | Happy path | Relative file name provided | Returns `<mount_point>/<relative_file_name>` | |

### `get_mount_point() -> path`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 70 | Happy path | After construction | Returns the mount point passed to constructor | |

### `mount_vfs_from_manifests(s3_bucket, manifests_by_root, session, session_dir, os_env_vars, fs_permission_settings, cas_prefix?)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 71 | Happy path | Single manifest root | VFS cache directory created; manifest written to `.vfs_manifests` folder; VFS logs directory created; VFS process started for that root | |
| 72 | Happy path | Multiple manifest roots | One VFS process started per root | |
| 73 | Happy path | `cas_prefix` provided | Cache path includes CAS prefix as subdirectory; path validated to be within cache root | |
| 74 | Happy path | `cas_prefix` is absent | Cache path is `<session_dir>/.vfs_object_cache` directly | |
| 75 | Error handling | File system permission settings are not POSIX | Returns error (VFS only supported on POSIX) | |
| 76 | Error handling | Manifest contains a file path that escapes the mount point root | Returns error (path outside directory) | |
| 77 | Happy path | Mount point already has an active VFS mount | Existing manifest merged with new manifest; old VFS process killed; new VFS started with merged manifest | Via `handle_existing_vfs` |
| 78 | Happy path | Directory permissions applied | Group ownership set on cache dir, manifest dir, and logs dir | |

### `handle_existing_vfs(manifest, session_dir, mount_point, os_user) -> manifest`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 79 | Happy path | Mount point is not currently mounted | Returns original manifest unchanged | |
| 80 | Happy path | Mount point is mounted and existing input manifest found | Merges existing and new manifests; kills existing VFS process; returns merged manifest | |
| 81 | Error handling | Mount point is mounted but input manifest path not found on disk | Logs error; returns original manifest unchanged | |
| 82 | Boundary values | Merge of existing and new manifests returns none | Returns original manifest unchanged | |

### Constants & configuration

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 83 | Happy path | VFS path environment variable name | Value is `"DEADLINE_VFS_PATH"` | |
| 84 | Happy path | Default VFS install path | Value is `"/opt/deadline_vfs"` | |
| 85 | Happy path | VFS executable name | Value is `"deadline_vfs"` | |
| 86 | Happy path | VFS manifest folder directory permission mode | Value is `0o31` | |
| 87 | Happy path | VFS manifest folder file permission mode | Value is `0o64` | |
| 88 | Happy path | Manifest group-read permission mask | Value is `0o640` | |

### Platform support

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 89 | Error handling | VFS operations attempted on Windows | VFS is not supported; system falls back to COPIED file transfer mode | Enforced at the asset sync orchestration layer, not within VFS module directly |

> ✅ Complete (89 cases)

---

## Section 29: Job attachments: OS file permissions

> **Rust crate:** `deadline-job-attachments` · **Module:** (new) `permissions`
>
> **Logic under test:** Setting file and directory ownership and permissions after
> downloading job attachments. Two platform-specific flows: POSIX (sets group
> ownership and bitwise-ORs a permission mode onto files and directories) and
> Windows (grants a named user access rights on files and directories via access
> control entries). Both flows validate that file paths reside within the specified
> root directory before applying changes, and both collect unique parent directories
> to apply directory-level permissions.

### Data structures

#### `PosixFileSystemPermissionSettings(os_user, os_group, dir_mode, file_mode)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 1 | Happy path | Construct with user, group, dir_mode=0o770, file_mode=0o660 | All four fields stored and accessible | |
| 2 | Boundary values | dir_mode or file_mode is 0 | Fields stored as 0; OR-ing with existing permissions produces no change | |

#### `WindowsPermissionEnum`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 3 | Happy path | Enum has variants READ, WRITE, EXECUTE, READ_WRITE, FULL_CONTROL | All five variants exist with string values matching their names | |

#### `WindowsFileSystemPermissionSettings(os_user, dir_mode, file_mode)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 4 | Happy path | Construct with user, dir_mode=READ_WRITE, file_mode=READ | All three fields stored and accessible | |

### `set_fs_group_for_posix(file_paths, local_root, permission_settings)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 5 | Happy path | Single file under local_root | File group ownership set to configured group; file permissions OR'd with file_mode | |
| 6 | Happy path | Single file nested several directories deep | File permissions applied; every parent directory up to and including local_root has group ownership set and permissions OR'd with dir_mode | |
| 7 | Happy path | Multiple files sharing a common parent directory | Shared parent directory permissions applied only once (deduplicated) | |
| 8 | Happy path | Multiple files in different subdirectories | Each unique directory in the path hierarchy up to local_root receives dir_mode permissions | |
| 9 | Happy path | File directly inside local_root (no intermediate directories) | File permissions applied; local_root itself receives dir_mode permissions | |
| 10 | Error handling | File path is outside local_root | Returns error (path outside directory) | |
| 11 | Error handling | File path uses `..` segments to escape local_root | Returns error (path outside directory) | Path is resolved before the containment check |
| 12 | Boundary values | Empty file_paths list | No permissions changed; no error raised | |
| 13 | Happy path | File already has permissions 0o644 and file_mode is 0o060 | Resulting permissions are 0o664 | Permissions are additive, never removed |

### `set_fs_permission_for_windows(file_paths, local_root, permission_settings)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 14 | Happy path | Single file under local_root | Configured user granted file_mode access on the file | |
| 15 | Happy path | Single file nested several directories deep | User granted file_mode on file; user granted dir_mode on every parent directory up to local_root | |
| 16 | Happy path | Multiple files sharing a common parent directory | Shared parent directory access granted only once (deduplicated) | |
| 17 | Error handling | File path is outside local_root | Returns error (path outside directory) | |
| 18 | Boundary values | Empty file_paths list | No permissions changed; no error raised | |
| 19 | Happy path | File path has extended-length prefix (`\\?\`) | Prefix stripped before computing relative parent directories; parent paths computed correctly | |

### `change_permission_for_posix(path, os_group, mode)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 20 | Happy path | Valid path, group, and mode on a POSIX system | File group ownership changed; file permission bits OR'd with mode | |
| 21 | Error handling | Called on a Windows system | Returns error (wrong platform: POSIX only) | |
| 22 | Error handling | Group name does not exist on the system | Returns OS-level error (group not found) | |
| 23 | Error handling | Path does not exist | Returns OS-level error (file not found) | |
| 24 | Error handling | Caller lacks permission to change ownership | Returns OS-level error (permission denied) | |

### `change_permission_for_windows(path, os_user, mode)`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 25 | Happy path | Valid path, user, and mode on a Windows system | User granted the requested access on the file or directory | |
| 26 | Error handling | Called on a non-Windows system | Returns error (wrong platform: Windows only) | |
| 27 | Error handling | User name does not exist on the system | Returns error (asset sync: failed to set permissions) | |
| 28 | Error handling | Path does not exist | Returns error (asset sync: failed to set permissions) | |
| 29 | Error handling | Caller lacks permission to modify access control | Returns error (asset sync: failed to set permissions) | |
| 30 | Happy path | File has no existing access control list | New list created containing the granted access; applied to the file | |
| 31 | Happy path | File already has existing access entries for other users | New entry appended; previous entries preserved | |

### `get_ntsecuritycon_mode(mode) -> integer`

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 32 | Happy path | mode=READ | Returns the platform constant for generic read access | |
| 33 | Happy path | mode=WRITE | Returns the platform constant for generic write access | |
| 34 | Happy path | mode=EXECUTE | Returns the platform constant for generic execute access | |
| 35 | Happy path | mode=READ_WRITE | Returns generic read OR'd with generic write | |
| 36 | Happy path | mode=FULL_CONTROL | Returns the platform constant for full access | |
| 37 | Error handling | Called on a non-Windows system | Returns error (wrong platform: Windows only) | |

### Cross-cutting: permission settings union type

| # | Category | Test Case | Expected Behavior | Notes |
|---|----------|-----------|-------------------|-------|
| 38 | Happy path | POSIX permission settings accepted wherever the union type is expected | Callers that accept either platform's settings work with POSIX variant | |
| 39 | Happy path | Windows permission settings accepted wherever the union type is expected | Callers that accept either platform's settings work with Windows variant | |

> ✅ Complete (39 cases)

---
