# Storage Profile Support in CLI Commands

## 1. Issue Reference
GitHub Issue: [#829 - Enhancement: job attachments interface does not use storage profiles by default](https://github.com/aws-deadline/deadline-cloud/issues/829)

## 2. Executive Summary

This document provides an analysis of how storage profiles are used throughout the deadline-cloud CLI, tracing code paths from CLI commands down to file hashing and S3 upload/download operations. The goal is to identify gaps in storage profile support and propose enhancements.

---

## 3. Background

### 3.1 Storage Profile Configuration

Storage profiles are configured in `~/.deadline/config` under the setting `settings.storage_profile_id`. A storage profile describes the current host's file system layout (shared mount points, local scratch paths, OS family). It is scoped under `farm_id` rather than `queue_id` because the host is the same regardless of which queue it accesses within a farm. The configuration hierarchy is:

```
defaults.aws_profile_name
  └── defaults.farm_id
        └── settings.storage_profile_id  (depends on farm_id, not queue — host is the same across queues)
        └── defaults.queue_id
```

**Source:** `src/deadline/client/config/config_file.py`

### 3.2 CLI Commands with Storage Profile Support

| Command | `--storage-profile-id` | `--ignore-storage-profiles` | Notes |
|---------|:----------------------:|:---------------------------:|-------|
| `deadline bundle submit` | ✅ | N/A | Reads `settings.storage_profile_id` from config or `--storage-profile-id` CLI option. Passed to `create_job_from_job_bundle()` which attaches it to the CreateJob API call and uses it for path grouping during upload. If not set, submission proceeds without a storage profile. No ignore flag needed — uploads don't require path mapping. |
| `deadline bundle gui-submit` | N/A | N/A | GUI auto-populates a storage profile dropdown by listing profiles for the selected queue, filtered to the current OS. Pre-selects from `settings.storage_profile_id` in config. User can change selection before submitting. No ignore flag needed — uploads don't require path mapping. |
| `deadline queue sync-output` | ✅ | ✅ | Full support (reference implementation). See §4.2 for code details. |
| `deadline job download-output` | ❌ → ✅ | ❌ → ✅ | **Gap (proposed in this design)** — currently prompts for manual path entry on OS mismatch. This design adds `--ignore-storage-profiles` and automatic path mapping via config. |

### 3.3 Existing `--ignore-storage-profiles` Implementation

The `deadline queue sync-output` command already implements `--ignore-storage-profiles`:

**File:** `src/deadline/client/cli/_groups/queue_group.py`

```python
@click.option(
    "--ignore-storage-profiles",
    is_flag=True,
    help="Ignores the storage profile configuration. Only use if all jobs in the queue "
         "are submitted and downloaded from the same machine. Downloads all jobs to "
         "unmapped paths regardless of operating system.\n"
         "Default value is False.",
    default=False,
)
```

This option is used when:
- All submitting and downloading machines share the **same OS and mount points**
- No path mapping is needed
- Downloads go to the original unmapped paths

The proposal is to extend this existing pattern to `deadline job download-output`.

For a code deep dive into the upload path (job submission), see §12 (Appendix C).

---

## 4. Download Code Path

### 4.1 `deadline job download-output` (Single Job)

**File:** `src/deadline/client/cli/_groups/job_group.py`

```
CLI Command (job_download_output)
    │
    ├── No --storage-profile-id option!
    │
    └── _download_job_output()
            │
            ├── deadline.get_job() → get attachments metadata
            │
            ├── OutputDownloader.__init__()
            │       └── get_job_output_paths_by_asset_root()
            │
            ├── OS Mismatch Detection:
            │       │
            │       └── If rootPathFormat != host format:
            │               └── PROMPT USER for new root path (manual entry)
            │
            └── OutputDownloader.download_job_output()
```

#### 4.1.1 Current OS Mismatch Handling

```python
# Check if the asset roots came from different OS
for asset_root in asset_roots:
    root_path_format = root_path_format_mapping.get(asset_root, "")
    if PathFormat.get_host_path_format_string() != root_path_format:
        click.echo(_get_mismatch_os_root_warning(...))
        
        # Manual prompt - NO storage profile support!
        new_root = click.prompt(
            "> Please enter a new root path",
            type=click.Path(exists=False),
        )
        job_output_downloader.set_root_path(asset_root, os.path.expanduser(new_root))
```

**This is the gap identified in issue #829** - no automatic path mapping via storage profiles.

### 4.2 `deadline queue sync-output` — Reference Implementation

This is the reference implementation for storage profile support in a download command. The proposed changes in §8 follow this same pattern for `job download-output`.

**File:** `src/deadline/client/cli/_groups/queue_group.py`

```
CLI Command (sync_output)
    │
    ├── --storage-profile-id option ✅
    ├── --ignore-storage-profiles option ✅
    │
    ├── Validate storage profile exists
    │
    └── _incremental_output_download()
```

#### 4.2.1 Storage Profile Validation

```python
if ignore_storage_profiles:
    local_storage_profile_id = None
else:
    local_storage_profile_id = config_file.get_setting(
        "settings.storage_profile_id", config=config
    )
    if not local_storage_profile_id:
        raise DeadlineOperationError(
            "The sync-output operation requires a storage profile..."
        )
    
    # Validate profile exists
    local_storage_profile = deadline.get_storage_profile_for_queue(
        farmId=farm_id,
        queueId=queue_id,
        storageProfileId=local_storage_profile_id,
    )
```

### 4.3 Incremental Download Implementation

**File:** `src/deadline/client/cli/_incremental_download.py`

```
_incremental_output_download()
    │
    ├── _get_download_candidate_jobs()
    │       └── SearchJobs API to find jobs with outputs
    │
    ├── _categorize_jobs_in_checkpoint()
    │       └── Categorize jobs: added, updated, unchanged, completed, etc.
    │
    ├── _get_job_sessions()
    │       └── ListSessions API for each job
    │
    ├── _get_storage_profiles()
    │       └── GetStorageProfileForQueue for each unique profile
    │
    ├── _create_path_mapping_rule_appliers()
    │       │
    │       └── For each job's storage profile:
    │               │
    │               └── _generate_path_mapping_rules(
    │                       source_storage_profile=job_profile,
    │                       destination_storage_profile=local_profile
    │                   )
    │
    └── Download with path mapping applied
```

### 4.4 Path Mapping Implementation

**File:** `src/deadline/job_attachments/_path_mapping.py`

#### 4.4.1 Rule Generation

```python
def _generate_path_mapping_rules(
    source_storage_profile: dict[str, Any],
    destination_storage_profile: dict[str, Any],
) -> list[PathMappingRule]:
    # If same profile, no mapping needed
    if source_storage_profile["storageProfileId"] == \
       destination_storage_profile["storageProfileId"]:
        return []
    
    # Match file system locations by NAME
    source_locations = {loc["name"]: loc for loc in source_storage_profile["fileSystemLocations"]}
    dest_locations = {loc["name"]: loc for loc in destination_storage_profile["fileSystemLocations"]}
    
    # Generate rules for matching names
    for source_name, source_loc in source_locations.items():
        if source_name in dest_locations:
            rules.append(PathMappingRule(
                source_path_format,
                source_loc["path"],
                dest_locations[source_name]["path"],
            ))
```

#### 4.4.2 Rule Application (Trie-based)

```python
class _PathMappingRuleApplier:
    """
    Uses a trie data structure for efficient path transformation.
    
    Example rules:
        '/mnt/Projects -> X:\\Projects'
        '/mnt/Projects/Special -> Y:\\'
    
    Path '/mnt/Projects/Special/data.txt' maps to 'Y:\\data.txt'
    (most specific rule wins)
    """
```

### 4.5 Download File Operations

**File:** `src/deadline/job_attachments/download.py`

#### 4.5.1 OutputDownloader Class

```python
class OutputDownloader:
    def __init__(
        self,
        s3_settings: JobAttachmentS3Settings,
        farm_id: str,
        queue_id: str,
        job_id: str,
        step_id: Optional[str] = None,
        task_id: Optional[str] = None,
        session_action_id: Optional[str] = None,
        session: Optional[boto3.Session] = None,
    ) -> None:
        # NOTE: No storage_profile parameter!
        self.outputs_by_root = get_job_output_paths_by_asset_root(...)
```

#### 4.5.2 File Download

```python
def download_files(
    files: list[RelativeFilePath],
    hash_algorithm: HashAlgorithm,
    local_download_dir: str,
    s3_settings: JobAttachmentS3Settings,
    session: Optional[boto3.Session] = None,
    progress_tracker: Optional[ProgressTracker] = None,
    file_conflict_resolution: Optional[FileConflictResolution] = ...,
) -> list[str]:
    # Parallel download using ThreadPoolExecutor
    return _download_files_parallel(...)
```

---

## 5. Data Models

### 5.1 StorageProfile

**File:** `src/deadline/job_attachments/models.py`

```python
@dataclass
class StorageProfile:
    storageProfileId: str
    displayName: str
    osFamily: StorageProfileOperatingSystemFamily  # WINDOWS, LINUX, MACOS
    fileSystemLocations: List[FileSystemLocation]
```

### 5.2 FileSystemLocation

**File:** `src/deadline/job_attachments/models.py`

```python
@dataclass
class FileSystemLocation:
    name: str   # Logical name (e.g., "ProjectFiles")
    path: str   # Physical path (e.g., "/mnt/projects" or "X:\\Projects")
    type: FileSystemLocationType  # SHARED or LOCAL
```

### 5.3 PathMappingRule

```python
@dataclass
class PathMappingRule:
    source_path_format: str  # "posix" or "windows"
    source_path: str
    destination_path: str
```

---

## 6. Identified Gaps

### 6.1 `deadline job download-output` lacks storage profile support

**Current behavior:**
- Detects OS mismatch between job submission and download
- Prompts user to manually enter a new root path
- No automatic path mapping

**Desired behavior:**
- Accept `--ignore-storage-profiles` option
- Automatically read storage profile from config (`settings.storage_profile_id`)
- Automatically generate path mapping rules when both profiles exist
- Apply mappings without user prompts
- Fall back to manual prompt with guidance when profiles are missing or mismatched

### 6.2 `OutputDownloader` has a stale TODO for path mapping

The `OutputDownloader` class contains this TODO:

```python
# TODO: The download location is OS-specific to the *submitting machine* matching
# the profile of the submitting machine. The OS of the *downloading machine* might be different,
# so we need to check that and apply path mapping rules in that case.
```

Both download commands already handle path mapping at the CLI layer using `_PathMappingRuleApplier` (trie-based), not inside `OutputDownloader`:

- `queue sync-output` transforms every individual file path (joins `rootPath + relativePath`, then calls `strict_transform()`) inside `_download_all_manifests_with_absolute_paths()`. It does not use `OutputDownloader` at all — it has its own manifest-based download pipeline.
- `job download-output` (this proposal) transforms root paths via `OutputDownloader.set_root_path()`, then `OutputDownloader` joins root + relative internally during download.

These are equivalent in outcome because each manifest has a single root path — transforming the root produces the same final absolute paths as transforming each `root + relative` individually. **Update (§13):** This equivalence only holds when all rules match exactly at the root level. With nested file system locations, a rule can match deeper than the root, and root-only transformation would miss it. The implementation now uses absolute-path transformation (joining root + relative before applying rules) when path mapping rules exist, matching `sync-output` behavior. See §13 for details.

Since both download commands handle path mapping externally using the same trie, this TODO is stale and should be removed as part of Stage 3 (§8.3). `OutputDownloader` remains a download-only concern with no path mapping responsibility.

### 6.3 `_generate_path_mapping_rules` accepts raw dicts, not `StorageProfile` dataclass

The existing `_generate_path_mapping_rules()` in `_path_mapping.py` accepts `dict[str, Any]` (raw boto3 response shape). The typed API helper `api.get_storage_profile_for_queue()` returns a `StorageProfile` dataclass. The `sync-output` path sidesteps this by using the raw boto3 client directly.

The function is underscore-prefixed (private) and only consumed by `_incremental_download.py` and tests — so its signature can be changed freely. Rather than adding an adapter, we can refactor `_generate_path_mapping_rules` to accept `StorageProfile` directly and update the two call sites. This is a small change that eliminates the type mismatch at the source. See §8.2.3 for the updated approach.

### 6.4 `S3AssetManager` doesn't auto-fetch storage profile (future work)

**Current behavior:**
- `S3AssetManager.__init__()` takes `farm_id` and `queue_id`
- `prepare_paths_for_upload()` requires `storage_profile` as optional parameter
- Caller must fetch and pass storage profile

This is the broader ask from issue #829. It is out of scope for this proposal and tracked separately.

---

## 7. Decision Matrix: Storage Profile Combinations

When downloading, the behavior depends on whether the local machine has a storage profile configured and whether the job was submitted with one:

| Local profile configured? | Job has `storageProfileId`? | Action |
|:---:|:---:|---|
| Yes | Yes | Generate path mapping rules, apply automatically |
| Yes (no matching names) | Yes | Warn about mismatched location names, recommend aligning storage profiles, fall back to manual prompt |
| Yes | No | Warn: job was submitted without a storage profile, fall back to manual prompt |
| No | Yes | Warn: recommend configuring a storage profile, fall back to manual prompt |
| No | No | No mapping needed, proceed as-is (same-machine case) |
| `--ignore-storage-profiles` | Any | Skip all mapping, download to original paths |

All mismatch/missing cases now fall back to the existing manual prompt rather than erroring, with a log message recommending storage profile setup.

---

## 8. Proposed Changes

### 8.1 Stage 1: Add CLI options and wiring (small, reviewable)

Add `--ignore-storage-profiles` to `job download-output`, wire it through to `_download_job_output()`.

#### 8.1.1 Add CLI Options

**File:** `src/deadline/client/cli/_groups/job_group.py`

Add one new option to the existing `job_download_output` click command:

```python
@click.option(
    "--ignore-storage-profiles",
    is_flag=True,
    help="Ignores the storage profile configuration. Only use if the job was "
         "submitted and downloaded from the same machine. Downloads to "
         "unmapped paths regardless of operating system.\n"
         "Default value is False.",
    default=False,
)
```

The local storage profile is read from `settings.storage_profile_id` in the config (set via `deadline config`). No explicit `--storage-profile-id` override is needed for `download-output` — the config setting is sufficient.

#### 8.1.2 Update `job_download_output()` entry point

Pass `ignore_storage_profiles` through to `_download_job_output()`:

```python
def job_download_output(step_id, task_id, output, ignore_storage_profiles, **args):
    # ... existing validation ...
    config = _apply_cli_options_to_config(
        required_options={"farm_id", "queue_id", "job_id"}, **args
    )
    # ... existing config reads ...
    try:
        _download_job_output(
            config, farm_id, queue_id, job_id, step_id, task_id,
            is_json_format,
            ignore_storage_profiles=ignore_storage_profiles,
        )
```

#### 8.1.3 Add `ignore_storage_profiles` parameter to `_download_job_output()`

```python
def _download_job_output(
    config: Optional[ConfigParser],
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: Optional[str],
    task_id: Optional[str],
    is_json_format: bool = False,
    ignore_storage_profiles: bool = False,  # NEW
):
```

At this stage, the parameter is accepted but not yet used — the existing manual prompt behavior is preserved. This makes the CLI change independently reviewable.

---

### 8.2 Stage 2: Single-responsibility helper functions

Add the core logic as small, testable functions. Each function has a single job and stays under 100 lines.

**File:** `src/deadline/client/cli/_groups/_job_download_helpers.py` (new file)

#### 8.2.1 `_resolve_storage_profiles()`

Fetches the local and job storage profiles. Returns `(None, None)` when ignoring profiles. Handles all error cases from the decision matrix.

```python
@dataclass
class ResolvedStorageProfiles:
    """The result of resolving storage profiles for a download operation."""
    job_profile: StorageProfile       # profile the job was submitted with (source paths)
    local_profile: StorageProfile     # profile on this machine (destination paths)


def _resolve_storage_profiles(
    config: Optional[ConfigParser],
    deadline: "DeadlineClient",
    farm_id: str,
    queue_id: str,
    job: dict[str, Any],
    ignore_storage_profiles: bool,
) -> Optional[ResolvedStorageProfiles]:
    """Resolve the storage profiles needed to map a job's output paths to local paths.

    The job_profile is where paths came from (the submitting machine).
    The local_profile is where paths should go (this machine).

    Returns:
        ResolvedStorageProfiles if path mapping is needed, None otherwise.
    """
    if ignore_storage_profiles:
        return None

    local_storage_profile_id = config_file.get_setting(
        "settings.storage_profile_id", config=config
    )
    job_storage_profile_id = job.get("storageProfileId")

    if not local_storage_profile_id and not job_storage_profile_id:
        # Same-machine case: no profiles on either side
        return None

    if not local_storage_profile_id and job_storage_profile_id:
        # Consistent with queue sync-output which always requires a local storage profile.
        click.echo(
            "Warning: The job was submitted with a storage profile but no local storage "
            "profile is configured. Path mapping will be skipped.\n\n"
            "Options:\n"
            "  1. Configure a storage profile: deadline config set "
            "settings.storage_profile_id <id>\n"
            "  2. Skip path mapping (same-machine only): --ignore-storage-profiles\n\n"
            "See https://docs.aws.amazon.com/deadline-cloud/latest/developerguide/"
            "modeling-your-shared-filesystem-locations-with-storage-profiles.html"
        )
        return None

    if local_storage_profile_id and not job_storage_profile_id:
        click.echo(
            "Warning: A local storage profile is configured but the job was submitted "
            "without one. Path mapping will be skipped."
        )
        return None

    # Both profiles exist — fetch them
    local_profile = api.get_storage_profile_for_queue(
        farm_id, queue_id, local_storage_profile_id, deadline, config=config
    )
    job_profile = api.get_storage_profile_for_queue(
        farm_id, queue_id, job_storage_profile_id, deadline, config=config
    )

    return ResolvedStorageProfiles(job_profile=job_profile, local_profile=local_profile)
```

Note: Both profiles are fetched via the typed API (`api.get_storage_profile_for_queue`) which returns `StorageProfile` dataclass, since `_generate_path_mapping_rules()` is refactored to accept `StorageProfile` directly (see §6.3).

#### 8.2.2 Refactor `_generate_path_mapping_rules()` to accept `StorageProfile`

Since the function is private (underscore-prefixed) with only two call sites (`_incremental_download.py` and the new `_job_download_helpers.py`), we refactor it directly instead of adding an adapter.

**File:** `src/deadline/job_attachments/_path_mapping.py`

```python
def _generate_path_mapping_rules(
    source_storage_profile: StorageProfile,
    destination_storage_profile: StorageProfile,
) -> list[PathMappingRule]:
    """
    Given a pair of storage profiles, generate all the path mapping rules to
    transform paths from the source to the destination.
    """
    if source_storage_profile.storageProfileId == destination_storage_profile.storageProfileId:
        return []

    source_locations = {
        loc.name: loc for loc in source_storage_profile.fileSystemLocations
    }
    destination_locations = {
        loc.name: loc for loc in destination_storage_profile.fileSystemLocations
    }

    if source_storage_profile.osFamily == StorageProfileOperatingSystemFamily.WINDOWS:
        source_path_format = PathFormat.WINDOWS.value
    else:
        source_path_format = PathFormat.POSIX.value

    path_mapping_rules: list[PathMappingRule] = []
    for source_name, source_location in source_locations.items():
        if source_name in destination_locations:
            path_mapping_rules.append(
                PathMappingRule(
                    source_path_format,
                    source_location.path,
                    destination_locations[source_name].path,
                )
            )

    return path_mapping_rules
```

**Call site updates required:**

1. `_incremental_download.py` — update `_get_storage_profiles()` to use the typed API (`api.get_storage_profile_for_queue`) instead of raw boto3, and pass `StorageProfile` objects to `_generate_path_mapping_rules()`.
2. `test_path_mapping.py` — update test fixtures to use `StorageProfile` dataclass instead of raw dicts.

#### 8.2.3 `_apply_path_mappings_to_roots()`

Applies generated path mapping rules to the output downloader's root paths.

```python
def _apply_path_mappings_to_roots(
    job_output_downloader: OutputDownloader,
    output_paths_by_root: dict[str, list[str]],
    rules: list[PathMappingRule],
) -> None:
    """Apply path mapping rules to remap output root directories.

    Modifies the downloader in-place via set_root_path().
    """
    if not rules:
        return

    applier = _PathMappingRuleApplier(rules)
    for original_root in list(output_paths_by_root.keys()):
        mapped_root = applier.transform(original_root)
        if str(mapped_root) != original_root:
            click.echo(f"  Mapping root: {original_root} -> {mapped_root}")
            job_output_downloader.set_root_path(original_root, str(mapped_root))
```

---

### 8.3 Stage 3: Integrate helpers into `_download_job_output()`

Wire the helper functions into the existing download flow. The key change is replacing the manual OS-mismatch prompt block with automatic path mapping when storage profiles are available.

#### 8.3.1 Updated flow in `_download_job_output()`

Insert after `OutputDownloader` construction and `output_paths_by_root` retrieval, replacing the existing OS mismatch block:

```python
    # --- Storage profile path mapping (replaces manual OS mismatch prompt) ---
    resolved = _resolve_storage_profiles(
        config, deadline, farm_id, queue_id, job, ignore_storage_profiles
    )

    if resolved:
        # Automatic path mapping via storage profiles
        rules = _generate_path_mapping_rules(resolved.job_profile, resolved.local_profile)
        click.echo(f"Using storage profile: {resolved.local_profile.displayName}")
        _apply_path_mappings_to_roots(job_output_downloader, output_paths_by_root, rules)
        output_paths_by_root = job_output_downloader.get_output_paths_by_root()
    else:
        # No storage profiles — fall back to manual prompt on OS mismatch
        # (existing behavior, unchanged)
        asset_roots = list(output_paths_by_root.keys())
        for asset_root in asset_roots:
            root_path_format = root_path_format_mapping.get(asset_root, "")
            if root_path_format == "":
                raise DeadlineOperationError(
                    f"No root path format found for {asset_root}."
                )
            if PathFormat.get_host_path_format_string() != root_path_format:
                click.echo(
                    _get_mismatch_os_root_warning(
                        asset_root, root_path_format, is_json_format
                    )
                )
                # ... existing manual prompt code (unchanged) ...
```

The rest of `_download_job_output()` (conflict resolution, progress bar, download) remains unchanged.

#### 8.3.2 Entry point

The `job_download_output()` function passes `ignore_storage_profiles` through to `_download_job_output()`. No validation needed since `--storage-profile-id` is not an option on this command.

---

## 9. Testing

### 9.1 Test Cases

Each case traces the full code path from CLI entry through to download. All cases are implemented in `test/unit/deadline_client/cli/test_cli_job_download_storage_profiles.py`.

#### Case 1: Both profiles exist, location names match → automatic mapping

Local machine has `sp-local-111` (Linux, locations: `shared=/mnt/shared`, `temp=/tmp/render`).
Job was submitted with `sp-job-222` (Windows, locations: `shared=Z:\shared`, `temp=C:\temp\render`).

```
job_download_output()
  _validate_storage_profile_options(None, False)           → passes
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(config, deadline, farm, queue, job, False)
      config_file.get_setting("settings.storage_profile_id") → "sp-local-111"
      job.get("storageProfileId")                            → "sp-job-222"
      Both truthy → fetch both via api.get_storage_profile_for_queue()
      → ResolvedStorageProfiles(job_profile=sp-job-222, local_profile=sp-local-111)
    resolved is not None → enters `if resolved:` branch
    _generate_path_mapping_rules(sp-job-222, sp-local-111)
      _normalize_storage_profile() on each
      IDs differ → match locations by name
      "shared" matches → PathMappingRule(WINDOWS, "Z:\shared", "/mnt/shared")
      "temp" matches   → PathMappingRule(WINDOWS, "C:\temp\render", "/tmp/render")
      → [rule1, rule2]
    _apply_path_mappings_to_roots(downloader, roots, [rule1, rule2])
      _PathMappingRuleApplier([rule1, rule2])  → builds trie
      transform("C:\temp\render") → "/tmp/render" → set_root_path()
      transform("Z:\shared")      → "/mnt/shared" → set_root_path()
    download proceeds to mapped local paths
```

Expected: roots are remapped, user sees "Using storage profile: Local Linux Profile" and "Mapping root: ..." messages. Download goes to `/mnt/shared` and `/tmp/render`.

#### Case 2: Both profiles exist, location names don't match → no rules, no mapping

Local machine has `sp-local-111` (locations: `shared`, `temp`).
Job was submitted with `sp-job-333` (locations: `ProjectFiles`, `OutputDir`).

```
job_download_output()
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(...)
      Both IDs present → fetch both → ResolvedStorageProfiles
    resolved is not None → enters `if resolved:` branch
    _generate_path_mapping_rules(sp-job-333, sp-local-111)
      IDs differ → match locations by name
      "ProjectFiles" not in {"shared", "temp"}, "OutputDir" not in {"shared", "temp"}
      → []  (empty)
    _apply_path_mappings_to_roots(downloader, roots, [])
      `if not rules:` → prints warning about mismatched location names → return
    output_paths_by_root re-read (unchanged)
    user sees root confirmation prompt (if not auto_accept)
    download proceeds to original unmapped paths
```

Expected: no mapping applied. User sees "Using storage profile: ..." and a warning about mismatched location names recommending alignment. If OS mismatch exists, the manual root editing prompt appears as before.

#### Case 3: Job has profile, local machine does not → warning, manual fallback

Local machine has no storage profile configured.
Job was submitted with `sp-job-222`.

```
job_download_output()
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(...)
      config_file.get_setting("settings.storage_profile_id") → ""
      job.get("storageProfileId")                            → "sp-job-222"
      Hits: `if not local_storage_profile_id and job_storage_profile_id:`
      → click.echo("Warning: ...no local storage profile is configured...")
      → None
    resolved is None → enters `else:` branch
    if OS mismatch → manual prompt fallback
    if same OS → download to original paths
```

Expected: warning printed recommending storage profile setup, then falls back to manual prompt if OS mismatch exists. No error thrown.

#### Case 4: Neither submitter nor downloader has a profile → no mapping, direct download

Local machine has no storage profile. Job was submitted without one.

```
job_download_output()
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(...)
      config_file.get_setting("settings.storage_profile_id") → ""
      job.get("storageProfileId")                            → None
      Hits: `if not local_storage_profile_id and not job_storage_profile_id:`
      → None
    resolved is None → enters `else:` branch
    for each asset_root:
      root_path_format matches host format (same OS)
      no OS mismatch → no prompt
    download proceeds to original paths
```

Expected: same-machine case. No storage profiles, no path mapping, no prompts. Identical to pre-change behavior.

#### Case 5: Local profile configured, job submitted without one → warning, skip mapping

Local machine has `sp-local-111`. Job was submitted without a storage profile (e.g., older job or tool that didn't set one).

```
job_download_output()
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(...)
      config_file.get_setting("settings.storage_profile_id") → "sp-local-111"
      job.get("storageProfileId")                            → None
      Hits: `if local_storage_profile_id and not job_storage_profile_id:`
      → click.echo("Warning: ...job was submitted without one. Path mapping will be skipped.")
      → None
    resolved is None → enters `else:` branch
    if OS mismatch → manual prompt fallback
    if same OS → download to original paths
```

Expected: warning printed, no mapping. Falls back to existing behavior. If cross-OS, user gets the manual root path prompt.

#### Case 6: `--ignore-storage-profiles` flag → skip everything

Both profiles may exist, but user explicitly opts out.

```
job_download_output(..., ignore_storage_profiles=True)
  _validate_storage_profile_options(None, True) → passes
  _download_job_output(..., ignore_storage_profiles=True)
    _resolve_storage_profiles(..., ignore_storage_profiles=True)
      → return None immediately (first line of function)
    resolved is None → enters `else:` branch
    if OS mismatch → manual prompt fallback
    download to original unmapped paths
```

Expected: all storage profile logic bypassed. Same behavior as pre-change code. Use when submitting and downloading machines share the same OS and mount points.

#### Case 7: Same storage profile on both sides → no mapping needed

Job was submitted with `sp-local-111`. Local machine also has `sp-local-111`.

```
job_download_output()
  _download_job_output(..., ignore_storage_profiles=False)
    _resolve_storage_profiles(...)
      Both IDs present, both "sp-local-111" → fetch both → ResolvedStorageProfiles
    resolved is not None → enters `if resolved:` branch
    _generate_path_mapping_rules(sp-local-111, sp-local-111)
      storageProfileId matches → return []
    _apply_path_mappings_to_roots(downloader, roots, [])
      `if not rules: return` → no-op
    download proceeds to original paths
```

Expected: same profile means same paths. No mapping, no prompts. Download to original locations.

### 9.2 Unit Tests

Each helper function is independently testable in `test/unit/deadline_client/cli/test_job_download_helpers.py`:

| Function | Test cases |
|----------|-----------|
| `_resolve_storage_profiles` | All rows of the decision matrix; API error handling (`ClientError`) |
| `_generate_path_mapping_rules` | Update existing tests to use `StorageProfile` dataclass; matching location names; no matching names; same profile id (empty rules) |
| `_apply_path_mappings_to_roots` | Empty rules (no-op); single mapping; multiple mappings; root already matches |

### 9.3 Backward Compatibility

- Existing behavior (manual prompt on OS mismatch) is preserved when no storage profiles are configured
- `--ignore-storage-profiles` explicitly opts out of all mapping
- No changes to `OutputDownloader` or `_incremental_download.py`

---

## 10. Appendix A: Key File Locations

| Component | File Path |
|-----------|-----------|
| CLI bundle commands | `src/deadline/client/cli/_groups/bundle_group.py` |
| CLI job commands | `src/deadline/client/cli/_groups/job_group.py` |
| CLI queue commands | `src/deadline/client/cli/_groups/queue_group.py` |
| CLI common utilities | `src/deadline/client/cli/_common.py` |
| Job download helpers (new) | `src/deadline/client/cli/_groups/_job_download_helpers.py` |
| Incremental download | `src/deadline/client/cli/_incremental_download.py` |
| Job submission API | `src/deadline/client/api/_submit_job_bundle.py` |
| Storage profile API | `src/deadline/client/api/_get_storage_profile_for_queue.py` |
| S3AssetManager | `src/deadline/job_attachments/upload.py` |
| OutputDownloader | `src/deadline/job_attachments/download.py` |
| Path mapping | `src/deadline/job_attachments/_path_mapping.py` |
| Hash algorithms | `src/deadline/job_attachments/asset_manifests/hash_algorithms.py` |
| Hash cache | `src/deadline/job_attachments/caches/hash_cache.py` |
| Models | `src/deadline/job_attachments/models.py` |
| Config file | `src/deadline/client/config/config_file.py` |

---

## 11. Appendix B: Future Refactoring Stages

These are follow-up improvements that build on the Stage 1-3 work above. Each is independently reviewable.

### 11.1 Future Stage A: Move path mapping into `OutputDownloader`

The `OutputDownloader` class has a TODO acknowledging it should handle path mapping internally. Once Stage A is done:

1. Add an optional `path_mapping_rules: Optional[list[PathMappingRule]]` parameter to `OutputDownloader.__init__()`
2. Apply rules automatically in `get_output_paths_by_root()` instead of requiring external `set_root_path()` calls
3. Simplify both `_download_job_output()` and `_incremental_output_download()` to pass rules at construction time

This is a larger refactor that changes the `OutputDownloader` public interface, so it warrants its own review.

### 11.2 Future Stage B: `S3AssetManager` auto-fetches storage profile (issue #829 broader ask)

The original issue requests that `S3AssetManager` automatically resolve the storage profile from the queue. Options:

1. A `StorageProfileResolver` class that takes `farm_id`, `queue_id`, `session` and lazily fetches/caches the profile. Inject into both `S3AssetManager` and `OutputDownloader`.
2. A higher-level `create_job()` function that wraps job inputs and job attachments together, so consumers don't need to understand storage profiles at all.

This is a larger API design effort and should be scoped separately.

---

## 12. Appendix C: Upload Code Path (Reference)

This section documents the upload (job submission) code path for reference. It is not changed by this proposal.

### 12.1 CLI Entry Point: `deadline bundle submit`

**File:** `src/deadline/client/cli/_groups/bundle_group.py`

```
CLI Command
    │
    ▼
bundle_submit()
    │
    ├── _apply_cli_options_to_config()  ─── Applies --storage-profile-id to config
    │
    └── api.create_job_from_job_bundle()
```

The `--storage-profile-id` option is processed in `_apply_cli_options_to_config()`.

### 12.2 API Layer: `create_job_from_job_bundle()`

**File:** `src/deadline/client/api/_submit_job_bundle.py`

```python
# Storage profile retrieval
storage_profile_id = get_setting("settings.storage_profile_id", config=config)
storage_profile = None
if storage_profile_id:
    create_job_args["storageProfileId"] = storage_profile_id
    storage_profile = api.get_storage_profile_for_queue(
        farm_id, queue_id, storage_profile_id, deadline
    )
```

Key operations:
1. Retrieves `storage_profile_id` from config
2. If set, adds to `create_job_args` for the CreateJob API call
3. Fetches full `StorageProfile` object for path grouping

### 12.3 Asset Manager: `S3AssetManager`

**File:** `src/deadline/job_attachments/upload.py`

```
S3AssetManager.__init__()
    │
    ├── farm_id, queue_id, job_attachment_settings
    │
    └── Does NOT take storage_profile (passed to methods instead)
```

Storage profile affects path grouping during upload:
- **SHARED locations**: Paths are SKIPPED (not uploaded) — they're on shared storage
- **LOCAL locations**: Paths are GROUPED together under the location's root

### 12.4 Hashing and Upload

- `hash_assets_and_create_manifest()` hashes files using xxHash XXH128 with a SQLite-backed cache
- `upload_assets()` uploads to S3 using Content-Addressable Storage (CAS), with an S3CheckCache to prevent redundant uploads


---

## 13. Revision: Absolute-Path Transformation (Option E)

### 13.1 Problem

During code review, @mwiebe identified that transforming only root paths via `set_root_path()` is not equivalent to transforming full absolute paths (`root + relative`). A path mapping rule can match at a depth deeper than the asset root. For example, with nested file system locations:

**Source profile (Windows):**
- "Projects": `C:\Projects`
- "SpecialProjects": `C:\Projects\Special`

**Destination profile (Linux):**
- "Projects": `/mnt/projects`
- "SpecialProjects": `/opt/special`

Asset root: `C:\Projects`, relative path: `Special\data.txt`.

- Root-only: transforms `C:\Projects` → `/mnt/projects`. Final: `/mnt/projects/Special/data.txt`. **Wrong.**
- Absolute-path: transforms `C:\Projects\Special\data.txt` → `/opt/special/data.txt`. **Correct** (more specific rule wins).

The Deadline Cloud API does not prevent nested file system locations in storage profiles, so this is a real scenario.

### 13.2 Solution: Option E — Dual Code Path

When path mapping rules exist (both profiles resolved, rules generated), bypass `OutputDownloader` and use the manifest-based download pipeline directly — the same approach `queue sync-output` uses:

1. Call `get_output_manifests_by_asset_root()` to fetch manifests from S3 (same data `OutputDownloader.__init__` fetches internally)
2. For each manifest path, join `root + relative` using the source OS path module (`ntpath` or `posixpath`)
3. Call `strict_transform()` on the full absolute path (trie picks the most specific matching rule)
4. Download via `download_files_from_manifests()` with the transformed absolute paths

When no path mapping rules exist (no profiles, ignore flag, same profile, mismatch warnings), the existing `OutputDownloader` code path is completely unchanged.

### 13.3 Implementation

**New function:** `_transform_manifests_to_absolute_paths()` in `_job_download_helpers.py`

```python
def _transform_manifests_to_absolute_paths(
    manifests_by_root: dict[str, list[Any]],
    rules: list[PathMappingRule],
    source_os_family: StorageProfileOperatingSystemFamily,
) -> dict[str, Any]:
```

- Joins root + relative using `ntpath` (Windows source) or `posixpath` (POSIX source)
- Applies `strict_transform()` on each absolute path
- Merges all manifests into a single result keyed by `""` (empty string)
- `download_files_from_manifests()` with key `""` works because `Path("").joinpath("/abs/path")` == `Path("/abs/path")`

**Updated flow in `_download_job_output()`:**

```
resolved = _resolve_storage_profiles(...)
if resolved:
    rules = _generate_path_mapping_rules(...)
    if rules:
        manifests = get_output_manifests_by_asset_root(...)  # same S3 data
        mapped = _transform_manifests_to_absolute_paths(manifests, rules, ...)
        download_files_from_manifests(mapped)                # absolute-path download
        return  # early return, skip OutputDownloader path
    elif different profiles:
        warn about no matching location names
else:
    # existing OutputDownloader path (manual prompt, etc.) — unchanged
```

### 13.4 Test Coverage

New unit tests in `test_job_download_helpers.py`:

| Test | Description |
|------|-------------|
| `test_basic_windows_to_posix_mapping` | Standard cross-OS mapping via absolute paths |
| `test_nested_location_picks_most_specific_rule` | Key case: nested locations, most specific rule wins |
| `test_unmapped_paths_are_skipped` | Paths without matching rules are excluded |
| `test_multiple_roots_merged` | Multiple asset roots merged into single download |
| `test_empty_manifests_returns_empty` | Edge case: no paths to map |

Updated CLI test `test_case1_both_profiles_match_auto_mapping` to verify the new code path calls `get_output_manifests_by_asset_root` instead of `set_root_path`.
