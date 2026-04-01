# Design: Progress Callback Decoupling from Job Attachments

## Overview

Decouple progress reporting from the Job Attachments library by removing its direct dependency on `_ProgressBarCallbackManager` from `deadline.client.cli`. Progress reporting is now an optional callback that callers provide, following dependency inversion.

## Problem Statement

`_create_manifest_for_single_root` in `job_attachments` had two upward dependencies into `deadline.client`:

1. Imported `_ProgressBarCallbackManager` from `deadline.client.cli._common`
2. Imported `_hash_attachments` from `deadline.client.api._job_attachment`

This created a circular dependency: `job_attachments` is a lower-level library that `client.cli` consumes. Having it reach back up into `client.cli` for a UI concern (progress bars) violates the dependency hierarchy.

```
BEFORE (circular):

deadline.client.cli.manifest_group
        │
        ▼
deadline.job_attachments._create_manifest
        │
        ▼  (circular!)
deadline.client.cli._common._ProgressBarCallbackManager
deadline.client.api._job_attachment._hash_attachments
```

## Solution

Apply dependency inversion: the lower-level library defines the callback interface, and the higher-level caller provides the implementation.

```
AFTER (clean):

deadline.client.cli.manifest_group
        │  creates _ProgressBarCallbackManager
        │  passes .callback, telemetry_callback, and hash_cache_dir down
        ▼
deadline.job_attachments._create_manifest
        │  accepts Optional[Callable[[ProgressReportMetadata], bool]]
        │  accepts Optional[Callable[[SummaryStatistics], None]]
        │  accepts Optional[str] for hash_cache_dir
        ▼
deadline.job_attachments.api._hashing._hash_attachments
        │  (within job_attachments package)
        ▼
deadline.job_attachments.upload.S3AssetManager
        │  hash_assets_and_create_manifest
        ▼
deadline.job_attachments.progress_tracker
        (ProgressReportMetadata defined here)
```

## Key Changes

| File | Change |
|------|--------|
| `job_attachments/asset_manifests/_create_manifest.py` | Removed imports of `_ProgressBarCallbackManager`. Added optional `hashing_progress_callback`, `telemetry_callback`, and `hash_cache_dir` parameters. Calls `_hash_attachments` from `job_attachments.api._hashing` (within the same package). |
| `job_attachments/api/manifest.py` | Added `hashing_progress_callback`, `telemetry_callback`, and `hash_cache_dir` parameters to `_manifest_snapshot` and threads them through to `_create_manifest_for_single_root`. |
| `client/cli/_groups/manifest_group.py` | Creates `_ProgressBarCallbackManager` and passes `.callback`, `hashing_telemetry_callback`, and `config_file.get_cache_directory()` to `_manifest_snapshot`. |

## Callback Interface

The callback uses the existing `ProgressReportMetadata` type already defined in `job_attachments.progress_tracker`:

```python
# Signature
hashing_progress_callback: Optional[Callable[[ProgressReportMetadata], bool]] = None
```

- Input: `ProgressReportMetadata` with `status`, `progress` (%), `transferRate`, `progressMessage`, `processedFiles`
- Return: `bool` — `True` to continue, `False` to cancel the operation
- Default: `None` (no progress reporting, hashing proceeds silently)

## Usage Examples

### CLI caller (with progress bar and telemetry)
```python
from deadline.client.cli._common import _ProgressBarCallbackManager
from deadline.client.api._submit_job_bundle import hashing_telemetry_callback

hash_callback_manager = _ProgressBarCallbackManager(length=100, label="Hashing Attachments")

_manifest_snapshot(
    root=root,
    # ...
    hashing_progress_callback=hash_callback_manager.callback,
    telemetry_callback=hashing_telemetry_callback,
    hash_cache_dir=config_file.get_cache_directory(),
)
```

### Library caller (no progress reporting)
```python
# Simply omit the callback — hashing runs silently
_manifest_snapshot(
    root=root,
    # ...
)
```

### Custom caller (e.g. GUI or logging)
```python
def my_progress_handler(metadata: ProgressReportMetadata) -> bool:
    print(f"{metadata.progress}% - {metadata.progressMessage}")
    return True  # continue

_manifest_snapshot(
    root=root,
    # ...
    hashing_progress_callback=my_progress_handler,
)
```

## Design Principles

- **Dependency inversion**: Lower-level modules define interfaces, higher-level modules provide implementations
- **Optional by default**: Progress reporting is opt-in. Callers that don't need it pay no cost
- **Existing types**: Uses `ProgressReportMetadata` already defined in `job_attachments.progress_tracker` — no new types introduced
- **Cancellation support**: The `bool` return value preserves the existing cancellation mechanism
