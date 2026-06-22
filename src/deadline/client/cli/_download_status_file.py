# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["write_download_status_file"]

import json
import logging
import os
import socket
import tempfile
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ._incremental_download import CategorizedJobIds

logger = logging.getLogger(__name__)

DOWNLOAD_STATUS_FILE_SCHEMA_VERSION = 1


def _determine_job_download_status(
    job_id: str,
    job: dict[str, Any],
    categorized_job_ids: CategorizedJobIds,
    local_storage_profile_id: Optional[str],
) -> dict[str, Any]:
    """
    Determines the download status entry for a single job based on its category.

    Returns a dict representing the job's status in the status file.
    """
    if job_id in categorized_job_ids.attachments_free:
        return {
            "download_status": "skipped",
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    if job_id in categorized_job_ids.missing_storage_profile:
        return {
            "download_status": "skipped",
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    if job_id in categorized_job_ids.completed:
        return {
            "download_status": "downloaded",
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    if job_id in categorized_job_ids.added:
        task_counts = job.get("taskRunStatusCounts", {})
        succeeded = task_counts.get("SUCCEEDED", 0)
        total = sum(task_counts.values()) if task_counts else 0
        active_tasks = sum(
            task_counts.get(s, 0) for s in ["READY", "RUNNING", "ASSIGNED", "STARTING", "SCHEDULED"]
        )
        if succeeded == total and total > 0 and active_tasks == 0 and "endedAt" in job:
            status = "downloaded"
        else:
            status = "in_progress"
        return {
            "download_status": status,
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    if job_id in categorized_job_ids.updated:
        task_counts = job.get("taskRunStatusCounts", {})
        succeeded = task_counts.get("SUCCEEDED", 0)
        total = sum(task_counts.values()) if task_counts else 0
        if succeeded == total and total > 0 and "endedAt" in job:
            status = "downloaded"
        else:
            status = "in_progress"
        return {
            "download_status": status,
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    # unchanged jobs — check if all tasks succeeded to determine if fully downloaded
    if job_id in categorized_job_ids.unchanged:
        task_counts = job.get("taskRunStatusCounts", {})
        succeeded = task_counts.get("SUCCEEDED", 0)
        total = sum(task_counts.values()) if task_counts else 0
        if succeeded == total and total > 0 and "endedAt" in job:
            status = "downloaded"
        else:
            status = "in_progress"
        return {
            "download_status": status,
            "total_files": 0,
            "downloaded_files": 0,
            "failed_files": 0,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "error_code": None,
            "error_message": None,
        }

    # fallback — should not typically reach here
    return {
        "download_status": "in_progress",
        "total_files": 0,
        "downloaded_files": 0,
        "failed_files": 0,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "error_code": None,
        "error_message": None,
    }


def _build_status_file_content(
    queue_id: str,
    storage_profile_id: Optional[str],
    categorized_job_ids: CategorizedJobIds,
    download_candidate_jobs: dict[str, dict[str, Any]],
    local_storage_profile_id: Optional[str],
    existing_jobs: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Builds the full status file JSON structure by merging existing job entries
    with the current run's categorized job data. Existing entries are preserved;
    current run entries are added or updated on top.
    """
    now = datetime.now(timezone.utc).isoformat()

    # Start with existing jobs (preserves old completed jobs that dropped out of tracking)
    jobs_status: dict[str, Any] = dict(existing_jobs) if existing_jobs else {}

    # Update/add entries from this run's categorized jobs
    # Inactive jobs are excluded — they dropped out of download_candidate_jobs so we can't
    # look up their task counts. Their existing entry from the merge is preserved as-is.
    all_job_ids = (
        categorized_job_ids.completed
        | categorized_job_ids.added
        | categorized_job_ids.updated
        | categorized_job_ids.unchanged
        | categorized_job_ids.attachments_free
        | categorized_job_ids.missing_storage_profile
    )

    for job_id in all_job_ids:
        job = download_candidate_jobs.get(job_id, {})
        jobs_status[job_id] = _determine_job_download_status(
            job_id, job, categorized_job_ids, local_storage_profile_id
        )

    return {
        "schema_version": DOWNLOAD_STATUS_FILE_SCHEMA_VERSION,
        "sync_metadata": {
            "queue_id": queue_id,
            "storage_profile_id": storage_profile_id,
            "last_sync_completed_at": now,
            "last_run_status": "success",
            "hostname": socket.gethostname(),
        },
        "jobs": jobs_status,
    }


def _read_existing_status_file(file_path: str) -> dict[str, Any]:
    """
    Reads an existing status file and returns its jobs dict.
    Returns empty dict if file doesn't exist or is invalid.
    """
    try:
        if os.path.exists(file_path):
            with open(file_path, "r") as f:
                data = json.load(f)
            return data.get("jobs", {})
    except (json.JSONDecodeError, OSError, KeyError):
        pass  # Gracefully handle corrupt or inaccessible status files
    return {}


def _atomic_write_json(file_path: str, data: dict[str, Any]) -> None:
    """
    Writes JSON data atomically using a temp file + rename to prevent partial reads.
    """
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_path, file_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass  # Best-effort cleanup of temp file
        raise


def _get_status_file_paths(
    queue_id: str,
    local_storage_profile_id: Optional[str],
    local_storage_profile: Optional[dict[str, Any]],
    checkpoint_dir: str,
) -> list[str]:
    """
    Determines all paths where the status file should be written.

    With storage profile: writes to {each_file_system_location}/.deadline/{queue_id}_download_status.json
    Without storage profile: writes to ~/.deadline/incremental_download/{queue_id}_ignore-storage-profiles_download_status.json
    """
    if local_storage_profile_id and local_storage_profile:
        paths = []
        for location in local_storage_profile.get("fileSystemLocations", []):
            location_path = location["path"]
            status_file_path = os.path.join(
                location_path, ".deadline", f"{queue_id}_download_status.json"
            )
            paths.append(status_file_path)
        return paths
    else:
        status_file_path = os.path.join(
            checkpoint_dir, f"{queue_id}_ignore-storage-profiles_download_status.json"
        )
        return [status_file_path]


def write_download_status_file(
    queue_id: str,
    categorized_job_ids: CategorizedJobIds,
    download_candidate_jobs: dict[str, dict[str, Any]],
    local_storage_profile_id: Optional[str],
    local_storage_profile: Optional[dict[str, Any]],
    checkpoint_dir: str,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
) -> None:
    """
    Writes the download status JSON file to the shared filesystem (or local default path).

    This is called at the end of each sync-output run, inside the PID lock, before the
    checkpoint is saved. If the write fails, it logs a warning but does not abort.

    Args:
        queue_id: The queue ID.
        categorized_job_ids: The categorized job IDs from the current sync run.
        download_candidate_jobs: The dict of {job_id: job} from the current sync run.
        local_storage_profile_id: The local storage profile ID, or None if --ignore-storage-profiles.
        local_storage_profile: The full storage profile dict (with fileSystemLocations), or None.
        checkpoint_dir: The checkpoint directory path (used for --ignore-storage-profiles case).
        print_function_callback: Callback for printing output.
    """
    status_file_paths = _get_status_file_paths(
        queue_id=queue_id,
        local_storage_profile_id=local_storage_profile_id,
        local_storage_profile=local_storage_profile,
        checkpoint_dir=checkpoint_dir,
    )

    for status_file_path in status_file_paths:
        try:
            existing_jobs = _read_existing_status_file(status_file_path)

            status_content = _build_status_file_content(
                queue_id=queue_id,
                storage_profile_id=local_storage_profile_id,
                categorized_job_ids=categorized_job_ids,
                download_candidate_jobs=download_candidate_jobs,
                local_storage_profile_id=local_storage_profile_id,
                existing_jobs=existing_jobs,
            )

            _atomic_write_json(status_file_path, status_content)
            print_function_callback(f"Download status file saved: {status_file_path}")
        except Exception as e:
            logger.warning(f"Failed to write download status file to {status_file_path}: {e}")
            print_function_callback(
                f"WARNING: Failed to write download status file to {status_file_path}: {e}"
            )
