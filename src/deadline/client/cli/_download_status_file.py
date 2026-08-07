# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["write_download_status_file"]

import json
import logging
import os
import socket
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Generator, Optional

from ._incremental_download import CategorizedJobIds

logger = logging.getLogger(__name__)

DOWNLOAD_STATUS_FILE_SCHEMA_VERSION = 1
_STATUS_FILE_LOCK_TTL_SECONDS = 60
_STATUS_FILE_LOCK_RETRY_INTERVAL_SECONDS = 1
# MAX_WAIT must be >= TTL so a waiter is willing to wait at least as long as a lock
# can legitimately be held before considering it stale.
_STATUS_FILE_LOCK_MAX_WAIT_SECONDS = 90


@contextmanager
def _status_file_lock(status_file_path: str) -> Generator[None, None, None]:
    """Cooperative cross-machine lock for the shared NAS status file.

    Creates a sentinel lock file next to the status file. Both machines agree
    to check for it before reading/writing, preventing last-writer-wins races
    when two machines sync the same queue to the same NAS simultaneously.
    Locks older than _STATUS_FILE_LOCK_TTL_SECONDS are considered stale
    (e.g. from a crashed process) and overwritten.
    """
    lock_path = status_file_path + ".lock"
    dir_path = os.path.dirname(status_file_path)
    os.makedirs(dir_path, exist_ok=True)

    acquired = False
    deadline_time = time.monotonic() + _STATUS_FILE_LOCK_MAX_WAIT_SECONDS
    while time.monotonic() < deadline_time:
        # Check if a lock exists and whether it is stale.
        # Read the timestamp from the lock file content rather than the NAS mtime to avoid
        # NAS-server-vs-client clock skew. Note: client-to-client clock skew (between the
        # two sync machines) is still a factor but is bounded by NTP drift (typically <1s),
        # well within the 60s TTL. Worst case is a stale badge on the next poll, not data loss.
        try:
            with open(lock_path, "r") as _lf:
                _lock_data = json.load(_lf)
            lock_written_at = float(_lock_data.get("time", 0))
            if time.time() - lock_written_at < _STATUS_FILE_LOCK_TTL_SECONDS:
                # Lock is fresh — wait for the holder to release it
                time.sleep(_STATUS_FILE_LOCK_RETRY_INTERVAL_SECONDS)
                continue
            else:
                # Lock is stale — remove it so we can acquire with O_EXCL
                try:
                    os.unlink(lock_path)
                except OSError:
                    pass  # Another process may have already removed it
        except (OSError, json.JSONDecodeError, KeyError, ValueError):
            pass  # Lock file doesn't exist or is unreadable — proceed to acquire

        # Acquire the lock using O_CREAT|O_EXCL for atomic exclusive create.
        # Note: O_EXCL over NFS/SMB is best-effort — not guaranteed on all network filesystems,
        # but substantially better than os.replace which gives no exclusion at all.
        try:
            lock_time = time.time()
            lock_content = json.dumps({"hostname": socket.gethostname(), "time": lock_time})
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w") as f:
                f.write(lock_content)
            acquired = True
            break
        except FileExistsError:
            time.sleep(_STATUS_FILE_LOCK_RETRY_INTERVAL_SECONDS)
        except OSError:
            time.sleep(_STATUS_FILE_LOCK_RETRY_INTERVAL_SECONDS)
    else:
        logger.warning(
            f"Could not acquire status file lock at {lock_path} after {_STATUS_FILE_LOCK_MAX_WAIT_SECONDS}s — proceeding without lock."
        )

    try:
        yield
    finally:
        if acquired:
            # Verify we still own the lock before deleting — if our hold exceeded the TTL,
            # another process may have taken it over. Only unlink if the stored time matches.
            try:
                with open(lock_path, "r") as _lf:
                    _on_disk = json.load(_lf)
                if _on_disk.get("time") == lock_time:
                    os.unlink(lock_path)
            except (OSError, json.JSONDecodeError, KeyError, ValueError):
                pass  # Lock already gone or unreadable — nothing to clean up


def _make_status_entry(
    status: str,
    total_files: int = 0,
    downloaded_files: int = 0,
    failed_files: int = 0,
    error_code: Optional[str] = None,
    error_message: Optional[str] = None,
    skip_reason: Optional[str] = None,
    tasks: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Constructs a per-job status entry for the status file."""
    return {
        "download_status": status,
        "total_files": total_files,
        "downloaded_files": downloaded_files,
        "failed_files": failed_files,
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "error_code": error_code,
        "error_message": error_message,
        "skip_reason": skip_reason,
        "tasks": tasks or {},
    }


# Job statuses that assert the job is finished and nothing failed to download. An errored task
# contradicts any of them, so these are the states _reconcile_entry_with_tasks overrides.
# "in_progress" is excluded on purpose: it claims nothing, and the download may still be retried
# before the job ends, so the honest report is a failed task row under a still-running job.
_NO_DOWNLOAD_FAILURE_STATUSES = ("downloaded", "skipped")


def _reconcile_entry_with_tasks(entry: dict[str, Any]) -> None:
    """Forces a job entry back to "failed" while any of its tasks still carries a download error.

    A job's category-derived status only says the job finished on the farm, which is not
    evidence that its outputs reached disk. Without this, a job whose task failed in an
    earlier run and which produced no new results this run flips to "downloaded" while its
    task entry still reads "failed" — a green job badge over a red task, and a real failure
    silently dropped. The same applies to "skipped": a job stopped mid-download reports
    "skipped" once it goes inactive, which reads as "nothing to fetch" rather than "a file is
    missing and here is why". Preferring the task evidence keeps the two levels consistent
    without claiming a missing file is present.

    Tasks that failed on the farm carry no error_code and are deliberately excluded: the
    render failed, the download didn't, so they must not drag the job to a download failure.
    """
    if entry.get("download_status") not in _NO_DOWNLOAD_FAILURE_STATUSES:
        return
    errored = [t for t in entry.get("tasks", {}).values() if t.get("error_code")]
    if not errored:
        return
    entry["download_status"] = "failed"
    entry["error_code"] = errored[0]["error_code"]
    entry["error_message"] = errored[0].get("error_message")
    # A download failure is not a skip. Leaving skip_reason set would produce an entry that
    # claims both, and the monitor keys its "why is this file missing" copy off skip_reason.
    entry["skip_reason"] = None
    missing = sum(t.get("total_files", 0) - t.get("downloaded_files", 0) for t in errored)
    if missing > 0:
        entry["failed_files"] = missing


def _is_job_fully_complete(job: dict[str, Any]) -> bool:
    """Returns True if the job has ended with no active tasks remaining.

    A job is considered complete when it has ended and no tasks are still
    active — even if some tasks failed or were canceled. All available
    outputs from succeeded tasks have been downloaded.
    """
    task_counts = job.get("taskRunStatusCounts", {})
    active_tasks = sum(
        task_counts.get(s, 0) for s in ["READY", "RUNNING", "ASSIGNED", "STARTING", "SCHEDULED"]
    )
    return "endedAt" in job and active_tasks == 0


def _determine_job_download_status(
    job_id: str,
    job: dict[str, Any],
    categorized_job_ids: CategorizedJobIds,
    job_download_results: Optional[dict[str, dict[str, Any]]] = None,
) -> dict[str, Any]:
    """
    Determines the download status entry for a single job based on its category
    and download results.

    Returns a dict representing the job's status in the status file.
    """
    # If we have per-job download results, use them for file counts and error info
    results = (job_download_results or {}).get(job_id)

    # If download failed for this job, report it immediately
    if results and results.get("error_code") is not None:
        return _make_status_entry(
            "failed",
            total_files=results["total_files"],
            downloaded_files=results["downloaded_files"],
            failed_files=results["failed_files"],
            error_code=results["error_code"],
            error_message=results.get("error_message"),
        )

    # File counts from results (or 0 if no results available)
    total_files = results["total_files"] if results else 0
    downloaded_files = results["downloaded_files"] if results else 0

    if job_id in categorized_job_ids.attachments_free:
        return _make_status_entry("skipped", skip_reason="no_attachments")

    if job_id in categorized_job_ids.missing_storage_profile:
        return _make_status_entry("skipped", skip_reason="missing_storage_profile")

    if job_id in categorized_job_ids.completed:
        return _make_status_entry("downloaded", total_files, downloaded_files)

    if job_id in categorized_job_ids.added:
        if _is_job_fully_complete(job):
            return _make_status_entry("downloaded", total_files, downloaded_files)
        return _make_status_entry("in_progress", total_files, downloaded_files)

    if job_id in categorized_job_ids.updated:
        if _is_job_fully_complete(job):
            return _make_status_entry("downloaded", total_files, downloaded_files)
        return _make_status_entry("in_progress", total_files, downloaded_files)

    if job_id in categorized_job_ids.unchanged:
        if _is_job_fully_complete(job):
            return _make_status_entry("downloaded", total_files, downloaded_files)
        return _make_status_entry("in_progress", total_files, downloaded_files)

    return _make_status_entry("in_progress", total_files, downloaded_files)


def _build_status_file_content(
    queue_id: str,
    storage_profile_id: Optional[str],
    categorized_job_ids: CategorizedJobIds,
    download_candidate_jobs: dict[str, dict[str, Any]],
    existing_jobs: Optional[dict[str, Any]] = None,
    job_download_results: Optional[dict[str, dict[str, Any]]] = None,
    task_download_results: Optional[dict[str, dict[str, dict[str, Any]]]] = None,
    succeeded_task_ids: Optional[dict[str, set[str]]] = None,
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
        new_entry = _determine_job_download_status(
            job_id, job, categorized_job_ids, job_download_results
        )
        existing_entry = jobs_status.get(job_id)

        # Merge task entries: preserve existing tasks, overwrite only tasks seen this run
        existing_tasks: dict[str, Any] = (existing_entry or {}).get("tasks", {})
        new_tasks: dict[str, Any] = (task_download_results or {}).get(job_id, {})
        merged_tasks = dict(existing_tasks)
        # A task that SUCCEEDED this run with no output must clear any stale farm_failed
        # entry carried forward from a prior run. The farm FAILED action is reported by the
        # API indefinitely, so _get_job_sessions subtracts succeeded IDs from
        # farm_failed_task_ids — preventing a new entry — but the prior disk entry is
        # preserved by the merge above unless we explicitly remove it here.
        job_succeeded_ids: set[str] = (succeeded_task_ids or {}).get(job_id, set())
        for task_id in job_succeeded_ids:
            if (
                task_id not in new_tasks
                and merged_tasks.get(task_id, {}).get("download_status") == "farm_failed"
            ):
                del merged_tasks[task_id]
        for task_id, r in new_tasks.items():
            # Honor an explicit download_status (e.g. "farm_failed" for a task that
            # failed on the farm); otherwise derive it from whether the download errored.
            status = r.get(
                "download_status",
                "downloaded" if r.get("error_code") is None else "failed",
            )
            # A prior-run download success is terminal: the output is on disk. Never let a stale
            # farm_failed action (the API reports it indefinitely) overwrite it.
            # "failed" is intentionally not guarded here: a task may have succeeded on the farm
            # but its download failed, so a subsequent run re-collects the FAILED action as
            # farm_failed and should overwrite the stale download error.
            if (
                status == "farm_failed"
                and existing_tasks.get(task_id, {}).get("download_status") == "downloaded"
            ):
                continue
            merged_tasks[task_id] = {
                "download_status": status,
                "total_files": r["total_files"],
                "downloaded_files": r["downloaded_files"],
                "error_code": r.get("error_code"),
                "error_message": r.get("error_message"),
            }
        new_entry["tasks"] = merged_tasks

        if (
            existing_entry
            and existing_entry.get("total_files", 0) > 0
            and new_entry.get("total_files", 0) == 0
        ):
            # No download this run — preserve existing counts, update status if changed.
            # Also clear stale error fields when transitioning away from a failed state to
            # avoid an inconsistent entry with download_status="downloaded" + error_code set.
            if new_entry["download_status"] != existing_entry["download_status"]:
                existing_entry["download_status"] = new_entry["download_status"]
                existing_entry["last_updated"] = new_entry["last_updated"]
                if new_entry["download_status"] != "failed":
                    existing_entry["error_code"] = None
                    existing_entry["error_message"] = None
                    existing_entry["failed_files"] = 0
            existing_entry["tasks"] = merged_tasks
            _reconcile_entry_with_tasks(existing_entry)
        else:
            _reconcile_entry_with_tasks(new_entry)
            jobs_status[job_id] = new_entry

    # Update inactive jobs with non-terminal status to a terminal state
    for job_id in categorized_job_ids.inactive:
        existing_entry = jobs_status.get(job_id)
        if existing_entry and existing_entry.get("download_status") == "in_progress":
            if existing_entry.get("downloaded_files", 0) > 0:
                # Had some downloads — mark as downloaded (all available outputs are on disk)
                existing_entry["download_status"] = "downloaded"
            else:
                # No downloads at all — mark as skipped (job stopped before any output)
                existing_entry["download_status"] = "skipped"
            existing_entry["last_updated"] = now
            _reconcile_entry_with_tasks(existing_entry)

    # Determine run status — "failed" if any job in this run had errors
    has_failures = any(
        r.get("error_code") is not None for r in (job_download_results or {}).values()
    )

    return {
        "schema_version": DOWNLOAD_STATUS_FILE_SCHEMA_VERSION,
        "sync_metadata": {
            "queue_id": queue_id,
            "storage_profile_id": storage_profile_id,
            "last_sync_completed_at": now,
            "last_run_status": "failed" if has_failures else "success",
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
            # Only include paths whose root location already exists — avoids writing
            # phantom files under an empty mount point when the NAS is unmounted.
            if not os.path.isdir(location_path):
                continue
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
    job_download_results: Optional[dict[str, dict[str, Any]]] = None,
    task_download_results: Optional[dict[str, dict[str, dict[str, Any]]]] = None,
    succeeded_task_ids: Optional[dict[str, set[str]]] = None,
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
        job_download_results: Per-job download results with file counts and error info.
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
            with _status_file_lock(status_file_path):
                existing_jobs = _read_existing_status_file(status_file_path)

                status_content = _build_status_file_content(
                    queue_id=queue_id,
                    storage_profile_id=local_storage_profile_id,
                    categorized_job_ids=categorized_job_ids,
                    download_candidate_jobs=download_candidate_jobs,
                    existing_jobs=existing_jobs,
                    job_download_results=job_download_results,
                    task_download_results=task_download_results,
                    succeeded_task_ids=succeeded_task_ids,
                )

                _atomic_write_json(status_file_path, status_content)
            print_function_callback(f"Download status file saved: {status_file_path}")
        except Exception as e:
            logger.warning(f"Failed to write download status file to {status_file_path}: {e}")
            print_function_callback(
                f"WARNING: Failed to write download status file to {status_file_path}: {e}"
            )
