# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
from __future__ import annotations

__all__ = ["CategorizedJobIds", "_incremental_output_download"]

from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
import difflib
import errno
import json
import os
import re
import tempfile
import threading
from typing import Optional
from configparser import ConfigParser
from typing import Any, Callable
import time
import concurrent.futures
import textwrap

from .. import api
import boto3
from botocore.client import BaseClient  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from ..api._list_jobs_by_filter_expression import _list_jobs_by_filter_expression
from ..api._session import get_session_client, _resolve_region
from ...job_attachments.api import summarize_path_list, human_readable_file_size
from ...job_attachments._aws.aws_clients import get_s3_client as _get_s3_client
from ...job_attachments._incremental_downloads.incremental_download_state import (
    IncrementalDownloadState,
    IncrementalDownloadJob,
    _datetimes_to_str,
)
from ...job_attachments._incremental_downloads._manifest_s3_downloads import (
    _add_output_manifests_from_s3,
    _download_all_manifests_with_absolute_paths,
    _get_manifests_to_download,
    _download_manifest_paths,
)
from ...job_attachments.exceptions import (
    AssetSyncCancelledError,
    JobAttachmentsS3ClientError,
    JobAttachmentS3BotoCoreError,
)
from ...job_attachments._path_mapping import (
    _generate_path_mapping_rules,
    _PathMappingRuleApplier,
)
from ...job_attachments.asset_manifests import (
    BaseAssetManifest,
    BaseManifestPath,
)
from ...job_attachments.asset_manifests import (
    HashAlgorithm,
)
from ...job_attachments.models import (
    FileConflictResolution,
    PathFormat,
    StorageProfileOperatingSystemFamily,
)
from ...job_attachments.progress_tracker import (
    ProgressReportMetadata,
)
from ._common import _cli_object_repr, sigint_handler

SESSIONS_API_MAX_CONCURRENCY = 3


def _classify_single_error(e: BaseException) -> Optional[str]:
    """Classifies one exception from its own structured signals, or None if it has none."""
    # job_attachments surfaces S3 failures as JobAttachmentsS3ClientError with an HTTP
    # status code, and botocore transport failures as JobAttachmentS3BotoCoreError.
    if isinstance(e, JobAttachmentsS3ClientError):
        if e.status_code == 403:
            return "PERMISSION_DENIED"
        if e.status_code == 404:
            return "PATH_NOT_FOUND"
    if isinstance(e, JobAttachmentS3BotoCoreError):
        return "NETWORK_ERROR"

    if isinstance(e, PermissionError):
        return "PERMISSION_DENIED"
    if isinstance(e, OSError) and e.errno == errno.ENOSPC:
        return "DISK_FULL"
    if isinstance(e, FileNotFoundError):
        return "PATH_NOT_FOUND"
    if isinstance(e, (ConnectionError, TimeoutError)):
        return "NETWORK_ERROR"

    if isinstance(e, ClientError):
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code in ("AccessDenied", "AccessDeniedException", "403", "Forbidden"):
            return "PERMISSION_DENIED"
        if error_code in ("NoSuchKey", "NoSuchBucket", "404", "NotFound"):
            return "PATH_NOT_FOUND"
        if error_code in ("RequestTimeout", "RequestTimeTooSkewed"):
            return "NETWORK_ERROR"

    return None


def _classify_error(e: BaseException) -> str:
    """Classifies a download exception into a standard error code.

    Uses only structured signals — exception type, errno, S3 HTTP status codes, and
    boto error codes — which are reliable. Message-substring matching is intentionally
    avoided: wrapped S3 errors carry verbose guidance text that produces confidently
    wrong codes. job_attachments wraps low-level failures (e.g. an OSError or a botocore
    ClientError) inside its own exception types, so we also walk the __cause__ chain to
    reach the structured signal underneath. Anything without such a signal is UNKNOWN.
    """
    # Walk the __cause__ chain iteratively, tracking visited exceptions by identity so a
    # cyclic chain (A raised `from` B and B raised `from` A) can't cause infinite recursion.
    current: Optional[BaseException] = e
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        code = _classify_single_error(current)
        if code is not None:
            return code
        current = current.__cause__

    return "UNKNOWN"


_MAX_FAILED_JOB_RETRIES = 5


class _FailedJobsTracker:
    """Persists per-job download failure counts so the global timestamp can advance freely.

    When a job fails to download, it is tracked here instead of freezing the checkpoint
    timestamp. On the next run, failed jobs are fetched individually via GetJob and merged
    into the download candidates. After _MAX_FAILED_JOB_RETRIES attempts, the job is
    abandoned and a warning is logged.
    """

    def __init__(self, file_path: str) -> None:
        self._file_path = file_path
        self._counts: dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        try:
            if os.path.exists(self._file_path):
                with open(self._file_path, "r") as f:
                    self._counts = json.load(f)
        except (json.JSONDecodeError, OSError):
            self._counts = {}

    def save(self) -> None:
        dir_path = os.path.dirname(self._file_path)
        tmp_path: Optional[str] = None
        try:
            os.makedirs(dir_path, exist_ok=True)
            fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix=".tmp")
            with os.fdopen(fd, "w") as f:
                json.dump(self._counts, f)
            os.replace(tmp_path, self._file_path)
            tmp_path = None  # Rename succeeded — no cleanup needed
        except OSError:
            pass  # Best-effort write — if the checkpoint dir is unwritable, skip silently
        finally:
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass  # Best-effort cleanup of temp file

    def get_tracked_job_ids(self) -> set[str]:
        """Returns job IDs that should be retried (below the retry cap)."""
        return {job_id for job_id, count in self._counts.items() if count < _MAX_FAILED_JOB_RETRIES}

    def is_abandoned(self, job_id: str) -> bool:
        """Returns True if the job has hit the retry cap and should not be retried."""
        return self._counts.get(job_id, 0) >= _MAX_FAILED_JOB_RETRIES

    def record_failures(
        self, failed_job_ids: set[str], print_function_callback: Callable[[Any], None]
    ) -> None:
        for job_id in failed_job_ids:
            self._counts[job_id] = self._counts.get(job_id, 0) + 1
            if self._counts[job_id] >= _MAX_FAILED_JOB_RETRIES:
                print_function_callback(
                    f"WARNING: Job {job_id} has failed to download {_MAX_FAILED_JOB_RETRIES} "
                    f"times and will no longer be retried automatically."
                )
                # Keep in _counts at the cap value so subsequent timestamp-window rediscoveries
                # are suppressed — do NOT delete here.

    def record_successes(self, succeeded_job_ids: set[str]) -> None:
        for job_id in succeeded_job_ids:
            self._counts.pop(job_id, None)


# Anchor to the step-<id>/task-<id>/ layout rather than the first "task-" hit: the key is
# prefixed with the queue's customer-controlled rootPrefix, so a prefix like "my-task-outputs/"
# would otherwise be misread as the task id.
_TASK_ID_RE = re.compile(r"step-[^/]+/(task-[^/]+)/")


def _extract_task_id_from_s3_key(manifest_s3_key: str) -> Optional[str]:
    """Extracts the task ID from an S3 manifest key path.

    S3 manifest keys follow the pattern:
    .../step-<id>/task-<id>/<timestamp>_<sessionActionId>/<hash>_output
    """
    match = _TASK_ID_RE.search(manifest_s3_key)
    return match.group(1) if match else None


@dataclass
class IncrementalOutputDownloadLatencies:
    """Dataclass for tracking latencies of operations in this command"""

    _get_download_candidate_jobs: int = 0
    _categorize_jobs_in_checkpoint: int = 0
    _get_job_sessions: int = 0
    _update_checkpoint_jobs_list: int = 0
    _download_all_manifests_with_absolute_paths: int = 0
    download: Optional[int] = None
    path_mapping: Optional[int] = None


def _get_download_candidate_jobs(
    boto3_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    starting_timestamp: datetime,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    region: Optional[str] = None,
) -> dict[str, dict[str, Any]]:
    """
    Uses deadline:SearchJobs queries to get a dict {job_id: job} of download candidates for the queue.
    This is a superset of all the jobs that have produced any output for download since
    the provided starting_timestamp.

    Args:
        boto3_session: The boto3.Session for accessing AWS.
        farm_id: The farm id for the operation.
        queue_id: The queue id for the operation.
        starting_timestamp: The point in time from which to look for new download outputs.
        print_function_callback: Callback for printing output to the terminal or log.
        region: The AWS region to scope the deadline client to. When None, the session's
            default region is used.

    Returns:
        A dictionary mapping job id to the job as returned by the deadline.search_jobs API.
    """
    print_function_callback("Retrieving updated data from Deadline Cloud...")
    start_time = datetime.now(tz=timezone.utc)

    # Construct the full set of jobs that may have new available downloads.
    # - Any active job (job with taskRunStatus in READY, ASSIGNED,
    #   STARTING, SCHEDULED, or RUNNING), that has at least one SUCCEEDED task.
    # Use stringListFilter with ANY_EQUALS to query all statuses in a single API call
    download_candidate_jobs = {
        job["jobId"]: job
        for job in _list_jobs_by_filter_expression(
            boto3_session,
            farm_id,
            queue_id,
            filter_expression={
                "filters": [
                    {
                        "stringListFilter": {
                            "name": "TASK_RUN_STATUS",
                            "operator": "ANY_EQUALS",
                            "values": [
                                "READY",
                                "ASSIGNED",
                                "STARTING",
                                "SCHEDULED",
                                "RUNNING",
                            ],
                        },
                    }
                ],
                "operator": "OR",
            },
            region=region,
        )
    }
    print(f"DEBUG: Got {len(download_candidate_jobs)} active jobs")
    download_candidate_jobs = {
        job_id: _datetimes_to_str(job)
        for job_id, job in download_candidate_jobs.items()
        if job["taskRunStatusCounts"]["SUCCEEDED"] > 0
    }
    print(
        f"DEBUG: Filtered down to {len(download_candidate_jobs)} active jobs based on SUCCEEDED task filter"
    )

    # - Any recently ended job (job went from active to terminal with a taskRunStatus
    #   in SUSPENDED, CANCELED, FAILED, SUCCEEDED, NOT_COMPATIBLE), that has at least
    #   one SUCCEEDED task. The endedAt timestamp field gets updated when that occurs.
    recently_ended_jobs = _list_jobs_by_filter_expression(
        boto3_session,
        farm_id,
        queue_id,
        filter_expression={
            "filters": [
                {
                    "dateTimeFilter": {
                        "name": "ENDED_AT",
                        "dateTime": starting_timestamp,
                        "operator": "GREATER_THAN_EQUAL_TO",
                    }
                }
            ],
            "operator": "AND",
        },
        region=region,
    )
    print(
        f"DEBUG: Got {len(recently_ended_jobs)} jobs with job[endedAt] >= {starting_timestamp.astimezone().isoformat()}"
    )
    # Filter to jobs where the count of SUCCEEDED tasks is positive.
    recently_ended_jobs = [
        job for job in recently_ended_jobs if job["taskRunStatusCounts"]["SUCCEEDED"] > 0
    ]
    print(f"DEBUG: Filtered down to {len(recently_ended_jobs)} jobs based on SUCCEEDED task filter")
    download_candidate_jobs.update(
        {job["jobId"]: _datetimes_to_str(job) for job in recently_ended_jobs}
    )

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...retrieval completed in {duration}")

    return download_candidate_jobs


class CategorizedJobIds:
    """
    Takes jobs loaded from a loaded checkpoint and a query to get download candidate jobs,
    analyzes all the jobs by looking at fields like task run status counds to categorize them.

    Job categories:
        added: The job was created or requeued so it now can produce new downloads.
        updated: The job changed since the previous incremental download operation.
        unchanged: The job did not change since the previous incremental download operation.
        completed: The job finished running so all output is available for download.
        inactive: The job can no longer have any new downloads unless it is requeued. Minimal
            metadata is tracked to detect if it is requeued.
        missing_storage_profile: The job has no storage profile, but the operation requires one.
            If incremental download was called with local_storage_profile_id=None, this set
            will always be empty.
        attachments_free: The job has no job attachments associated that can produce
            outputs for download.
    """

    added: set[str] = set()
    updated: set[str] = set()
    unchanged: set[str] = set()
    completed: set[str] = set()
    inactive: set[str] = set()
    missing_storage_profile: set[str] = set()
    attachments_free: set[str] = set()


def _categorize_jobs_in_checkpoint(
    boto3_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    checkpoint: IncrementalDownloadState,
    download_candidate_jobs: dict[str, dict[str, Any]],
    new_completed_timestamp: datetime,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    region: Optional[str] = None,
) -> CategorizedJobIds:
    """
    Categorizes the provided download candidate jobs by id into a CategorizedJobIds object,
    updating the jobs within download_candidate_jobs where necessary.

    * Calls boto3 deadline.get_job() to get job attachments manifest information and storage profile id if it is not stored yet.

    Args:
        boto3_session: The boto3.Session for accessing AWS.
        farm_id: The farm id for the operation.
        queue_id: The queue id for the operation.
        checkpoint: The checkpoint for the incremental download.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
        new_completed_timestamp: This is the timestamp value that will be placed in
            checkpoint.downloads_completed_timestamp when saving the checkpoint.
        print_function_callback: Callback for printing output to the terminal or log.
        region: The AWS region to scope the deadline client to. When None, the session's
            default region is used.
    """
    deadline = get_session_client(boto3_session, "deadline", region=region)
    checkpoint_jobs = {job.job_id: job.job for job in checkpoint.jobs}
    checkpoint_job_ids = set(checkpoint_jobs.keys())

    download_candidate_job_ids = set(download_candidate_jobs.keys())

    print_function_callback(
        f"Categorizing {len(checkpoint_jobs)} checkpoint jobs against {len(download_candidate_jobs)} download candidate jobs..."
    )
    start_time = datetime.now(tz=timezone.utc)

    finished_tracking_job_ids = checkpoint_job_ids.difference(download_candidate_job_ids)
    updated_job_ids = checkpoint_job_ids.intersection(download_candidate_job_ids)
    new_job_ids = download_candidate_job_ids.difference(checkpoint_job_ids)
    # The following sets get populated while analyzing the jobs
    unchanged_job_ids = set()
    attachments_free_job_ids = set()
    missing_storage_profile = set()
    completed_job_ids = set()

    # Copy the job attachments manifest data and storage profile id from the checkpoint to the new job objects. This data
    # is not returned by deadline:SearchJobs, so we need to call deadline:GetJob on every job to retrieve it. This data
    # on a job don't change, so after the call to deadline:GetJob we can cache it indefinitely.
    for job_id in updated_job_ids:
        ip_job = checkpoint_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]

        if set(ip_job.keys()) == {"jobId"}:
            # If the job has a minimal placeholder, move the job id to the new job ids
            new_job_ids.add(job_id)
        elif ip_job["attachments"] is None:
            # Carry over the minimal placeholder identifying the job as not using job attachments
            download_candidate_jobs[job_id] = ip_job
            attachments_free_job_ids.add(job_id)
        elif ip_job["storageProfileId"] is None and checkpoint.local_storage_profile_id is not None:
            # Carry over the minimal placeholder identifying the job as missing a storage profile
            download_candidate_jobs[job_id] = ip_job
            missing_storage_profile.add(job_id)
        else:
            # Copy the attachments manifest metadata as it is not returned by deadline:SearchJobs
            dc_job["attachments"] = ip_job["attachments"]
            dc_job["storageProfileId"] = ip_job["storageProfileId"]

    updated_job_ids.difference_update(attachments_free_job_ids)
    updated_job_ids.difference_update(new_job_ids)
    updated_job_ids.difference_update(missing_storage_profile)

    # Prune jobs that we are (almost) certain have no changes by looking at its task status counts. We treat a job as unchanged if its
    # value job["taskRunStatusCounts"]["SUCCEEDED"] stayed the same and its timestamp job["endedAt"] stayed the same.
    #
    # The case this misses (and causes a delay in task output download) is the following sequence: 1/ User requeues one or more steps/tasks.
    # 2/ Tasks succeed in the correct number to equal the previous value 3/ The incremental output download command sees an equal count
    # and miscategorizes it as unchanged. If that count is all the tasks, the job["endedAt"] timestamp will catch it, and if the count
    # is less, the next time a task completes the succeeded count will be different.
    #
    # Because of this potential delay, the checkpoint needs to keep tracking all of the sessions it has seen, and cannot assume
    # that a session ending before the downloads completed timestamp was already processed.
    for job_id in updated_job_ids:
        ip_job = checkpoint_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]

        if ip_job["taskRunStatusCounts"]["SUCCEEDED"] == dc_job["taskRunStatusCounts"][
            "SUCCEEDED"
        ] and ip_job.get("endedAt") == dc_job.get("endedAt"):
            print_function_callback(f"UNCHANGED Job: {dc_job['name']} ({job_id})")
            unchanged_job_ids.add(job_id)
    updated_job_ids.difference_update(unchanged_job_ids)

    # First make note of any jobs that were dropped from tracking, for example if they were canceled or they failed
    for job_id in finished_tracking_job_ids:
        ip_job = checkpoint_jobs[job_id]
        if "taskRunStatusCounts" in ip_job:
            ip_succeeded_task_count = ip_job["taskRunStatusCounts"]["SUCCEEDED"]
            ip_total_task_count = sum(value for _, value in ip_job["taskRunStatusCounts"].items())
        else:
            ip_succeeded_task_count = 0
            ip_total_task_count = -1

        # Print something only if the job is more than a minimal "jobId" tracker
        if set(ip_job.keys()) != {"jobId"}:
            print_function_callback(f"FINISHED TRACKING Job: {ip_job['name']} ({job_id})")
            if ip_job["attachments"] is None:
                print_function_callback("  Job without job attachments is no longer active")
            elif ip_succeeded_task_count == ip_total_task_count:
                print_function_callback("   Job succeeded")
            else:
                print_function_callback(
                    "   Job is not a download candidate anymore (likely suspended, canceled or failed)"
                )

    # Process all the jobs that have updates
    for job_id in updated_job_ids:
        ip_job = checkpoint_jobs[job_id]
        dc_job = download_candidate_jobs[job_id]
        ip_succeeded_task_count = ip_job["taskRunStatusCounts"]["SUCCEEDED"]
        ip_total_task_count = sum(value for _, value in ip_job["taskRunStatusCounts"].items())
        dc_succeeded_task_count = dc_job["taskRunStatusCounts"]["SUCCEEDED"]
        dc_total_task_count = sum(value for _, value in dc_job["taskRunStatusCounts"].items())

        print_function_callback(f"EXISTING Job: {ip_job['name']} ({job_id})")
        print_function_callback(
            f"  Succeeded tasks (before): {ip_succeeded_task_count} / {ip_total_task_count}"
        )
        print_function_callback(
            f"  Succeeded tasks (now)   : {dc_succeeded_task_count} / {dc_total_task_count}"
        )

        # Use the CLI output format to produce a diff of the changes
        ip_job_repr: list[str] = _cli_object_repr(ip_job).splitlines()
        dc_job_repr: list[str] = _cli_object_repr(dc_job).splitlines()

        for line in difflib.unified_diff(
            ip_job_repr,
            dc_job_repr,
            fromfile="Previous update",
            tofile="Current update",
            lineterm="",
        ):
            print_function_callback(f"  {line}")

        if (
            dc_succeeded_task_count == dc_total_task_count
            and "endedAt" in dc_job
            and datetime.fromisoformat(dc_job["endedAt"]) < new_completed_timestamp
        ):
            completed_job_ids.add(job_id)
    updated_job_ids.difference_update(completed_job_ids)

    # Process all the jobs that are new
    for job_id in new_job_ids:
        dc_job = download_candidate_jobs[job_id]

        # Call deadline:GetJob to retrieve attachments manifest information, unless the candidate
        # already carries it. Retried failed jobs are injected via GetJob upstream, so their
        # attachments are already present — re-fetching here would be a redundant API call.
        if "attachments" not in dc_job:
            job = deadline.get_job(jobId=job_id, queueId=queue_id, farmId=farm_id)
            dc_job["attachments"] = job.get("attachments")
            dc_job["storageProfileId"] = job.get("storageProfileId")
        dc_succeeded_task_count = dc_job["taskRunStatusCounts"]["SUCCEEDED"]
        dc_total_task_count = sum(value for _, value in dc_job["taskRunStatusCounts"].items())

        print_function_callback(f"NEW Job: {dc_job['name']} ({job_id})")

        if (
            dc_job["attachments"] is not None
            and dc_job["storageProfileId"] is None
            and checkpoint.local_storage_profile_id is not None
        ):
            print_function_callback(
                "  WARNING: THE JOB OUTPUT WILL NOT BE DOWNLOADED, IT HAS NO STORAGE PROFILE."
            )
            missing_storage_profile.add(job_id)
            continue

        print_function_callback(
            f"  Succeeded tasks: {dc_succeeded_task_count} / {dc_total_task_count}"
        )
        if dc_job["attachments"] is None:
            # If the job does not use job attachments, save a minimal placeholder to avoid
            # repeatedly calling deadline:GetJob.
            download_candidate_jobs[job_id] = dc_job = {
                "jobId": job_id,
                "name": dc_job["name"],
                "attachments": None,
            }
            attachments_free_job_ids.add(job_id)
            print_function_callback("  Job does not use job attachments.")
        else:
            print_function_callback("  Manifest file system paths:")
            for manifest in dc_job["attachments"]["manifests"]:
                print_function_callback(
                    f"    - {manifest['rootPath']} ({manifest['rootPathFormat']})"
                )

        if (
            dc_succeeded_task_count == dc_total_task_count
            and "endedAt" in dc_job
            and datetime.fromisoformat(dc_job["endedAt"]) < new_completed_timestamp
        ):
            completed_job_ids.add(job_id)
    new_job_ids.difference_update(attachments_free_job_ids)
    new_job_ids.difference_update(completed_job_ids)
    new_job_ids.difference_update(missing_storage_profile)

    result = CategorizedJobIds()
    result.attachments_free = attachments_free_job_ids
    result.missing_storage_profile = missing_storage_profile
    result.completed = completed_job_ids
    result.inactive = finished_tracking_job_ids
    result.added = new_job_ids
    result.unchanged = unchanged_job_ids
    result.updated = updated_job_ids

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...categorization completed in {duration}")

    return result


def _retrieve_sessions_for_job(
    deadline_client: BaseClient,
    farm_id: str,
    queue_id: str,
    job_id: str,
    session_ended_threshold: datetime,
    output_job_sessions: dict[str, list],
):
    """
    Uses deadline.list_sessions to get all sessions of the specified job that are still running or
    that ended after session_ended_threshold.

    Places the output into output_job_sessions[job_id]

    Args:
        deadline_client: A boto3 client for accessing Deadline.
        farm_id: The farm id for the operation.
        queue_id: The queue id for the operation.
        job_id: The job id to process.
        session_ended_threshold: The timestamp threshold to filter out older sessions based on the endedAt field.
        output_job_sessions: A dictionary {job_id: session_list} to populate for the provided job id.
    """
    sessions_paginator = deadline_client.get_paginator("list_sessions")

    session_list: list[dict[str, Any]] = []
    for sessions_page in sessions_paginator.paginate(
        farmId=farm_id, queueId=queue_id, jobId=job_id
    ):
        for session in sessions_page.get("sessions", []):
            if "endedAt" not in session or session["endedAt"] >= session_ended_threshold:
                session_list.append(session)
    if session_list:
        output_job_sessions[job_id] = session_list


def _retrieve_session_actions_for_session(
    deadline_client: BaseClient,
    checkpoint_job_session_completed_indexes: dict[str, dict[str, int]],
    farm_id: str,
    queue_id: str,
    job_id: str,
    output_session: dict[str, Any],
):
    """
    Args:
        deadline_client: A boto3 client for accessing Deadline.
        checkpoint_job_session_completed_indexes: All the jobs' session action indexes loaded from the checkpoint.
            The value checkpoint_job_session_completed_indexes[job_id][session_id] is the session action index of
            the latest session action that is completed download.
        farm_id: The farm id for the operation.
        queue_id: The queue id for the operation.
        job_id: The job id to process.
        output_session: The session to populate with a sessionActions field.
    """
    session_actions_paginator = deadline_client.get_paginator("list_session_actions")

    session_action_list: list[dict[str, Any]] = []
    for session_actions_page in session_actions_paginator.paginate(
        farmId=farm_id,
        queueId=queue_id,
        jobId=job_id,
        sessionId=output_session["sessionId"],
    ):
        # Include only succeeded taskRun actions.
        for session_action in session_actions_page.get("sessionActions", []):
            succeeded = session_action.get("status") == "SUCCEEDED"
            is_task_run = "taskRun" in session_action.get("definition", {})
            if succeeded and is_task_run:
                session_action_list.append(session_action)

    if session_action_list:
        # Extract the session action indexes from the ids
        for session_action in session_action_list:
            # Session action IDs look like "sessionaction-abc123-12" for index 12
            session_action_index = int(session_action["sessionActionId"].rsplit("-", 1)[-1])
            session_action["sessionActionIndex"] = session_action_index
        # Include only session action indexes newer than latest downloaded ones from the checkpoint
        session_completed_index: Optional[int] = checkpoint_job_session_completed_indexes.get(
            job_id, {}
        ).get(output_session["sessionId"])
        if session_completed_index is not None:
            # Filter out older session actions that were already downloaded
            session_action_list = [
                session_action
                for session_action in session_action_list
                if session_action["sessionActionIndex"] > session_completed_index
            ]
        if session_action_list:
            output_session["sessionActions"] = session_action_list


def _get_job_sessions(
    boto3_session: boto3.Session,
    boto3_session_for_s3: boto3.Session,
    farm_id: str,
    queue: dict[str, Any],
    checkpoint_job_session_completed_indexes: dict[str, dict[str, int]],
    categorized_job_ids: CategorizedJobIds,
    checkpoint: IncrementalDownloadState,
    download_candidate_jobs: dict[str, dict[str, Any]],
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    region: Optional[str] = None,
) -> dict[str, list]:
    """
    This function gets all the job sessions and session actions from the completed, added, and updated jobs.
    It uses the checkpoint's session_completed_indexes to filter out older session actions that are already downloaded.

    Args:
        boto3_session: The boto3.Session for accessing AWS.
        boto3_session_for_s3: The boto3.Session to use for accessing S3.
        farm_id: The farm id for the operation.
        queue: The queue as returned by boto3 deadline.get_queue().
        checkpoint_job_session_completed_indexes: All the jobs' session action indexes loaded from the checkpoint.
            The value checkpoint_job_session_completed_indexes[job_id][session_id] is the session action index of
            the latest session action that is completed download.
        categorized_job_ids: The categorized job ids as returned by _categorize_jobs_in_checkpoint().
        checkpoint: The checkpoint for the incremental download.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
        print_function_callback: Callback for printing output to the terminal or log.
        region: The AWS region to scope the deadline client to. When None, the session's
            default region is used.

    Returns:
        Access a session action in the returned job_sessions with
            job_sessions[job_id][session_index]["sessionActions"][session_action_index]
        The returned structure looks like this:
        {
            "<job_id>": [
                {
                    "sessionId": "<session_id>",
                    ...,
                    "sessionActions": [
                        {
                            "sessionActionId": "<session_action_id>",
                            ...
                        },
                        ...
                    ]
                },
                ...
            ],
            ...
        }
    """
    job_ids = categorized_job_ids.completed.union(categorized_job_ids.added).union(
        categorized_job_ids.updated
    )
    print_function_callback(f"Retrieving sessions for {len(job_ids)} jobs...")
    start_time = datetime.now(tz=timezone.utc)

    # The max timestamp of a downloaded session's endedAt provides a lower bound to filter sessions by.
    # This is tracked in the checkpoint.
    job_session_ended_timestamp: dict[str, datetime] = {
        job.job_id: job.session_ended_timestamp
        for job in checkpoint.jobs
        if job.session_ended_timestamp is not None
    }

    deadline = get_session_client(boto3_session, "deadline", region=region)
    job_sessions: dict[str, list] = {}

    # Retrieve all the sessions with some parallelism
    max_workers = SESSIONS_API_MAX_CONCURRENCY
    print_function_callback(f"Using {max_workers} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for job_id in job_ids:
            # Use the greater of the bootstrap command timestamp and the session ended timestamps
            # recorded in the checkpoint.
            session_ended_threshold = job_session_ended_timestamp.get(job_id)
            if session_ended_threshold is None:
                session_ended_threshold = checkpoint.downloads_started_timestamp

            # For all jobs that are not NEW (including re-queued jobs) - i.e. completed and updated jobs
            # Use an eventual consistency window to accept a little extra
            if job_id not in categorized_job_ids.added:
                session_ended_threshold = session_ended_threshold - timedelta(
                    seconds=checkpoint.eventual_consistency_max_seconds
                )

            futures.append(
                executor.submit(
                    _retrieve_sessions_for_job,
                    deadline,
                    farm_id,
                    queue["queueId"],
                    job_id,
                    session_ended_threshold,
                    job_sessions,
                )
            )

        # surfaces any exceptions in the thread
        for future in concurrent.futures.as_completed(futures):
            future.result()

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...retrieval completed in {duration}")

    print_function_callback("")
    print_function_callback(
        f"Retrieving session actions for {sum(len(session_list) for session_list in job_sessions.values())} sessions..."
    )
    start_time = datetime.now(tz=timezone.utc)

    # Retrieve all the session actions with some parallelism
    max_workers = SESSIONS_API_MAX_CONCURRENCY
    print_function_callback(f"Using {max_workers} threads")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for job_id, session_list in job_sessions.items():
            for session in session_list:
                futures.append(
                    executor.submit(
                        _retrieve_session_actions_for_session,
                        deadline,
                        checkpoint_job_session_completed_indexes,
                        farm_id,
                        queue["queueId"],
                        job_id,
                        session,
                    )
                )
        # surfaces any exceptions in the thread
        for future in concurrent.futures.as_completed(futures):
            future.result()

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...retrieval completed in {duration}")

    print_function_callback("")
    print_function_callback("Populating missing manifest S3 keys...")
    start_time = datetime.now(tz=timezone.utc)

    _add_missing_output_manifests_to_job_sessions(
        boto3_session_for_s3, farm_id, queue, job_sessions, download_candidate_jobs
    )

    _filter_session_actions_without_manifests_from_job_sessions(
        job_sessions,
        download_candidate_jobs,
        print_function_callback,
    )

    duration = datetime.now(tz=timezone.utc) - start_time
    print_function_callback(f"...populated in {duration}")

    return job_sessions


def _get_storage_profiles(
    deadline: BaseClient,
    farm_id: str,
    queue: dict[str, Any],
    job_sessions: dict[str, list],
    checkpoint: IncrementalDownloadState,
    download_candidate_jobs: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """
    Retrieves all the needed storage profiles. To call this function,
    checkpoint.local_storage_profile_id must be set.

    Args:
        deadline: An AWS client for calling deadline APIs.
        farm_id: The farm id for the operation.
        queue: The queue as returned by boto3 deadline.get_queue().
        checkpoint: The checkpoint for the incremental download.
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
    """
    if checkpoint.local_storage_profile_id is None:
        raise ValueError("The checkpoint local storage profile id must be set.")

    # Collect all the storage profile ids
    storage_profile_ids: set[str] = {checkpoint.local_storage_profile_id}
    for job_id in job_sessions.keys():
        storage_profile_ids.add(download_candidate_jobs[job_id]["storageProfileId"])

    # Load all the storage profiles from Deadline Cloud
    storage_profiles = {
        storage_profile_id: deadline.get_storage_profile_for_queue(
            farmId=farm_id,
            queueId=queue["queueId"],
            storageProfileId=storage_profile_id,
        )
        for storage_profile_id in storage_profile_ids
    }

    return storage_profiles


def _create_path_mapping_rule_appliers(
    storage_profiles: dict[str, dict[str, Any]],
    checkpoint: IncrementalDownloadState,
    download_candidate_jobs: dict[str, dict[str, Any]],
    print_function_callback: Callable[[Any], None] = lambda msg: None,
) -> dict[str, Optional[_PathMappingRuleApplier]]:
    """
    Retrieves all the needed storage profiles and constructs path mapping rule applies for them.
    To call this function, checkpoint.local_storage_profile_id must be set.

    Args:
        storage_profiles: A mapping from storage profile id to the storage profile as returned by boto3 deadline.get_storage_profile_for_queue.
        checkpoint: The checkpoint for the incremental download.
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
        print_function_callback: Callback for printing output to the terminal or log.
    """
    if checkpoint.local_storage_profile_id is None:
        raise ValueError("The checkpoint local storage profile id must be set.")

    path_mapping_rule_appliers: dict[str, Optional[_PathMappingRuleApplier]] = {}

    # Create a path mapping rule applier for each storage profile
    local_storage_profile = storage_profiles[checkpoint.local_storage_profile_id]
    local_storage_profile_name = local_storage_profile["displayName"]
    print_function_callback("")
    print_function_callback(
        f"Local storage profile is {local_storage_profile_name} ({checkpoint.local_storage_profile_id})"
    )
    print_function_callback(
        f"  {len([job for job in download_candidate_jobs.values() if job.get('storageProfileId') == checkpoint.local_storage_profile_id])} download candidate jobs have the same storage profile and will be downloaded to their original specified paths"
    )
    for storage_profile_id, storage_profile in storage_profiles.items():
        storage_profile_name = storage_profile["displayName"]
        if storage_profile_id == checkpoint.local_storage_profile_id:
            path_mapping_rule_appliers[storage_profile_id] = None
        else:
            rules = _generate_path_mapping_rules(storage_profile, local_storage_profile)
            path_mapping_rule_appliers[storage_profile_id] = _PathMappingRuleApplier(rules)

            # Print the path mapping rules for each source storage profile
            print_function_callback("")
            job_count = len(
                [
                    job
                    for job in download_candidate_jobs.values()
                    if job.get("storageProfileId") == storage_profile_id
                ]
            )
            print_function_callback(
                f"Path mapping rules for {job_count} download candidate jobs with storage profile {storage_profile_name} ({storage_profile_id})"
            )
            print_function_callback(
                f"  job storage profile: {storage_profile_name} ({storage_profile['osFamily']})"
            )
            print_function_callback(
                f"  local storage profile: {local_storage_profile_name} ({local_storage_profile['osFamily']})"
            )
            if rules:
                for rule in rules:
                    print_function_callback(f"  - from: {rule.source_path}")
                    print_function_callback(f"    to:   {rule.destination_path}")
            else:
                print_function_callback(
                    f"   No rules generated. Storage profiles {local_storage_profile_name} and {storage_profile_name} share no file system location names."
                )
    return path_mapping_rule_appliers


def _add_missing_output_manifests_to_job_sessions(
    boto3_session_for_s3: boto3.Session,
    farm_id: str,
    queue: dict[str, Any],
    job_sessions: dict[str, list],
    download_candidate_jobs: dict[str, dict[str, Any]],
):
    """
    Args:
        boto3_session_for_s3: The boto3.Session to use for accessing S3.
        farm_id: The farm id for the operation.
        queue: The queue as returned by boto3 deadline.get_queue().
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
    """
    for job_id, session_list in job_sessions.items():
        job = download_candidate_jobs[job_id]
        session_action_list = [
            session_action
            for session in session_list
            for session_action in session.get("sessionActions", [])
        ]
        _add_output_manifests_from_s3(
            farm_id, queue, job, boto3_session_for_s3, session_action_list
        )


def _filter_session_actions_without_manifests_from_job_sessions(
    job_sessions: dict[str, list],
    download_candidate_jobs: dict[str, dict[str, Any]],
    print_function_callback: Callable[[Any], None] = lambda msg: None,
):
    """
    Modify job_sessions in place to filter out any session actions that lack any output manifests.
    Print a warning message for any job that had a session action like this.

    Args:
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
        print_function_callback: Callback for printing output to the terminal or log.
    """
    for job_id, session_list in job_sessions.items():
        job = download_candidate_jobs[job_id]
        total_count = 0
        filtered_count = 0
        for session in session_list:
            total_count += len(session.get("sessionActions", []))
            # Filter out session actions with no manifest files
            filtered_session_action_list = [
                session_action
                for session_action in session.get("sessionActions", [])
                if any(item != {} for item in session_action["manifests"])
            ]
            filtered_count += len(filtered_session_action_list)
            if total_count != filtered_count:
                session["sessionActions"] = filtered_session_action_list
        if total_count != filtered_count:
            print_function_callback(
                f"WARNING: Job {job['name']} ({job_id}) ran {total_count - filtered_count} / {total_count} session actions with no output."
            )
            print_function_callback(
                "         This may indicate steps in the job that strictly perform validation or save results elsewhere like a shared file system or S3."
            )


def _update_checkpoint_jobs_list(
    checkpoint: IncrementalDownloadState,
    download_candidate_jobs: dict[str, dict[str, Any]],
    categorized_job_ids: CategorizedJobIds,
    job_sessions: dict[str, list],
):
    """
    Update the jobs list in the checkpoint object.

    Args:
        checkpoint: The checkpoint for the incremental download.
        download_candidate_jobs: The result of a _get_download_candidate_jobs call, {job_id: job} where
            job is a result from a deadline.search_jobs() or deadline.get_job() call.
        categorized_job_ids: The categorized job ids as returned by _categorize_jobs_in_checkpoint().
        job_sessions: Contains each job's sessions and session actions, structured as job_sessions[job_id][session_index]["sessionActions"][session_action_index].
                      See the function _get_job_sessions for more details.
    """
    updated_jobs: list[IncrementalDownloadJob] = []

    # Produce the session_ended_timestamp for all the job ids. Start
    # with the values from the previous checkpoint, and then overwrite
    # them from job_sessions
    job_session_ended_timestamps: dict[str, Optional[datetime]] = {
        job.job_id: job.session_ended_timestamp
        for job in checkpoint.jobs
        if job.session_ended_timestamp is not None
    }
    for job_id, session_list in job_sessions.items():
        max_session_ended_timestamp = None
        for session in session_list:
            if "endedAt" in session:
                if max_session_ended_timestamp is None:
                    max_session_ended_timestamp = session["endedAt"]
                else:
                    max_session_ended_timestamp = max(
                        max_session_ended_timestamp, session["endedAt"]
                    )
        job_session_ended_timestamps[job_id] = max_session_ended_timestamp

    # Produce the session_completed_indexes for all the job ids. Start
    # with the values from the previous checkpoint, then overwrite
    # them from job_sessions.
    job_session_completed_indexes: dict[str, dict[str, int]] = {
        job.job_id: job.session_completed_indexes for job in checkpoint.jobs
    }
    for job_id, session_list in job_sessions.items():
        for session in session_list:
            session_actions = session.get("sessionActions", [])
            if session_actions:
                job_session_completed_indexes.setdefault(job_id, {})[session["sessionId"]] = max(
                    session_action["sessionActionIndex"] for session_action in session_actions
                )
        job_session_ended_timestamps[job_id] = max_session_ended_timestamp

    # These categories keep the download_candidate_jobs job as is.
    for job_id in (
        categorized_job_ids.added | categorized_job_ids.updated | categorized_job_ids.unchanged
    ):
        updated_jobs.append(
            IncrementalDownloadJob(
                download_candidate_jobs[job_id],
                job_session_ended_timestamps.get(job_id),
                job_session_completed_indexes.get(job_id, {}),
            )
        )
    # This category keeps a signal that it has no job attachments to process by having an attachments field with None in it
    for job_id in categorized_job_ids.attachments_free:
        updated_jobs.append(
            IncrementalDownloadJob(
                {
                    "jobId": job_id,
                    "name": download_candidate_jobs[job_id]["name"],
                    "attachments": None,
                },
                None,
                {},
            )
        )
    # This category keeps a signal that it is missing a storage profile by populating the attachments but
    # having a storageProfileId field with None in it. By keeping the attachments in the checkpoint, someone
    # inspecting the checkpoint to understand what's happening can get an idea about the paths for the job
    # and diagnose problems quicker.
    for job_id in categorized_job_ids.missing_storage_profile:
        updated_jobs.append(
            IncrementalDownloadJob(
                {
                    "jobId": job_id,
                    "name": download_candidate_jobs[job_id]["name"],
                    "attachments": download_candidate_jobs[job_id]["attachments"],
                    "storageProfileId": None,
                },
                None,
                {},
            )
        )
    # Keep completed jobs around until they become inactive
    for job_id in categorized_job_ids.completed:
        updated_jobs.append(
            IncrementalDownloadJob(
                download_candidate_jobs[job_id],
                job_session_ended_timestamps.get(job_id),
                job_session_completed_indexes.get(job_id, {}),
            )
        )
    # When a job becomes inactive, keep it around in minimal form when it has a session_ended_timestamp.
    # This is necessary for the case where a completed job gets requeued later. We can't tell
    # that it was requeued from the deadline.search_jobs query, so we hold this metadata in the checkpoint.
    for job_id in categorized_job_ids.inactive:
        session_ended_timestamp = job_session_ended_timestamps.get(job_id)
        if session_ended_timestamp is not None:
            updated_jobs.append(
                IncrementalDownloadJob({"jobId": job_id}, session_ended_timestamp, {})
            )

    checkpoint.jobs = updated_jobs


@api.record_function_latency_telemetry_event()
def _incremental_output_download(
    farm_id: str,
    queue: dict[str, Any],
    boto3_session: boto3.Session,
    checkpoint: IncrementalDownloadState,
    file_conflict_resolution: FileConflictResolution,
    checkpoint_dir: str,
    config: Optional[ConfigParser] = None,
    print_function_callback: Callable[[Any], None] = lambda msg: None,
    *,
    dry_run: bool = False,
) -> tuple[
    IncrementalDownloadState,
    CategorizedJobIds,
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, dict[str, Any]]],
]:
    """
    This function downloads all the task run outputs from the specified queue, that have become
    available since the last time the function was called. The checkpoint object
    keeps track of all state needed to keep track of what needs to be downloaded.

    Pre-condition: The input checkpoint holds all information needed to understand the state of downloads
        completed up to the timestamp checkpoint.downloads_completed_timestamp. See the documentation
        in the IncrementalDownloadState to understand the invariants of the checkpoint.

    Post-condition: The output checkpoint has an updated checkpoint.downloads_completed_timestamp,
        all downloads were performed up to at least this timestamp, and the checkpoint data
        is updated to satisfy the next call's pre-condition.

    Args:
        farm_id: The farm id for the operation.
        queue: The queue as returned by boto3 deadline.get_queue().
        boto3_session: The boto3.Session for accessing AWS.
        checkpoint: The checkpoint for the incremental download.
        config: Optional, a Deadline Cloud configuration as loaded from config_file.read_config().
        print_function_callback: Callback for printing output to the terminal or log.
        dry_run: If True, the operation will print out information but not perform any data downloads.

    Returns:
        A tuple of (updated checkpoint, categorized job IDs, download candidate jobs dict,
        per-job download results, per-task download results).
    """
    durations = IncrementalOutputDownloadLatencies()
    # Operations here are within a single farm, so scope the deadline client to that
    # farm's region. _resolve_region returns None when nothing is configured, preserving
    # the session's default-region behavior.
    region = _resolve_region(config=config, farm_id=farm_id)
    deadline = get_session_client(boto3_session, "deadline", region=region)

    # When this function is done, we will be confident that downloads are complete up to
    # new_completed_timestamp. We subtract a duration from now() that gives a generous amount of
    # time for the deadline:SearchJobs API's eventual consistency to converge.
    current_timestamp = datetime.now(timezone.utc)
    new_completed_timestamp = max(
        checkpoint.downloads_started_timestamp,
        current_timestamp - timedelta(seconds=checkpoint.eventual_consistency_max_seconds),
    )

    # The queue role is used for accessing S3
    boto3_session_for_s3 = api.get_queue_user_boto3_session(
        deadline=deadline,
        config=config,
        farm_id=farm_id,
        queue_id=queue["queueId"],
        queue_display_name=queue["displayName"],
    )

    print_function_callback("Updating download state across time interval:")
    print_function_callback(
        f"    From: {checkpoint.downloads_completed_timestamp.astimezone().isoformat()}"
    )
    print_function_callback(f"      To: {current_timestamp.astimezone().isoformat()}")
    update_length = current_timestamp - checkpoint.downloads_completed_timestamp
    eventual_consistency_delta = timedelta(seconds=checkpoint.eventual_consistency_max_seconds)
    if update_length > eventual_consistency_delta:
        print_function_callback(
            f"  Length: {update_length - eventual_consistency_delta} + {eventual_consistency_delta} (eventual consistency allowance)"
        )
    else:
        # Immediately after bootstrapping, this length will be shorter than the eventual consistency window
        print_function_callback(f"  Length: {update_length}")
    print_function_callback("")

    # Save all the jobs' session action indexes from the checkpoint, before we update the checkpoint's jobs list
    checkpoint_job_session_completed_indexes: dict[str, dict[str, int]] = {
        job.job_id: job.session_completed_indexes for job in checkpoint.jobs
    }

    # Load failed jobs tracker — tracks jobs that failed in previous runs so the global
    # timestamp can advance freely while still retrying failed jobs individually.
    queue_id = queue["queueId"]
    storage_profile_key = checkpoint.local_storage_profile_id or "ignore-storage-profiles"
    failed_jobs_file = os.path.join(
        checkpoint_dir, f"{queue_id}_{storage_profile_key}_failed_jobs.json"
    )
    failed_jobs_tracker = _FailedJobsTracker(failed_jobs_file)

    # Call deadline:SearchJobs to get a set of jobs that includes every job with downloads available.
    start_t = time.perf_counter_ns()
    download_candidate_jobs: dict[str, dict[str, Any]] = _get_download_candidate_jobs(
        boto3_session,
        farm_id,
        queue_id,
        checkpoint.downloads_completed_timestamp,
        print_function_callback,
        region=region,
    )
    durations._get_download_candidate_jobs = time.perf_counter_ns() - start_t

    # Inject previously failed jobs that fell outside the timestamp window
    previously_failed_job_ids = failed_jobs_tracker.get_tracked_job_ids()
    if previously_failed_job_ids:
        print_function_callback(
            f"Retrying {len(previously_failed_job_ids)} previously failed job(s)..."
        )
        jobs_not_found: set[str] = set()
        for job_id in previously_failed_job_ids:
            if job_id not in download_candidate_jobs:
                try:
                    job = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
                    injected_job = _datetimes_to_str(job)
                    # Normalize the attachments/storageProfileId keys so downstream
                    # categorization can index them directly (and skip a redundant GetJob).
                    injected_job.setdefault("attachments", job.get("attachments"))
                    injected_job.setdefault("storageProfileId", job.get("storageProfileId"))
                    download_candidate_jobs[job_id] = injected_job
                except ClientError as e:
                    if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                        # Job was deleted — remove from tracker to stop retrying
                        jobs_not_found.add(job_id)
                    # All other errors (throttling, network, auth) are transient — leave in tracker
                except Exception:
                    pass  # Unexpected error — leave in tracker to retry next run
        if jobs_not_found:
            failed_jobs_tracker.record_successes(jobs_not_found)
            if not dry_run:
                failed_jobs_tracker.save()

    # Remove abandoned jobs (hit retry cap) from download candidates so they are not
    # re-queued even when rediscovered via the timestamp window.
    abandoned_job_ids = {
        job_id
        for job_id in list(download_candidate_jobs.keys())
        if failed_jobs_tracker.is_abandoned(job_id)
    }
    for job_id in abandoned_job_ids:
        del download_candidate_jobs[job_id]

    print_function_callback("")

    # Compare the download candidates with the previously saved checkpoint state to categorize the jobs
    start_t = time.perf_counter_ns()
    categorized_job_ids: CategorizedJobIds = _categorize_jobs_in_checkpoint(
        boto3_session,
        farm_id,
        queue["queueId"],
        checkpoint,
        download_candidate_jobs,
        new_completed_timestamp,
        print_function_callback,
        region=region,
    )
    durations._categorize_jobs_in_checkpoint = time.perf_counter_ns() - start_t

    print_function_callback("")

    # All the completed, added, and updated jobs might have downloads available. Retrieve the sessions for these jobs.
    start_t = time.perf_counter_ns()
    job_sessions: dict[str, list] = _get_job_sessions(
        boto3_session,
        boto3_session_for_s3,
        farm_id,
        queue,
        checkpoint_job_session_completed_indexes,
        categorized_job_ids,
        checkpoint,
        download_candidate_jobs,
        print_function_callback,
        region=region,
    )
    durations._get_job_sessions = time.perf_counter_ns() - start_t

    # If storage profiles are being used, get them and construct all the path mapping rules
    storage_profiles: dict[str, dict[str, Any]] = {}
    path_mapping_rule_appliers: dict[str, Optional[_PathMappingRuleApplier]] = {}
    if checkpoint.local_storage_profile_id:
        start_t = time.perf_counter_ns()
        storage_profiles = _get_storage_profiles(
            deadline, farm_id, queue, job_sessions, checkpoint, download_candidate_jobs
        )
        path_mapping_rule_appliers = _create_path_mapping_rule_appliers(
            storage_profiles,
            checkpoint,
            download_candidate_jobs,
            print_function_callback,
        )
        durations.path_mapping = time.perf_counter_ns() - start_t

    # Use the information collected so far to update the jobs list in checkpoint
    start_t = time.perf_counter_ns()
    _update_checkpoint_jobs_list(
        checkpoint, download_candidate_jobs, categorized_job_ids, job_sessions
    )
    durations._update_checkpoint_jobs_list = time.perf_counter_ns() - start_t

    start_t = time.perf_counter_ns()
    unmapped_paths: dict[str, list[str]] = {}
    downloaded_manifests: list[tuple[datetime, BaseAssetManifest]] = (
        _download_all_manifests_with_absolute_paths(
            queue,
            download_candidate_jobs,
            job_sessions,
            path_mapping_rule_appliers,
            unmapped_paths,
            boto3_session_for_s3,
            print_function_callback,
        )
    )
    durations._download_all_manifests_with_absolute_paths = time.perf_counter_ns() - start_t

    # Print warning messages about all the output paths that will not be downloaded due to lack of path mapping.
    if unmapped_paths:
        print_function_callback("")
        print_function_callback("WARNING: THE FOLLOWING FILES WILL NOT BE DOWNLOADED")
        for job_id, unmapped_path_list in unmapped_paths.items():
            print_function_callback(
                f"    Job {download_candidate_jobs[job_id]['name']} ({job_id}) has outputs with unmapped paths that will not be downloaded"
            )
            storage_profile = storage_profiles.get(
                download_candidate_jobs[job_id].get("storageProfileId", "")
            )
            if storage_profile is not None:
                print_function_callback(
                    f"      Job storage profile is {storage_profile['displayName']} ({storage_profile['storageProfileId']})"
                )
            print_function_callback("      Summary of unmapped paths:")
            path_format = (
                PathFormat.WINDOWS
                if storage_profile is not None
                and storage_profile["osFamily"] == StorageProfileOperatingSystemFamily.WINDOWS.value
                else PathFormat.POSIX
            )
            paths_summary = summarize_path_list(
                unmapped_path_list, max_entries=30, path_format=path_format
            )
            print_function_callback(textwrap.indent(paths_summary, "      "))

    # Build per-job file mapping by correlating downloaded manifests with their job IDs
    manifests_to_download = _get_manifests_to_download(
        queue["jobAttachmentSettings"]["rootPrefix"],
        download_candidate_jobs,
        job_sessions,
        path_mapping_rule_appliers,
    )
    # Correlate manifests_to_download with downloaded_manifests by position to attribute each
    # downloaded manifest to its job (and task). Both lists come from _get_manifests_to_download
    # with identical inputs, so their lengths match in practice. If they ever diverge we skip only
    # the per-job/per-task attribution — the downloads still proceed via the fallback bucket below,
    # so a mismatch degrades reporting but never causes a silent zero-download run. (Downloads are
    # decoupled from attribution: what gets downloaded comes from downloaded_manifests either way;
    # attribution is best-effort layered on top.)
    skip_attribution = len(manifests_to_download) != len(downloaded_manifests)
    if skip_attribution:
        print_function_callback(
            f"WARNING: Manifest list length mismatch ({len(manifests_to_download)} vs "
            f"{len(downloaded_manifests)}) — per-job and per-task download tracking will not "
            f"be populated for this run; files will still be downloaded."
        )
    job_manifest_paths: dict[str, list[BaseManifestPath]] = {}
    # job_id -> task_id -> [files]: used for per-task download tracking
    job_task_manifest_paths: dict[str, dict[str, list[BaseManifestPath]]] = {}
    # job_id -> task_id -> [file_paths]: record of the paths originally attributed to each task.
    # Unlike job_task_manifest_paths, this is NOT emptied by cross-job transfers — a losing job
    # keeps its record so the deduped-job block below can still report its filesystem-based status
    # after the winning job takes over the paths.
    all_task_file_paths: dict[str, dict[str, list[str]]] = {}
    # job_id -> task_id -> {paths}: membership set mirroring all_task_file_paths so the per-file
    # dedup check is O(1) instead of a linear scan of the growing list.
    all_task_file_paths_seen: dict[str, dict[str, set[str]]] = {}
    # job_id -> normcased_path -> task_id that currently owns it, so a path emitted by more than
    # one task of the same job (e.g. a requeue) is attributed to a single task rather than
    # duplicated across tasks (which would double-download it and inflate the file count).
    task_owner_by_job: dict[str, dict[str, str]] = {}
    # global_seen_paths maps normcased_path -> job_id that claimed it first.
    # Prevents concurrent writes to the same destination file across jobs — parallel threads
    # could write the same path simultaneously, corrupting the file under OVERWRITE. When two
    # jobs share an output path, ownership transfers to the later-iterated job so exactly one
    # job downloads it. Content correctness is unaffected: jobs sharing a path produce the same
    # rendered file, so whichever job writes it, the bytes on disk are identical. The transfer
    # only determines which job_id is credited in the status file; deduped jobs still report
    # accurate status via the post-download filesystem check.
    global_seen_paths: dict[str, str] = {}
    # job_path_index tracks the list index for each (job_id, normcased_path) pair so a later
    # occurrence of the same path within a job overwrites the earlier one in place.
    job_path_index: dict[str, dict[str, int]] = {}
    # job_id -> task_id -> normcased_path -> list_index, mirrors job_path_index at the task level
    task_path_index: dict[str, dict[str, dict[str, int]]] = {}
    if not skip_attribution:
        # Visit manifests oldest-to-newest by their S3 LastModified timestamp so the
        # last write of any shared path wins. downloaded_manifests is filled positionally
        # (correlated to manifests_to_download by index) and is NOT pre-sorted, so we derive
        # the chronological order here rather than relying on iteration order.
        ordered_indices = sorted(
            (i for i in range(len(manifests_to_download)) if downloaded_manifests[i] is not None),
            key=lambda i: downloaded_manifests[i][0],
        )
        for i in ordered_indices:
            _, job_id, _, manifest_s3_key = manifests_to_download[i]
            manifest_tuple = downloaded_manifests[i]
            if manifest_tuple is not None:
                _, manifest = manifest_tuple
                task_id = _extract_task_id_from_s3_key(manifest_s3_key)
                for manifest_path in manifest.paths:
                    normcased = os.path.normcase(manifest_path.path)
                    prior_job_id = global_seen_paths.get(normcased)
                    if prior_job_id is not None and prior_job_id != job_id:
                        # A different job previously claimed this path with an older manifest.
                        # We iterate manifests oldest-to-newest (see ordered_indices above), so
                        # the current manifest is newer — transfer ownership to preserve
                        # newest-wins semantics.
                        prior_paths = job_manifest_paths.get(prior_job_id, [])
                        prior_idx_map = job_path_index.get(prior_job_id, {})
                        if normcased in prior_idx_map:
                            # Remove from old job's manifest list — mark as None so indices stay stable
                            prior_paths[prior_idx_map[normcased]] = None  # type: ignore[call-overload]
                            del prior_idx_map[normcased]
                        # Remove from all of the old job's per-task paths using the index for O(1) lookup.
                        # Scan all tasks since the same path could appear in multiple tasks.
                        for t_id, t_idx_map in task_path_index.get(prior_job_id, {}).items():
                            if normcased in t_idx_map:
                                job_task_manifest_paths[prior_job_id][t_id][
                                    t_idx_map[normcased]
                                ] = None  # type: ignore[call-overload]
                                del t_idx_map[normcased]
                    job_paths = job_manifest_paths.setdefault(job_id, [])
                    idx_map = job_path_index.setdefault(job_id, {})
                    if normcased in idx_map:
                        # Overwrite with newer version within same job
                        job_paths[idx_map[normcased]] = manifest_path
                    else:
                        idx_map[normcased] = len(job_paths)
                        job_paths.append(manifest_path)
                    global_seen_paths[normcased] = job_id
                    if task_id:
                        # If another task in this same job already owns this path (e.g. a requeue
                        # re-emitted it under a different task), transfer ownership to the current
                        # task. We iterate oldest-to-newest, so the current task is the newer
                        # writer; leaving the path in both tasks would download it twice and
                        # inflate the file count.
                        owner_map = task_owner_by_job.setdefault(job_id, {})
                        prior_task_id = owner_map.get(normcased)
                        if prior_task_id is not None and prior_task_id != task_id:
                            prior_t_idx_map = task_path_index.get(job_id, {}).get(prior_task_id, {})
                            if normcased in prior_t_idx_map:
                                job_task_manifest_paths[job_id][prior_task_id][
                                    prior_t_idx_map[normcased]
                                ] = None  # type: ignore[call-overload]
                                del prior_t_idx_map[normcased]
                        owner_map[normcased] = task_id
                        job_task_manifest_paths.setdefault(job_id, {}).setdefault(task_id, [])
                        task_paths = job_task_manifest_paths[job_id][task_id]
                        t_idx_map = task_path_index.setdefault(job_id, {}).setdefault(task_id, {})
                        if normcased in t_idx_map:
                            # Overwrite with newer version (O(1) lookup)
                            task_paths[t_idx_map[normcased]] = manifest_path
                        else:
                            t_idx_map[normcased] = len(task_paths)
                            task_paths.append(manifest_path)
                        # Record the path immutably for deduped-job status generation. This record
                        # is intentionally NOT pruned by cross-job transfers, so a losing job can
                        # still report filesystem-based status after another job claims its paths.
                        # Dedup per task (O(1) via the _seen set) to avoid inflating counts when the
                        # same file reappears across manifests via task retry.
                        task_file_list = all_task_file_paths.setdefault(job_id, {}).setdefault(
                            task_id, []
                        )
                        task_file_seen = all_task_file_paths_seen.setdefault(job_id, {}).setdefault(
                            task_id, set()
                        )
                        if manifest_path.path not in task_file_seen:
                            task_file_seen.add(manifest_path.path)
                            task_file_list.append(manifest_path.path)
    else:
        # Attribution skipped: download everything anyway, decoupled from per-job tracking.
        # Collect every downloaded path (deduped) under a synthetic bucket keyed by "" so it
        # never collides with a real job id. Per-job (and per-task) counts aren't populated this
        # run, but no files are lost; the "" bucket feeds the run-level stats and never becomes a
        # per-job status entry.
        fallback_seen: set[str] = set()
        fallback_paths: list[BaseManifestPath] = []
        for manifest_tuple in downloaded_manifests:
            if manifest_tuple is not None:
                _, manifest = manifest_tuple
                for manifest_path in manifest.paths:
                    normcased = os.path.normcase(manifest_path.path)
                    if normcased not in fallback_seen:
                        fallback_seen.add(normcased)
                        fallback_paths.append(manifest_path)
        if fallback_paths:
            job_manifest_paths[""] = fallback_paths

    # Filter out None entries left by cross-job path transfers (newer job took over the path)
    for job_id in list(job_manifest_paths.keys()):
        job_manifest_paths[job_id] = [p for p in job_manifest_paths[job_id] if p is not None]
        if not job_manifest_paths[job_id]:
            del job_manifest_paths[job_id]
    for job_id, task_map in job_task_manifest_paths.items():
        for task_id in list(task_map.keys()):
            task_map[task_id] = [p for p in task_map[task_id] if p is not None]
            if not task_map[task_id]:
                del task_map[task_id]

    # Print a summary of all the paths before starting the download
    all_manifest_paths = [path for paths in job_manifest_paths.values() for path in paths]
    local_path_list = [manifest_path.path for manifest_path in all_manifest_paths]
    file_size_by_path = {
        manifest_path.path: manifest_path.size for manifest_path in all_manifest_paths
    }
    print_function_callback("")
    print_function_callback("Summary of paths to download:")
    print_function_callback(
        summarize_path_list(local_path_list, total_size_by_path=file_size_by_path, max_entries=30)
    )
    print_function_callback("")

    # Download per-job with error isolation, running jobs in parallel to restore throughput.
    job_download_results: dict[str, dict[str, Any]] = {}
    # task_download_results: job_id -> task_id -> {total_files, downloaded_files, error_code, error_message}
    task_download_results: dict[str, dict[str, dict[str, Any]]] = {}
    # Set when the synthetic "" fallback bucket (attribution skipped) failed to download —
    # gates the timestamp advance below so the lost window is re-attempted next run.
    fallback_download_failed = False

    if not dry_run:
        total_files = sum(len(paths) for paths in job_manifest_paths.values())
        print_function_callback(
            f"Downloading {total_files} files from S3 across {len(job_manifest_paths)} jobs..."
        )
        start_t = time.perf_counter_ns()
        start_time = datetime.now(tz=timezone.utc)

        # Pre-warm the S3 client so all threads hit the lru_cache and share one
        # pre-built client — avoids concurrent session.client() calls across threads.
        _get_s3_client(session=boto3_session_for_s3)

        print_lock = threading.Lock()

        def _download_job(job_id: str, job_files: list) -> dict[str, Any]:
            job_name = download_candidate_jobs.get(job_id, {}).get("name", job_id)
            with print_lock:
                print_function_callback(f"  Downloading {len(job_files)} files for job: {job_name}")

            MIN_DELAY_BETWEEN_PRINTOUTS = 20

            def _make_progress_callback() -> Callable[[ProgressReportMetadata], bool]:
                # Fresh state per download call: the per-job loop below invokes one
                # download per task, and the 100%-printed latch must reset each time or
                # every task after the first would print no progress at all.
                last_call_time = time.time() - MIN_DELAY_BETWEEN_PRINTOUTS
                printed_100_percent = False

                def _update_download_progress(
                    download_metadata: ProgressReportMetadata,
                ) -> bool:
                    nonlocal last_call_time, printed_100_percent
                    if not printed_100_percent and download_metadata.progress == 100:
                        with print_lock:
                            print_function_callback(f"    {download_metadata.progressMessage}")
                        last_call_time = time.time()
                        printed_100_percent = True
                    elif (
                        not printed_100_percent
                        and time.time() - last_call_time > MIN_DELAY_BETWEEN_PRINTOUTS
                    ):
                        with print_lock:
                            print_function_callback(f"    {download_metadata.progressMessage}")
                        last_call_time = time.time()
                    return sigint_handler.continue_operation

                return _update_download_progress

            job_task_results: dict[str, dict[str, Any]] = {}
            job_error_code: Optional[str] = None
            job_error_message: Optional[str] = None

            task_map = job_task_manifest_paths.get(job_id, {})
            fallback_succeeded = False
            leftover_downloaded = 0
            leftover_failed = 0
            if task_map:
                # Per-task isolation: download each task independently so individual
                # task failures don't mark succeeded tasks as failed.
                for task_id, task_files in task_map.items():
                    try:
                        _download_manifest_paths(
                            task_files,
                            HashAlgorithm.XXH128,
                            queue,
                            boto3_session_for_s3,
                            file_conflict_resolution,
                            on_downloading_files=_make_progress_callback(),
                            print_function_callback=print_function_callback,
                        )
                        if not sigint_handler.continue_operation:
                            raise AssetSyncCancelledError("File download cancelled.")
                        job_task_results[task_id] = {
                            "total_files": len(task_files),
                            "downloaded_files": len(task_files),
                            "error_code": None,
                            "error_message": None,
                        }
                    except AssetSyncCancelledError:
                        raise
                    except Exception as e:
                        error_code = _classify_error(e)
                        with print_lock:
                            print_function_callback(
                                f"  ERROR downloading task {task_id} for job {job_name}: {e}"
                            )
                        job_task_results[task_id] = {
                            "total_files": len(task_files),
                            "downloaded_files": sum(
                                1 for f in task_files if os.path.exists(f.path)
                            ),
                            "error_code": error_code,
                            "error_message": str(e),
                        }
                        if job_error_code is None:
                            job_error_code = error_code
                            job_error_message = str(e)

                # Some of a job's paths may not carry a step-/task- segment in their manifest key
                # (so they were never attributed to a task). Download those leftovers here rather
                # than dropping them silently just because the job happened to also have task files.
                task_owned = {
                    os.path.normcase(f.path) for files in task_map.values() for f in files
                }
                leftover_files = [
                    f for f in job_files if os.path.normcase(f.path) not in task_owned
                ]
                if leftover_files:
                    try:
                        _download_manifest_paths(
                            leftover_files,
                            HashAlgorithm.XXH128,
                            queue,
                            boto3_session_for_s3,
                            file_conflict_resolution,
                            on_downloading_files=_make_progress_callback(),
                            print_function_callback=print_function_callback,
                        )
                        if not sigint_handler.continue_operation:
                            raise AssetSyncCancelledError("File download cancelled.")
                        leftover_downloaded = len(leftover_files)
                    except AssetSyncCancelledError:
                        raise
                    except Exception as e:
                        error_code = _classify_error(e)
                        with print_lock:
                            print_function_callback(
                                f"  ERROR downloading job {job_name} ({job_id}): {e}"
                            )
                        leftover_downloaded = sum(
                            1 for f in leftover_files if os.path.exists(f.path)
                        )
                        leftover_failed = len(leftover_files) - leftover_downloaded
                        if job_error_code is None:
                            job_error_code = error_code
                            job_error_message = str(e)

            else:
                # No task attribution available — fall back to per-job download
                try:
                    _download_manifest_paths(
                        job_files,
                        HashAlgorithm.XXH128,
                        queue,
                        boto3_session_for_s3,
                        file_conflict_resolution,
                        on_downloading_files=_make_progress_callback(),
                        print_function_callback=print_function_callback,
                    )
                    if not sigint_handler.continue_operation:
                        raise AssetSyncCancelledError("File download cancelled.")
                    fallback_succeeded = True
                except AssetSyncCancelledError:
                    raise
                except Exception as e:
                    job_error_code = _classify_error(e)
                    job_error_message = str(e)
                    with print_lock:
                        print_function_callback(
                            f"  ERROR downloading job {job_name} ({job_id}): {e}"
                        )

            if job_task_results:
                # Per-task path: derive counts from task results directly to avoid
                # inconsistencies from len(job_files) vs sum of per-task file lists.
                # Add any non-task-attributed leftover paths downloaded above.
                downloaded_count = (
                    sum(r["downloaded_files"] for r in job_task_results.values())
                    + leftover_downloaded
                )
                failed_count = (
                    sum(
                        r["total_files"]
                        for r in job_task_results.values()
                        if r["error_code"] is not None
                    )
                    - sum(
                        r["downloaded_files"]
                        for r in job_task_results.values()
                        if r["error_code"] is not None
                    )
                    + leftover_failed
                )
            elif fallback_succeeded:
                # Fallback path success: all files downloaded, use exact count.
                downloaded_count = len(job_files)
                failed_count = 0
            else:
                # Fallback path failure: use filesystem check for partial progress.
                downloaded_count = sum(1 for f in job_files if os.path.exists(f.path))
                failed_count = len(job_files) - downloaded_count

            # Bytes actually written. On full success sum the manifest sizes directly; on any
            # failure an isolated task may have downloaded only some of its files, so consult the
            # filesystem so a partially-failed job's succeeded bytes are counted, not dropped.
            if job_error_code is None:
                downloaded_bytes = sum(f.size for f in job_files)
            else:
                downloaded_bytes = sum(f.size for f in job_files if os.path.exists(f.path))

            return {
                "total_files": len(job_files),
                "downloaded_files": downloaded_count,
                "downloaded_bytes": downloaded_bytes,
                "failed_files": failed_count,
                "error_code": job_error_code,
                "error_message": job_error_message,
                "task_results": job_task_results,
            }

        cancelled = False
        # Bound concurrency to avoid S3 throttling and socket exhaustion — each
        # _download_manifest_paths call has its own internal thread pool, so total
        # S3 fan-out is max_workers × per-call threads.
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=SESSIONS_API_MAX_CONCURRENCY
        ) as executor:
            future_to_job = {
                executor.submit(_download_job, job_id, job_files): job_id
                for job_id, job_files in job_manifest_paths.items()
            }
            for future in concurrent.futures.as_completed(future_to_job):
                job_id = future_to_job[future]
                try:
                    result = future.result()
                    task_results = result.pop("task_results", {})
                    job_download_results[job_id] = result
                    if task_results:
                        task_download_results[job_id] = task_results
                except AssetSyncCancelledError:
                    cancelled = True
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

        if cancelled:
            raise AssetSyncCancelledError("File download cancelled.")

        # The synthetic fallback bucket (used when attribution was skipped) is retained in
        # job_download_results so the run-level file/byte stats and the success/failure signal are
        # computed from it — dropping it would report zero downloads (on success) or a false
        # success (on failure) for a run that actually moved the full window. It is never a real
        # job id, so it stays out of the per-job status entries (built from categorized_job_ids
        # only) and the failed-jobs tracker (comprehensions below skip falsy job ids). On failure
        # the flag also holds the timestamp back so the lost window is re-attempted next run.
        fallback_download_failed = job_download_results.get("", {}).get("error_code") is not None

        durations.download = time.perf_counter_ns() - start_t
        duration = datetime.now(tz=timezone.utc) - start_time
        print_function_callback(f"...downloaded in {duration}")
    else:
        print_function_callback("Skipping downloads due to DRY RUN")

    # For jobs whose paths were entirely claimed by a newer job (cross-job dedup),
    # generate task results based on what's actually on disk. The winning job may have
    # partially failed, so we check the filesystem rather than assuming all downloaded.
    # When files are missing, reuse the winning job's error for the specific path (same
    # path, same root cause) so all jobs sharing a path show a consistent error. This
    # scales to mixed errors — each path maps to its own winning task's error.
    if not dry_run:
        # Build path -> (error_code, error_message) from every winning job's per-task results.
        # A path belongs to the winning job that downloaded it; that task's error explains
        # why the file is (or isn't) on disk.
        path_error_map: dict[str, tuple[Optional[str], Optional[str]]] = {}
        for win_job_id, win_task_results in task_download_results.items():
            win_task_paths = all_task_file_paths.get(win_job_id, {})
            for win_task_id, win_result in win_task_results.items():
                err_code = win_result.get("error_code")
                if err_code is not None:
                    for p in win_task_paths.get(win_task_id, []):
                        path_error_map[os.path.normcase(p)] = (
                            err_code,
                            win_result.get("error_message"),
                        )

        for job_id, task_paths_map in all_task_file_paths.items():
            if job_id not in job_download_results and job_id not in task_download_results:
                deduped_task_results: dict[str, dict[str, Any]] = {}
                for task_id, file_paths in task_paths_map.items():
                    on_disk = sum(1 for p in file_paths if os.path.exists(p))
                    if on_disk == len(file_paths):
                        deduped_task_results[task_id] = {
                            "total_files": len(file_paths),
                            "downloaded_files": len(file_paths),
                            "error_code": None,
                            "error_message": None,
                        }
                    else:
                        # Inherit the winning job's actual error for a missing path (could be
                        # PERMISSION_DENIED, DISK_FULL, NETWORK_ERROR, etc.). Fall back to
                        # UNKNOWN only when no winning job reported an error for this path —
                        # the file is missing but this run has no attempt explaining why.
                        err_code = "UNKNOWN"
                        err_msg = "Output files not found on disk"
                        for p in file_paths:
                            if not os.path.exists(p):
                                mapped = path_error_map.get(os.path.normcase(p))
                                if mapped is not None and mapped[0] is not None:
                                    err_code = mapped[0]
                                    err_msg = mapped[1] or err_msg
                                    break
                        deduped_task_results[task_id] = {
                            "total_files": len(file_paths),
                            "downloaded_files": on_disk,
                            "error_code": err_code,
                            "error_message": err_msg,
                        }
                task_download_results[job_id] = deduped_task_results

    # Synthesize job_download_results for deduped jobs so _determine_job_download_status
    # sees their file counts and errors. These jobs share output paths with the winning
    # job — their status reflects what's actually on disk.
    for job_id in list(task_download_results.keys()):
        if job_id not in job_download_results:
            task_results = task_download_results[job_id]
            total = sum(r["total_files"] for r in task_results.values())
            downloaded = sum(r["downloaded_files"] for r in task_results.values())
            any_error = next((r for r in task_results.values() if r.get("error_code")), None)
            job_download_results[job_id] = {
                "total_files": total,
                "downloaded_files": downloaded,
                # These paths were downloaded by (and counted under) the winning job, so the
                # run-level byte total must not count them again here.
                "downloaded_bytes": 0,
                "failed_files": total - downloaded,
                "error_code": any_error["error_code"] if any_error else None,
                "error_message": any_error["error_message"] if any_error else None,
            }

    # Remove failed jobs from checkpoint so they're treated as new (added) next run.
    # Track them in the failed jobs file so they're retried even after the timestamp advances.
    # The synthetic "" fallback bucket (kept above only when it failed) is not a real job, so it
    # is excluded here — it must never reach the checkpoint filter or the failed-jobs tracker. Its
    # retry is driven by the held-back timestamp (fallback_download_failed), not per-job tracking.
    failed_job_ids = {
        job_id
        for job_id, r in job_download_results.items()
        if job_id and r.get("error_code") is not None
    }
    succeeded_job_ids = {
        job_id
        for job_id, r in job_download_results.items()
        if job_id and r.get("error_code") is None
    }
    # Any previously-tracked job that was in this run's candidate set but produced no result
    # entry (e.g. deleted, attachments_free, missing storage profile, or all paths claimed by
    # cross-job dedup) should be cleared from the tracker — no retry needed.
    previously_failed_attempted = previously_failed_job_ids & set(download_candidate_jobs.keys())
    succeeded_job_ids |= previously_failed_attempted - failed_job_ids
    if failed_job_ids:
        checkpoint.jobs = [job for job in checkpoint.jobs if job.job_id not in failed_job_ids]
        failed_jobs_tracker.record_failures(failed_job_ids, print_function_callback)
    failed_jobs_tracker.record_successes(succeeded_job_ids)
    if not dry_run:
        failed_jobs_tracker.save()

    # Always advance the timestamp — failed jobs are tracked separately so they don't
    # pin the global window. This prevents a single stuck job from degrading the entire queue.
    # Exception: when attribution was skipped, there is no per-job tracker entry to carry a
    # failed download's retry, so the global window is the only retry lever — hold it back on
    # a fallback failure so the lost outputs are re-attempted next run.
    if not fallback_download_failed:
        checkpoint.downloads_completed_timestamp = new_completed_timestamp

    stats: dict[str, Any] = {
        "downloaded_session_actions": sum(
            len(session.get("sessionActions", []))
            for session_list in job_sessions.values()
            for session in session_list
        ),
        # On dry runs job_download_results is empty — fall back to job_manifest_paths so the
        # summary reports the count/size of files that would be downloaded (the preview value).
        # Restrict to jobs that actually owned a download this run (job_id in job_manifest_paths)
        # so deduped jobs — whose files were downloaded and counted under the winning job — are
        # not counted twice. Per-task isolation makes partial success normal, so we no longer gate
        # out a job that carries an error_code: a job where only one task failed still downloaded
        # its other tasks' files, and dropping the whole job would undercount those to zero.
        "downloaded_files": sum(
            r.get("downloaded_files", 0)
            for job_id, r in job_download_results.items()
            if job_id in job_manifest_paths
        )
        if not dry_run
        else sum(len(paths) for paths in job_manifest_paths.values()),
        "downloaded_bytes": sum(
            r.get("downloaded_bytes", 0)
            for job_id, r in job_download_results.items()
            if job_id in job_manifest_paths
        )
        if not dry_run
        else sum(path.size for paths in job_manifest_paths.values() for path in paths),
        "jobs_with_downloads": {
            "completed": len(categorized_job_ids.completed),
            "added": len(categorized_job_ids.added),
            "updated": len(categorized_job_ids.updated),
        },
        "jobs_without_downloads": {
            "not_using_job_attachments": len(categorized_job_ids.attachments_free),
            "missing_storage_profile": len(categorized_job_ids.missing_storage_profile),
            "unchanged": len(categorized_job_ids.unchanged),
            "inactive": len(categorized_job_ids.inactive),
        },
        "unmapped_paths": len(unmapped_paths),
    }
    api.get_deadline_cloud_library_telemetry_client().record_event(
        event_type="com.amazon.rum.deadline.queue_sync_output_stats",
        event_details={
            "latencies": asdict(durations),
            "dry_run": dry_run,
            **stats,
        },
    )

    print_function_callback("")
    if dry_run:
        print_function_callback(
            "Summary of DRY RUN for incremental output download (no files were downloaded to the file system):"
        )
    else:
        print_function_callback("Summary of incremental output download:")
    print_function_callback(f"  Downloaded session actions: {stats['downloaded_session_actions']}")
    print_function_callback(f"  Downloaded files: {stats['downloaded_files']}")
    print_function_callback(
        f"  Downloaded bytes: {human_readable_file_size(stats['downloaded_bytes'])}"
    )
    print_function_callback("  Jobs with downloads:")
    print_function_callback(f"    completed: {stats['jobs_with_downloads']['completed']}")
    print_function_callback(f"    added: {stats['jobs_with_downloads']['added']}")
    print_function_callback(f"    updated: {stats['jobs_with_downloads']['updated']}")
    print_function_callback("  Jobs without downloads:")
    print_function_callback(
        f"    not using job attachments: {stats['jobs_without_downloads']['not_using_job_attachments']}"
    )
    print_function_callback(
        f"    missing storage profile: {stats['jobs_without_downloads']['missing_storage_profile']}"
    )
    print_function_callback(f"    unchanged: {stats['jobs_without_downloads']['unchanged']}")
    print_function_callback(f"    inactive: {stats['jobs_without_downloads']['inactive']}")

    return (
        checkpoint,
        categorized_job_ids,
        download_candidate_jobs,
        job_download_results,
        task_download_results,
    )
