# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Deadline Cloud Job tools.
"""

import datetime
import io
import json
import os
import time
from contextlib import redirect_stdout
from typing import Any, Dict, Optional

from ...client.api import create_job_from_job_bundle, get_session_logs, get_boto3_client
from ...client.cli._groups.job_group import _download_job_output
from ...client.config import config_file
from ...client.exceptions import DeadlineOperationError

# TODO: Make submit_job tool async once progress reporting feature is supported in clients


def submit_job(
    job_bundle_dir: str,
    job_parameters: Optional[str] = None,
    name: Optional[str] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    storage_profile_id: Optional[str] = None,
    priority: Optional[int] = 50,
    max_failed_tasks_count: Optional[int] = None,
    max_retries_per_task: Optional[int] = None,
    max_worker_count: Optional[int] = None,
    job_attachments_file_system: Optional[str] = None,
    require_paths_exist: bool = False,
    submitter_name: Optional[str] = None,
    known_asset_paths: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Submit an Open Job Description job bundle to AWS Deadline Cloud.

    Args:
        job_bundle_dir: Path to the job bundle directory containing template.json/yaml
        job_parameters: JSON string of job parameters in format [{"name": "param_name", "value": "param_value"}]
        name: Job name to override the one in the bundle
        farm_id: Farm ID to submit to (uses default if not provided)
        queue_id: Queue ID to submit to (uses default if not provided)
        storage_profile_id: Storage profile ID to use
        priority: Job priority (1-100, default 50)
        max_failed_tasks_count: Maximum failed tasks before job fails
        max_retries_per_task: Maximum retries per task
        max_worker_count: Maximum worker count for the job
        job_attachments_file_system: File system type (COPIED or VIRTUAL)
        require_paths_exist: Return error if input files are missing
        submitter_name: Name of the submitting application
        known_asset_paths: JSON array of paths that shouldn't generate warnings

    Returns:
        Dictionary containing job_id and submission status
    """
    start_time = time.time()

    if not os.path.exists(job_bundle_dir):
        raise ValueError(f"Job bundle directory does not exist: {job_bundle_dir}")

    if not os.path.isdir(job_bundle_dir):
        raise ValueError(f"Path is not a directory: {job_bundle_dir}")

    # Parse job parameters
    parsed_job_parameters = []
    if job_parameters:
        parsed_job_parameters = json.loads(job_parameters)
        if not isinstance(parsed_job_parameters, list):
            raise ValueError(
                "job_parameters must be a JSON array of objects with 'name' and 'value' keys"
            )

    # Parse known asset paths
    parsed_known_asset_paths = []
    if known_asset_paths:
        parsed_known_asset_paths = json.loads(known_asset_paths)
        if not isinstance(parsed_known_asset_paths, list):
            raise ValueError("known_asset_paths must be a JSON array of strings")

    # Read config and use parameter values if provided, otherwise fall back to config defaults
    config = config_file.read_config()

    farm_id = farm_id or config.get("defaults", "farm_id", fallback=None)
    queue_id = queue_id or config.get("defaults", "queue_id", fallback=None)
    storage_profile_id = storage_profile_id or config.get(
        "defaults", "storage_profile_id", fallback=None
    )

    if not farm_id:
        raise ValueError("farm_id is required")
    if not queue_id:
        raise ValueError("queue_id is required")

    config.set("defaults", "farm_id", farm_id)
    config.set("defaults", "queue_id", queue_id)
    if storage_profile_id:
        config.set("defaults", "storage_profile_id", storage_profile_id)

    # Submit the job
    job_id = create_job_from_job_bundle(
        job_bundle_dir=job_bundle_dir,
        job_parameters=parsed_job_parameters,
        name=name,
        config=config,
        priority=priority,
        max_failed_tasks_count=max_failed_tasks_count,
        max_retries_per_task=max_retries_per_task,
        max_worker_count=max_worker_count,
        job_attachments_file_system=job_attachments_file_system,
        require_paths_exist=require_paths_exist,
        submitter_name=submitter_name or "MCP",
        known_asset_paths=parsed_known_asset_paths,
    )

    total_time = time.time() - start_time

    return {
        "status": "success",
        "job_id": job_id,
        "message": f"Successfully submitted job bundle from {job_bundle_dir}",
        "total_time_seconds": round(total_time, 1),
    }


def download_job_output(
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    job_id: Optional[str] = None,
    step_id: Optional[str] = None,
    task_id: Optional[str] = None,
    conflict_resolution: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Download job output files from AWS Deadline Cloud.

    Args:
        farm_id: Farm ID (uses default if not provided)
        queue_id: Queue ID (uses default if not provided)
        job_id: Job ID to download output from
        step_id: Optional step ID to download output from specific step
        task_id: Optional task ID to download output from specific task (requires step_id)
        conflict_resolution: How to handle file conflicts - SKIP, OVERWRITE, or CREATE_COPY (default)

    Returns:
        Dictionary containing download status and summary
    """
    start_time = time.time()

    if task_id and not step_id:
        raise ValueError("step_id is required when task_id is provided")
    if not job_id:
        raise ValueError("job_id is required")
    if conflict_resolution and conflict_resolution.upper() not in [
        "SKIP",
        "OVERWRITE",
        "CREATE_COPY",
    ]:
        raise ValueError(
            f"Invalid conflict_resolution: {conflict_resolution}. Must be SKIP, OVERWRITE, or CREATE_COPY"
        )

    config = config_file.read_config()
    config.set("defaults", "farm_id", farm_id or config.get("defaults", "farm_id", fallback=""))
    config.set("defaults", "queue_id", queue_id or config.get("defaults", "queue_id", fallback=""))
    config.set("defaults", "job_id", job_id)

    if not config.has_section("settings"):
        config.add_section("settings")
    config.set("settings", "auto_accept", "true")
    if conflict_resolution:
        config.set("settings", "conflict_resolution", conflict_resolution.upper())

    captured_output = io.StringIO()
    with redirect_stdout(captured_output):
        _download_job_output(
            config,
            config.get("defaults", "farm_id"),
            config.get("defaults", "queue_id"),
            job_id,
            step_id,
            task_id,
            is_json_format=False,
        )

    output_text = captured_output.getvalue()
    total_time = time.time() - start_time

    return {
        "status": "success",
        "job_id": job_id,
        "step_id": step_id,
        "task_id": task_id,
        "total_time_seconds": round(total_time, 1),
        "output": output_text.strip(),
    }


def _parse_time(time_str: Optional[str]) -> Optional[datetime.datetime]:
    return datetime.datetime.fromisoformat(time_str.replace("Z", "+00:00")) if time_str else None


def _get_session_sort_key(session: Dict[str, Any]) -> tuple:
    is_completed = "endedAt" in session
    default_time = datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

    if is_completed:
        time_value = session.get("endedAt", default_time).timestamp()
    else:
        time_value = session.get("startedAt", default_time).timestamp()

    # Sort: ongoing first (False < True), then by time descending (negative)
    return (is_completed, -time_value)


def _format_timestamp(timestamp: datetime.datetime, use_local_time: bool) -> str:
    if use_local_time:
        return timestamp.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    return timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")


def get_job_logs(
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    job_id: Optional[str] = None,
    session_id: Optional[str] = None,
    limit: Optional[int] = 100,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    next_token: Optional[str] = None,
    timezone: Optional[str] = "utc",
) -> Dict[str, Any]:
    """
    Get CloudWatch logs for a specific session.

    If session_id is not provided, auto-selects the most recent session
    (prioritizing ongoing sessions over completed ones).

    Args:
        farm_id: Farm ID (uses default if not provided)
        queue_id: Queue ID (uses default if not provided)
        job_id: Job ID to get logs for (required if session_id not provided)
        session_id: Session ID to get logs for (optional, will auto-select if not provided)
        limit: Maximum number of log lines to return (default: 100)
        start_time: Start time for logs in ISO format (e.g., 2023-01-01T12:00:00Z)
        end_time: End time for logs in ISO format (e.g., 2023-01-01T13:00:00Z)
        next_token: Token for pagination of results
        timezone: Timezone for timestamps - "utc" or "local" (default: "utc")

    Returns:
        Dictionary containing log events, metadata, and pagination info
    """
    # Read config and apply defaults for farm_id and queue_id only
    config = config_file.read_config()
    farm_id = farm_id or config.get("defaults", "farm_id", fallback=None)
    queue_id = queue_id or config.get("defaults", "queue_id", fallback=None)

    # Validate required parameters
    if not farm_id or not queue_id:
        raise ValueError("farm_id and queue_id are required")

    if not job_id and not session_id:
        raise ValueError("Either job_id or session_id must be provided")

    deadline = get_boto3_client("deadline", config=config)

    # Auto-select session if not provided (job_id is guaranteed to exist here)
    if not session_id:
        paginator = deadline.get_paginator("list_sessions")
        sessions = [
            s
            for page in paginator.paginate(farmId=farm_id, queueId=queue_id, jobId=job_id)
            for s in page.get("sessions", [])
        ]

        if not sessions:
            raise DeadlineOperationError(f"No sessions found for job {job_id}")

        # Select the most recent session (prefer ongoing over completed)
        sessions.sort(key=_get_session_sort_key)
        session_id = sessions[0]["sessionId"]

    # Get logs
    result = get_session_logs(
        farm_id=farm_id,
        queue_id=queue_id,
        session_id=session_id,
        limit=limit or 100,
        start_time=_parse_time(start_time),
        end_time=_parse_time(end_time),
        next_token=next_token,
        config=config,
    )

    # Format timestamps
    use_local = (timezone or "utc").lower() == "local"

    return {
        "status": "success",
        "jobId": job_id,
        "sessionId": session_id,
        "events": [
            {
                "timestamp": _format_timestamp(e.timestamp, use_local),
                "message": e.message,
                "ingestionTime": (
                    _format_timestamp(e.ingestion_time, use_local) if e.ingestion_time else None
                ),
                "eventId": e.event_id,
            }
            for e in result.events
        ],
        "count": result.count,
        "nextToken": result.next_token,
        "logGroup": result.log_group,
        "logStream": result.log_stream,
    }
