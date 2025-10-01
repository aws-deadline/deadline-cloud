# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Deadline Cloud Job tools.
"""

import io
import json
import os
import time
from contextlib import redirect_stdout
from typing import Any, Dict, Optional

from ...client.api import create_job_from_job_bundle
from ...client.cli._groups.job_group import _download_job_output
from ...client.config import config_file

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
