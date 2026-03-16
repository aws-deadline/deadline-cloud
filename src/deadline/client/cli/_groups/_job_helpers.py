# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Private helper functions for job CLI commands.
"""

from configparser import ConfigParser
from datetime import datetime, timezone
from typing import Optional

import click
from botocore.exceptions import ClientError

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _cli_object_repr, _suggest_resources_on_client_error


def _format_timestamp(dt: datetime) -> str:
    """Format a datetime to local time with timezone offset."""
    if dt is None:
        return ""
    local_dt = dt.astimezone()
    return local_dt.strftime("%Y-%m-%d %H:%M:%S %z")


def _truncate_middle(text: str, max_length: int) -> str:
    """Truncate text in the middle with '...' if too long."""
    if len(text) <= max_length:
        return text
    # Keep more at start than end for readability
    keep = max_length - 3  # account for "..."
    start = (keep * 2) // 3
    end = keep - start
    return text[:start] + "..." + text[-end:]


def _format_task_summary(task_counts: dict) -> str:
    """Format task status counts into a brief summary."""
    ready = task_counts.get("READY", 0)
    running = (
        task_counts.get("RUNNING", 0)
        + task_counts.get("STARTING", 0)
        + task_counts.get("ASSIGNED", 0)
        + task_counts.get("SCHEDULED", 0)
    )
    interrupting = task_counts.get("INTERRUPTING", 0)
    pending = task_counts.get("PENDING", 0)
    suspended = task_counts.get("SUSPENDED", 0)
    succeeded = task_counts.get("SUCCEEDED", 0)
    failed = task_counts.get("FAILED", 0)
    canceled = task_counts.get("CANCELED", 0)
    not_compatible = task_counts.get("NOT_COMPATIBLE", 0)

    parts = []
    if ready:
        parts.append(f"{ready} ready")
    if running:
        parts.append(f"{running} running")
    if interrupting:
        parts.append(f"{interrupting} interrupting")
    if pending:
        parts.append(f"{pending} pending")
    if suspended:
        parts.append(f"{suspended} suspended")
    if succeeded:
        parts.append(f"{succeeded} succeeded")
    if failed:
        parts.append(f"{failed} failed")
    if canceled:
        parts.append(f"{canceled} canceled")
    if not_compatible:
        parts.append(f"{not_compatible} not compatible")

    return ", ".join(parts) if parts else "no tasks"


def _resolve_job_search(config: Optional[ConfigParser], search_term: str) -> Optional[str]:
    """
    Search for jobs with a search term and resolve to a single job ID.

    Returns the job ID if exactly one job matches.
    If multiple jobs match, prints a summary table and returns None.
    If no jobs match, prints a message and returns None.
    """
    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)

    try:
        response = deadline.search_jobs(
            farmId=farm_id,
            queueIds=[queue_id],
            itemOffset=0,
            pageSize=5,
            filterExpressions={
                "filters": [
                    {
                        "searchTermFilter": {
                            "searchTerm": search_term,
                            "matchType": "CONTAINS",
                        }
                    }
                ],
                "operator": "AND",
            },
            sortExpressions=[{"fieldSort": {"name": "CREATED_AT", "sortOrder": "DESCENDING"}}],
        )
    except ClientError as exc:
        raise DeadlineOperationError(f"Failed to search jobs:\n{exc}") from exc

    jobs = response.get("jobs", [])
    total = response.get("totalResults", 0)

    if not jobs:
        click.echo(f'No jobs found matching "{search_term}"')
        return None

    # Single result - return job ID for detailed lookup
    if total == 1:
        return jobs[0]["jobId"]

    # Multiple results - display summary
    click.echo(f'Found {total} job(s) matching "{search_term}", showing most recent {len(jobs)}:\n')

    for job in jobs:
        name = _truncate_middle(job.get("name", job.get("displayName", "")), 80)
        created = _format_timestamp(job.get("createdAt"))
        task_counts = job.get("taskRunStatusCounts", {})
        task_summary = _format_task_summary(task_counts)

        click.echo(f"  {name}")
        click.echo(f"    {job['jobId']}  {job['taskRunStatus']:<12}  {created}")
        click.echo(f"    Tasks: {task_summary}")
        click.echo()

    if total > len(jobs):
        click.echo(f"\n  ... and {total - len(jobs)} more")

    click.echo("\nTo get details, run: deadline job get --job-id <job-id>")
    return None


def _print_job_details(config: Optional[ConfigParser], job_id: str) -> None:
    """Fetch and print full job details."""
    farm_id = config_file.get_setting("defaults.farm_id", config=config)
    queue_id = config_file.get_setting("defaults.queue_id", config=config)

    deadline = api.get_boto3_client("deadline", config=config)
    try:
        response = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    except ClientError as exc:
        suggestion = _suggest_resources_on_client_error(
            exc, farm_id=farm_id, queue_id=queue_id, config=config
        )
        raise DeadlineOperationError(
            f"Failed to get Job from Deadline:\n{exc}{suggestion}"
        ) from exc
    response.pop("ResponseMetadata", None)
    click.echo(_cli_object_repr(response))
    est = _estimate_remaining_time(response)
    click.echo(f"estimatedTimeRemaining: {est if est else 'N/A'}")


def _format_duration(seconds: float) -> str:
    """Format seconds as human-readable duration (e.g., '1 hour, 30 minutes')."""
    if seconds < 60:
        return f"{int(seconds)} seconds"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    hours = minutes // 60
    mins = minutes % 60
    parts = [f"{hours} hour{'s' if hours != 1 else ''}"]
    if mins:
        parts.append(f"{mins} minute{'s' if mins != 1 else ''}")
    return ", ".join(parts)


def _count_tasks_by_group(counts: dict) -> tuple:
    """Group task status counts into (completed, in_progress, pending) totals."""
    completed = sum(counts.get(s, 0) for s in ("SUCCEEDED", "FAILED", "CANCELED"))
    in_progress = sum(counts.get(s, 0) for s in ("RUNNING", "STARTING", "ASSIGNED"))
    pending = sum(counts.get(s, 0) for s in ("PENDING", "READY", "SCHEDULED"))
    return completed, in_progress, pending


def _estimate_remaining_time(job: dict) -> Optional[str]:
    """Estimate job completion time based on task progress and elapsed time."""
    counts = job.get("taskRunStatusCounts", {})
    started_at = job.get("startedAt")
    if not counts or not started_at:
        return None

    completed, in_progress, pending = _count_tasks_by_group(counts)

    if completed == 0 or (pending == 0 and in_progress == 0):
        return None

    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    if elapsed <= 0:
        return None

    estimated_remaining = (elapsed / completed) * (in_progress + pending)
    return _format_duration(estimated_remaining)
