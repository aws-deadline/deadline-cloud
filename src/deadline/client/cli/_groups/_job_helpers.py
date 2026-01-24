# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Private helper functions for job CLI commands.
"""

from configparser import ConfigParser
from datetime import datetime
from typing import Optional

import click
from botocore.exceptions import ClientError

from ... import api
from ...config import config_file
from ...exceptions import DeadlineOperationError
from .._common import _cli_object_repr


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
                    {"searchTermFilter": {"searchTerm": search_term, "matchType": "CONTAINS"}}
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
    response = deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)
    response.pop("ResponseMetadata", None)
    click.echo(_cli_object_repr(response))
