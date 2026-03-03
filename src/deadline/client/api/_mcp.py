# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
APIs for job diagnostics - get, list, and search operations for jobs, sessions, steps, and tasks.
"""

from configparser import ConfigParser
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ._session import get_boto3_client
from . import record_function_latency_telemetry_event

if TYPE_CHECKING:
    from mypy_boto3_deadline import DeadlineClient


def _call_paginated_deadline_list_api(
    list_api, list_property_name: str, **kwargs
) -> Dict[str, Any]:
    """
    Calls a deadline:List* API repeatedly to concatenate all pages.

    Args:
        list_api: The List* API function to call, from the boto3 client.
        list_property_name: The name of the property in the response that contains the list.
        **kwargs: Additional arguments passed to the API (including maxResults if provided).
    """
    response = list_api(**kwargs)
    result = {list_property_name: response[list_property_name]}

    while "nextToken" in response:
        response = list_api(nextToken=response["nextToken"], **kwargs)
        result[list_property_name].extend(response[list_property_name])

    return result


@record_function_latency_telemetry_event()
def get_job(
    farm_id: str,
    queue_id: str,
    job_id: str,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific job.

    Args:
        farm_id: The ID of the farm containing the job.
        queue_id: The ID of the queue containing the job.
        job_id: The ID of the job to retrieve.
        config: Optional configuration object.

    Returns:
        Job details including name, status, taskRunStatusCounts, timestamps, and lifecycle info.
    """
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)
    return deadline.get_job(farmId=farm_id, queueId=queue_id, jobId=job_id)


@record_function_latency_telemetry_event()
def get_session(
    farm_id: str,
    queue_id: str,
    job_id: str,
    session_id: str,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific session.

    Args:
        farm_id: The ID of the farm containing the session.
        queue_id: The ID of the queue containing the session.
        job_id: The ID of the job containing the session.
        session_id: The ID of the session to retrieve.
        config: Optional configuration object.

    Returns:
        Session details including lifecycleStatus, log configuration, worker info.
    """
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)
    return deadline.get_session(
        farmId=farm_id, queueId=queue_id, jobId=job_id, sessionId=session_id
    )


@record_function_latency_telemetry_event()
def list_sessions(
    farm_id: str,
    queue_id: str,
    job_id: str,
    max_results: Optional[int] = None,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    List all sessions for a job.

    Args:
        farm_id: The ID of the farm containing the job.
        queue_id: The ID of the queue containing the job.
        job_id: The ID of the job to list sessions for.
        max_results: Optional maximum number of sessions to return per page (API default if not provided).
        config: Optional configuration object.

    Returns:
        {"sessions": [...]} with session summaries including sessionId, lifecycleStatus, workerId.
    """
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)
    kwargs: Dict[str, Any] = {
        "farmId": farm_id,
        "queueId": queue_id,
        "jobId": job_id,
    }
    if max_results is not None:
        kwargs["maxResults"] = max_results
    return _call_paginated_deadline_list_api(
        deadline.list_sessions,
        "sessions",
        **kwargs,
    )


@record_function_latency_telemetry_event()
def list_steps(
    farm_id: str,
    queue_id: str,
    job_id: str,
    max_results: Optional[int] = None,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    List all steps for a job.

    Args:
        farm_id: The ID of the farm containing the job.
        queue_id: The ID of the queue containing the job.
        job_id: The ID of the job to list steps for.
        max_results: Optional maximum number of steps to return per page (API default if not provided).
        config: Optional configuration object.

    Returns:
        {"steps": [...]} with step summaries including stepId, name, taskRunStatus, taskRunStatusCounts.
    """
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)
    kwargs: Dict[str, Any] = {
        "farmId": farm_id,
        "queueId": queue_id,
        "jobId": job_id,
    }
    if max_results is not None:
        kwargs["maxResults"] = max_results
    return _call_paginated_deadline_list_api(
        deadline.list_steps,
        "steps",
        **kwargs,
    )


@record_function_latency_telemetry_event()
def list_tasks(
    farm_id: str,
    queue_id: str,
    job_id: str,
    step_id: str,
    max_results: Optional[int] = None,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    List all tasks for a step.

    Args:
        farm_id: The ID of the farm containing the job.
        queue_id: The ID of the queue containing the job.
        job_id: The ID of the job containing the step.
        step_id: The ID of the step to list tasks for.
        max_results: Optional maximum number of tasks to return per page (API default if not provided).
        config: Optional configuration object.

    Returns:
        {"tasks": [...]} with task summaries including taskId, runStatus, parameters.
    """
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)
    kwargs: Dict[str, Any] = {
        "farmId": farm_id,
        "queueId": queue_id,
        "jobId": job_id,
        "stepId": step_id,
    }
    if max_results is not None:
        kwargs["maxResults"] = max_results
    return _call_paginated_deadline_list_api(
        deadline.list_tasks,
        "tasks",
        **kwargs,
    )


@record_function_latency_telemetry_event()
def search_jobs(
    farm_id: Optional[str] = None,
    queue_ids: Optional[List[str]] = None,
    task_run_status: Optional[str] = None,
    name_contains: Optional[str] = None,
    page_size: int = 25,
    item_offset: int = 0,
    config: Optional[ConfigParser] = None,
) -> Dict[str, Any]:
    """
    Search for jobs with optional filters.

    Args:
        farm_id: Farm ID to search in (uses default from config if not provided).
        queue_ids: List of queue IDs to search, 1-10 (uses default from config if not provided).
        task_run_status: Filter by status (PENDING, READY, RUNNING, FAILED, SUCCEEDED, etc.).
        name_contains: Filter jobs by name substring.
        page_size: Results per page (1-100, default 25).
        item_offset: Offset for pagination (0-10000).
        config: Optional configuration object.

    Returns:
        {"jobs": [...], "totalResults": N, "nextItemOffset": N}
    """
    from ..config import config_file

    farm_id = farm_id or config_file.get_setting("defaults.farm_id", config=config)
    if not farm_id:
        raise ValueError("farm_id is required (not found in config defaults)")

    queue_ids = queue_ids or (
        [q] if (q := config_file.get_setting("defaults.queue_id", config=config)) else None
    )
    if not queue_ids:
        raise ValueError("queue_ids is required (not found in config defaults)")

    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config)

    # Build filter expressions
    filter_expressions: List[Dict[str, Any]] = []

    if task_run_status:
        filter_expressions.append(
            {
                "stringFilter": {
                    "name": "TASK_RUN_STATUS",
                    "operator": "EQUAL",
                    "value": task_run_status,
                }
            }
        )

    if name_contains:
        filter_expressions.append(
            {
                "searchTermFilter": {
                    "searchTerm": name_contains,
                }
            }
        )

    # Build request parameters
    params: Dict[str, Any] = {
        "farmId": farm_id,
        "queueIds": queue_ids,
        "pageSize": min(max(page_size, 1), 100),
        "itemOffset": min(max(item_offset, 0), 10000),
    }

    if filter_expressions:
        params["filterExpressions"] = {
            "filters": filter_expressions,
            "operator": "AND",
        }

    response = deadline.search_jobs(**params)

    return {
        "jobs": response.get("jobs", []),
        "totalResults": response.get("totalResults", 0),
        "nextItemOffset": response.get("nextItemOffset"),
    }
