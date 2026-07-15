# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
APIs for job diagnostics - get, list, and search operations for jobs, sessions, steps, and tasks.
"""

import warnings
from configparser import ConfigParser
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from ._session import get_boto3_client, _resolve_region
from . import record_function_latency_telemetry_event

if TYPE_CHECKING:
    from mypy_boto3_deadline import DeadlineClient


def _accept_deprecated_snake_case(aliases: Dict[str, str]) -> Callable:
    """
    Decorator that lets a function keep accepting its old snake_case keyword arguments
    while its canonical parameters are the camelCase (boto3-style) names.

    ``aliases`` maps each deprecated ``snake_case`` name to its canonical ``camelCase``
    equivalent. When a caller passes a snake_case keyword, its value is forwarded to the
    matching camelCase parameter and a :class:`DeprecationWarning` is emitted.

    .. deprecated::
        The snake_case aliases exist only to avoid breaking existing callers in this
        release. They are slated for removal in the next breaking release -- at which
        point this decorator (and the ``aliases`` maps below) should be deleted and the
        functions left with their camelCase parameters only.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for snake, camel in aliases.items():
                if snake not in kwargs:
                    continue
                if kwargs.get(camel) is not None:
                    raise TypeError(
                        f"{func.__name__}() received both '{camel}' and its deprecated "
                        f"alias '{snake}'; pass only '{camel}'."
                    )
                warnings.warn(
                    f"The '{snake}' parameter of {func.__name__}() is deprecated and will be "
                    f"removed in a future release; use '{camel}' instead.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                kwargs[camel] = kwargs.pop(snake)
            return func(*args, **kwargs)

        return wrapper

    return decorator


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
@_accept_deprecated_snake_case({"farm_id": "farmId", "queue_id": "queueId", "job_id": "jobId"})
def get_job(
    farmId: str,
    queueId: str,
    jobId: str,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific job.

    Args:
        farmId: The ID of the farm containing the job.
        queueId: The ID of the queue containing the job.
        jobId: The ID of the job to retrieve.
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for this farm
            from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        Job details including name, status, taskRunStatusCounts, timestamps, and lifecycle info.

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_id`` and ``job_id`` are
        deprecated in favor of the camelCase ``farmId``, ``queueId`` and ``jobId``. They
        still work in this release but will be removed in the next breaking release.
    """
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)
    return deadline.get_job(farmId=farmId, queueId=queueId, jobId=jobId)


@record_function_latency_telemetry_event()
@_accept_deprecated_snake_case(
    {
        "farm_id": "farmId",
        "queue_id": "queueId",
        "job_id": "jobId",
        "session_id": "sessionId",
    }
)
def get_session(
    farmId: str,
    queueId: str,
    jobId: str,
    sessionId: str,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get detailed information about a specific session.

    Args:
        farmId: The ID of the farm containing the session.
        queueId: The ID of the queue containing the session.
        jobId: The ID of the job containing the session.
        sessionId: The ID of the session to retrieve.
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for this farm
            from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        Session details including lifecycleStatus, log configuration, worker info.

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_id``, ``job_id`` and
        ``session_id`` are deprecated in favor of the camelCase ``farmId``, ``queueId``,
        ``jobId`` and ``sessionId``. They still work in this release but will be removed
        in the next breaking release.
    """
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)
    return deadline.get_session(farmId=farmId, queueId=queueId, jobId=jobId, sessionId=sessionId)


@record_function_latency_telemetry_event()
@_accept_deprecated_snake_case(
    {
        "farm_id": "farmId",
        "queue_id": "queueId",
        "job_id": "jobId",
        "max_results": "maxResults",
    }
)
def list_sessions(
    farmId: str,
    queueId: str,
    jobId: str,
    maxResults: Optional[int] = None,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all sessions for a job.

    Args:
        farmId: The ID of the farm containing the job.
        queueId: The ID of the queue containing the job.
        jobId: The ID of the job to list sessions for.
        maxResults: Optional maximum number of sessions to return per page (API default if not provided).
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for this farm
            from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        {"sessions": [...]} with session summaries including sessionId, lifecycleStatus, workerId.

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_id``, ``job_id`` and
        ``max_results`` are deprecated in favor of the camelCase ``farmId``, ``queueId``,
        ``jobId`` and ``maxResults``. They still work in this release but will be removed
        in the next breaking release.
    """
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)
    kwargs: Dict[str, Any] = {
        "farmId": farmId,
        "queueId": queueId,
        "jobId": jobId,
    }
    if maxResults is not None:
        kwargs["maxResults"] = maxResults
    return _call_paginated_deadline_list_api(
        deadline.list_sessions,
        "sessions",
        **kwargs,
    )


@record_function_latency_telemetry_event()
@_accept_deprecated_snake_case(
    {
        "farm_id": "farmId",
        "queue_id": "queueId",
        "job_id": "jobId",
        "max_results": "maxResults",
    }
)
def list_steps(
    farmId: str,
    queueId: str,
    jobId: str,
    maxResults: Optional[int] = None,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all steps for a job.

    Args:
        farmId: The ID of the farm containing the job.
        queueId: The ID of the queue containing the job.
        jobId: The ID of the job to list steps for.
        maxResults: Optional maximum number of steps to return per page (API default if not provided).
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for this farm
            from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        {"steps": [...]} with step summaries including stepId, name, taskRunStatus, taskRunStatusCounts.

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_id``, ``job_id`` and
        ``max_results`` are deprecated in favor of the camelCase ``farmId``, ``queueId``,
        ``jobId`` and ``maxResults``. They still work in this release but will be removed
        in the next breaking release.
    """
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)
    kwargs: Dict[str, Any] = {
        "farmId": farmId,
        "queueId": queueId,
        "jobId": jobId,
    }
    if maxResults is not None:
        kwargs["maxResults"] = maxResults
    return _call_paginated_deadline_list_api(
        deadline.list_steps,
        "steps",
        **kwargs,
    )


@record_function_latency_telemetry_event()
@_accept_deprecated_snake_case(
    {
        "farm_id": "farmId",
        "queue_id": "queueId",
        "job_id": "jobId",
        "step_id": "stepId",
        "max_results": "maxResults",
    }
)
def list_tasks(
    farmId: str,
    queueId: str,
    jobId: str,
    stepId: str,
    maxResults: Optional[int] = None,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    List all tasks for a step.

    Args:
        farmId: The ID of the farm containing the job.
        queueId: The ID of the queue containing the job.
        jobId: The ID of the job containing the step.
        stepId: The ID of the step to list tasks for.
        maxResults: Optional maximum number of tasks to return per page (API default if not provided).
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for this farm
            from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        {"tasks": [...]} with task summaries including taskId, runStatus, parameters.

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_id``, ``job_id``, ``step_id``
        and ``max_results`` are deprecated in favor of the camelCase ``farmId``, ``queueId``,
        ``jobId``, ``stepId`` and ``maxResults``. They still work in this release but will be
        removed in the next breaking release.
    """
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)
    kwargs: Dict[str, Any] = {
        "farmId": farmId,
        "queueId": queueId,
        "jobId": jobId,
        "stepId": stepId,
    }
    if maxResults is not None:
        kwargs["maxResults"] = maxResults
    return _call_paginated_deadline_list_api(
        deadline.list_tasks,
        "tasks",
        **kwargs,
    )


@record_function_latency_telemetry_event()
@_accept_deprecated_snake_case(
    {
        "farm_id": "farmId",
        "queue_ids": "queueIds",
        "task_run_status": "taskRunStatus",
        "name_contains": "nameContains",
        "page_size": "pageSize",
        "item_offset": "itemOffset",
    }
)
def search_jobs(
    farmId: Optional[str] = None,
    queueIds: Optional[List[str]] = None,
    taskRunStatus: Optional[str] = None,
    nameContains: Optional[str] = None,
    pageSize: int = 25,
    itemOffset: int = 0,
    config: Optional[ConfigParser] = None,
    region: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Search for jobs with optional filters.

    Args:
        farmId: Farm ID to search in (uses default from config if not provided).
        queueIds: List of queue IDs to search, 1-10 (uses default from config if not provided).
        taskRunStatus: Filter by status (PENDING, READY, RUNNING, FAILED, SUCCEEDED, etc.).
        nameContains: Filter jobs by name substring.
        pageSize: Results per page (1-100, default 25).
        itemOffset: Offset for pagination (0-10000).
        config: Optional configuration object.
        region: The AWS region of the farm. When omitted, it is resolved for the resolved
            farm from the config (defaults.farm_region), otherwise the session/profile region.

    Returns:
        {"jobs": [...], "totalResults": N, "nextItemOffset": N}

    .. deprecated::
        The snake_case keyword arguments ``farm_id``, ``queue_ids``, ``task_run_status``,
        ``name_contains``, ``page_size`` and ``item_offset`` are deprecated in favor of the
        camelCase ``farmId``, ``queueIds``, ``taskRunStatus``, ``nameContains``, ``pageSize``
        and ``itemOffset``. They still work in this release but will be removed in the next
        breaking release.
    """
    from ..config import config_file

    farmId = farmId or config_file.get_setting("defaults.farm_id", config=config)
    if not farmId:
        raise ValueError("farmId is required (not found in config defaults)")

    queueIds = queueIds or (
        [q] if (q := config_file.get_setting("defaults.queue_id", config=config)) else None
    )
    if not queueIds:
        raise ValueError("queueIds is required (not found in config defaults)")

    # Resolve the region only after farmId is finalized (it may come from config defaults).
    region = _resolve_region(config=config, region=region, farm_id=farmId)
    deadline: "DeadlineClient" = get_boto3_client("deadline", config=config, region=region)

    # Build filter expressions
    filter_expressions: List[Dict[str, Any]] = []

    if taskRunStatus:
        filter_expressions.append(
            {
                "stringFilter": {
                    "name": "TASK_RUN_STATUS",
                    "operator": "EQUAL",
                    "value": taskRunStatus,
                }
            }
        )

    if nameContains:
        filter_expressions.append(
            {
                "searchTermFilter": {
                    "searchTerm": nameContains,
                }
            }
        )

    # Build request parameters
    params: Dict[str, Any] = {
        "farmId": farmId,
        "queueIds": queueIds,
        "pageSize": min(max(pageSize, 1), 100),
        "itemOffset": min(max(itemOffset, 0), 10000),
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
