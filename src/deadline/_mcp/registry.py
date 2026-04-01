# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tool registry and mapping definitions for MCP tools in Deadline Cloud."""

from typing import Any, Callable, List, Optional, TypedDict, Dict

from ..client import api
from .tools import job, logs


class ToolDefinition(TypedDict):
    """Definition of a single MCP tool including its function and parameters."""

    func: Callable[..., Any]
    param_names: Optional[List[str]]


def get_tool_definition(tool_name: str) -> ToolDefinition:
    """Get the definition for a specific tool."""
    if tool_name not in TOOL_REGISTRY:
        raise ValueError(f"Tool '{tool_name}' not found in registry")
    return TOOL_REGISTRY[tool_name]


def get_all_tool_names() -> List[str]:
    """Get all registered tool names."""
    return list(TOOL_REGISTRY.keys())


TOOL_REGISTRY: Dict[str, ToolDefinition] = {
    "list_farms": {
        "func": api.list_farms,
        "param_names": ["nextToken", "principalId"],
    },
    "list_queues": {
        "func": api.list_queues,
        "param_names": ["farmId", "principalId", "status", "nextToken"],
    },
    "list_jobs": {
        "func": api.list_jobs,
        "param_names": ["farmId", "queueId", "principalId", "nextToken"],
    },
    "list_fleets": {
        "func": api.list_fleets,
        "param_names": [
            "farmId",
            "principalId",
            "displayName",
            "status",
            "nextToken",
        ],
    },
    "list_storage_profiles_for_queue": {
        "func": api.list_storage_profiles_for_queue,
        "param_names": ["farmId", "queueId", "nextToken"],
    },
    "check_authentication_status": {
        "func": api.check_authentication_status,
        "param_names": None,
    },
    "get_session_logs": {
        "func": api.get_session_logs,
        "param_names": [
            "farm_id",
            "queue_id",
            "session_id",
            "job_id",
            "limit",
            "start_time",
            "end_time",
            "next_token",
        ],
    },
    "submit_job": {
        "func": job.submit_job,
        "param_names": None,
    },
    "download_job_output": {
        "func": job.download_job_output,
        "param_names": None,
    },
    "get_session_and_worker_logs": {
        "func": logs.get_session_and_worker_logs,
        "param_names": None,
    },
    # Diagnostics - Primitive APIs
    "get_job": {
        "func": api.get_job,
        "param_names": ["farm_id", "queue_id", "job_id"],
    },
    "get_session": {
        "func": api.get_session,
        "param_names": ["farm_id", "queue_id", "job_id", "session_id"],
    },
    "list_sessions": {
        "func": api.list_sessions,
        "param_names": ["farm_id", "queue_id", "job_id"],
    },
    "list_steps": {
        "func": api.list_steps,
        "param_names": ["farm_id", "queue_id", "job_id"],
    },
    "list_tasks": {
        "func": api.list_tasks,
        "param_names": ["farm_id", "queue_id", "job_id", "step_id"],
    },
    "search_jobs": {
        "func": api.search_jobs,
        "param_names": [
            "farm_id",
            "queue_ids",
            "task_run_status",
            "name_contains",
            "page_size",
            "item_offset",
        ],
    },
}
