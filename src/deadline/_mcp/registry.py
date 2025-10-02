# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tool registry and mapping definitions for MCP tools in Deadline Cloud."""

from typing import Any, Callable, List, Optional, TypedDict, Dict

from ..client import api
from .tools import job


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
        "param_names": ["nextToken", "principalId", "maxResults"],
    },
    "list_queues": {
        "func": api.list_queues,
        "param_names": ["farmId", "principalId", "status", "nextToken", "maxResults"],
    },
    "list_jobs": {
        "func": api.list_jobs,
        "param_names": ["farmId", "queueId", "principalId", "nextToken", "maxResults"],
    },
    "list_fleets": {
        "func": api.list_fleets,
        "param_names": [
            "farmId",
            "principalId",
            "displayName",
            "status",
            "nextToken",
            "maxResults",
        ],
    },
    "list_storage_profiles_for_queue": {
        "func": api.list_storage_profiles_for_queue,
        "param_names": ["farmId", "queueId", "nextToken", "maxResults"],
    },
    "check_authentication_status": {
        "func": api.check_authentication_status,
        "param_names": None,
    },
    "submit_job": {
        "func": job.submit_job,
        "param_names": None,
    },
    "download_job_output": {
        "func": job.download_job_output,
        "param_names": None,
    },
}
