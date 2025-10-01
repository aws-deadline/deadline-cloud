# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Utility functions for MCP tool registration in Deadline Cloud."""

import inspect
import json
import logging
import time
from typing import Any, Callable, Dict, List, Optional

from mcp.server.fastmcp import FastMCP

from .registry import get_all_tool_names, get_tool_definition, ToolDefinition
from deadline.client.api._telemetry import get_deadline_cloud_library_telemetry_client

logger = logging.getLogger(__name__)


def _default_serializer(obj: Any) -> Any:
    """Default serializer for API responses."""
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    elif hasattr(obj, "to_dict"):
        return obj.to_dict()
    return str(obj)


def _default_error_handler(e: Exception) -> Dict:
    """Default error handler for API calls."""
    error_info = {"error": str(e), "type": type(e).__name__}
    if hasattr(e, "response") and hasattr(e.response, "status_code"):
        error_info["status_code"] = e.response.status_code
    logger.error(f"API tool error: {error_info}", exc_info=True)
    return error_info


def _create_wrapper(
    config: ToolDefinition, serializer: Callable, error_handler: Callable
) -> Callable:
    """Create a wrapper function based on the tool configuration."""
    func = config["func"]
    param_names = config["param_names"]

    # Inspect signature once and reuse
    func_sig = inspect.signature(func)

    if param_names is None:
        param_names = [p for p in func_sig.parameters.keys() if p != "config"]

    # Check if function accepts 'config' parameter
    has_config_param = "config" in func_sig.parameters

    signature = inspect.Signature(
        [
            inspect.Parameter(
                name,
                inspect.Parameter.KEYWORD_ONLY,
                default=None,
                annotation=func_sig.parameters[name].annotation
                if name in func_sig.parameters
                else str,
            )
            for name in param_names
        ],
        return_annotation=dict,
    )

    def wrapper(**kwargs) -> dict:
        start_t = time.perf_counter_ns()
        success = True
        error_type = None

        try:
            # Filter out empty/null values that MCP clients might send
            filtered_kwargs = {k: v for k, v in kwargs.items() if v not in (None, "", "null")}

            # Type conversion now handled by preserving original function annotations

            # Add config parameter if function accepts it
            if has_config_param:
                filtered_kwargs["config"] = None

            result = json.loads(json.dumps(func(**filtered_kwargs), default=serializer))
        except Exception as e:
            success = False
            error_type = type(e).__name__
            result = error_handler(e)

        # Record telemetry data
        try:
            telemetry_client = get_deadline_cloud_library_telemetry_client()
            latency = time.perf_counter_ns() - start_t

            telemetry_client.record_event(
                event_type="com.amazon.rum.deadline.mcp.latency",
                event_details={
                    "latency": latency,
                    "tool_name": wrapper.__name__,
                    "usage_mode": "MCP",
                },
            )

            telemetry_client.record_event(
                event_type="com.amazon.rum.deadline.mcp.usage",
                event_details={
                    "tool_name": wrapper.__name__,
                    "is_success": success,
                    "error_type": error_type,
                    "usage_mode": "MCP",
                },
            )
        except Exception as telemetry_error:
            logger.debug(
                f"Failed to record telemetry for MCP tool {func.__name__}: {telemetry_error}"
            )

        return result

    wrapper.__signature__ = signature  # type: ignore[attr-defined]
    return wrapper


def register_api_tools(
    app: FastMCP,
    tools: Optional[List[Callable]] = None,
    prefix: str = "",
    error_handler: Optional[Callable] = None,
    serializer: Optional[Callable] = None,
) -> None:
    """Register API tools with the MCP server.

    Args:
        app: FastMCP application instance
        tools: Optional list of specific functions to register. If None, registers all configured tools.
        prefix: Prefix to add to tool names
        error_handler: Optional custom error handler
        serializer: Optional custom serializer
    """
    error_handler = error_handler or _default_error_handler
    serializer = serializer or _default_serializer

    if tools is not None:
        # Register only the specified tools
        for func in tools:
            if not callable(func):
                raise ValueError(f"Tool {func} is not callable")

            if hasattr(func, "_mcp_tool_registered"):
                continue  # Already registered, skip

            # Find the tool name for this function in the registry
            tool_name = None
            for name in get_all_tool_names():
                config = get_tool_definition(name)
                if config["func"] == func:
                    tool_name = name
                    break

            if tool_name is None:
                raise ValueError(f"Function {func.__name__} not found in tool registry")

            config = get_tool_definition(tool_name)
            wrapper = _create_wrapper(config, serializer, error_handler)
            wrapper.__name__ = tool_name

            description = func.__doc__
            wrapper.__doc__ = description

            app.tool(name=f"{prefix}{tool_name}", description=description)(wrapper)
            func._mcp_tool_registered = True  # type: ignore[attr-defined]
    else:
        # Register all configured tools
        for tool_name in get_all_tool_names():
            config = get_tool_definition(tool_name)
            func = config["func"]

            if not callable(func) or hasattr(func, "_mcp_tool_registered"):
                continue

            wrapper = _create_wrapper(config, serializer, error_handler)
            wrapper.__name__ = tool_name

            description = func.__doc__
            wrapper.__doc__ = description

            app.tool(name=f"{prefix}{tool_name}", description=description)(wrapper)
            func._mcp_tool_registered = True  # type: ignore[attr-defined]
