# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Unit tests for MCP telemetry functionality."""

import pytest
from unittest.mock import patch, MagicMock

# Skip all tests in this module if MCP dependencies are not available
pytest.importorskip("mcp", reason="MCP dependencies not available")

from deadline._mcp.utils import _create_wrapper
from deadline._mcp.registry import ToolDefinition


@pytest.fixture(scope="function", name="mock_telemetry_client")
def fixture_mock_telemetry_client():
    """Fixture to provide a mock telemetry client for MCP tests."""
    mock_client = MagicMock()
    mock_client.is_initialized = True
    mock_client.event_queue = MagicMock()
    return mock_client


def test_mcp_tool_telemetry_success(mock_telemetry_client):
    """Test that MCP tools record telemetry on successful execution."""
    mock_func = MagicMock()
    mock_func.__name__ = "test_function"
    mock_func.return_value = {"result": "success"}

    config: ToolDefinition = {"func": mock_func, "param_names": ["param1", "param2"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        result = wrapper(param1="value1", param2="value2")

        mock_func.assert_called_once_with(param1="value1", param2="value2")
        assert result == {"result": "success"}
        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list

        latency_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.latency"
        )
        latency_details = latency_call[1]["event_details"]
        assert "latency" in latency_details
        assert latency_details["tool_name"] == "wrapper"  # In unit tests, wrapper name is used
        assert latency_details["usage_mode"] == "MCP"

        usage_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.usage"
        )
        usage_details = usage_call[1]["event_details"]
        assert usage_details["tool_name"] == "wrapper"  # In unit tests, wrapper name is used
        assert usage_details["is_success"] is True
        assert usage_details["error_type"] is None
        assert usage_details["usage_mode"] == "MCP"


def test_mcp_tool_telemetry_failure(mock_telemetry_client):
    """Test that MCP tools record telemetry on failed execution."""
    mock_func = MagicMock()
    mock_func.__name__ = "test_function"
    mock_func.side_effect = ValueError("Test error")

    config: ToolDefinition = {"func": mock_func, "param_names": ["param1"]}

    mock_serializer = MagicMock()
    mock_error_handler = MagicMock(return_value={"error": "Test error", "type": "ValueError"})

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        result = wrapper(param1="value1")

        mock_error_handler.assert_called_once()
        assert result == {"error": "Test error", "type": "ValueError"}
        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list

        latency_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.latency"
        )
        latency_details = latency_call[1]["event_details"]
        assert "latency" in latency_details
        assert latency_details["tool_name"] == "wrapper"  # In unit tests, wrapper name is used
        assert latency_details["usage_mode"] == "MCP"

        usage_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.usage"
        )
        usage_details = usage_call[1]["event_details"]
        assert usage_details["tool_name"] == "wrapper"  # In unit tests, wrapper name is used
        assert usage_details["is_success"] is False
        assert usage_details["error_type"] == "ValueError"
        assert usage_details["usage_mode"] == "MCP"


def test_mcp_tool_telemetry_error_handling(mock_telemetry_client):
    """Test that telemetry errors don't affect tool execution."""
    mock_func = MagicMock()
    mock_func.__name__ = "test_function"
    mock_func.return_value = {"result": "success"}

    mock_telemetry_client.record_event.side_effect = Exception("Telemetry error")

    config: ToolDefinition = {"func": mock_func, "param_names": ["param1"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        # Should not raise exception despite telemetry error
        result = wrapper(param1="value1")

        mock_func.assert_called_once_with(param1="value1")
        assert result == {"result": "success"}
        assert mock_telemetry_client.record_event.call_count > 0


def test_mcp_tool_parameter_filtering():
    """Test that empty/null parameters are filtered correctly."""
    mock_func = MagicMock()
    mock_func.__name__ = "test_function"
    mock_func.return_value = {"result": "success"}

    mock_telemetry_client = MagicMock()

    config: ToolDefinition = {"func": mock_func, "param_names": ["param1", "param2", "param3"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        result = wrapper(param1="value1", param2=None, param3="", param4="null")

        mock_func.assert_called_once_with(param1="value1")
        assert result == {"result": "success"}


def test_mcp_tool_telemetry_client_initialization_error():
    """Test behavior when telemetry client fails to initialize."""
    mock_func = MagicMock()
    mock_func.__name__ = "test_function"
    mock_func.return_value = {"result": "success"}

    config: ToolDefinition = {"func": mock_func, "param_names": ["param1"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.side_effect = Exception("Telemetry client initialization failed")

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        # Should not raise exception despite telemetry client error
        result = wrapper(param1="value1")

        mock_func.assert_called_once_with(param1="value1")
        assert result == {"result": "success"}
        mock_get_client.assert_called()


def test_mcp_tool_aws_error_handling():
    """Test that MCP tools properly handle AWS service errors."""
    mock_func = MagicMock()
    mock_func.__name__ = "list_farms"

    from botocore.exceptions import ClientError

    error_response = {
        "Error": {"Code": "ValidationException", "Message": "Invalid farm ID format"},
        "ResponseMetadata": {"HTTPStatusCode": 400},
    }
    mock_func.side_effect = ClientError(error_response, "ListFarms")

    mock_telemetry_client = MagicMock()

    config: ToolDefinition = {"func": mock_func, "param_names": ["farmId"]}

    from deadline._mcp.utils import _default_error_handler, _default_serializer

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, _default_serializer, _default_error_handler)
        result = wrapper(farmId="invalid-farm-id")

        mock_func.assert_called_once_with(farmId="invalid-farm-id")

        assert "error" in result, "Error response should contain 'error' field"
        assert "type" in result, "Error response should contain 'type' field"

        assert result["type"] == "ClientError"
        assert "ValidationException" in result["error"] or "Invalid farm ID" in result["error"]

        if "status_code" in result:
            assert result["status_code"] == 400

        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list
        usage_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.usage"
        )
        usage_details = usage_call[1]["event_details"]
        assert usage_details["is_success"] is False
        assert usage_details["error_type"] == "ClientError"


def test_mcp_tool_network_error_handling():
    """Test that MCP tools properly handle network-related errors."""
    mock_func = MagicMock()
    mock_func.__name__ = "list_queues"

    from botocore.exceptions import ConnectTimeoutError

    mock_func.side_effect = ConnectTimeoutError(
        endpoint_url="https://deadline.us-west-2.amazonaws.com"
    )

    mock_telemetry_client = MagicMock()

    config: ToolDefinition = {"func": mock_func, "param_names": ["farmId"]}

    from deadline._mcp.utils import _default_error_handler, _default_serializer

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, _default_serializer, _default_error_handler)
        result = wrapper(farmId="farm-123")

        mock_func.assert_called_once_with(farmId="farm-123")

        assert "error" in result, "Error response should contain 'error' field"
        assert "type" in result, "Error response should contain 'type' field"

        assert result["type"] == "ConnectTimeoutError"
        assert "timeout" in result["error"].lower() or "connect" in result["error"].lower()

        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list
        usage_call = next(
            call for call in calls if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.usage"
        )
        usage_details = usage_call[1]["event_details"]
        assert usage_details["is_success"] is False
        assert usage_details["error_type"] == "ConnectTimeoutError"


def test_mcp_tool_telemetry_event_structure(mock_telemetry_client):
    """Test that MCP telemetry events have the correct structure."""
    mock_func = MagicMock()
    mock_func.__name__ = "list_farms"
    mock_func.return_value = {"farms": []}

    config: ToolDefinition = {"func": mock_func, "param_names": ["farmId"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        result = wrapper(farmId="farm-0000001234567adba49becbca1fce5f6")

        mock_func.assert_called_once_with(farmId="farm-0000001234567adba49becbca1fce5f6")
        assert result == {"farms": []}
        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list

        for call in calls:
            assert "event_type" in call[1]
            assert "event_details" in call[1]

            event_type = call[1]["event_type"]
            event_details = call[1]["event_details"]

            assert event_type.startswith("com.amazon.rum.deadline.mcp.")
            assert event_type in [
                "com.amazon.rum.deadline.mcp.latency",
                "com.amazon.rum.deadline.mcp.usage",
            ]

            assert "tool_name" in event_details
            assert "usage_mode" in event_details
            assert event_details["usage_mode"] == "MCP"

            if event_type == "com.amazon.rum.deadline.mcp.latency":
                assert "latency" in event_details
                assert isinstance(event_details["latency"], (int, float))
            elif event_type == "com.amazon.rum.deadline.mcp.usage":
                assert "is_success" in event_details
                assert isinstance(event_details["is_success"], bool)
                assert "error_type" in event_details


def test_mcp_tool_telemetry_queue_integration(mock_telemetry_client):
    """Test that telemetry events are properly queued."""
    mock_func = MagicMock()
    mock_func.__name__ = "submit_job"
    mock_func.return_value = {"jobId": "job-0000001234567adba49becbca1fce5f6"}

    config: ToolDefinition = {"func": mock_func, "param_names": ["job_bundle_dir"]}

    mock_serializer = MagicMock(side_effect=lambda x: x)
    mock_error_handler = MagicMock()

    with patch(
        "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client:
        mock_get_client.return_value = mock_telemetry_client

        wrapper = _create_wrapper(config, mock_serializer, mock_error_handler)
        result = wrapper(job_bundle_dir="/path/to/bundle")

        mock_func.assert_called_once_with(job_bundle_dir="/path/to/bundle")
        assert result == {"jobId": "job-0000001234567adba49becbca1fce5f6"}
        assert mock_telemetry_client.record_event.call_count == 2

        calls = mock_telemetry_client.record_event.call_args_list
        for call in calls:
            event_details = call[1]["event_details"]
            # In unit tests, the wrapper function name is used
            assert event_details["tool_name"] == "wrapper"
