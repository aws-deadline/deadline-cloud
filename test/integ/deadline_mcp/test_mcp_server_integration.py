# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Integration test for the Deadline Cloud MCP Server."""

import json
import pytest
import boto3
import asyncio
import sys
from unittest.mock import patch, MagicMock

# Skip all tests in this module if MCP dependencies are not available
pytest.importorskip("mcp", reason="MCP dependencies not available")

from deadline._mcp import server
from deadline._mcp.registry import TOOL_REGISTRY, get_tool_definition

from deadline.client.api import list_farms
from deadline.client.cli._groups.mcp_server_command import cli_mcp_server
from click.testing import CliRunner


@pytest.fixture(scope="session")
def get_boto_session():
    """
    Fixture to establish authenticated session for MCP integration tests.
    """
    session = boto3.Session()
    sts_client = session.client("sts")
    _ = sts_client.get_caller_identity()
    return session


@pytest.mark.asyncio
@pytest.mark.integ
async def test_mcp_server_integration(get_boto_session):
    """Test that MCP server starts, registers tools, and handles tool calls."""
    with patch("deadline.client.api._session.get_boto3_session") as mock_session:
        mock_session.return_value = get_boto_session

        app = server.app

        # 1. Check if all tools in TOOL_REGISTRY are available
        tools_result = await app.list_tools()
        tools = tools_result if isinstance(tools_result, list) else tools_result.tools
        available_tool_names = [tool.name for tool in tools]
        expected_tools = [f"deadline_{tool_name}" for tool_name in TOOL_REGISTRY.keys()]
        missing_tools = [tool for tool in expected_tools if tool not in available_tool_names]
        assert not missing_tools, f"Missing expected tools from TOOL_REGISTRY: {missing_tools}"

        # 2. Call deadline_check_authentication_status tool and verify response
        auth_result = await app.call_tool("deadline_check_authentication_status", {})
        assert auth_result is not None, "Authentication tool call should return a result"

        if auth_result and hasattr(auth_result[0], "text"):  # type: ignore[index]
            try:
                auth_data = json.loads(auth_result[0].text)  # type: ignore[index]
                assert "error" not in auth_data, (
                    f"Authentication check failed with error: {auth_data.get('error')}"
                )
            except json.JSONDecodeError as e:
                pytest.fail(
                    f"Check authentication tool returned non-JSON response: {auth_result[0].text[:200]}... (JSONDecodeError: {e})"  # type: ignore[index]
                )

        # 3. Call deadline_list_farms tool and verify response
        farms_result = await app.call_tool("deadline_list_farms", {})
        assert farms_result is not None, "List farms tool call should return a result"
        if farms_result and hasattr(farms_result[0], "text"):  # type: ignore[index]
            try:
                farms_data = json.loads(farms_result[0].text)  # type: ignore[index]
                assert "error" not in farms_data, (
                    f"List farms failed with error: {farms_data.get('error')}"
                )
            except json.JSONDecodeError as e:
                pytest.fail(
                    f"List farms returned non-JSON response: {farms_result[0].text[:200]}... (JSONDecodeError: {e})"  # type: ignore[index]
                )

        # 4. Check metadata on deadline_list_queues tool
        list_queues_tool = None
        for tool in tools:
            if tool.name == "deadline_list_queues":
                list_queues_tool = tool
                break

        assert list_queues_tool is not None, "deadline_list_queues tool not found"

        expected_params = get_tool_definition("list_queues")["param_names"]
        assert expected_params is not None, (
            "list_queues should have parameters defined in TOOL_REGISTRY"
        )

        assert hasattr(list_queues_tool, "inputSchema"), (
            "deadline_list_queues tool should have an inputSchema"
        )

        input_schema = list_queues_tool.inputSchema
        assert input_schema is not None, "inputSchema should not be None"

        available_params = []
        if isinstance(input_schema, dict) and "properties" in input_schema:
            available_params = list(input_schema["properties"].keys())
        elif hasattr(input_schema, "properties") and input_schema.properties:
            available_params = list(input_schema.properties.keys())
        elif hasattr(input_schema, "model_fields"):
            available_params = list(input_schema.model_fields.keys())

        assert "farmId" in available_params, (
            f"farmId parameter should be available in list_queues tool. "
            f"Available params: {available_params}, Expected params: {expected_params}"
        )

        missing_params = [param for param in expected_params if param not in available_params]
        assert not missing_params, (
            f"Missing expected parameters in list_queues tool: {missing_params}. "
            f"Available: {available_params}, Expected: {expected_params}"
        )

        return True


@pytest.mark.asyncio
@pytest.mark.integ
async def test_tool_description_extraction(get_boto_session):
    """Test that tool descriptions are properly extracted from original function docstrings."""
    app = server.app

    tools_result = await app.list_tools()
    tools = tools_result if isinstance(tools_result, list) else tools_result.tools

    list_farms_tool = None
    for tool in tools:
        if tool.name == "deadline_list_farms":
            list_farms_tool = tool
            break

    assert list_farms_tool is not None, "deadline_list_farms tool not found"

    original_docstring = list_farms.__doc__
    assert original_docstring is not None, "Original docstring should be available"

    assert list_farms_tool.description == original_docstring, (
        f"Tool description should match original docstring.\n"
        f"Expected: {original_docstring}\n"
        f"Got: {list_farms_tool.description}"
    )

    assert "Calls the [deadline:ListFarms] API call" in list_farms_tool.description, (
        "Tool description should come from the function's doc string"
    )

    return True


class MCPClient:
    """Simple MCP client for testing the server process via stdio."""

    def __init__(self, process):
        self.process = process
        self.request_id = 0

    async def send_request(self, method, params=None):
        """Send a JSON-RPC request to the MCP server."""
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params or {},
        }

        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()

        response_line = await self.process.stdout.readline()
        if not response_line:
            raise RuntimeError("No response from MCP server")

        return json.loads(response_line.decode().strip())

    async def initialize(self):
        """Initialize the MCP session."""
        response = await self.send_request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "test-client", "version": "1.0.0"},
            },
        )

        if "error" in response:
            raise RuntimeError(f"Initialize failed: {response['error']}")

        # Send initialized notification (no response expected)
        await self.send_notification("notifications/initialized")
        return response

    async def send_notification(self, method, params=None):
        """Send a JSON-RPC notification (no response expected)."""
        request = {"jsonrpc": "2.0", "method": method, "params": params or {}}

        request_json = json.dumps(request) + "\n"
        self.process.stdin.write(request_json.encode())
        await self.process.stdin.drain()

    async def list_tools(self):
        """List available tools."""
        response = await self.send_request("tools/list", {})
        if "error" in response:
            raise RuntimeError(f"List tools failed: {response['error']}")
        return response["result"]["tools"]

    async def call_tool(self, name, arguments=None):
        """Call a tool."""
        response = await self.send_request(
            "tools/call", {"name": name, "arguments": arguments or {}}
        )
        if "error" in response:
            raise RuntimeError(f"Tool call failed: {response['error']}")
        return response["result"]


@pytest.mark.asyncio
@pytest.mark.integ
async def test_mcp_server_process_integration(get_boto_session):
    """Test MCP server as a separate process via stdio communication."""
    with patch("deadline.client.api._session.get_boto3_session") as mock_session:
        mock_session.return_value = get_boto_session
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "from deadline._mcp.server import main; main()",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )

    try:
        client = MCPClient(process)
        init_response = await client.initialize()

        assert "result" in init_response, "Initialize should return a result"
        assert "capabilities" in init_response["result"], "Server should return capabilities"

        tools = await client.list_tools()
        assert len(tools) > 0, "Server should have tools available"

        tool_names = [tool["name"] for tool in tools]
        expected_tools = [f"deadline_{tool_name}" for tool_name in TOOL_REGISTRY.keys()]
        missing_tools = [tool for tool in expected_tools if tool not in tool_names]
        assert not missing_tools, f"Missing expected tools: {missing_tools}"

        farms_result = await client.call_tool("deadline_list_farms")
        assert "content" in farms_result, "List farms should return content"
        assert len(farms_result["content"]) > 0, "List farms should return non-empty content"

        farms_content = farms_result["content"][0]["text"]
        try:
            farms_data = json.loads(farms_content)
            if "error" in farms_data:
                print(f"List farms returned error (may be expected): {farms_data['error']}")
        except json.JSONDecodeError:
            pytest.fail(f"List farms tool returned invalid JSON: {farms_content[:200]}...")

        return True

    finally:
        if process and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()


@pytest.mark.asyncio
@pytest.mark.integ
async def test_mcp_tool_telemetry(get_boto_session):
    """Test that MCP tools record telemetry events."""
    with patch("deadline.client.api._session.get_boto3_session") as mock_session:
        mock_session.return_value = get_boto_session

        mock_telemetry_client = MagicMock()
        with patch(
            "deadline._mcp.utils.get_deadline_cloud_library_telemetry_client"
        ) as mock_get_client:
            mock_get_client.return_value = mock_telemetry_client

            app = server.app

            auth_result = await app.call_tool("deadline_check_authentication_status", {})
            assert auth_result is not None, "Authentication tool call should return a result"
            assert mock_telemetry_client.record_event.call_count >= 2, (
                "Should record at least 2 telemetry events (latency and usage)"
            )

            recorded_calls = mock_telemetry_client.record_event.call_args_list
            event_types = [call[1]["event_type"] for call in recorded_calls]

            assert "com.amazon.rum.deadline.mcp.latency" in event_types, (
                "Should record latency telemetry event"
            )
            assert "com.amazon.rum.deadline.mcp.usage" in event_types, (
                "Should record usage telemetry event"
            )

            latency_events = [
                call
                for call in recorded_calls
                if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.latency"
            ]
            usage_events = [
                call
                for call in recorded_calls
                if call[1]["event_type"] == "com.amazon.rum.deadline.mcp.usage"
            ]

            assert len(latency_events) >= 1, "Should have at least one latency event"
            assert len(usage_events) >= 1, "Should have at least one usage event"

            # Assert latency event
            latency_event_details = latency_events[0][1]["event_details"]
            expected_latency_keys = {"latency", "tool_name", "usage_mode"}
            assert set(latency_event_details.keys()) == expected_latency_keys, (
                f"Latency event should contain exactly {expected_latency_keys}, "
                f"but got {set(latency_event_details.keys())}"
            )
            assert isinstance(latency_event_details["latency"], int), "Latency should be an integer"
            assert latency_event_details["tool_name"] == "check_authentication_status"
            assert latency_event_details["usage_mode"] == "MCP"

            # Assert usage event
            usage_event_details = usage_events[0][1]["event_details"]
            expected_usage_keys = {"tool_name", "is_success", "error_type", "usage_mode"}
            assert set(usage_event_details.keys()) == expected_usage_keys, (
                f"Usage event should contain exactly {expected_usage_keys}, "
                f"but got {set(usage_event_details.keys())}"
            )
            assert usage_event_details["tool_name"] == "check_authentication_status"
            assert isinstance(usage_event_details["is_success"], bool), (
                "is_success should be a boolean"
            )
            assert usage_event_details["usage_mode"] == "MCP"
            assert usage_event_details["error_type"] is None, (
                "error_type should be None for successful calls"
            )

        return True


@pytest.mark.integ
def test_mcp_server_startup_telemetry():
    """Test that MCP server startup records telemetry."""

    mock_telemetry_client = MagicMock()

    with patch(
        "deadline.client.cli._groups.mcp_server_command.get_deadline_cloud_library_telemetry_client"
    ) as mock_get_client, patch("deadline._mcp.server.main") as mock_mcp_main:
        mock_get_client.return_value = mock_telemetry_client

        runner = CliRunner()
        result = runner.invoke(cli_mcp_server, [])

        assert result.exit_code == 0, f"Command failed with output: {result.output}"

        mock_telemetry_client.record_event.assert_called_once_with(
            event_type="com.amazon.rum.deadline.mcp.server_startup",
            event_details={"usage_mode": "MCP", "startup_method": "cli"},
        )

        mock_mcp_main.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.integ
async def test_mcp_tool_error_handling():
    """Test that MCP tools properly handle and return errors using real AWS service validation."""

    def parse_tool_result(result):
        """Helper to parse and validate tool result JSON."""
        assert result is not None, "Tool call should return a result"
        if result and hasattr(result[0], "text"):
            return json.loads(result[0].text)
        return {}

    mock_session = MagicMock()

    deadline_client = boto3.client("deadline", region_name="us-west-2")

    mock_session.client.side_effect = lambda service, **kwargs: {"deadline": deadline_client}.get(
        service, MagicMock()
    )

    with patch("deadline.client.api._session.get_boto3_session") as mock_get_session:
        mock_get_session.return_value = mock_session
        app = server.app

        # Test 1: Invalid farm ID format
        queues_data = parse_tool_result(
            await app.call_tool("deadline_list_queues", {"farmId": "invalid-farm-id-format"})
        )
        assert "error" in queues_data, f"Expected AWS error, got: {queues_data}"
        assert "type" in queues_data, "Error response should contain 'type' field"
        # Check for specific AWS exception types that indicate authentication/authorization issues
        expected_error_types = [
            "AccessDeniedException",
            "ValidationException",
            "UnauthorizedException",
            "ExpiredTokenException",
        ]
        assert any(error_type in queues_data["type"] for error_type in expected_error_types), (
            f"Expected one of {expected_error_types}, got: {queues_data['type']}"
        )
        assert "error occurred" in queues_data["error"].lower()
        print(f"✅ Test 1 - Got expected error type: {queues_data['type']}")

        # Test 2: Invalid queue ID format
        jobs_data = parse_tool_result(
            await app.call_tool(
                "deadline_list_jobs",
                {
                    "farmId": "farm-1234567890abcdef1234567890abcdef",  # Valid format but likely non-existent
                    "queueId": "invalid-queue-format",  # Invalid format will trigger validation error
                },
            )
        )
        assert "error" in jobs_data, f"Expected AWS error, got: {jobs_data}"
        assert "type" in jobs_data, "Error response should contain 'type' field"
        assert any(error_type in jobs_data["type"] for error_type in expected_error_types), (
            f"Expected one of {expected_error_types}, got: {jobs_data['type']}"
        )
        assert "error occurred" in jobs_data["error"].lower()
        print(f"✅ Test 2 - Got expected error type: {jobs_data['type']}")

        # Test 3: Non-existent IDs
        fleets_data = parse_tool_result(
            await app.call_tool(
                "deadline_list_fleets",
                {
                    "farmId": "farm-0000000000000000000000000000000"  # Properly formatted but non-existent
                },
            )
        )
        assert "error" in fleets_data, f"Expected AWS error, got: {fleets_data}"
        assert "type" in fleets_data, "Error response should contain 'type' field"
        resource_error_types = expected_error_types + [
            "ResourceNotFoundException",
            "ForbiddenException",
        ]
        assert any(error_type in fleets_data["type"] for error_type in resource_error_types), (
            f"Expected one of {resource_error_types}, got: {fleets_data['type']}"
        )
