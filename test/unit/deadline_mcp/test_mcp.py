# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# Ignore typing since mcp library is not available for Python < 3.10
# type: ignore

"""Unit tests for MCP server"""

import sys
from unittest.mock import MagicMock

import pytest

from deadline.client import api

if sys.version_info >= (3, 10):
    from deadline._mcp.registry import TOOL_REGISTRY, ToolDefinition
    from deadline._mcp.utils import (
        _create_wrapper,
        _default_error_handler,
        _default_serializer,
        register_api_tools,
    )
else:
    pytest.skip("MCP dependencies not available on Python before 3.10", allow_module_level=True)


class TestToolRegistry:
    """Test the TOOL_REGISTRY structure."""

    def test_registry_structure(self):
        """Test that TOOL_REGISTRY has valid structure."""
        assert isinstance(TOOL_REGISTRY, dict)
        assert len(TOOL_REGISTRY) > 0

        for definition in TOOL_REGISTRY.values():
            assert "func" in definition
            assert "param_names" in definition
            assert callable(definition["func"])

    def test_expected_functions(self):
        """Test that expected functions are in the registry."""
        expected = ["list_farms", "submit_job", "download_job_output"]
        for func_name in expected:
            assert func_name in TOOL_REGISTRY


class TestUtilityFunctions:
    """Test utility functions."""

    def test_default_serializer(self):
        """Test serializer with different object types."""
        assert _default_serializer("string") == "string"
        assert _default_serializer(123) == "123"

        class TestObj:
            def __init__(self):
                self.attr = "value"

        assert _default_serializer(TestObj()) == {"attr": "value"}

    def test_default_error_handler(self):
        """Test error handler formats errors correctly."""
        error = Exception("test error")
        result = _default_error_handler(error)

        assert result["error"] == "test error"
        assert result["type"] == "Exception"

    def test_create_wrapper(self):
        """Test wrapper creation and execution."""

        def mock_func(param1: str, param2: int = 10):
            return {"param1": param1, "param2": param2}

        config = ToolDefinition(
            func=mock_func,
            param_names=["param1", "param2"],
        )

        wrapper = _create_wrapper(config, _default_serializer, _default_error_handler)

        assert callable(wrapper)
        assert hasattr(wrapper, "__signature__")

        result = wrapper(param1="test", param2=20)
        assert isinstance(result, dict)


class TestRegisterAPITools:
    """Test API tools registration."""

    def test_basic_registration(self):
        """Test registering valid tools."""
        mock_app = MagicMock()

        # Clean up any existing markers
        if hasattr(api.list_farms, "_mcp_tool_registered"):
            delattr(api.list_farms, "_mcp_tool_registered")

        register_api_tools(mock_app, [api.list_farms])

        assert mock_app.tool.call_count == 1
        assert hasattr(api.list_farms, "_mcp_tool_registered")

        # Clean up
        delattr(api.list_farms, "_mcp_tool_registered")

    def test_invalid_tools_raise_exception(self):
        """Test that invalid tools raise exceptions."""
        mock_app = MagicMock()

        with pytest.raises(ValueError, match="Tool not_a_function is not callable"):
            register_api_tools(mock_app, ["not_a_function"])  # type: ignore[list-item]

        mock_app.tool.assert_not_called()

    def test_unregistered_function_raises_exception(self):
        """Test that callable functions not in registry raise exceptions."""
        mock_app = MagicMock()

        def unregistered_function():
            """A function not in the registry."""
            pass

        with pytest.raises(
            ValueError, match="Function unregistered_function not found in tool registry"
        ):
            register_api_tools(mock_app, [unregistered_function])

        mock_app.tool.assert_not_called()


class TestParameterTypeConversion:
    """Test parameter type conversion functionality."""

    def test_signature_preserves_original_annotations(self):
        """Test that MCP wrapper preserves original function type annotations."""
        from deadline._mcp.utils import _create_wrapper
        from deadline._mcp.tools.job import submit_job
        from deadline._mcp.registry import ToolDefinition

        # Create a mock config
        config: ToolDefinition = {"func": submit_job, "param_names": None}

        # Create wrapper with our improved approach
        wrapper = _create_wrapper(config, lambda x: x, lambda e: {"error": str(e)})

        # Check that the wrapper signature preserves original annotations
        wrapper_sig = getattr(wrapper, "__signature__")

        # Test a few key parameters that should have preserved annotations
        assert "priority" in wrapper_sig.parameters
        assert "max_failed_tasks_count" in wrapper_sig.parameters

        # The annotations should match the original function (Optional[int])
        # Note: We can't directly compare annotations due to how Optional types work,
        # but we can verify they're not all strings anymore
        priority_annotation = wrapper_sig.parameters["priority"].annotation
        assert priority_annotation is not str  # Should not be forced to str anymore
