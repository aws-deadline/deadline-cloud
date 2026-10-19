# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
The `deadline mcp-server` command.
"""

import sys
import click

from .._common import _handle_error
from .._main import deadline as main
from ...api._telemetry import get_deadline_cloud_library_telemetry_client


@main.command(name="mcp-server")
@_handle_error
def cli_mcp_server():
    """
    EXPERIMENTAL - Start the AWS Deadline Cloud MCP (Model Context Protocol) server.

    The MCP server provides LLM tools with access to AWS Deadline Cloud operations
    through the Model Context Protocol. This allows AI assistants to interact with
    Deadline Cloud services on your behalf.

    The server will run until interrupted with Ctrl+C or Ctrl+D.

    Note: This command requires Python 3.10+ and MCP dependencies. Install them with:
    pip install 'deadline[mcp]'
    """
    if sys.version_info < (3, 10):
        click.echo(
            "Error: The MCP server requires Python 3.10 or later "
            f"(detected Python {sys.version_info.major}.{sys.version_info.minor}).",
            err=True,
        )
        sys.exit(1)

    try:
        from ...._mcp.server import main as mcp_main
    except ImportError:
        click.echo(
            "Error: MCP dependencies are not installed or are out of date (mcp 2.x is required).\n"
            "Please install them with: pip install --upgrade 'deadline[mcp]'",
            err=True,
        )
        sys.exit(1)

    # Record server startup telemetry
    try:
        telemetry_client = get_deadline_cloud_library_telemetry_client()
        telemetry_client.record_event(
            event_type="com.amazon.rum.deadline.mcp.server_startup",
            event_details={"usage_mode": "MCP", "startup_method": "cli"},
        )
    except Exception:
        # Don't let telemetry errors affect server startup
        pass

    mcp_main()
