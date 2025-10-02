# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
This module exists to work around circular imports between the main CLI and each CLI group
"""

from ._main import main  # noqa: F401

# New groups must be imported here to get added to the main CLI as subcommands
from ._groups import (  # noqa: F401
    bundle_group,
    config_group,
    auth_group,
    farm_group,
    fleet_group,
    handle_web_url_command,
    job_group,
    queue_group,
    worker_group,
    attachment_group,
    manifest_group,
)

# MCP server command
from ._mcp_server import cli_mcp_server  # noqa: F401
