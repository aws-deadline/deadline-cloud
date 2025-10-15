# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = ["deadline_dev_gui_main", "main"]

from . import deadline_dev_gui_main

# Explicitly importing _groups and _mcp_server adds all the CLI subcommands
from . import _groups  # noqa: F401
from ._groups import mcp_server_command  # noqa: F401

from ._main import deadline as main
