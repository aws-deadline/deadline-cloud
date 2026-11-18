# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import sys
from collections import namedtuple
from unittest.mock import patch

from click.testing import CliRunner

from deadline.client.cli import main

_VersionInfo = namedtuple("_VersionInfo", "major minor micro releaselevel serial")


def test_mcp_server_rejects_python_below_3_10():
    with patch("sys.version_info", _VersionInfo(3, 9, 18, "final", 0)):
        result = CliRunner().invoke(main, ["mcp-server"])

    assert result.exit_code == 1
    assert "requires Python 3.10 or later" in result.output
    assert "detected Python 3.9" in result.output


def test_mcp_server_reports_missing_dependencies():
    with (
        patch("sys.version_info", _VersionInfo(3, 12, 0, "final", 0)),
        patch.dict(sys.modules, {"deadline._mcp.server": None}),
    ):
        result = CliRunner().invoke(main, ["mcp-server"])

    assert result.exit_code == 1
    assert "pip install --upgrade 'deadline[mcp]'" in result.output
