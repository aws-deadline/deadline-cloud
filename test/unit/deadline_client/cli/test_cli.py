# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI generally.
"""
import subprocess
import sys
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from deadline.client import api
from deadline.client.cli import main
from deadline.client.cli._common import _cli_object_repr


def test_cli_debug_logging_on(fresh_deadline_config):
    """
    Confirm that --log-level DEBUG turns on debug logging.
    """
    # The CliRunner environment already has the logger configured,
    # so we instead run it as a subprocess to match the actual
    # environment.
    output = subprocess.check_output(
        args=[sys.executable, "-m", "deadline", "--log-level", "DEBUG", "config"],
        stderr=subprocess.STDOUT,
        text=True,
    )

    assert "Debug logging is on" in output


def test_cli_unfamiliar_exception(fresh_deadline_config):
    """
    Test that unfamiliar exceptions get the extra context
    """
    # Change the `login` function so it just raises an exception
    with patch.object(api._session, "get_boto3_session"), patch.object(api, "login") as login_mock:
        login_mock.side_effect = Exception("An unexpected exception")

        runner = CliRunner()
        result = runner.invoke(main, ["auth", "login"])

        assert "encountered the following exception" in result.output
        assert "An unexpected exception" in result.output
        assert result.exit_code == 1


@pytest.mark.parametrize("cli_group", ["config", "farm", "queue", "bundle"])
def test_cli_group_without_command(fresh_deadline_config, cli_group):
    """
    Test that each group prints the the usage screen if no command is provided
    """
    runner = CliRunner()
    result = runner.invoke(main, [cli_group])

    assert result.output.startswith("Usage:")
    # Click's default, with no clear way to override, is to return success in this case
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "obj,expected",
    [
        pytest.param([], "[]\n", id="empty list"),
        pytest.param([{"x": "y"}], "- x: y\n", id="single-element list"),
        pytest.param([{"x": "y"}, {"z": "w"}], "- x: y\n- z: w\n", id="multi-element list"),
        pytest.param(
            {"x": "single-line string"}, "x: single-line string\n", id="single-line string"
        ),
        pytest.param(
            {"x": "multi-line string\nthat goes\n for multiple\nlines\n"},
            "x: |\n  multi-line string\n  that goes\n   for multiple\n  lines\n",
            id="multi-line string",
        ),
        pytest.param(
            {"x": "multi-line string\nthat goes\n for multiple\nlines"},
            "x: |\n  multi-line string\n  that goes\n   for multiple\n  lines\n",
            id="multi-line string no final newline",
        ),
    ],
)
def test_cli_object_repr(obj, expected):
    """
    Test that the CLI object represntation is expected.
    """
    assert _cli_object_repr(obj) == expected
