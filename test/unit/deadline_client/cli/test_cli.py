# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI generally.
"""

import subprocess
import sys
from unittest.mock import patch
from typing import List

import click
import pytest
from click.testing import CliRunner

from deadline.client import api
from deadline.client.cli import main
from deadline.client.cli._common import _cli_object_repr
from deadline.client.cli._main import ContextTrackingCommand, ContextTrackingGroup
from deadline.client.api._session import get_default_client_config


def test_cli_debug_logging_on(fresh_deadline_config):
    """
    Confirm that --log-level DEBUG turns on debug logging.
    """
    # The CliRunner environment already has the logger configured,
    # so we instead run it as a subprocess to match the actual
    # environment.
    output = subprocess.check_output(
        args=[sys.executable, "-m", "deadline", "--log-level", "DEBUG", "config", "--help"],
        stderr=subprocess.STDOUT,
        text=True,
    )

    assert "Debug logging is on" in output


def test_cli_redirect_output(fresh_deadline_config, tmp_path):
    """
    Confirm that --redirect-output FILENAME sends stdout/stderr to a file.
    """
    # The CliRunner environment already has the logger configured,
    # so we instead run it as a subprocess to match the actual
    # environment.
    out_file = tmp_path / "out.txt"
    output = subprocess.check_output(
        args=[
            sys.executable,
            "-m",
            "deadline",
            "--redirect-output",
            str(out_file),
            "config",
            "--help",
        ],
        stderr=subprocess.STDOUT,
        text=True,
    )

    # No output should be printed to stdout or stderr.
    assert output == ""

    # The help information should be in the provided output file.
    with open(out_file, encoding="utf-8") as fh:
        file_output = fh.read()
    assert file_output.startswith("Usage: ")
    assert "Manage Deadline's workstation configuration." in file_output


@pytest.mark.parametrize("redirect_mode", ("append", "replace"))
def test_cli_redirect_output_with_mode(fresh_deadline_config, tmp_path, redirect_mode):
    """
    Confirm that --redirect-output FILENAME sends stdout/stderr to a file,
    and --redirect-mode controls appending vs replacing.
    """

    out_file = tmp_path / "out.txt"
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write("Initial contents\n")

    # The CliRunner environment already has the logger configured,
    # so we instead run it as a subprocess to match the actual
    # environment.
    output = subprocess.check_output(
        args=[
            sys.executable,
            "-m",
            "deadline",
            "--redirect-output",
            str(out_file),
            "--redirect-mode",
            redirect_mode,
            "config",
            "--help",
        ],
        stderr=subprocess.STDOUT,
        text=True,
    )

    # No output should be printed to stdout or stderr.
    assert output == ""

    # The help information should be in the provided output file.
    with open(out_file, encoding="utf-8") as fh:
        file_output = fh.read()
    if redirect_mode == "append":
        # Should be appended to the starting file contents.
        assert file_output.startswith("Initial contents\nUsage: "), file_output
    else:
        # The starting file contents should be replaced
        assert file_output.startswith("Usage: "), file_output
    assert "Manage Deadline's workstation configuration." in file_output, file_output


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
    Test that each group prints the usage screen if no command is provided
    """
    runner = CliRunner()
    result = runner.invoke(main, [cli_group])

    assert result.output.startswith("Usage:")


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


def test_all_cli_commands_use_context_tracking_command():
    """
    Retrieves all click commands and verifies they all use the ContextTrackingCommand subclass
    """

    def _get_all_click_commands(
        cmd: click.Command,
        found_cmds: List[click.Command],
    ) -> List[click.Command]:
        """Gets all leaf commands under a given command"""
        if isinstance(cmd, click.Group):
            for subcmd in cmd.commands.values():
                _get_all_click_commands(subcmd, found_cmds)
        else:
            found_cmds.append(cmd)
        return found_cmds

    all_commands = _get_all_click_commands(cmd=main, found_cmds=[])
    assert all([isinstance(cmd, ContextTrackingCommand) for cmd in all_commands])


def test_context_tracking_command_sets_boto_user_agent_extra():
    """
    Verifies that the ContextTrackingCommand sets the user_agent_extra in boto clients
    """

    @click.group(cls=ContextTrackingGroup, name="main")
    def test_main():
        pass

    @test_main.group(name="subcommand")
    def test_subcmd():
        pass

    @test_subcmd.command(name="command")
    def test_command():
        pass

    CliRunner().invoke(test_main, args=["subcommand", "command"])

    config = get_default_client_config()

    assert "cli-command/main.subcommand.command" in config.user_agent_extra
