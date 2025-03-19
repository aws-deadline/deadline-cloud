# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI attachment commands.
"""

import os

from click.testing import CliRunner

from deadline.client.cli import main


def test_cli_attachment_existence(fresh_deadline_config):
    """
    Confirm that the subcommand group is availble for environment with no JOB_ATTACHMENT_CLI
    """

    assert not os.environ.get("JOB_ATTACHMENT_CLI")

    runner = CliRunner()
    response = runner.invoke(main, ["attachment"])

    assert "Usage: main attachment" in response.output
