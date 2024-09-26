# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI attachment commands.
"""
import os

from click.testing import CliRunner

from deadline.client.cli import main


def test_cli_attachment_existence(fresh_deadline_config):
    """
    Confirm that the CLI is not availble for environment with no ATTACHMENT_SYNC_JOB_USER variable
    """

    assert not os.environ.get("ATTACHMENT_SYNC_JOB_USER")

    runner = CliRunner()
    response = runner.invoke(main, ["attachment"])

    assert "Error: No such command 'attachment'" in response.output
