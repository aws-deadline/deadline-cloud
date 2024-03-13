# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI queue commands.
"""
from unittest.mock import patch

import boto3  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import main

from ..shared_constants import MOCK_FARM_ID, MOCK_QUEUES_LIST


def test_cli_queue_list(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    queues, given mock data.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_queues.return_value = {"queues": MOCK_QUEUES_LIST}

        runner = CliRunner()
        result = runner.invoke(main, ["queue", "list"])

        assert (
            result.output
            == """- queueId: queue-0123456789abcdef0123456789abcdef
  displayName: Testing Queue
- queueId: queue-0123456789abcdef0123456789abcdeg
  displayName: Another Queue

"""
        )
        assert result.exit_code == 0


def test_cli_queue_list_explicit_farm_id(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    queues, given mock data.
    """
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_queues.return_value = {"queues": MOCK_QUEUES_LIST}

        runner = CliRunner()
        result = runner.invoke(main, ["queue", "list", "--farm-id", MOCK_FARM_ID])

        assert (
            result.output
            == """- queueId: queue-0123456789abcdef0123456789abcdef
  displayName: Testing Queue
- queueId: queue-0123456789abcdef0123456789abcdeg
  displayName: Another Queue

"""
        )
        assert result.exit_code == 0


def test_cli_queue_list_override_profile(fresh_deadline_config):
    """
    Confirms that the --profile option overrides the option to boto3.Session.
    """
    # set the farm id for the overridden profile
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfileName")
    config.set_setting("defaults.farm_id", "farm-overriddenid")
    config.set_setting("defaults.aws_profile_name", "DifferentProfileName")

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").list_queues.return_value = {"queues": MOCK_QUEUES_LIST}
        session_mock.reset_mock()

        runner = CliRunner()
        result = runner.invoke(main, ["queue", "list", "--profile", "NonDefaultProfileName"])

        assert result.exit_code == 0
        session_mock.assert_called_with(profile_name="NonDefaultProfileName")
        session_mock().client().list_queues.assert_called_once_with(farmId="farm-overriddenid")


def test_cli_queue_list_no_farm_id(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_queues.return_value = {"queues": MOCK_QUEUES_LIST}

        runner = CliRunner()
        result = runner.invoke(main, ["queue", "list"])

        assert "Missing '--farm-id' or default Farm ID configuration" in result.output
        assert result.exit_code != 0


def test_cli_queue_list_client_error(fresh_deadline_config):
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_queues.side_effect = ClientError(
            {"Error": {"Message": "A botocore client error"}}, "client error"
        )

        runner = CliRunner()
        result = runner.invoke(main, ["queue", "list"])

        assert "Failed to get Queues" in result.output
        assert "A botocore client error" in result.output
        assert result.exit_code != 0


def test_cli_queue_get(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected queue, given mock data.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_queue.return_value = MOCK_QUEUES_LIST[0]

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "queue",
                "get",
                "--farm-id",
                MOCK_FARM_ID,
                "--queue-id",
                MOCK_QUEUES_LIST[0]["queueId"],
            ],
        )

        assert (
            result.output
            == """queueId: queue-0123456789abcdef0123456789abcdef
displayName: Testing Queue
description: ''

"""
        )
        session_mock().client("deadline").get_queue.assert_called_once_with(
            farmId=MOCK_FARM_ID, queueId=MOCK_QUEUES_LIST[0]["queueId"]
        )
        assert result.exit_code == 0
