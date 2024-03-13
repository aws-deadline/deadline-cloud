# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI farm commands.
"""
from unittest.mock import patch
import os

import boto3  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import main

MOCK_FARMS_LIST = [
    {
        "farmId": "farm-0123456789abcdef0123456789abcdef",
        "description": "A Description.",
        "displayName": "Testing Farm",
    },
    {
        "farmId": "farm-0123456789abcdef0123456789abcdeg",
        "description": "",
        "displayName": "Another Farm",
    },
]

os.environ["AWS_ENDPOINT_URL_DEADLINE"] = "https://fake-endpoint"


def test_cli_farm_list(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    farms, given mock data.
    """
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.return_value = {"farms": MOCK_FARMS_LIST}

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "list"])

        assert (
            result.output
            == """- farmId: farm-0123456789abcdef0123456789abcdef
  displayName: Testing Farm
- farmId: farm-0123456789abcdef0123456789abcdeg
  displayName: Another Farm

"""
        )
        assert result.exit_code == 0


def test_cli_farm_list_override_profile(fresh_deadline_config):
    """
    Confirms that the --profile option overrides the option to boto3.Session.
    """
    # set the "user identities" property to True so it doesn't probe the boto3.Session
    # for configuration.
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfileName")
    config.set_setting("defaults.aws_profile_name", "DifferentProfileName")

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").list_farms.return_value = {"farms": MOCK_FARMS_LIST}
        session_mock()._session.get_scoped_config().get.return_value = "some-monitor-id"
        session_mock.reset_mock()

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "list", "--profile", "NonDefaultProfileName"])

        assert result.exit_code == 0
        session_mock.assert_called_with(profile_name="NonDefaultProfileName")
        session_mock().client().list_farms.assert_called_once_with()


def test_cli_farm_list_client_error(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.side_effect = ClientError(
            {"Error": {"Message": "A botocore client error"}}, "client error"
        )

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "list"])

        assert "Failed to get Farms" in result.output
        assert "A botocore client error" in result.output
        assert result.exit_code != 0


def test_cli_farm_get(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected farm, given mock data.
    """
    config.set_setting("defaults.farm_id", "farm-0123456789abcdef0123456789abcdef")

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_farm.return_value = MOCK_FARMS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "get"])

        assert (
            result.output
            == """farmId: farm-0123456789abcdef0123456789abcdef
description: A Description.
displayName: Testing Farm

"""
        )
        assert result.exit_code == 0
        session_mock().client("deadline").get_farm.assert_called_once_with(
            farmId="farm-0123456789abcdef0123456789abcdef"
        )


def test_cli_farm_get_override_profile(fresh_deadline_config):
    """
    Confirms that the --profile option overrides the option to boto3.Session.
    """
    # set the farm id for the overridden profile
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfileName")
    config.set_setting("defaults.farm_id", "farm-overriddenid")
    config.set_setting("defaults.aws_profile_name", "DifferentProfileName")

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").get_farm.return_value = MOCK_FARMS_LIST[0]
        session_mock.reset_mock()

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "get", "--profile", "NonDefaultProfileName"])

        assert result.exit_code == 0
        session_mock.assert_called_once_with(profile_name="NonDefaultProfileName")
        session_mock().client().get_farm.assert_called_once_with(farmId="farm-overriddenid")


def test_cli_farm_get_no_default_set(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected farm, given mock data.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_farm.return_value = MOCK_FARMS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(main, ["farm", "get"])

        assert "Missing '--farm-id' or default Farm ID configuration" in result.output
        assert result.exit_code != 0


def test_cli_farm_get_explicit_farm_id(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected farm, given mock data.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_farm.return_value = MOCK_FARMS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["farm", "get", "--farm-id", "farm-0123456789abcdef0123456789abcdef"],
        )

        assert (
            result.output
            == """farmId: farm-0123456789abcdef0123456789abcdef
description: A Description.
displayName: Testing Farm

"""
        )
        assert result.exit_code == 0
        session_mock().client("deadline").get_farm.assert_called_once_with(
            farmId="farm-0123456789abcdef0123456789abcdef"
        )
