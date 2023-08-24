# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI fleet commands.
"""
from unittest.mock import patch

import boto3  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from click.testing import CliRunner

from deadline.client import api, config
from deadline.client.cli import deadline_cli

from ..shared_constants import MOCK_FARM_ID

MOCK_FLEET_ID = "fleet-0123456789abcdef0123456789abcdef"

MOCK_FLEETS_LIST = [
    {
        "fleetId": MOCK_FLEET_ID,
        "farmId": MOCK_FARM_ID,
        "description": "The best fleet.",
        "displayName": "MadFleet",
        "status": "ACTIVE",
        "platform": "EC2_SPOT",
        "workerRequirements": {"vCpus": {"min": 2, "max": 4}, "memInGiB": {"min": 8, "max": 16}},
        "autoScalerCapacities": {"min": 0, "max": 10},
        "createdAt": "2022-11-22T06:37:36+00:00",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin",
    },
    {
        "fleetId": MOCK_FLEET_ID.replace("1", "2"),
        "farmId": MOCK_FARM_ID,
        "description": "The maddest fleet.",
        "displayName": "MadderFleet",
        "status": "ACTIVE",
        "platform": "EC2_SPOT",
        "workerRequirements": {"vCpus": {"min": 2, "max": 4}, "memInGiB": {"min": 8, "max": 16}},
        "autoScalerCapacities": {"min": 0, "max": 50},
        "createdAt": "2022-11-22T06:37:36+00:00",
        "createdBy": "arn:aws:sts::123456789012:assumed-role/Admin",
    },
]


def test_cli_fleet_list(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected list of
    fleets, given mock data.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_fleets.return_value = {"fleets": MOCK_FLEETS_LIST}

        runner = CliRunner()
        result = runner.invoke(deadline_cli.cli, ["fleet", "list"])

        assert (
            result.output
            == """- fleetId: fleet-0123456789abcdef0123456789abcdef
  displayName: MadFleet
- fleetId: fleet-0223456789abcdef0223456789abcdef
  displayName: MadderFleet

"""
        )
        assert result.exit_code == 0


def test_cli_fleet_list_client_error(fresh_deadline_config):
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_fleets.side_effect = ClientError(
            {"Error": {"Message": "A botocore client error"}}, "client error"
        )

        runner = CliRunner()
        result = runner.invoke(deadline_cli.cli, ["fleet", "list"])

        assert "Failed to get Fleets" in result.output
        assert "A botocore client error" in result.output
        assert result.exit_code != 0


def test_cli_fleet_get(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected fleet, given mock data.
    """
    config.set_setting("defaults.farm_id", MOCK_FARM_ID)

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_fleet.return_value = MOCK_FLEETS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(deadline_cli.cli, ["fleet", "get", "--fleet-id", MOCK_FLEET_ID])

        assert (
            result.output
            == """fleetId: fleet-0123456789abcdef0123456789abcdef
farmId: farm-0123456789abcdefabcdefabcdefabcd
description: The best fleet.
displayName: MadFleet
status: ACTIVE
platform: EC2_SPOT
workerRequirements:
  vCpus:
    min: 2
    max: 4
  memInGiB:
    min: 8
    max: 16
autoScalerCapacities:
  min: 0
  max: 10
createdAt: '2022-11-22T06:37:36+00:00'
createdBy: arn:aws:sts::123456789012:assumed-role/Admin

"""
        )
        session_mock().client("deadline").get_fleet.assert_called_once_with(
            farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID
        )
        assert result.exit_code == 0


def test_cli_fleet_get_override_profile(fresh_deadline_config):
    """
    Confirms that the --profile option overrides the option to boto3.Session.
    """
    # set the farm id for the overridden profile
    config.set_setting("defaults.aws_profile_name", "NonDefaultProfileName")
    config.set_setting("defaults.farm_id", "farm-overriddenid")
    config.set_setting("defaults.aws_profile_name", "DifferentProfileName")

    with patch.object(boto3, "Session") as session_mock:
        session_mock().client("deadline").get_fleet.return_value = MOCK_FLEETS_LIST[0]
        session_mock.reset_mock()

        runner = CliRunner()
        result = runner.invoke(
            deadline_cli.cli,
            ["fleet", "get", "--profile", "NonDefaultProfileName", "--fleet-id", MOCK_FLEET_ID],
        )

        session_mock.assert_called_once_with(profile_name="NonDefaultProfileName")
        session_mock().client().get_fleet.assert_called_once_with(
            farmId="farm-overriddenid", fleetId=MOCK_FLEET_ID
        )
        assert result.exit_code == 0


def test_cli_fleet_get_no_fleet_id_provided(fresh_deadline_config):
    """
    Confirm that the CLI interface fails when no fleet id is provided
    """
    config.set_setting("defaults.farm_id", "farm-overriddenid")

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_fleet.return_value = MOCK_FLEETS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(deadline_cli.cli, ["fleet", "get"])

        assert "Missing option '--fleet-id'" in result.output
        assert result.exit_code != 0


def test_cli_fleet_get_explicit_farm_id(fresh_deadline_config):
    """
    Confirm that the CLI interface prints out the expected fleet, given mock data.
    """

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").get_fleet.return_value = MOCK_FLEETS_LIST[0]

        runner = CliRunner()
        result = runner.invoke(
            deadline_cli.cli,
            ["fleet", "get", "--farm-id", MOCK_FARM_ID, "--fleet-id", MOCK_FLEET_ID],
        )

        assert (
            result.output
            == """fleetId: fleet-0123456789abcdef0123456789abcdef
farmId: farm-0123456789abcdefabcdefabcdefabcd
description: The best fleet.
displayName: MadFleet
status: ACTIVE
platform: EC2_SPOT
workerRequirements:
  vCpus:
    min: 2
    max: 4
  memInGiB:
    min: 8
    max: 16
autoScalerCapacities:
  min: 0
  max: 10
createdAt: '2022-11-22T06:37:36+00:00'
createdBy: arn:aws:sts::123456789012:assumed-role/Admin

"""
        )
        session_mock().client("deadline").get_fleet.assert_called_once_with(
            farmId=MOCK_FARM_ID, fleetId=MOCK_FLEET_ID
        )
        assert result.exit_code == 0
