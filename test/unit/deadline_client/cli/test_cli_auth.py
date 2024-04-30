# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI auth commands.
"""
import json
import subprocess
import sys

from unittest.mock import patch

from click.testing import CliRunner

from deadline.client import api, config

from deadline.client.cli import main


def test_cli_deadline_cloud_monitor_login_and_logout(fresh_deadline_config):
    """
    Confirm that the CLI login/logout command invokes Deadline Cloud monitor as expected
    """
    scoped_config = {
        "credential_process": "/bin/DeadlineCloudMonitor get-credentials --profile sandbox-us-west-2",
        "monitor_id": "monitor-1g9neezauta8ease",
        "region": "us-west-2",
    }

    profile_name = "sandbox-us-west-2"
    config.set_setting("deadline-cloud-monitor.path", "/bin/DeadlineCloudMonitor")
    config.set_setting("defaults.aws_profile_name", profile_name)

    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ), patch.object(subprocess, "Popen") as popen_mock, patch.object(
        subprocess, "check_output"
    ) as check_output_mock:
        # The profile name
        session_mock().profile_name = profile_name
        # This configuration includes the IdC profile
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}
        check_output_mock.return_value = bytes("Successfully logged out", "utf8")

        runner = CliRunner()
        result = runner.invoke(main, ["auth", "login"])

        assert result.exit_code == 0, result.output

        if sys.platform.startswith("win"):
            popen_mock.assert_called_once_with(
                ["/bin/DeadlineCloudMonitor", "login", "--profile", "sandbox-us-west-2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
            )
        else:
            popen_mock.assert_called_once_with(
                ["/bin/DeadlineCloudMonitor", "login", "--profile", "sandbox-us-west-2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
            )

        assert result.exit_code == 0

        assert (
            "Successfully logged in: Deadline Cloud monitor profile: sandbox-us-west-2"
            in result.output
        )
        assert result.exit_code == 0

        # Now lets logout
        runner = CliRunner()
        result = runner.invoke(main, ["auth", "logout"])

        check_output_mock.assert_called_once_with(
            ["/bin/DeadlineCloudMonitor", "logout", "--profile", "sandbox-us-west-2"]
        )

        assert "Successfully logged out" in result.output

        # Verify that the logout call resets the cached session to None
        assert api._session.__cached_boto3_session is None


def test_cli_auth_status(fresh_deadline_config):
    """
    Confirm that the CLI status command prints out as expected
    """
    # GIVEN
    profile_name = "sandbox-us-west-2"
    config.set_setting("defaults.aws_profile_name", profile_name)

    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        # The profile name
        session_mock().profile_name = profile_name

        # WHEN
        runner = CliRunner()
        result = runner.invoke(main, ["auth", "status"])

    # THEN
    assert result.exit_code == 0
    assert "Profile Name: " in result.output
    assert "Source: " in result.output
    assert "Status: " in result.output
    assert "API Availability: " in result.output


def test_cli_auth_status_json(fresh_deadline_config):
    """
    Confirm that the CLI status command gives valid json back
    """
    # GIVEN
    profile_name = "sandbox-us-west-2"
    expected = {
        "profile_name": profile_name,
        "source": "DEADLINE_CLOUD_MONITOR_LOGIN",
        "status": "AUTHENTICATED",
        "api_availability": False,
    }
    scoped_config = {
        "credential_process": "/bin/DeadlineCloudMonitor get-credentials --profile sandbox-us-west-2",
        "monitor_id": "monitor-1g9neezauta8ease",
        "region": "us-west-2",
    }
    config.set_setting("defaults.aws_profile_name", profile_name)

    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        # The profile name
        session_mock().profile_name = profile_name
        # This configuration includes the IdC profile
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}

        # WHEN
        runner = CliRunner()
        result = runner.invoke(main, ["auth", "status", "--output", "json"])
        actual = json.loads(result.output)

    # THEN
    assert result.exit_code == 0
    assert actual == expected
