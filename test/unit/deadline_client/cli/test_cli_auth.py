# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI auth commands.
"""

import json
import subprocess
import sys

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from deadline.client import api, config

from deadline.client.cli import main


def test_cli_deadline_cloud_monitor_login_and_logout(fresh_deadline_config):
    """
    Confirm that the CLI login/logout command invokes Deadline Cloud monitor as expected
    """
    if sys.platform.startswith("win"):
        dcm = "C:/Programs/bin/DeadlineCloudMonitor"
    else:
        dcm = "/bin/DeadlineCloudMonitor"
    scoped_config = {
        "credential_process": f"{dcm} get-credentials --profile sandbox-us-west-2",
        "monitor_id": "monitor-1g9neezauta8ease",
        "region": "us-west-2",
        "user_id": "user-1234",
        "identity_store_id": "d-abcdef0123",
    }

    profile_name = "sandbox-us-west-2"
    config.set_setting("deadline-cloud-monitor.path", dcm)
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(
            api._session._get_boto3_session_for_profile, "cache_clear"
        ) as mock_profile_session_cache_clear,
        patch.object(
            api._session._get_queue_user_boto3_session, "cache_clear"
        ) as mock_queue_session_cache_clear,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(subprocess, "Popen") as popen_mock,
        patch.object(subprocess, "check_output") as check_output_mock,
    ):
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
                [
                    "C:\\Programs\\bin\\DeadlineCloudMonitor",
                    "login",
                    "--profile",
                    "sandbox-us-west-2",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.PIPE,
            )
        else:
            popen_mock.assert_called_once_with(
                ["/bin/DeadlineCloudMonitor", "login", "--profile", "sandbox-us-west-2"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
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

        if sys.platform.startswith("win"):
            check_output_mock.assert_called_once_with(
                [
                    "C:\\Programs\\bin\\DeadlineCloudMonitor",
                    "logout",
                    "--profile",
                    "sandbox-us-west-2",
                ]
            )
        else:
            check_output_mock.assert_called_once_with(
                ["/bin/DeadlineCloudMonitor", "logout", "--profile", "sandbox-us-west-2"]
            )

        assert "Successfully logged out" in result.output
        mock_profile_session_cache_clear.assert_called()
        mock_queue_session_cache_clear.assert_called()


def test_cli_console_login_and_logout(fresh_deadline_config):
    """
    Confirm the CLI login/logout commands drive the AWS CLI for AWS Console sign-in
    profiles, which Deadline Cloud monitor does not know about.
    """
    aws_cli = (
        "C:/Program Files/Amazon/AWSCLIV2/aws.exe"
        if sys.platform.startswith("win")
        else "/usr/local/bin/aws"
    )
    profile_name = "console-us-west-2"
    scoped_config = {
        "region": "us-west-2",
        "login_session": "arn:aws:sts::123456789012:assumed-role/Admin/someone",
    }
    config.set_setting("defaults.aws_profile_name", profile_name)

    login_process = MagicMock()
    login_process.poll.return_value = 0
    login_process.stdout.read.return_value = b""

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(api._loginout.shutil, "which", return_value=aws_cli),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=login_process) as popen_mock,
        patch.object(subprocess, "check_output") as check_output_mock,
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        session_mock().profile_name = profile_name
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}
        check_output_mock.return_value = bytes("Successfully logged out", "utf8")

        result = CliRunner().invoke(main, ["auth", "login"])

        assert result.exit_code == 0, result.output
        assert popen_mock.call_args[0][0] == [aws_cli, "login", "--profile", profile_name]
        assert "AWS Console sign-in page" in result.output
        assert f"AWS Console sign-in profile: {profile_name}" in result.output

        result = CliRunner().invoke(main, ["auth", "logout"])

        assert result.exit_code == 0, result.output
        assert check_output_mock.call_args[0][0] == [
            aws_cli,
            "logout",
            "--profile",
            profile_name,
        ]


def test_cli_auth_status_console_profile(fresh_deadline_config):
    """`auth status` reports the console sign-in source rather than HOST_PROVIDED."""
    profile_name = "console-us-west-2"
    scoped_config = {
        "region": "us-west-2",
        "login_session": "arn:aws:sts::123456789012:assumed-role/Admin/someone",
    }
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(
            api,
            "check_authentication_status",
            return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        session_mock().profile_name = profile_name
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}

        result = CliRunner().invoke(main, ["auth", "status", "--output", "json"])

    assert result.exit_code == 0
    assert json.loads(result.output)["source"] == "AWS_CONSOLE_LOGIN"


def test_cli_auth_status(fresh_deadline_config):
    """
    Confirm that the CLI status command prints out as expected
    """
    # GIVEN
    profile_name = "sandbox-us-west-2"
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
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
        # api_availability is derived from status: AUTHENTICATED -> True.
        "api_availability": True,
    }
    scoped_config = {
        "credential_process": "/bin/DeadlineCloudMonitor get-credentials --profile sandbox-us-west-2",
        "monitor_id": "monitor-1g9neezauta8ease",
        "region": "us-west-2",
        "user_id": "user-1234",
        "identity_store_id": "d-abcdef0123",
    }
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(
            api,
            "check_authentication_status",
            return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
        ),
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
