# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the CLI auth commands.
"""

import json
import os
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


def test_cli_console_login_opens_deadline_cloud_monitor(fresh_deadline_config, aws_config):
    """
    `auth login` on an AWS Console sign-in profile hands off to Deadline Cloud monitor:
    the browser handshake is not an API this package can call, and the monitor already
    implements it. The prompt has to name the AWS Console, not the monitor's own log-in,
    so the user knows which credentials to enter.
    """
    dcm = (
        "C:/Programs/bin/DeadlineCloudMonitor"
        if sys.platform.startswith("win")
        else "/bin/DeadlineCloudMonitor"
    )
    expected_dcm = (
        "C:\\Programs\\bin\\DeadlineCloudMonitor" if sys.platform.startswith("win") else dcm
    )
    profile_name = "console-us-west-2"
    login_session = "arn:aws:sts::123456789012:assumed-role/Admin/someone"
    scoped_config = {"region": "us-west-2", "login_session": login_session}
    config.set_setting("deadline-cloud-monitor.path", dcm)
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(subprocess, "Popen") as popen_mock,
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        session_mock().profile_name = profile_name
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}

        result = CliRunner().invoke(main, ["auth", "login"])

    assert result.exit_code == 0, result.output
    assert popen_mock.call_args[0][0] == [expected_dcm, "login", "--profile", profile_name]
    assert "Opening Deadline Cloud monitor to sign in with the AWS Console" in result.output
    # Deadline Cloud monitor performs the sign-in, but the profile is a console one and
    # `logout` names it that way, so `login` must agree.
    assert f"Successfully logged in: AWS Console sign-in profile: {profile_name}" in result.output
    assert "Deadline Cloud monitor profile" not in result.output


def test_cli_console_login_without_monitor_reports_both_routes(fresh_deadline_config, aws_config):
    """
    With no `deadline-cloud-monitor.path` there is nothing to hand off to, so `auth login`
    must fail with the two routes that would work rather than launching anything.
    """
    profile_name = "console-us-west-2"
    login_session = "arn:aws:sts::123456789012:assumed-role/Admin/someone"
    scoped_config = {"region": "us-west-2", "login_session": login_session}
    config.set_setting("defaults.aws_profile_name", profile_name)

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.object(subprocess, "Popen") as popen_mock,
    ):
        session_mock().profile_name = profile_name
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}

        result = CliRunner().invoke(main, ["auth", "login"])

    assert result.exit_code != 0
    assert "Deadline Cloud monitor" in result.output
    assert f"aws login --profile {profile_name}" in result.output
    popen_mock.assert_not_called()


def test_cli_console_logout_clears_cached_token(fresh_deadline_config, aws_config, tmp_path):
    """
    `auth logout` on a console profile deletes botocore's cached token in-process. No
    subprocess: it's a file deletion, and the AWS CLI is not a dependency of this package.
    """
    from botocore.utils import generate_login_cache_key

    profile_name = "console-us-west-2"
    login_session = "arn:aws:sts::123456789012:assumed-role/Admin/someone"
    scoped_config = {"region": "us-west-2", "login_session": login_session}
    config.set_setting("defaults.aws_profile_name", profile_name)
    # _logout_aws_console resolves the ARN through a fresh botocore session, which reads
    # the file at AWS_CONFIG_FILE.
    aws_config.write_text(
        f"[profile {profile_name}]\nregion = us-west-2\nlogin_session = {login_session}\n"
    )

    cache_dir = tmp_path / "login-cache"
    cache_dir.mkdir()
    cached_token = cache_dir / f"{generate_login_cache_key(login_session)}.json"
    cached_token.write_text('{"accessToken": "token"}')

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
        patch.dict(os.environ, {"AWS_LOGIN_CACHE_DIRECTORY": str(cache_dir)}),
        patch.object(subprocess, "check_output") as check_output_mock,
    ):
        session_mock().profile_name = profile_name
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {profile_name: scoped_config}}

        result = CliRunner().invoke(main, ["auth", "logout"])

    assert result.exit_code == 0, result.output
    assert not cached_token.exists()
    check_output_mock.assert_not_called()
    # `auth logout` echoes what logout actually did. Before that, it hardcoded the monitor
    # message and claimed to have logged out "all Deadline Cloud monitor AWS profiles".
    assert (
        f"Successfully logged out of AWS Console sign-in profile: {profile_name}" in result.output
    )
    assert "Deadline Cloud monitor AWS profiles" not in result.output


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
