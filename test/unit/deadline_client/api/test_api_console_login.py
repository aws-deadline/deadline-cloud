# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for logging in and out of AWS Console sign-in profiles.

Console sign-in profiles are created by `aws login` or by Deadline Cloud monitor's
console sign-in flow. They carry a `login_session` key in ~/.aws/config rather than
the `monitor_id` a Deadline Cloud monitor profile has, and are refreshed through the
AWS CLI rather than through Deadline Cloud monitor.
"""

import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from deadline.client import api, config
from deadline.client.api._loginout import UnsupportedProfileTypeForLoginLogout
from deadline.client.api._session import AwsAuthenticationStatus, AwsCredentialsSource
from deadline.client.exceptions import DeadlineOperationError

PROFILE_NAME = "console-us-west-2"
AWS_CLI_PATH = (
    "C:/Program Files/Amazon/AWSCLIV2/aws.exe"
    if sys.platform.startswith("win")
    else "/usr/local/bin/aws"
)

# A console sign-in profile as `aws login` / Deadline Cloud monitor writes it: only
# `region` and `login_session`, with the token cached under ~/.aws/login/cache.
CONSOLE_SCOPED_CONFIG = {
    "region": "us-west-2",
    "login_session": "arn:aws:sts::123456789012:assumed-role/Admin/someone",
}

MONITOR_SCOPED_CONFIG = {
    "credential_process": "/bin/DeadlineCloudMonitor get-credentials --profile sandbox-us-west-2",
    "monitor_id": "monitor-1g9neezauta8ease",
    "region": "us-west-2",
    "user_id": "user-1234",
    "identity_store_id": "d-abcdef0123",
}


@pytest.fixture
def console_profile(fresh_deadline_config):
    """Configures a console sign-in profile as the active AWS profile."""
    config.set_setting("defaults.aws_profile_name", PROFILE_NAME)
    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock().profile_name = PROFILE_NAME
        session_mock()._session.get_scoped_config.return_value = CONSOLE_SCOPED_CONFIG
        session_mock()._session.full_config = {"profiles": {PROFILE_NAME: CONSOLE_SCOPED_CONFIG}}
        yield session_mock


def _completed_popen(returncode=0, output=b""):
    """A Popen mock standing in for an `aws login` that has already exited."""
    process = MagicMock()
    process.poll.return_value = returncode
    process.stdout.read.return_value = output
    return process


def test_get_credentials_source_detects_console_profile(console_profile):
    """A profile with `login_session` is an AWS Console sign-in profile, not host-provided."""
    assert api.get_credentials_source() == AwsCredentialsSource.AWS_CONSOLE_LOGIN


def test_get_credentials_source_prefers_monitor_when_both_keys_present(fresh_deadline_config):
    """
    Deadline Cloud monitor writes `credential_process`, so a profile carrying both keys
    is still a monitor profile: `monitor_id` wins.
    """
    config.set_setting("defaults.aws_profile_name", PROFILE_NAME)
    scoped_config = {**MONITOR_SCOPED_CONFIG, **CONSOLE_SCOPED_CONFIG}

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock()._session.get_scoped_config.return_value = scoped_config

        assert api.get_credentials_source() == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN


def test_get_credentials_source_plain_profile_still_host_provided(fresh_deadline_config):
    """A profile with neither key remains HOST_PROVIDED."""
    config.set_setting("defaults.aws_profile_name", "plain-profile")

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock()._session.get_scoped_config.return_value = {"region": "us-west-2"}

        assert api.get_credentials_source() == AwsCredentialsSource.HOST_PROVIDED


def test_console_login_runs_aws_login(console_profile):
    """`api.login` on a console profile shells out to `aws login --profile <name>`."""
    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=_completed_popen()) as popen_mock,
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        on_pending_authorization = MagicMock()

        message = api.login(on_pending_authorization, None)

    args = popen_mock.call_args[0][0]
    assert args == [AWS_CLI_PATH, "login", "--profile", PROFILE_NAME]
    assert PROFILE_NAME in message
    # The UI/CLI needs the source to render the right "signing in" message.
    on_pending_authorization.assert_called_once_with(
        credentials_source=AwsCredentialsSource.AWS_CONSOLE_LOGIN
    )


def test_console_login_does_not_invoke_deadline_cloud_monitor(console_profile):
    """
    Deadline Cloud monitor rejects console profiles by name -- they aren't in its own
    settings -- so login must not shell out to it.
    """
    config.set_setting("deadline-cloud-monitor.path", "/bin/DeadlineCloudMonitor")

    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=_completed_popen()) as popen_mock,
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        api.login(None, None)

    assert "DeadlineCloudMonitor" not in popen_mock.call_args[0][0][0]


def test_console_login_reports_aws_cli_failure(console_profile):
    """A non-zero `aws login` exit surfaces the CLI's own output to the user."""
    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(
            subprocess,
            "Popen",
            return_value=_completed_popen(returncode=1, output=b"Browser handshake failed"),
        ),
    ):
        with pytest.raises(DeadlineOperationError, match="Browser handshake failed"):
            api.login(None, None)


def test_console_login_requires_aws_cli_on_path(console_profile):
    """Without the AWS CLI there's no way to refresh, so say so instead of failing obscurely."""
    with (
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(api._loginout.shutil, "which", return_value=None),
    ):
        with pytest.raises(DeadlineOperationError, match="Could not find the AWS CLI"):
            api.login(None, None)


def test_console_login_requires_awscrt(console_profile):
    """
    Without awscrt, botocore can't load the profile at all, so the post-login probe
    could never pass. Fail up front with install guidance rather than spinning.
    """
    with patch("botocore.compat.EC", None):
        with pytest.raises(DeadlineOperationError, match=r"deadline\[console\]"):
            api.login(None, None)


def test_console_login_dependency_check_passes_with_awscrt():
    """
    The guard must not fire when awscrt is present, or it would block every console
    login. Exercises the real check rather than the patched-out stand-in other tests use.
    """
    pytest.importorskip("awscrt", reason="requires the 'console' extra")

    api._loginout._check_console_login_dependency()


def test_console_login_awscrt_guard_precedes_aws_cli_launch(console_profile):
    """
    The dependency check runs before the browser handshake -- sending the user through
    a sign-in whose credentials can't then be loaded wastes their time.
    """
    with (
        patch("botocore.compat.EC", None),
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(subprocess, "Popen") as popen_mock,
    ):
        with pytest.raises(DeadlineOperationError, match=r"deadline\[console\]"):
            api.login(None, None)

    popen_mock.assert_not_called()


def test_console_login_refreshes_session_before_probing(console_profile):
    """
    `aws login` writes a new token to the login cache, so the cached boto3 session must
    be dropped before the authentication probe -- otherwise it reuses expired creds.
    """
    refresh_calls = []
    # Wrap rather than replace: get_credentials_source resolves the profile through
    # this same function, so it has to keep returning the console-profile session.
    real_get_boto3_session = api._loginout._session.get_boto3_session

    def recording_get_boto3_session(*args, **kwargs):
        refresh_calls.append(kwargs)
        return real_get_boto3_session(*args, **kwargs)

    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=_completed_popen()),
        patch.object(api._loginout._session, "get_boto3_session", new=recording_get_boto3_session),
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.AUTHENTICATED,
        ),
    ):
        api.login(None, None)

    assert any(call.get("force_refresh") for call in refresh_calls)


def test_console_login_reports_still_unauthenticated(console_profile):
    """
    A successful sign-in to an identity that can't reach Deadline Cloud is not a
    successful login -- don't report success.
    """
    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=_completed_popen()),
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.NEEDS_LOGIN,
        ),
    ):
        with pytest.raises(DeadlineOperationError, match="still not accessible"):
            api.login(None, None)


def test_console_login_cancellation_kills_aws_cli(console_profile):
    """Cancelling from the GUI terminates the `aws login` subprocess."""
    process = MagicMock()
    # Still running, so the loop reaches the cancellation check.
    process.poll.return_value = None

    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(api._loginout, "_check_console_login_dependency"),
        patch.object(subprocess, "Popen", return_value=process),
        patch.object(api._loginout.time, "sleep"),
    ):
        with pytest.raises(Exception):
            api.login(None, lambda: True)

    process.kill.assert_called_once()


def test_console_logout_runs_aws_logout(console_profile):
    """`api.logout` on a console profile clears the cached token via `aws logout`."""
    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(
            subprocess, "check_output", return_value=b"Successfully logged out"
        ) as check_output_mock,
        patch.object(api._session, "invalidate_boto3_session_cache") as invalidate_mock,
    ):
        output = api.logout()

    assert check_output_mock.call_args[0][0] == [
        AWS_CLI_PATH,
        "logout",
        "--profile",
        PROFILE_NAME,
    ]
    assert "Successfully logged out" in output
    # The cleared token must not linger in the cached session.
    invalidate_mock.assert_called()


def test_console_logout_does_not_require_awscrt(console_profile):
    """
    Clearing a cached token needs no DPoP signing, so logout must still work without
    awscrt -- otherwise a user missing the extra couldn't clear a broken profile.
    """
    with (
        patch("botocore.compat.EC", None),
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(subprocess, "check_output", return_value=b"Logged out") as check_output_mock,
    ):
        api.logout()

    assert check_output_mock.call_args[0][0][1] == "logout"


def test_console_logout_reports_aws_cli_failure(console_profile):
    """A failing `aws logout` is reported rather than silently treated as success."""
    with (
        patch.object(api._loginout.shutil, "which", return_value=AWS_CLI_PATH),
        patch.object(
            subprocess,
            "check_output",
            side_effect=subprocess.CalledProcessError(1, "aws", output=b"boom"),
        ),
    ):
        with pytest.raises(DeadlineOperationError, match="unable to log out"):
            api.logout()


def test_host_provided_profile_still_rejected(fresh_deadline_config):
    """Profiles with no login flow keep raising, with both supported types named."""
    config.set_setting("defaults.aws_profile_name", "plain-profile")

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock()._session.get_scoped_config.return_value = {"region": "us-west-2"}

        with pytest.raises(UnsupportedProfileTypeForLoginLogout, match="AWS Console sign-in"):
            api.login(None, None)
        with pytest.raises(UnsupportedProfileTypeForLoginLogout, match="AWS Console sign-in"):
            api.logout()


def test_expired_console_creds_report_needs_login(console_profile):
    """
    An expired console token should offer a login, not a configuration error --
    the widget only shows the "Log in" button in the NEEDS_LOGIN state.
    """
    with patch.object(
        api._session, "_list_farms_for_auth_probe", side_effect=Exception("ExpiredToken")
    ):
        assert api.check_authentication_status() == AwsAuthenticationStatus.NEEDS_LOGIN
