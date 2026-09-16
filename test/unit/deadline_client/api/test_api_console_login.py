# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for logging in and out of AWS Console sign-in profiles.

Console sign-in profiles are created by `aws login` or by Deadline Cloud monitor's
console sign-in flow. They carry a `login_session` key in ~/.aws/config rather than
the `monitor_id` a Deadline Cloud monitor profile has, and botocore's LoginProvider
refreshes their cached token in-process -- no external tool is involved once a session
exists. Starting a session needs an interactive browser handshake, so login hands off to
Deadline Cloud monitor, which implements it; logging out is just deleting the cached
token file, done in-process.
"""

import builtins
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest
from botocore.utils import generate_login_cache_key

from deadline.client import api, config
from deadline.client.api._loginout import UnsupportedProfileTypeForLoginLogout
from deadline.client.api._session import AwsAuthenticationStatus, AwsCredentialsSource
from deadline.client.exceptions import DeadlineOperationError

PROFILE_NAME = "console-us-west-2"
LOGIN_SESSION_ARN = "arn:aws:sts::123456789012:assumed-role/Admin/someone"

# Config settings marked `is_path` are stored with forward slashes and read back in the
# native format, so what Popen is called with differs from what was written on Windows.
if sys.platform.startswith("win"):
    MONITOR_PATH = "C:/Programs/bin/DeadlineCloudMonitor"
    EXPECTED_MONITOR_ARGV0 = "C:\\Programs\\bin\\DeadlineCloudMonitor"
else:
    MONITOR_PATH = "/bin/DeadlineCloudMonitor"
    EXPECTED_MONITOR_ARGV0 = MONITOR_PATH

# A console sign-in profile as `aws login` / Deadline Cloud monitor writes it: only
# `region` and `login_session`, with the token cached under ~/.aws/login/cache.
CONSOLE_SCOPED_CONFIG = {
    "region": "us-west-2",
    "login_session": LOGIN_SESSION_ARN,
}

MONITOR_SCOPED_CONFIG = {
    "credential_process": "/bin/DeadlineCloudMonitor get-credentials --profile sandbox-us-west-2",
    "monitor_id": "monitor-1g9neezauta8ease",
    "region": "us-west-2",
    "user_id": "user-1234",
    "identity_store_id": "d-abcdef0123",
}


@pytest.fixture
def console_profile(fresh_deadline_config, aws_config):
    """
    Configures a console sign-in profile as the active AWS profile.

    The profile is written to the real AWS config file as well as mocked onto the boto3
    session: `get_credentials_source` reads the mocked session, but logout resolves the
    `login_session` ARN through a fresh `botocore.session.Session`, which parses the
    file at AWS_CONFIG_FILE.
    """
    config.set_setting("defaults.aws_profile_name", PROFILE_NAME)
    aws_config.write_text(
        f"[profile {PROFILE_NAME}]\nregion = us-west-2\nlogin_session = {LOGIN_SESSION_ARN}\n"
    )
    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock().profile_name = PROFILE_NAME
        session_mock()._session.get_scoped_config.return_value = CONSOLE_SCOPED_CONFIG
        session_mock()._session.full_config = {"profiles": {PROFILE_NAME: CONSOLE_SCOPED_CONFIG}}
        yield session_mock


@pytest.fixture
def console_profile_with_monitor(console_profile):
    """
    A console sign-in profile on a workstation that has Deadline Cloud monitor.

    Deadline Cloud monitor writes its own path to `deadline-cloud-monitor.path` when it
    creates a profile, and login hands off to it. Kept separate from `console_profile` so
    tests of the "monitor not installed" path see an unset path.

    The setting is read back rather than assumed. When it doesn't stick, `login` raises the
    "monitor is not configured" error, which reads as a product bug rather than a broken
    fixture -- so fail here instead, pointing at the actual cause.
    """
    config.set_setting("deadline-cloud-monitor.path", MONITOR_PATH)
    assert config.get_setting("deadline-cloud-monitor.path"), (
        "deadline-cloud-monitor.path did not persist, so this test's premise doesn't hold"
    )
    return console_profile


@pytest.fixture
def authenticated_after_login():
    """
    Makes the login poll loop exit on its first iteration.

    `_login_deadline_cloud_monitor_process` polls `check_authentication_status` until the
    profile authenticates, which never happens with a mocked subprocess.
    """
    with patch.object(
        api._loginout,
        "check_authentication_status",
        return_value=AwsAuthenticationStatus.AUTHENTICATED,
    ) as status_mock:
        yield status_mock


@pytest.fixture
def login_cache_dir(tmp_path, monkeypatch):
    """
    Redirects botocore's login token cache to a temp directory.

    `get_login_token_cache_directory` honours AWS_LOGIN_CACHE_DIRECTORY, so logout can
    be exercised against real files instead of a mocked `os.remove`.
    """
    cache_dir = tmp_path / "login-cache"
    cache_dir.mkdir()
    monkeypatch.setenv("AWS_LOGIN_CACHE_DIRECTORY", str(cache_dir))
    return cache_dir


def _cached_token_path(cache_dir, login_session=LOGIN_SESSION_ARN):
    """The file botocore caches a login session's token in."""
    return cache_dir / f"{generate_login_cache_key(login_session)}.json"


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


def test_console_login_launches_deadline_cloud_monitor(
    console_profile_with_monitor, authenticated_after_login
):
    """
    Starting a console session needs an interactive browser handshake, which is not an API
    this library can call. Deadline Cloud monitor implements it and recognises these
    profiles by their `login_session`, so login hands off to it with the same
    `login --profile` subcommand a monitor profile uses.
    """
    with patch.object(subprocess, "Popen") as popen_mock:
        output = api.login(None, None)

    popen_mock.assert_called_once()
    assert popen_mock.call_args[0][0] == [
        EXPECTED_MONITOR_ARGV0,
        "login",
        "--profile",
        PROFILE_NAME,
    ]
    assert PROFILE_NAME in output


def test_console_login_reports_console_credentials_source(
    console_profile_with_monitor, authenticated_after_login
):
    """
    The one behavioral difference from the monitor path: the callback must report
    AWS_CONSOLE_LOGIN so the UI says "sign in with the AWS Console" rather than the
    monitor's own log-in wording.
    """
    on_pending_authorization = MagicMock()

    with patch.object(subprocess, "Popen"):
        api.login(on_pending_authorization, None)

    on_pending_authorization.assert_called_once_with(
        credentials_source=AwsCredentialsSource.AWS_CONSOLE_LOGIN
    )


def test_console_login_without_monitor_path_raises_without_spawning(console_profile):
    """
    Deadline Cloud monitor writes its own path when it creates a profile, so an unset path
    means it isn't installed and there is nothing to hand off to. Say so, naming the
    `aws login` alternative, and don't shell out to a tool that isn't there.
    """
    assert config.get_setting("deadline-cloud-monitor.path") == ""

    with patch.object(subprocess, "Popen") as popen_mock:
        with pytest.raises(DeadlineOperationError) as excinfo:
            api.login(None, None)

    popen_mock.assert_not_called()
    message = str(excinfo.value)
    assert PROFILE_NAME in message
    assert "Deadline Cloud monitor" in message
    assert f"aws login --profile {PROFILE_NAME}" in message


def test_console_login_without_awscrt_raises_without_spawning(console_profile_with_monitor):
    """
    Without awscrt, botocore can't sign the DPoP proof the cached token is bound to, so the
    post-launch authentication probe can never succeed. Deadline Cloud monitor is a GUI that
    keeps running, so `p.poll()` stays None too and the poll loop would spin forever -- the user
    would sign in successfully in the browser and the CLI would still hang. Fail before launching.
    """
    with patch("botocore.compat.EC", None), patch.object(subprocess, "Popen") as popen_mock:
        with pytest.raises(DeadlineOperationError) as excinfo:
            api.login(None, None)

    popen_mock.assert_not_called()
    assert 'pip install "deadline[console]"' in str(excinfo.value)


def test_console_login_does_not_invoke_the_aws_cli(
    console_profile_with_monitor, authenticated_after_login
):
    """
    The AWS CLI is not a dependency of this package, so console login must go through
    Deadline Cloud monitor rather than shelling out to `aws login`.
    """
    with patch.object(subprocess, "Popen") as popen_mock:
        api.login(None, None)

    for call in popen_mock.call_args_list:
        argv = call[0][0]
        assert argv[0] == EXPECTED_MONITOR_ARGV0
        assert os.path.basename(argv[0]).split(".")[0] != "aws"


def test_console_login_surfaces_monitor_failure(console_profile_with_monitor):
    """
    Deadline Cloud monitor exiting before the profile authenticates means the sign-in
    failed. Its stdout is the only explanation the user gets, so include it.
    """
    login_process = MagicMock()
    login_process.poll.return_value = 1
    login_process.stdout.read.return_value = b"Sign-in was cancelled"

    with (
        patch.object(subprocess, "Popen", return_value=login_process),
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.NEEDS_LOGIN,
        ),
    ):
        with pytest.raises(DeadlineOperationError, match="Sign-in was cancelled"):
            api.login(None, None)


def test_console_login_cancellation_kills_monitor(console_profile_with_monitor):
    """
    A cancel from the UI has to stop the process it started: the monitor window was opened
    on the user's behalf, so leaving it running would strand a sign-in nobody is waiting on.
    """
    login_process = MagicMock()
    login_process.poll.return_value = None

    with (
        patch.object(subprocess, "Popen", return_value=login_process),
        patch.object(
            api._loginout,
            "check_authentication_status",
            return_value=AwsAuthenticationStatus.NEEDS_LOGIN,
        ),
    ):
        with pytest.raises(Exception):
            api.login(None, lambda: True)

    login_process.kill.assert_called_once()


def test_console_logout_removes_cached_token(console_profile, login_cache_dir):
    """
    Logout is the deletion of botocore's cached token: the file keyed by the sha256 of
    the `login_session` ARN. Nothing left to refresh means the session is over.
    """
    cached_token = _cached_token_path(login_cache_dir)
    cached_token.write_text('{"accessToken": "token"}')
    # A second session's token must survive -- logout is per-profile.
    other_token = _cached_token_path(login_cache_dir, "arn:aws:sts::123456789012:user/other")
    other_token.write_text('{"accessToken": "other"}')

    output = api.logout()

    assert not cached_token.exists()
    assert other_token.exists()
    assert PROFILE_NAME in output


def test_console_logout_tells_the_monitor_to_log_out(console_profile_with_monitor, login_cache_dir):
    """
    Deadline Cloud monitor holds its own signed-in state and doesn't watch the token cache. If
    it keeps running, it still shows the profile as logged in, and the next `login` finds the
    live instance, foregrounds it, and signs nobody in. So logout has to tell it as well.
    """
    _cached_token_path(login_cache_dir).write_text('{"accessToken": "token"}')

    with patch.object(subprocess, "check_output", return_value=b"") as check_output_mock:
        api.logout()

    assert check_output_mock.call_args[0][0][1:] == ["logout", "--profile", PROFILE_NAME]


def test_console_logout_succeeds_when_the_monitor_logout_fails(
    console_profile_with_monitor, login_cache_dir
):
    """
    The cached token is already gone by then, so the session is over either way. A monitor that
    can't be reached must not turn a successful logout into an error.
    """
    cached_token = _cached_token_path(login_cache_dir)
    cached_token.write_text('{"accessToken": "token"}')

    with patch.object(
        subprocess,
        "check_output",
        side_effect=subprocess.CalledProcessError(1, "dcm", output=b"boom"),
    ):
        output = api.logout()

    assert not cached_token.exists()
    assert PROFILE_NAME in output


def test_console_logout_without_a_monitor_still_clears_the_token(console_profile, login_cache_dir):
    """
    A profile created by `aws login` has no monitor path, so the in-process deletion is the only
    thing that ends the session. It must not depend on the monitor being installed.
    """
    cached_token = _cached_token_path(login_cache_dir)
    cached_token.write_text('{"accessToken": "token"}')

    with patch.object(subprocess, "check_output") as check_output_mock:
        api.logout()

    assert not cached_token.exists()
    check_output_mock.assert_not_called()


def test_console_logout_succeeds_when_already_logged_out(console_profile, login_cache_dir):
    """
    No cached token means nothing to revoke, which is the state logout is trying to
    reach -- report success rather than an error the user can't act on.
    """
    assert not _cached_token_path(login_cache_dir).exists()

    output = api.logout()

    assert PROFILE_NAME in output


def test_console_logout_invalidates_session_cache(console_profile, login_cache_dir):
    """The cleared token must not linger in the cached boto3 session."""
    _cached_token_path(login_cache_dir).write_text("{}")

    with patch.object(api._session, "invalidate_boto3_session_cache") as invalidate_mock:
        api.logout()

    invalidate_mock.assert_called()


def test_console_logout_does_not_require_awscrt(console_profile, login_cache_dir):
    """
    Clearing a cached token needs no DPoP signing, so logout must still work without
    awscrt -- otherwise a user missing the extra couldn't clear a broken profile.
    """
    cached_token = _cached_token_path(login_cache_dir)
    cached_token.write_text("{}")

    with patch("botocore.compat.EC", None):
        api.logout()

    assert not cached_token.exists()


def test_console_logout_without_login_session_raises(fresh_deadline_config, login_cache_dir):
    """
    The cache key is derived from the `login_session` ARN, so a profile missing it gives
    nothing to delete. Say that instead of computing a key from None.
    """
    config.set_setting("defaults.aws_profile_name", PROFILE_NAME)

    # Called directly: `get_credentials_source` only routes profiles here when it sees
    # a `login_session`, so this guard is unreachable through `api.logout`.
    with pytest.raises(DeadlineOperationError, match="no login_session entry"):
        api._loginout._logout_aws_console()


def test_console_logout_reports_removal_failure(console_profile, login_cache_dir):
    """
    A cached token that can't be deleted (locked file, read-only cache dir) leaves the
    session usable, so don't report a logout that didn't happen.
    """
    _cached_token_path(login_cache_dir).write_text("{}")

    with patch.object(os, "remove", side_effect=PermissionError("Access is denied")):
        with pytest.raises(DeadlineOperationError, match="Could not remove the cached credentials"):
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
    the widget only shows the "Log in" button in the NEEDS_LOGIN state. The login it
    offers happens outside this library, but a login is still what's needed.
    """
    with patch.object(
        api._session, "_list_farms_for_auth_probe", side_effect=Exception("ExpiredToken")
    ):
        assert api.check_authentication_status() == AwsAuthenticationStatus.NEEDS_LOGIN


def test_console_login_proceeds_when_the_awscrt_probe_is_unavailable(
    console_profile_with_monitor, authenticated_after_login
):
    """
    The pre-flight reads a private botocore symbol. If a future botocore drops it, blocking a
    login that would have worked is worse than the hang the check exists to prevent -- so an
    ImportError means "can't tell", not "refuse".
    """
    real_import = builtins.__import__

    def hide_botocore_compat_ec(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "botocore.compat" and fromlist and "EC" in fromlist:
            raise ImportError("no attribute EC")
        return real_import(name, globals, locals, fromlist, level)

    with (
        patch.object(builtins, "__import__", side_effect=hide_botocore_compat_ec),
        patch.object(subprocess, "Popen") as popen_mock,
    ):
        output = api.login(None, None)

    popen_mock.assert_called_once()
    assert PROFILE_NAME in output


def test_console_logout_works_when_the_console_profile_is_the_default(
    fresh_deadline_config, aws_config, login_cache_dir
):
    """
    `defaults.aws_profile_name` ships as the sentinel "(default)", which get_boto3_session
    normalizes away -- but `full_config["profiles"]` is keyed by the real name, "default".
    Resolving the ARN by the sentinel would miss a console profile that *is* the default
    profile, so logout would refuse a session get_credentials_source had just accepted.
    """
    config.set_setting("defaults.aws_profile_name", "(default)")
    aws_config.write_text(f"[default]\nregion = us-west-2\nlogin_session = {LOGIN_SESSION_ARN}\n")

    scoped_config = {"region": "us-west-2", "login_session": LOGIN_SESSION_ARN}
    cached_token = _cached_token_path(login_cache_dir)
    cached_token.write_text('{"accessToken": "token"}')

    with (
        patch.object(api._session, "get_boto3_session") as session_mock,
        patch.object(api, "get_boto3_session", new=session_mock),
    ):
        session_mock()._session.get_scoped_config.return_value = scoped_config
        session_mock()._session.full_config = {"profiles": {"default": scoped_config}}

        assert api.get_credentials_source() == AwsCredentialsSource.AWS_CONSOLE_LOGIN
        api.logout()

    assert not cached_token.exists(), "logout must end the session of a default console profile"
