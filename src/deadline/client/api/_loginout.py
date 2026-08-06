# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for logging in or out of the AWS Profile that is
configured for AWS Deadline Cloud to use on the local workstation.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import Callable, Optional
import shutil
import subprocess
import sys

from ._session import (
    get_credentials_source,
    check_authentication_status,
    AwsCredentialsSource,
    AwsAuthenticationStatus,
)
from . import _session
from .. import api
from ..config import get_setting
from ..exceptions import DeadlineOperationError
import time

logger = getLogger(__name__)

# Console sign-in profiles are keyed by `login_session` in ~/.aws/config, with the
# tokens cached under ~/.aws/login/cache, so the AWS CLI v2 — not Deadline Cloud
# monitor — owns refreshing them. Deadline Cloud monitor's `login` subcommand only
# knows the monitor profiles in its own settings, and rejects these by name.
_AWS_CLI_EXECUTABLE = "aws"


class UnsupportedProfileTypeForLoginLogout(DeadlineOperationError):
    pass


def _resolve_aws_cli_path() -> str:
    """
    Returns the path to the AWS CLI, raising a DeadlineOperationError if it isn't
    on the PATH. Console sign-in profiles can only be refreshed through it.
    """
    aws_cli_path = shutil.which(_AWS_CLI_EXECUTABLE)
    if not aws_cli_path:
        raise DeadlineOperationError(
            "Could not find the AWS CLI on the PATH. Logging in to an AWS Console "
            "sign-in profile requires AWS CLI v2 with the 'aws login' command. "
            "Install it from "
            "https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        )
    return aws_cli_path


def _check_console_login_dependency() -> None:
    """
    Verifies botocore can actually load AWS Console sign-in credentials before we
    start a login we couldn't observe completing.

    botocore resolves ``login_session`` profiles with its LoginProvider, which needs
    the ``awscrt`` extra to sign the DPoP proofs used to refresh the cached token.
    Without it every API call raises MissingDependencyException, so the post-login
    authentication probe could never succeed and the poll loop would spin forever.
    """
    from botocore.compat import EC

    if EC is None:
        raise DeadlineOperationError(
            "Logging in to an AWS Console sign-in profile requires an additional "
            'dependency. Install it with: pip install "deadline[console]"'
        )


def _login_aws_console(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    """
    Logs in to an AWS Console sign-in profile by running `aws login`, which opens a
    browser for the console sign-in and caches the resulting refreshable token.
    """
    _check_console_login_dependency()
    aws_cli_path = _resolve_aws_cli_path()
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    args = [aws_cli_path, "login", "--profile", profile_name]

    try:
        if sys.platform.startswith("win"):
            # We don't hook up to stdin but do this to avoid issues on Windows.
            # See https://docs.python.org/3/library/subprocess.html#subprocess.STARTUPINFO.lpAttributeList
            p = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
            )
        else:
            p = subprocess.Popen(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
            )
    except FileNotFoundError:
        raise DeadlineOperationError(
            f"Could not find the AWS CLI at {aws_cli_path}. Please ensure AWS CLI v2 "
            "is installed correctly and try again."
        )

    if on_pending_authorization:
        on_pending_authorization(credentials_source=AwsCredentialsSource.AWS_CONSOLE_LOGIN)

    # Unlike Deadline Cloud monitor, `aws login` is a CLI command that exits once the
    # browser handshake finishes, so we wait for it rather than polling credentials.
    while True:
        returncode = p.poll()
        if returncode is not None:
            break
        if on_cancellation_check and on_cancellation_check():
            p.kill()
            raise Exception()
        time.sleep(0.5)

    out = p.stdout.read().decode("utf-8") if p.stdout else ""
    if returncode != 0:
        raise DeadlineOperationError(
            f"The AWS CLI was not able to log in to the {profile_name} profile:\n{out}"
        )

    # `aws login` wrote a fresh token to the login cache. Drop the cached boto3
    # session so the next call picks it up instead of the expired credentials.
    _session.get_boto3_session(force_refresh=True, config=config)
    if check_authentication_status(config) != AwsAuthenticationStatus.AUTHENTICATED:
        raise DeadlineOperationError(
            f"The AWS CLI logged in to the {profile_name} profile, but AWS Deadline Cloud "
            "is still not accessible. Confirm the profile's region and that the signed-in "
            "identity is authorized for AWS Deadline Cloud."
        )
    return f"AWS Console sign-in profile: {profile_name}"


def _logout_aws_console(config: Optional[ConfigParser] = None) -> str:
    """
    Logs out of an AWS Console sign-in profile by running `aws logout`, which clears
    the profile's cached token.
    """
    aws_cli_path = _resolve_aws_cli_path()
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    args = [aws_cli_path, "logout", "--profile", profile_name]

    try:
        output = subprocess.check_output(args, stderr=subprocess.STDOUT)
    except FileNotFoundError:
        raise DeadlineOperationError(
            f"Could not find the AWS CLI at {aws_cli_path}. Please ensure AWS CLI v2 "
            "is installed correctly and try again."
        )
    except subprocess.CalledProcessError as e:
        raise DeadlineOperationError(
            f"The AWS CLI was unable to log out the profile {profile_name}."
            f"Return code {e.returncode}: {e.output}"
        )

    # Force a refresh of the cached boto3 Session
    _session.invalidate_boto3_session_cache()
    return output.decode("utf8")


def _login_deadline_cloud_monitor(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    # Deadline Cloud monitor writes the absolute path to itself to the config file
    deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    args = [deadline_cloud_monitor_path, "login", "--profile", profile_name]

    # Open Deadline Cloud monitor, non-blocking the user will keep Deadline Cloud monitor running in the background.
    try:
        if sys.platform.startswith("win"):
            # We don't hookup to stdin but do this to avoid issues on windows
            # See https://docs.python.org/3/library/subprocess.html#subprocess.STARTUPINFO.lpAttributeList
            p = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.PIPE
            )
        else:
            p = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL
            )
    except FileNotFoundError:
        raise DeadlineOperationError(
            f"Could not find Deadline Cloud monitor at {deadline_cloud_monitor_path}. "
            f"Please ensure Deadline Cloud monitor is installed correctly and set up the {profile_name} profile again."
        )
    if on_pending_authorization:
        on_pending_authorization(
            credentials_source=AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
        )
    # And wait for the user to complete login
    while True:
        # Deadline Cloud monitor is a GUI app that will keep on running
        # So we sit here and test that profile for validity until it works.
        # Force-refresh the session each iteration so the next probe picks up
        # profile keys DCM writes (user_id, identity_store_id, monitor_id) as
        # login completes — the GUI does the same on file-watch events in
        # DeadlineAuthenticationStatus.files_changed, but CLI has no watcher.
        _session.get_boto3_session(force_refresh=True, config=config)
        if check_authentication_status(config) == AwsAuthenticationStatus.AUTHENTICATED:
            return f"Deadline Cloud monitor profile: {profile_name}"
        if on_cancellation_check:
            # Check if the UI has signaled a cancel
            if on_cancellation_check():
                p.kill()
                raise Exception()
        if p.poll():
            # Deadline Cloud monitor has stopped, we assume it returned us an error on one line on stderr
            # but let's be specific about Deadline Cloud monitor failing incase the error is non-obvious
            # and let's tack on stdout incase it helps
            err_prefix = (
                f"Deadline Cloud monitor was not able to log into the {profile_name} profile:"
            )
            out = p.stdout.read().decode("utf-8") if p.stdout else ""
            raise DeadlineOperationError(f"{err_prefix}\n{out}")

        time.sleep(0.5)


@api.record_function_latency_telemetry_event()
def login(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor or by AWS Console sign-in,
    logs in to provide access to Deadline Cloud.

    Args:
        on_pending_authorization (Callable): A callback that receives method-specific information to continue login.
            All methods: 'credentials_source' parameter of type AwsCredentialsSource
            For Deadline Cloud monitor: No additional parameters
            For AWS Console sign-in: No additional parameters
        on_cancellation_check (Callable): A callback that allows the operation to cancel before login completes
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
    if credentials_source == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        return _login_deadline_cloud_monitor(
            on_pending_authorization, on_cancellation_check, config
        )
    if credentials_source == AwsCredentialsSource.AWS_CONSOLE_LOGIN:
        return _login_aws_console(on_pending_authorization, on_cancellation_check, config)
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor "
        "or by AWS Console sign-in."
    )


@api.record_function_latency_telemetry_event()
def logout(config: Optional[ConfigParser] = None) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor or by AWS Console sign-in,
    logs out of Deadline Cloud.

     Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
    if credentials_source == AwsCredentialsSource.AWS_CONSOLE_LOGIN:
        return _logout_aws_console(config)
    if credentials_source == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        # Deadline Cloud monitor writes the absolute path to itself to the config file
        deadline_cloud_monitor_path = get_setting("deadline-cloud-monitor.path", config=config)
        profile_name = get_setting("defaults.aws_profile_name", config=config)
        args = [deadline_cloud_monitor_path, "logout", "--profile", profile_name]

        # Open Deadline Cloud monitor, blocking
        # Unlike login, that opens the regular Deadline Cloud monitor GUI, logout is a CLI command that clears the profile
        # This makes it easier as we can execute and look at the return cdoe
        try:
            output = subprocess.check_output(args)
        except FileNotFoundError:
            raise DeadlineOperationError(
                f"Could not find Deadline Cloud monitor at {deadline_cloud_monitor_path}. "
                f"Please ensure Deadline Cloud monitor is installed correctly and set up the {profile_name} profile again."
            )
        except subprocess.CalledProcessError as e:
            raise DeadlineOperationError(
                f"Deadline Cloud monitor was unable to log out the profile {profile_name}."
                f"Return code {e.returncode}: {e.output}"
            )

        # Force a refresh of the cached boto3 Session
        _session.invalidate_boto3_session_cache()
        return output.decode("utf8")
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging out is only supported for AWS Profiles created by Deadline Cloud monitor "
        "or by AWS Console sign-in."
    )
