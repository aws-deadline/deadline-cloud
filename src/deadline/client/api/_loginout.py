# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for logging in or out of the AWS Profile that is
configured for AWS Deadline Cloud to use on the local workstation.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import Callable, Optional
import subprocess
import sys


from ._session import (
    invalidate_boto3_session_cache,
    get_credentials_source,
    check_authentication_status,
    AwsCredentialsSource,
    AwsAuthenticationStatus,
)
from ..config import get_setting
from ..exceptions import DeadlineOperationError
import time

logger = getLogger(__name__)


class UnsupportedProfileTypeForLoginLogout(DeadlineOperationError):
    pass


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
                args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE
            )
        else:
            p = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL
            )
        # Linux takes time to start DCM binary, which causes the TTY to suspend the process and send it to background job
        # With wait here, the DCM binary starts and TTY does not suspend the deadline process.
        if sys.platform == "linux":
            time.sleep(0.5)
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
        # So we sit here and test that profile for validity until it works
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


def login(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor, logs in to provide access to Deadline Cloud.

    Args:
        on_pending_authorization (Callable): A callback that receives method-specific information to continue login.
            All methods: 'credentials_source' parameter of type AwsCredentialsSource
            For Deadline Cloud monitor: No additional parameters
        on_cancellation_check (Callable): A callback that allows the operation to cancel before login completes
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
    if credentials_source == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
        return _login_deadline_cloud_monitor(
            on_pending_authorization, on_cancellation_check, config
        )
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging in is only supported for AWS Profiles created by Deadline Cloud monitor."
    )


def logout(config: Optional[ConfigParser] = None) -> str:
    """
    For AWS profiles created by Deadline Cloud monitor, logs out of Deadline Cloud.

     Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_source = get_credentials_source(config)
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
        invalidate_boto3_session_cache()
        return output.decode("utf8")
    raise UnsupportedProfileTypeForLoginLogout(
        "Logging out is only supported for AWS Profiles created by Deadline Cloud monitor."
    )
