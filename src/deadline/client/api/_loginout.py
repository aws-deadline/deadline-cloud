# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for logging in or out of the AWS Profile that is
configured for Amazon Deadline Cloud to use on the local workstation.
"""

from configparser import ConfigParser
from logging import getLogger
from typing import Callable, Optional
import subprocess


from ._session import (
    invalidate_boto3_session_cache,
    get_credentials_type,
    check_credentials_status,
    AwsCredentialsType,
    AwsCredentialsStatus,
)
from ..config import get_setting
import time

logger = getLogger(__name__)


class UnsupportedProfileTypeForLoginLogout(Exception):
    pass


def _login_cloud_companion(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
):
    # Cloud Companion writes the absolute path to itself to the config file
    cloud_companion_path = get_setting("cloud-companion.path", config=config)
    profile_name = get_setting("defaults.aws_profile_name", config=config)
    args = [cloud_companion_path, "login", "--profile", profile_name]

    # Open CloudCompanion, non-blocking the user will keep CloudCompanion running in the background.
    try:
        # We don't hookup to stdin but do this to avoid issues on windows
        # See https://docs.python.org/3/library/subprocess.html#subprocess.STARTUPINFO.lpAttributeList

        p = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, stdin=subprocess.PIPE
        )
    except FileNotFoundError:
        raise Exception(
            f"Could not find Cloud Companion at {cloud_companion_path}. Please ensure Cloud Companion is installed correctly and setup the {profile_name} profile again."
        )
    if on_pending_authorization:
        on_pending_authorization(credential_type=AwsCredentialsType.CLOUD_COMPANION_LOGIN)
    # And wait for the user to complete login
    while True:
        # Cloud Companion is a GUI app that will keep on running
        # So we sit here and test that profile for validity until it works
        if check_credentials_status(config) == AwsCredentialsStatus.AUTHENTICATED:
            return f"Cloud Companion Profile: {profile_name}"
        if on_cancellation_check:
            # Check if the UI has signaled a cancel
            if on_cancellation_check():
                p.kill()
                raise Exception()
        if p.poll():
            # Cloud Companion has stopped, we assume it returned us an error on one line on stderr
            # but let's be specific about Cloud Companion failing incase the error is non-obvious
            # and let's tack on stdout incase it helps
            err_prefix = f"Cloud Companion was not able to log into the {profile_name} profile: "
            out = p.stdout.read().decode("utf-8") if p.stdout else ""
            raise Exception(f"{err_prefix}: {out}")

        time.sleep(0.5)


def login(
    on_pending_authorization: Optional[Callable],
    on_cancellation_check: Optional[Callable],
    config: Optional[ConfigParser] = None,
) -> str:
    """
    Logs in to the provided session if supported.

    This method supports Nimble Cloud Companion
    If Amazon Deadline Cloud doesn't know how to login to the the requested session Profile UnsupportedProfileTypeForLoginLogout is thrown

     Args:
        on_pending_authorization (Callable): A callback that receives method-specific information to continue login.
            All methods: 'credential_type' parameter of type AwsCredentialsType
            For Cloud Companion: No additional parameters
        on_cancellation_check (Callable): A callback that allows the operation to cancel before login completes
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_type = get_credentials_type(config)
    if credentials_type == AwsCredentialsType.CLOUD_COMPANION_LOGIN:
        return _login_cloud_companion(on_pending_authorization, on_cancellation_check, config)
    raise UnsupportedProfileTypeForLoginLogout(
        "This action is only valid for Nimble Cloud Companion Profiles"
    )


def logout(config: Optional[ConfigParser] = None) -> str:
    """
    Logs out of supported credential providers
    For Nimble Cloud Companion, closes any running instance of Cloud Companion for the current profile

     Args:
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.
    """
    credentials_type = get_credentials_type(config)
    if credentials_type == AwsCredentialsType.CLOUD_COMPANION_LOGIN:
        # Cloud Companion writes the absolute path to itself to the config file
        cloud_companion_path = get_setting("cloud-companion.path", config=config)
        profile_name = get_setting("defaults.aws_profile_name", config=config)
        args = [cloud_companion_path, "logout", "--profile", profile_name]

        # Open CloudCompanion, blocking
        # Unlike login, that opens the regular Cloud Companion GUI, logout is a CLI command that clears the profile
        # This makes it easier as we can execute and look at the return cdoe
        try:
            output = subprocess.check_output(args)
        except FileNotFoundError:
            raise Exception(
                f"Could not find Cloud Companion at {cloud_companion_path}. Please ensure Cloud Companion is installed correctly and setup the {profile_name} profile again."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(
                f"Cloud Companion was unable to logout the profile {profile_name}. Return code {e.returncode}: {e.output}"
            )

        # Force a refresh of the cached boto3 Session
        invalidate_boto3_session_cache()
        return output.decode("utf8")
    raise UnsupportedProfileTypeForLoginLogout(
        "This action is only valid for Nimble Cloud Companion Profiles"
    )
