# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Provides functionality for boto3 Sessions, Clients, and properties
of the Deadline-configured IAM credentials.
"""
from __future__ import annotations

import logging
from configparser import ConfigParser
from contextlib import contextmanager
from enum import Enum
from functools import lru_cache
from typing import Optional

import boto3  # type: ignore[import]
import botocore
from botocore.client import BaseClient  # type: ignore[import]
from botocore.credentials import CredentialProvider, RefreshableCredentials
from botocore.exceptions import (  # type: ignore[import]
    ClientError,
    ProfileNotFound,
)
from botocore.session import get_session as get_botocore_session

from .. import version
from ..config import get_setting
from ..exceptions import DeadlineOperationError


class AwsCredentialsSource(Enum):
    NOT_VALID = 0
    HOST_PROVIDED = 2
    DEADLINE_CLOUD_MONITOR_LOGIN = 3


class AwsAuthenticationStatus(Enum):
    CONFIGURATION_ERROR = 1
    AUTHENTICATED = 2
    NEEDS_LOGIN = 3


# Place for stashing context to be attached to boto clients.
session_context: dict[str, Optional[str]] = {"submitter-name": None}


def get_boto3_session(
    force_refresh: bool = False, config: Optional[ConfigParser] = None
) -> boto3.Session:
    """
    Gets a boto3 session for the configured AWS Deadline Cloud aws profile. This may
    either use a named profile or the default credentials provider chain.

    This implementation caches the session object for use across the CLI code,
    so that we can use the following code pattern without repeated calls to
    an external credentials provider process, for example.

    Args:
        force_refresh (bool, optional): If set to True, forces a cache refresh.
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
    """

    profile_name: Optional[str] = get_setting("defaults.aws_profile_name", config)

    # If the default AWS profile name is either not set, or set to "default",
    # use the default credentials provider chain instead of a named profile.
    if profile_name in ("(default)", "default", ""):
        profile_name = None

    if force_refresh:
        invalidate_boto3_session_cache()

    return _get_boto3_session_for_profile(profile_name)


@lru_cache
def _get_boto3_session_for_profile(profile_name: str):
    session = boto3.Session(profile_name=profile_name)

    # By default, DCM returns creds that expire after 15 minutes, and boto3's RefreshableCredentials
    # class refreshes creds that are within 15 minutes of expiring, so credentials would never be reused.
    # Also DCM credentials currently take several seconds to refresh. Lower the refresh timeouts so creds
    # are reused between API calls to save time.
    # See https://github.com/boto/botocore/blob/develop/botocore/credentials.py#L342-L362

    try:
        credentials = session.get_credentials()
        if (
            isinstance(credentials, RefreshableCredentials)
            and credentials.method == "custom-process"
        ):
            credentials._advisory_refresh_timeout = 5 * 60  # 5 minutes
            credentials._mandatory_refresh_timeout = 2.5 * 60  # 2.5 minutes
    except:  # noqa: E722
        # Attempt to patch the timeouts but ignore any errors. These patched proeprties are internal and could change
        # without notice. Creds are functional without patching timeouts.
        pass

    return session


def invalidate_boto3_session_cache() -> None:
    _get_boto3_session_for_profile.cache_clear()
    _get_queue_user_boto3_session.cache_clear()


def get_default_client_config() -> botocore.config.Config:
    """
    Gets the default botocore Config object to use with `boto3 sessions`.
    This method adds user agent version and submitter context into botocore calls.
    """
    user_agent_extra = f"app/deadline-client#{version}"
    if session_context.get("submitter-name"):
        user_agent_extra += f" submitter/{session_context['submitter-name']}"
    session_config = botocore.config.Config(user_agent_extra=user_agent_extra)
    return session_config


def get_boto3_client(service_name: str, config: Optional[ConfigParser] = None) -> BaseClient:
    """
    Gets a client from the boto3 session returned by `get_boto3_session`.
    If the client requested is `deadline`, it uses the AWS_ENDPOINT_URL_DEADLINE
    deadline endpoint url.

    Args:
        service_name (str): The AWS service to get the client for, e.g. "deadline".
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
    """

    session = get_boto3_session(config=config)
    return session.client(service_name, config=get_default_client_config())


def get_credentials_source(config: Optional[ConfigParser] = None) -> AwsCredentialsSource:
    """
    Returns DEADLINE_CLOUD_MONITOR_LOGIN if Deadline Cloud monitor wrote the credentials, HOST_PROVIDED otherwise.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    try:
        session = get_boto3_session(config=config)
        profile_config = session._session.get_scoped_config()
    except ProfileNotFound:
        return AwsCredentialsSource.NOT_VALID
    if "monitor_id" in profile_config:
        # Deadline Cloud monitor Desktop adds the "monitor_id" key
        return AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN
    else:
        return AwsCredentialsSource.HOST_PROVIDED


def get_user_and_identity_store_id(
    config: Optional[ConfigParser] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    If logged in with Deadline Cloud monitor Desktop, returns a tuple
    (user_id, identity_store_id), otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    if "monitor_id" in profile_config:
        return (profile_config["user_id"], profile_config["identity_store_id"])
    else:
        return None, None


def get_monitor_id(
    config: Optional[ConfigParser] = None,
) -> Optional[str]:
    """
    If logged in with Deadline Cloud Monitor to a Deadline Monitor, returns Monitor Id, otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    return profile_config.get("monitor_id", None)


def get_queue_user_boto3_session(
    deadline: BaseClient,
    config: Optional[ConfigParser] = None,
    farm_id: Optional[str] = None,
    queue_id: Optional[str] = None,
    queue_display_name: Optional[str] = None,
    force_refresh: bool = False,
) -> boto3.Session:
    """
    Calls the AssumeQueueRoleForUser API to obtain the role configured in a Queue,
    and then creates and returns a boto3 session with those credentials.

    Args:
        deadline (BaseClient): A Deadline client.
        config (ConfigParser, optional): If provided, the AWS Deadline Cloud config to use.
        farm_id (str, optional): The ID of the farm to use.
        queue_id (str, optional): The ID of the queue to use.
        queue_display_name (str, optional): The display name of the queue.
        force_refresh (bool, optional): If True, forces a cache refresh.
    """

    base_session = get_boto3_session(config=config, force_refresh=force_refresh)

    if farm_id is None:
        farm_id = get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = get_setting("defaults.queue_id")

    return _get_queue_user_boto3_session(
        deadline, base_session, farm_id, queue_id, queue_display_name
    )


@lru_cache
def _get_queue_user_boto3_session(
    deadline: BaseClient,
    base_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    queue_display_name: Optional[str] = None,
):
    queue_credential_provider = QueueUserCredentialProvider(
        deadline,
        farm_id,
        queue_id,
        queue_display_name,
    )

    botocore_session = get_botocore_session()
    credential_provider = botocore_session.get_component("credential_provider")
    credential_provider.insert_before("env", queue_credential_provider)
    aws_profile_name: Optional[str] = None
    if base_session.profile_name != "default":
        aws_profile_name = base_session.profile_name

    return boto3.Session(
        botocore_session=botocore_session,
        profile_name=aws_profile_name,
        region_name=base_session.region_name,
    )


@contextmanager
def _modified_logging_level(logger, level):
    old_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)


def check_authentication_status(config: Optional[ConfigParser] = None) -> AwsAuthenticationStatus:
    """
    Checks the status of the provided session, by
    calling the sts::GetCallerIdentity API.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.

    Returns AwsAuthenticationStatus enum value:
      - CONFIGURATION_ERROR if there is an unexpected error accessing credentials
      - AUTHENTICATED if they are fine
      - NEEDS_LOGIN if a Deadline Cloud monitor login is required.
    """

    with _modified_logging_level(logging.getLogger("botocore.credentials"), logging.ERROR):
        try:
            get_boto3_session(config=config).client("sts").get_caller_identity()
            return AwsAuthenticationStatus.AUTHENTICATED
        except Exception:
            # We assume that the presence of a Deadline Cloud monitor profile
            # means we will know everything necessary to start it and login.

            if get_credentials_source(config) == AwsCredentialsSource.DEADLINE_CLOUD_MONITOR_LOGIN:
                return AwsAuthenticationStatus.NEEDS_LOGIN
            return AwsAuthenticationStatus.CONFIGURATION_ERROR


class QueueUserCredentialProvider(CredentialProvider):
    """A custom botocore CredentialProvider for handling AssumeQueueRoleForUser API
    credentials. If the credentials expire, the provider will automatically refresh
    them using the _get_queue_credentials method.
    """

    # The following two constants are part of botocore's CredentialProvider interface

    # A short name to identify the provider within botocore.
    METHOD = "queue-credential-provider"
    # A name to identify the provider for use in cross-sdk features. The AWS SDKs
    # require that providers outside of botocore are prefixed with "custom"
    CANONICAL_NAME = "custom-queue-credential-provider"

    deadline: BaseClient
    farm_id: str
    queue_id: str
    queue_display_name_or_id: Optional[str]

    def __init__(
        self,
        deadline: BaseClient,
        farm_id: str,
        queue_id: str,
        queue_display_name: Optional[str] = None,
    ):
        self.deadline = deadline
        self.farm_id = farm_id
        self.queue_id = queue_id
        self.queue_display_name_or_id = queue_display_name or queue_id

    def load(self):
        credentials = self._get_queue_credentials()
        return RefreshableCredentials.create_from_metadata(
            metadata=credentials,
            refresh_using=self._get_queue_credentials,
            method=self.METHOD,
        )

    def _get_queue_credentials(self):
        """
        Fetches or refreshes the credentials using the AssumeQueueRoleForUser API
        for the specified Farm ID and Queue ID.
        """
        try:
            queue_credentials = self.deadline.assume_queue_role_for_user(
                farmId=self.farm_id, queueId=self.queue_id
            ).get("credentials", None)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", None)
            if code == "ThrottlingException":
                raise DeadlineOperationError(
                    f"Throttled while attempting to assume Queue role for user on Queue '{self.queue_display_name_or_id}': {exc}\n"
                    "Please retry the operation later, or contact your administrator to increase the API's rate limit."
                ) from exc
            elif code == "InternalServerException":
                raise DeadlineOperationError(
                    f"An internal server error occurred while attempting to assume Queue role for user on "
                    f"Queue '{self.queue_display_name_or_id}': {exc}\n"
                ) from exc
            else:
                raise DeadlineOperationError(
                    f"Failed to assume Queue role for user on Queue '{self.queue_display_name_or_id}': {exc}\nPlease contact your "
                    "administrator to ensure a Queue role exists and that you have permissions to access this Queue."
                ) from exc
        if not queue_credentials:
            raise DeadlineOperationError(
                f"Failed to get credentials for '{self.queue_display_name_or_id}': Empty credentials received."
            )
        return {
            "access_key": queue_credentials["accessKeyId"],
            "secret_key": queue_credentials["secretAccessKey"],
            "token": queue_credentials["sessionToken"],
            "expiry_time": queue_credentials["expiration"].isoformat(),
        }
