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
from typing import Optional, Any
import boto3  # type: ignore[import]
from botocore.client import BaseClient  # type: ignore[import]
from botocore.credentials import CredentialProvider, RefreshableCredentials
from botocore.exceptions import (  # type: ignore[import]
    ClientError,
    ProfileNotFound,
)
from botocore.loaders import Loader, UnknownServiceError  # type: ignore[import]
from botocore.model import ServiceModel, OperationModel

from botocore.session import get_session as get_botocore_session

from ..config import get_setting
from ..exceptions import DeadlineOperationError

__cached_boto3_session = None
__cached_boto3_session_profile_name = None
__cached_boto3_queue_session = None
__cached_farm_id_for_queue_session = None
__cached_queue_id_for_queue_session = None


class AwsCredentialsType(Enum):
    NOT_VALID = 0
    HOST_PROVIDED = 2
    DEADLINE_CLOUD_MONITOR_LOGIN = 3


class AwsCredentialsStatus(Enum):
    CONFIGURATION_ERROR = 1
    AUTHENTICATED = 2
    NEEDS_LOGIN = 3


def get_boto3_session(
    force_refresh: bool = False, config: Optional[ConfigParser] = None
) -> boto3.Session:
    """
    Gets a boto3 session for the configured Amazon Deadline Cloud aws profile. This may
    either use a named profile or the default credentials provider chain.

    This implementation caches the session object for use across the CLI code,
    so that we can use the following code pattern without repeated calls to
    an external credentials provider process, for example.

    Args:
        force_refresh (bool, optional): If set to True, forces a cache refresh.
        config (ConfigParser, optional): If provided, the Amazon Deadline Cloud config to use.
    """
    global __cached_boto3_session
    global __cached_boto3_session_profile_name

    profile_name: Optional[str] = get_setting("defaults.aws_profile_name", config)

    # If the default AWS profile name is either not set, or set to "default",
    # use the default credentials provider chain instead of a named profile.
    if profile_name in ("", "default"):
        profile_name = None

    # If a config was provided, don't use the Session caching mechanism.
    if config:
        return boto3.Session(profile_name=profile_name)

    # If this is the first call or the profile name has changed, make a fresh Session
    if (
        force_refresh
        or not __cached_boto3_session
        or __cached_boto3_session_profile_name != profile_name
    ):
        __cached_boto3_session = boto3.Session(profile_name=profile_name)
        __cached_boto3_session_profile_name = profile_name

    return __cached_boto3_session


def invalidate_boto3_session_cache() -> None:
    """
    Invalidates the cached boto3 session.
    """
    global __cached_boto3_session
    global __cached_boto3_session_profile_name

    __cached_boto3_session = None
    __cached_boto3_session_profile_name = None


def get_boto3_client(service_name: str, config: Optional[ConfigParser] = None) -> BaseClient:
    """
    Gets a client from the boto3 session returned by `get_boto3_session`.
    If the client requested is `deadline`, it uses the configured
    deadline endpoint url.

    Args:
        service_name (str): The AWS service to get the client for, e.g. "deadline".
        config (ConfigParser, optional): If provided, the Amazon Deadline Cloud config to use.
    """
    session = get_boto3_session(config=config)

    if service_name == "deadline":
        deadline_endpoint_url = get_setting("settings.deadline_endpoint_url", config=config)
        client = session.client(service_name, endpoint_url=deadline_endpoint_url)
        return DeadlineClient(client)
    else:
        return session.client(service_name)


def get_credentials_type(config: Optional[ConfigParser] = None) -> AwsCredentialsType:
    """
    Returns DEADLINE_CLOUD_MONITOR_LOGIN if Deadline Cloud Monitor wrote the credentials, HOST_PROVIDED otherwise.

    Args:
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.
    """
    try:
        session = get_boto3_session(config=config)
        profile_config = session._session.get_scoped_config()
    except ProfileNotFound:
        return AwsCredentialsType.NOT_VALID
    if "studio_id" in profile_config:
        # CTDX adds some Nimble-specific keys here which we can use to know that this came from CTDX
        return AwsCredentialsType.DEADLINE_CLOUD_MONITOR_LOGIN
    else:
        return AwsCredentialsType.HOST_PROVIDED


def get_user_and_identity_store_id(
    config: Optional[ConfigParser] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    If logged in with Nimble Studio Deadline Cloud Monitor, returns a tuple
    (user_id, identity_store_id), otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    if "studio_id" in profile_config:
        return (profile_config["user_id"], profile_config["identity_store_id"])
    else:
        return None, None


def get_studio_id(
    config: Optional[ConfigParser] = None,
) -> Optional[str]:
    """
    If logged in with Nimble Studio Deadline Cloud Monitor, returns Studio Id, otherwise returns None.
    """
    session = get_boto3_session(config=config)
    profile_config = session._session.get_scoped_config()

    return profile_config.get("studio_id", None)


def get_queue_boto3_session(
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
        config (ConfigParser, optional): If provided, the Amazon Deadline Cloud config to use.
        farm_id (str, optional): The ID of the farm to use.
        queue_id (str, optional): The ID of the queue to use.
        queue_display_name (str, optional): The display name of the queue.
        force_refresh (bool, optional): If True, forces a cache refresh.
    """

    global __cached_boto3_queue_session
    global __cached_farm_id_for_queue_session
    global __cached_queue_id_for_queue_session

    base_session = get_boto3_session(config=config)

    if farm_id is None:
        farm_id = get_setting("defaults.farm_id")
    if queue_id is None:
        queue_id = get_setting("defaults.queue_id")

    # If a config was provided, don't use the Session caching mechanism.
    if config:
        return _get_queue_boto3_session(
            deadline, base_session, farm_id, queue_id, queue_display_name
        )

    # If this is the first call or the farm ID/queue ID has changed, make a fresh Session and cache it
    if (
        force_refresh
        or not __cached_boto3_queue_session
        or __cached_farm_id_for_queue_session != farm_id
        or __cached_queue_id_for_queue_session != queue_id
    ):
        __cached_boto3_queue_session = _get_queue_boto3_session(
            deadline, base_session, farm_id, queue_id, queue_display_name
        )

        __cached_farm_id_for_queue_session = farm_id
        __cached_queue_id_for_queue_session = queue_id

    return __cached_boto3_queue_session


def _get_queue_boto3_session(
    deadline: BaseClient,
    base_session: boto3.Session,
    farm_id: str,
    queue_id: str,
    queue_display_name: Optional[str] = None,
):
    queue_credential_provider = QueueCredentialProvider(
        deadline,
        farm_id,
        queue_id,
        queue_display_name,
    )

    botocore_session = get_botocore_session()
    credential_provider = botocore_session.get_component("credential_provider")
    credential_provider.insert_before("env", queue_credential_provider)

    return boto3.Session(
        botocore_session=botocore_session,
        profile_name=base_session.profile_name,
        region_name=base_session.region_name,
    )


def invalidate_boto3_queue_session_cache() -> None:
    """
    Invalidates the cached boto3 queue session.
    """
    global __cached_boto3_queue_session
    global __cached_farm_id_for_queue_session
    global __cached_queue_id_for_queue_session

    __cached_boto3_queue_session = None
    __cached_farm_id_for_queue_session = None
    __cached_queue_id_for_queue_session = None


@contextmanager
def _modified_logging_level(logger, level):
    old_level = logger.getEffectiveLevel()
    logger.setLevel(level)
    try:
        yield
    finally:
        logger.setLevel(old_level)


def check_credentials_status(config: Optional[ConfigParser] = None) -> AwsCredentialsStatus:
    """
    Checks the status of the provided session, by
    calling the sts::GetCallerIdentity API.

    Args:
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.

    Returns AwsCredentialsStatus enum value:
      - CONFIGURATION_ERROR if there is an unexpected error accessing credentials
      - AUTHENTICATED if they are fine
      - NEEDS_LOGIN if a Deadline Cloud Monitor login is required.
    """

    with _modified_logging_level(logging.getLogger("botocore.credentials"), logging.ERROR):
        try:
            get_boto3_session(config=config).client("sts").get_caller_identity()
            return AwsCredentialsStatus.AUTHENTICATED
        except Exception:
            # We assume that the presence of a Deadline Cloud Monitor profile
            # means we will know everything necessary to start it and login.

            if get_credentials_type(config) == AwsCredentialsType.DEADLINE_CLOUD_MONITOR_LOGIN:
                return AwsCredentialsStatus.NEEDS_LOGIN
            return AwsCredentialsStatus.CONFIGURATION_ERROR


class DeadlineClient:
    """
    A shim layer for boto Deadline client. This class will check if a method exists on the real
    boto3 Deadline client and call it if it exists. If it doesn't exist, an AttributeError will be raised.
    """

    _real_client: Any

    def __init__(self, real_client: Any) -> None:
        self._real_client = real_client

    def get_farm(self, *args, **kwargs) -> Any:
        response = self._real_client.get_farm(*args, **kwargs)
        if "name" in response and "displayName" not in response:
            response["displayName"] = response["name"]
            del response["name"]
        return response

    def list_farms(self, *args, **kwargs) -> Any:
        response = self._real_client.list_farms(*args, **kwargs)
        if "farms" in response:
            for farm in response["farms"]:
                if "name" in farm and "displayName" not in farm:
                    farm["displayName"] = farm["name"]
                    del farm["name"]
        return response

    def get_queue(self, *args, **kwargs) -> Any:
        response = self._real_client.get_queue(*args, **kwargs)
        if "name" in response and "displayName" not in response:
            response["displayName"] = response["name"]
            del response["name"]
        if "state" in response and "status" not in response:
            response["status"] = response["state"]
            del response["state"]
        return response

    def list_queues(self, *args, **kwards) -> Any:
        response = self._real_client.list_queues(*args, **kwards)
        if "queues" in response:
            for queue in response["queues"]:
                if "name" in queue and "displayName" not in queue:
                    queue["displayName"] = queue["name"]
                    del queue["name"]
        return response

    def get_fleet(self, *args, **kwargs) -> Any:
        response = self._real_client.get_fleet(*args, **kwargs)
        if "name" in response and "displayName" not in response:
            response["displayName"] = response["name"]
            del response["name"]
        if "state" in response and "status" not in response:
            response["status"] = response["state"]
            del response["state"]
        if "type" in response:
            del response["type"]
        return response

    def list_fleets(self, *args, **kwargs) -> Any:
        response = self._real_client.list_fleets(*args, **kwargs)
        if "fleets" in response:
            for fleet in response["fleets"]:
                if "name" in fleet and "displayName" not in fleet:
                    fleet["displayName"] = fleet["name"]
                    del fleet["name"]
        return response

    def create_job(self, *args, **kwargs) -> Any:
        create_job_input_members = self._get_deadline_api_input_shape("CreateJob")

        # revert to old parameter names if old service model is used
        if "maxRetriesPerTask" in kwargs:
            if "maxErrorsPerTask" in create_job_input_members:
                kwargs["maxErrorsPerTask"] = kwargs.pop("maxRetriesPerTask")
        if "template" in kwargs:
            if "jobTemplate" in create_job_input_members:
                kwargs["jobTemplate"] = kwargs.pop("template")
                kwargs["jobTemplateType"] = kwargs.pop("templateType")
                if "parameters" in kwargs:
                    kwargs["jobParameters"] = kwargs.pop("parameters")
        if "targetTaskRunStatus" in kwargs:
            if "initialState" in create_job_input_members:
                kwargs["initialState"] = kwargs.pop("targetTaskRunStatus")
        if "priority" not in kwargs:
            kwargs["priority"] = 50
        return self._real_client.create_job(*args, **kwargs)

    def assume_queue_role_for_user(self, *args, **kwargs) -> Any:
        return self._real_client.assume_queue_role_for_user(*args, **kwargs)

    def _get_deadline_api_input_shape(self, api_name: str) -> dict[str, Any]:
        """
        Given a string name of an API e.g. CreateJob, returns the shape of the
        inputs to that API.
        """
        api_model = self._get_deadline_api_model(api_name)
        if api_model:
            return api_model.input_shape.members
        return {}

    def _get_deadline_api_model(self, api_name: str) -> Optional[OperationModel]:
        """
        Given a string name of an API e.g. CreateJob, returns the OperationModel
        for that API from the service model.
        """
        loader = Loader()
        try:
            deadline_service_description = loader.load_service_model("deadline", "service-2")
        except UnknownServiceError:
            return None
        deadline_service_model = ServiceModel(deadline_service_description, service_name="deadline")
        return OperationModel(
            deadline_service_description["operations"][api_name], deadline_service_model
        )

    def __getattr__(self, __name: str) -> Any:
        """
        Respond to unknown method calls by calling the underlying _real_client
        If the underlying _real_client does not have a given method, an AttributeError
        will be raised.

        Note that __getattr__ is only called if the attribute cannot otherwise be found,
        so if this class alread has the called method defined, __getattr__ will not be called.
        This is in opposition to __getattribute__ which is called by default.
        """

        def method(*args, **kwargs) -> Any:
            return getattr(self._real_client, __name)(*args, **kwargs)

        return method


class QueueCredentialProvider(CredentialProvider):
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

    deadline: DeadlineClient
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
