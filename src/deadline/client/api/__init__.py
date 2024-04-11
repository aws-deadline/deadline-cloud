# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = [
    "login",
    "logout",
    "create_job_from_job_bundle",
    "wait_for_create_job_to_complete",
    "get_boto3_session",
    "get_boto3_client",
    "AwsAuthenticationStatus",
    "AwsCredentialsSource",
    "TelemetryClient",
    "check_authentication_status",
    "check_deadline_api_available",
    "get_credentials_source",
    "list_farms",
    "list_queues",
    "list_jobs",
    "list_fleets",
    "list_storage_profiles_for_queue",
    "get_queue_user_boto3_session",
    "get_queue_parameter_definitions",
    "get_telemetry_client",
    "get_deadline_cloud_library_telemetry_client",
    "get_storage_profile_for_queue",
]

# The following import is needed to prevent the following sporadic failure:
# botocore.exceptions.HTTPClientError: An HTTP Client raised an unhandled exception: unknown
# encoding: idna
import encodings.idna  # noqa # pylint: disable=unused-import
from configparser import ConfigParser
from logging import getLogger
from typing import Any, Dict, Optional

from ._loginout import login, logout
from ._session import (
    AwsAuthenticationStatus,
    AwsCredentialsSource,
    get_queue_user_boto3_session,
    check_authentication_status,
    get_boto3_client,
    get_boto3_session,
    get_credentials_source,
    get_user_and_identity_store_id,
)
from ._list_apis import (
    list_farms,
    list_queues,
    list_jobs,
    list_fleets,
    list_storage_profiles_for_queue,
)
from ._queue_parameters import get_queue_parameter_definitions
from ._submit_job_bundle import create_job_from_job_bundle, wait_for_create_job_to_complete
from ._telemetry import (
    get_telemetry_client,
    get_deadline_cloud_library_telemetry_client,
    TelemetryClient,
)
from ._get_storage_profile_for_queue import get_storage_profile_for_queue

logger = getLogger(__name__)


def check_deadline_api_available(config: Optional[ConfigParser] = None) -> bool:
    """
    Returns True if AWS Deadline Cloud APIs are authorized in the session,
    False otherwise. This only checks the deadline:ListFarms API by performing
    one call with just one result.

    Args:
        config (ConfigParser, optional): The AWS Deadline Cloud configuration
                object to use instead of the config file.
    """
    import logging

    from ._session import _modified_logging_level

    with _modified_logging_level(logging.getLogger("botocore.credentials"), logging.ERROR):
        try:
            list_farm_params: Dict[str, Any] = {"maxResults": 1}
            user_id, _ = get_user_and_identity_store_id(config=config)
            if user_id:
                list_farm_params["principalId"] = str(user_id)

            deadline = get_boto3_client("deadline", config=config)
            deadline.list_farms(**list_farm_params)
            return True
        except Exception:
            logger.exception("Error invoking ListFarms")
            return False
