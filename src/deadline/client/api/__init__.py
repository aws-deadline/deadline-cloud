# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

__all__ = [
    "login",
    "logout",
    "create_job_from_job_bundle",
    "wait_for_create_job_to_complete",
    "get_boto3_session",
    "get_boto3_client",
    "AwsCredentialsStatus",
    "AwsCredentialsType",
    "TelemetryClient",
    "check_credentials_status",
    "check_deadline_api_available",
    "get_credentials_type",
    "list_farms",
    "list_queues",
    "list_jobs",
    "list_fleets",
    "list_storage_profiles_for_queue",
    "get_queue_user_boto3_session",
    "get_queue_parameter_definitions",
    "get_telemetry_client",
    "get_deadline_cloud_library_telemetry_client",
]

from configparser import ConfigParser
from logging import getLogger
from typing import Optional

from ._loginout import login, logout
from ._session import (
    AwsCredentialsStatus,
    AwsCredentialsType,
    get_queue_user_boto3_session,
    check_credentials_status,
    get_boto3_client,
    get_boto3_session,
    get_credentials_type,
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

logger = getLogger(__name__)


def check_deadline_api_available(config: Optional[ConfigParser] = None) -> bool:
    """
    Returns True if Amazon Deadline Cloud APIs are authorized in the session,
    False otherwise. This only checks the deadline:ListFarms API,
    by performing a dry-run call.

    Args:
        config (ConfigParser, optional): The Amazon Deadline Cloud configuration
                object to use instead of the config file.
    """
    import logging

    from ._session import _modified_logging_level

    with _modified_logging_level(logging.getLogger("botocore.credentials"), logging.ERROR):
        try:
            list_farms(config=config, maxResults=1)
            return True
        except Exception:
            logger.exception("Error invoking ListFarms")
            return False
