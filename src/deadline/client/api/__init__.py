# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""The deadline.client.api module contains functions to complement usage of the
[boto3 deadline SDK](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/deadline.html).

These functions mostly match the interface style of boto3 APIs, using dictionary objects
and plain values instead of wrapping them in dataclasses. This approach helps keep a consistent
style for code calling AWS APIs with boto3 and also using these helpers.

The [create_job_from_job_bundle][deadline.client.api.create_job_from_job_bundle] function provides the
full capabilities of job bundle submission, and is the basis for the CLI commands `deadline bundle submit`
and `deadline bundle gui-submit`.

You can call [get_boto3_client][deadline.client.api.get_boto3_client]("deadline") to get a boto3 deadline client
based on the Deadline client configuration located in ~/.deadline/config that Deadline Cloud Monitor
login and the Deadline CLI use. You can use this boto3 deadline client directly,
and a few functions like [list_farms][deadline.client.api.list_farms] are provided here to adjust call arguments
depending on whether the credentials are from a Deadline Cloud Monitor login or a different credentials
provider.
"""

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
    "precache_clients",
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
    "record_success_fail_telemetry_event",
    "record_function_latency_telemetry_event",
    "assume_queue_role_for_user",
    "assume_queue_role_for_read",
    "wait_for_job_completion",
    "JobCompletionResult",
    "FailedTask",
    "get_session_logs",
    "SessionLogResult",
    "LogEvent",
]

# The following import is needed to prevent the following sporadic failure:
# botocore.exceptions.HTTPClientError: An HTTP Client raised an unhandled exception: unknown
# encoding: idna
import encodings.idna  # noqa # pylint: disable=unused-import
from configparser import ConfigParser
from logging import getLogger
from typing import Any, Dict, Optional


# Telemetry must be imported before Submit Job Bundle to avoid circular dependencies.
from ._telemetry import (
    get_telemetry_client,
    get_deadline_cloud_library_telemetry_client,
    TelemetryClient,
    record_success_fail_telemetry_event,
    record_function_latency_telemetry_event,
)
from ._loginout import login, logout
from ._session import (
    AwsAuthenticationStatus,
    AwsCredentialsSource,
    precache_clients,
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
from ._queue_credentials import (
    assume_queue_role_for_user,
    assume_queue_role_for_read,
)
from ._submit_job_bundle import (
    create_job_from_job_bundle,
    wait_for_create_job_to_complete,
)
from ._get_storage_profile_for_queue import get_storage_profile_for_queue
from ._job_monitoring import (
    wait_for_job_completion,
    JobCompletionResult,
    FailedTask,
    get_session_logs,
    SessionLogResult,
    LogEvent,
)

logger = getLogger(__name__)


def check_deadline_api_available(config: Optional[ConfigParser] = None) -> bool:
    """
    Returns True if [AWS Deadline Cloud APIs] are authorized in the session,
    False otherwise. This only checks the [deadline:ListFarms] API by performing
    one call that requests one result.

    [AWS Deadline Cloud APIs]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/Welcome.html
    [deadline:ListFarms]: https://docs.aws.amazon.com/deadline-cloud/latest/APIReference/API_ListFarms.html

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
