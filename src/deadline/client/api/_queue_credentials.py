# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
API methods for queue role credentials.
"""

from configparser import ConfigParser
from typing import Dict, Any, Optional

from . import _session, _telemetry


@_telemetry.record_function_latency_telemetry_event()
def assume_queue_role_for_user(
    farmId: str, queueId: str, *, config: Optional[ConfigParser] = None
) -> Dict[str, Any]:
    """
    Assumes the user role for a queue and returns temporary credentials.

    These credentials can be used to perform user-level operations on the queue,
    such as submitting jobs and monitoring job status.

    Args:
        farmId: The ID of the farm containing the queue.
        queueId: The ID of the queue to assume the role for.
        config: Optional configuration to use. If not provided, the default configuration is used.

    Returns:
        A dictionary containing the temporary credentials in the following format:
        {
            "credentials": {
                "accessKeyId": str,
                "secretAccessKey": str,
                "sessionToken": str,
                "expiration": datetime
            }
        }

    Raises:
        ClientError: If there is an error assuming the role.
    """
    client = _session.get_boto3_client("deadline", config=config)
    response = client.assume_queue_role_for_user(farmId=farmId, queueId=queueId)
    return response


@_telemetry.record_function_latency_telemetry_event()
def assume_queue_role_for_read(
    farmId: str, queueId: str, *, config: Optional[ConfigParser] = None
) -> Dict[str, Any]:
    """
    Assumes the read role for a queue and returns temporary credentials.

    These credentials can be used to perform read-only operations on the queue,
    such as viewing job status and queue information.

    Args:
        farmId: The ID of the farm containing the queue.
        queueId: The ID of the queue to assume the role for.
        config: Optional configuration to use. If not provided, the default configuration is used.

    Returns:
        A dictionary containing the temporary credentials in the following format:
        {
            "credentials": {
                "accessKeyId": str,
                "secretAccessKey": str,
                "sessionToken": str,
                "expiration": datetime
            }
        }

    Raises:
        ClientError: If there is an error assuming the role.
    """
    client = _session.get_boto3_client("deadline", config=config)
    response = client.assume_queue_role_for_read(farmId=farmId, queueId=queueId)
    return response
