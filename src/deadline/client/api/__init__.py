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
    "check_credentials_status",
    "check_deadline_api_available",
    "get_credentials_type",
    "list_farms",
    "list_queues",
    "list_jobs",
    "list_fleets",
    "get_queue_boto3_session",
]

from configparser import ConfigParser
from logging import getLogger
from typing import Optional

from ._loginout import login, logout
from ._session import (
    AwsCredentialsStatus,
    AwsCredentialsType,
    get_queue_boto3_session,
    check_credentials_status,
    get_boto3_client,
    get_boto3_session,
    get_credentials_type,
    get_user_and_identity_store_id,
    get_studio_id,
)
from ._submit_job_bundle import create_job_from_job_bundle, wait_for_create_job_to_complete

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
            list_farms(config=config, dryRun=True)
            return True
        except Exception:
            logger.exception("Error invoking ListFarms")
            return False


def _call_paginated_deadline_list_api(list_api, list_property_name, **kwargs):
    """
    Calls a deadline:List* API repeatedly to concatenate all pages.

    Example:
        deadline = get_boto3_client("deadline")
        return _call_paginated_deadline_list_api(deadline.list_farms, "farms", **kwargs)

    Args:
      list_api (callable): The List* API function to call, from the boto3 client.
      list_property_name (str): The name of the property in the response that contains
                                the list.
    """
    response = list_api(**kwargs)
    if kwargs.get("dryRun", False):
        return response
    else:
        result = {list_property_name: response[list_property_name]}

        while "nextToken" in response:
            response = list_api(nextToken=response["nextToken"], **kwargs)
            result[list_property_name].extend(response[list_property_name])

        return result


def list_farms(config=None, **kwargs):
    """
    Calls the deadline:ListFarms API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the farms.
    """
    if "principalId" not in kwargs:
        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            kwargs["principalId"] = user_id

    if "studioId" not in kwargs:
        studio_id = get_studio_id(config=config)
        if studio_id:
            kwargs["studioId"] = studio_id

    deadline = get_boto3_client("deadline", config=config)
    return _call_paginated_deadline_list_api(deadline.list_farms, "farms", **kwargs)


def list_queues(config=None, **kwargs):
    """
    Calls the deadline:ListQueues API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the queues.
    """
    if "principalId" not in kwargs:
        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            kwargs["principalId"] = user_id

    deadline = get_boto3_client("deadline", config=config)
    return _call_paginated_deadline_list_api(deadline.list_queues, "queues", **kwargs)


def list_jobs(config=None, **kwargs):
    """
    Calls the deadline:ListJobs API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the jobs.
    """
    if "principalId" not in kwargs:
        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            kwargs["principalId"] = user_id

    deadline = get_boto3_client("deadline", config=config)
    return _call_paginated_deadline_list_api(deadline.list_jobs, "jobs", **kwargs)


def list_fleets(config=None, **kwargs):
    """
    Calls the deadline:ListFleets API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the fleets.
    """
    if "principalId" not in kwargs:
        user_id, _ = get_user_and_identity_store_id(config=config)
        if user_id:
            kwargs["principalId"] = user_id

    deadline = get_boto3_client("deadline", config=config)
    return _call_paginated_deadline_list_api(deadline.list_fleets, "fleets", **kwargs)
