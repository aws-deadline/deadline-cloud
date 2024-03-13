# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from ._session import (
    get_boto3_client,
    get_user_and_identity_store_id,
)


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


def list_storage_profiles_for_queue(config=None, **kwargs):
    """
    Calls the deadline:ListStorageProfilesForQueue API call, applying the filter for user membership
    depending on the configuration. If the response is paginated, it repeated
    calls the API to get all the storage profiles.
    """
    deadline = get_boto3_client("deadline", config=config)

    return _call_paginated_deadline_list_api(
        deadline.list_storage_profiles_for_queue, "storageProfiles", **kwargs
    )
