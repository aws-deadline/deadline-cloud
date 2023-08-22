# Copyright 2023 Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to storage profiles
"""

from unittest.mock import patch

import pytest

from deadline.client import api

STORAGE_PROFILES_LIST = [
    {
        "storageProfileId": "sp-0123456789abcdef0123456789abcdef",
        "osFamily": "windows",
        "displayName": "Testing storage profile",
    },
    {
        "storageProfileId": "sp-0123456789abcdef0123456789abcdeg",
        "osFamily": "macos",
        "displayName": "Another storage profile",
    },
    {
        "storageProfileId": "sp-0123756789abcdef0123456789abcdeg",
        "osFamily": "linux",
        "displayName": "Third storage profile",
    },
    {
        "storageProfileId": "sp-0123456789abcdef012a456789abcdeg",
        "osFamily": "linux",
        "displayName": "storage profile six",
    },
    {
        "storageProfileId": "sp-0123456789abcdef0123450789abcaeg",
        "osFamily": "macos",
        "displayName": "storage profile",
    },
]


def test_list_storage_profiles_for_queue_paginated(fresh_deadline_config):
    """Confirm api.list_storage_profiles_for_queue concatenates multiple pages"""
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_storage_profiles_for_queue.side_effect = [
            {"storage_profiles": STORAGE_PROFILES_LIST[:2], "nextToken": "abc"},
            {"storage_profiles": STORAGE_PROFILES_LIST[2:3], "nextToken": "def"},
            {"storage_profiles": STORAGE_PROFILES_LIST[3:]},
        ]

        # Call the API
        storage_profiles = api.list_storage_profiles_for_queue()

        assert storage_profiles["storage_profiles"] == STORAGE_PROFILES_LIST


@pytest.mark.parametrize("pass_principal_id_filter", [True, False])
@pytest.mark.parametrize("user_identities", [True, False])
def test_list_storage_profiles_for_queue_principal_id(
    fresh_deadline_config, pass_principal_id_filter, user_identities
):
    """Confirm api.list_storage_profiles_for_queue sets the principalId parameter appropriately"""

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_storage_profiles_for_queue.side_effect = [
            {"storage_profiles": STORAGE_PROFILES_LIST},
        ]
        if user_identities:
            session_mock()._session.get_scoped_config.return_value = {
                "studio_id": "studioid",
                "user_id": "userid",
                "identity_store_id": "idstoreid",
            }

        # Call the API
        if pass_principal_id_filter:
            storage_profiles = api.list_storage_profiles_for_queue(principalId="otheruserid")
        else:
            storage_profiles = api.list_storage_profiles_for_queue()

        assert storage_profiles["storage_profiles"] == STORAGE_PROFILES_LIST

        if pass_principal_id_filter:
            session_mock().client(
                "deadline"
            ).list_storage_profiles_for_queue.assert_called_once_with(principalId="otheruserid")
        elif user_identities:
            session_mock().client(
                "deadline"
            ).list_storage_profiles_for_queue.assert_called_once_with(principalId="userid")
        else:
            session_mock().client(
                "deadline"
            ).list_storage_profiles_for_queue.assert_called_once_with()
