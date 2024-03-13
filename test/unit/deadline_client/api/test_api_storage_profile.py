# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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
            {"storageProfiles": STORAGE_PROFILES_LIST[:2], "nextToken": "abc"},
            {"storageProfiles": STORAGE_PROFILES_LIST[2:3], "nextToken": "def"},
            {"storageProfiles": STORAGE_PROFILES_LIST[3:]},
        ]

        # Call the API
        storage_profiles = api.list_storage_profiles_for_queue()

        assert storage_profiles["storageProfiles"] == STORAGE_PROFILES_LIST


@pytest.mark.parametrize("user_identities", [True, False])
def test_list_storage_profiles_for_queue(fresh_deadline_config, user_identities):
    """Confirm api.list_storage_profiles_for_queue sets the principalId parameter appropriately"""

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_storage_profiles_for_queue.side_effect = [
            {"storageProfiles": STORAGE_PROFILES_LIST},
        ]
        if user_identities:
            session_mock()._session.get_scoped_config.return_value = {
                "monitor_id": "monitorid",
                "user_id": "userid",
                "identity_store_id": "idstoreid",
            }

        storage_profiles = api.list_storage_profiles_for_queue()

        assert storage_profiles["storageProfiles"] == STORAGE_PROFILES_LIST

        session_mock().client("deadline").list_storage_profiles_for_queue.assert_called_once_with()
