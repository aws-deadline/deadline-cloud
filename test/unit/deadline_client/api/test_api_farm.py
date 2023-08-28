# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to Farms
"""

from unittest.mock import patch

import pytest

from deadline.client import api

FARMS_LIST = [
    {
        "farmId": "farm-0123456789abcdef0123456789abcdef",
        "name": "Testing Farm",
        "description": "",
    },
    {
        "farmId": "farm-0123456789abcdef0123456789abcdeg",
        "name": "Another Farm",
        "description": "With a description!",
    },
    {
        "farmId": "farm-0123756789abcdef0123456789abcdeg",
        "name": "Third Farm",
        "description": "Described",
    },
    {
        "farmId": "farm-0123456789abcdef012a456789abcdeg",
        "name": "Farm six",
        "description": "multiple\nline\ndescription",
    },
    {
        "farmId": "farm-0123456789abcdef0123450789abcaeg",
        "name": "Farm",
        "description": "Farm",
    },
]


def test_list_farms_paginated(fresh_deadline_config):
    """Confirm api.list_farms concatenates multiple pages"""
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.side_effect = [
            {"farms": FARMS_LIST[:2], "nextToken": "abc"},
            {"farms": FARMS_LIST[2:3], "nextToken": "def"},
            {"farms": FARMS_LIST[3:]},
        ]

        # Call the API
        farms = api.list_farms()

        assert farms["farms"] == FARMS_LIST


@pytest.mark.parametrize("pass_principal_id_filter", [True, False])
@pytest.mark.parametrize("user_identities", [True, False])
def test_list_farms_principal_id(fresh_deadline_config, pass_principal_id_filter, user_identities):
    """Confirm api.list_farms sets the principalId parameter appropriately"""

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.side_effect = [
            {"farms": FARMS_LIST},
        ]

        session_mock()._session.get_scoped_config().get.return_value = "some-studio-id"
        if user_identities:
            session_mock()._session.get_scoped_config.return_value = {
                "studio_id": "some-studio-id",
                "user_id": "userid",
                "identity_store_id": "idstoreid",
            }

        # Call the API
        if pass_principal_id_filter:
            farms = api.list_farms(principalId="otheruserid")
        else:
            farms = api.list_farms()

        assert farms["farms"] == FARMS_LIST

        if pass_principal_id_filter:
            session_mock().client("deadline").list_farms.assert_called_once_with(
                principalId="otheruserid", studioId="some-studio-id"
            )
        elif user_identities:
            session_mock().client("deadline").list_farms.assert_called_once_with(
                principalId="userid", studioId="some-studio-id"
            )
        else:
            session_mock().client("deadline").list_farms.assert_called_once_with(
                studioId="some-studio-id"
            )
