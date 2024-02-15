# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to jobs
"""

from unittest.mock import patch

import pytest

from deadline.client import api

JOBS_LIST = [
    {
        "jobId": "job-0123456789abcdef0123456789abcdef",
        "name": "Testing job",
    },
    {
        "jobId": "job-0123456789abcdef0123456789abcdeg",
        "name": "Another job",
    },
    {
        "jobId": "job-0123756789abcdef0123456789abcdeg",
        "name": "Third job",
    },
    {
        "jobId": "job-0123456789abcdef012a456789abcdeg",
        "name": "job six",
    },
    {
        "jobId": "job-0123456789abcdef0123450789abcaeg",
        "name": "Job",
    },
]


def test_list_jobs_paginated(fresh_deadline_config):
    """Confirm api.list_jobs concatenates multiple pages"""
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_jobs.side_effect = [
            {"jobs": JOBS_LIST[:2], "nextToken": "abc"},
            {"jobs": JOBS_LIST[2:3], "nextToken": "def"},
            {"jobs": JOBS_LIST[3:]},
        ]

        # Call the API
        jobs = api.list_jobs()

        assert jobs["jobs"] == JOBS_LIST


@pytest.mark.parametrize("pass_principal_id_filter", [True, False])
@pytest.mark.parametrize("user_identities", [True, False])
def test_list_jobs_principal_id(fresh_deadline_config, pass_principal_id_filter, user_identities):
    """Confirm api.list_jobs sets the principalId parameter appropriately"""

    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_jobs.side_effect = [
            {"jobs": JOBS_LIST},
        ]
        if user_identities:
            session_mock()._session.get_scoped_config.return_value = {
                "monitor_id": "monitor-amonitorid",
                "user_id": "userid",
                "identity_store_id": "idstoreid",
            }

        # Call the API
        if pass_principal_id_filter:
            jobs = api.list_jobs(principalId="otheruserid")
        else:
            jobs = api.list_jobs()

        assert jobs["jobs"] == JOBS_LIST

        if pass_principal_id_filter:
            session_mock().client("deadline").list_jobs.assert_called_once_with(
                principalId="otheruserid"
            )
        elif user_identities:
            session_mock().client("deadline").list_jobs.assert_called_once_with(
                principalId="userid"
            )
        else:
            session_mock().client("deadline").list_jobs.assert_called_once_with()
