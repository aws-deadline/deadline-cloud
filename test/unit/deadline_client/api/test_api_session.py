# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
tests the deadline.client.api functions relating to boto3.Client
"""

from typing import Optional
from unittest.mock import call, patch, MagicMock, ANY

import boto3  # type: ignore[import]
from deadline.client import api, config


def test_get_boto3_session(fresh_deadline_config):
    """Confirm that api.get_boto3_session gets a session for the configured profile"""
    config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")

    mock_session = MagicMock()
    with patch.object(boto3, "Session", return_value=mock_session) as boto3_session:
        # Testing this function
        result = api.get_boto3_session()

        # Confirm it returned the mocked value, and was called with the correct args
        assert result == mock_session
        boto3_session.assert_called_once_with(profile_name="SomeRandomProfileName")


def test_get_boto3_session_caching_behavior(fresh_deadline_config):
    """
    Confirm that api.get_boto3_session caches the session, and refreshes if
    the configured profile name changes
    """

    # mock boto3.Session to return a fresh object based on the input profile name
    def mock_create_session(profile_name: Optional[str]):
        session = MagicMock()
        session._profile_name = profile_name
        return session

    with patch.object(boto3, "Session", side_effect=mock_create_session) as boto3_session:
        # This is a session with the default profile name
        session0 = api.get_boto3_session()

        assert session0._profile_name is None

        # This should return the cached object, and not call boto3.Session
        session1 = api.get_boto3_session()

        assert session1 is session0

        # Configuring a new session name should result in a new Session object
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session2 = api.get_boto3_session()

        assert session2 is not session0
        assert session2._profile_name == "SomeRandomProfileName"

        # This should return the cached object, and not call boto3.Session
        session3 = api.get_boto3_session()

        assert session3 is session2

        # boto3.Session should have been called exactly twice, once for each
        # value of AWS profile name that was configured.
        boto3_session.assert_has_calls(
            [
                call(profile_name=None),
                call(profile_name="SomeRandomProfileName"),
            ]
        )


def test_get_check_authentication_status_authenticated(fresh_deadline_config):
    """Confirm that check_authentication_status returns AUTHENTICATED"""
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session_mock().client("sts").get_caller_identity.return_value = {}

        assert api.check_authentication_status() == api.AwsAuthenticationStatus.AUTHENTICATED


def test_get_check_authentication_status_configuration_error(fresh_deadline_config):
    """Confirm that check_authentication_status returns CONFIGURATION_ERROR"""
    with patch.object(api._session, "get_boto3_session") as session_mock, patch.object(
        api, "get_boto3_session", new=session_mock
    ):
        config.set_setting("defaults.aws_profile_name", "SomeRandomProfileName")
        session_mock().client("sts").get_caller_identity.side_effect = Exception(
            "some uncaught exception"
        )

        assert api.check_authentication_status() == api.AwsAuthenticationStatus.CONFIGURATION_ERROR


def test_get_queue_user_boto3_session_no_profile(fresh_deadline_config):
    """Make sure that boto3.Session gets called with profile_name=None for the default profile."""
    session_mock = MagicMock()
    # The value returned when no profile was selected is "default"
    session_mock.profile_name = "default"
    session_mock.region_name = "us-west-2"
    deadline_mock = MagicMock()
    mock_botocore_session = MagicMock()
    mock_botocore_session.get_config_variable = lambda name: (
        "default" if name == "profile" else None
    )

    with patch.object(api._session, "get_boto3_session", return_value=session_mock), patch(
        "botocore.session.Session", return_value=mock_botocore_session
    ), patch("boto3.Session") as boto3_session_mock:
        api.get_queue_user_boto3_session(
            deadline_mock, farm_id="farm-1234", queue_id="queue-1234", queue_display_name="queue"
        )
        boto3_session_mock.assert_called_once_with(
            botocore_session=ANY, profile_name=None, region_name="us-west-2"
        )


def test_check_deadline_api_available(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.return_value = {"farms": []}

        # Call the function under test
        result = api.check_deadline_api_available()

        assert result is True
        # It should have called list_farms to check the API
        session_mock().client("deadline").list_farms.assert_called_once_with(maxResults=1)


def test_check_deadline_api_available_fails(fresh_deadline_config):
    with patch.object(api._session, "get_boto3_session") as session_mock:
        session_mock().client("deadline").list_farms.side_effect = Exception()

        # Call the function under test
        result = api.check_deadline_api_available()

        assert result is False
        # It should have called list_farms with to check the API
        session_mock().client("deadline").list_farms.assert_called_once_with(maxResults=1)
