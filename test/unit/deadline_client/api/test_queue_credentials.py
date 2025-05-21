# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for queue role credentials API methods.
"""

import datetime
from unittest import mock
import pytest

from deadline.client.api import _queue_credentials as queue_credentials


@pytest.fixture
def mock_boto3_client():
    """Mock the boto3 client."""
    with mock.patch("deadline.client.api._session.get_boto3_client") as mock_get_client:
        mock_client = mock.MagicMock()
        mock_get_client.return_value = mock_client
        yield mock_client


def test_assume_queue_role_for_user(mock_boto3_client):
    """Test assuming user role for a queue."""
    # Setup
    expiration = datetime.datetime(2025, 4, 8, 20, 0, 46)
    mock_boto3_client.assume_queue_role_for_user.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "SessionToken": "AQoDYXdzEJr...<remainder of session token>",
            "Expiration": expiration,
        }
    }

    # Execute
    result = queue_credentials.assume_queue_role_for_user(
        farmId="farm-1234567890abcdefg", queueId="q-12345abcdef"
    )

    # Verify
    mock_boto3_client.assume_queue_role_for_user.assert_called_once_with(
        farmId="farm-1234567890abcdefg", queueId="q-12345abcdef"
    )
    assert result["Credentials"]["AccessKeyId"] == "ASIAEXAMPLE"
    assert result["Credentials"]["SecretAccessKey"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    assert result["Credentials"]["SessionToken"] == "AQoDYXdzEJr...<remainder of session token>"
    assert result["Credentials"]["Expiration"] == expiration


def test_assume_queue_role_for_read(mock_boto3_client):
    """Test assuming read role for a queue."""
    # Setup
    expiration = datetime.datetime(2025, 4, 8, 20, 0, 46)
    mock_boto3_client.assume_queue_role_for_read.return_value = {
        "Credentials": {
            "AccessKeyId": "ASIAEXAMPLE",
            "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "SessionToken": "AQoDYXdzEJr...<remainder of session token>",
            "Expiration": expiration,
        }
    }

    # Execute
    result = queue_credentials.assume_queue_role_for_read(
        farmId="farm-1234567890abcdefg", queueId="q-12345abcdef"
    )

    # Verify
    mock_boto3_client.assume_queue_role_for_read.assert_called_once_with(
        farmId="farm-1234567890abcdefg", queueId="q-12345abcdef"
    )
    assert result["Credentials"]["AccessKeyId"] == "ASIAEXAMPLE"
    assert result["Credentials"]["SecretAccessKey"] == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    assert result["Credentials"]["SessionToken"] == "AQoDYXdzEJr...<remainder of session token>"
    assert result["Credentials"]["Expiration"] == expiration


def test_assume_queue_role_client_error(mock_boto3_client):
    """Test error handling when assuming role fails."""
    # Setup
    from botocore.exceptions import ClientError

    mock_boto3_client.assume_queue_role_for_user.side_effect = ClientError(
        {"Error": {"Code": "AccessDenied", "Message": "Access denied"}},
        "AssumeQueueRoleForUser",
    )

    # Execute and verify
    with pytest.raises(ClientError) as excinfo:
        queue_credentials.assume_queue_role_for_user(
            farmId="farm-1234567890abcdefg", queueId="q-12345abcdef"
        )

    assert "AccessDenied" in str(excinfo.value)
