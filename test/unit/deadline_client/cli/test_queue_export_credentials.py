# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the queue export-credentials command.
"""

import json
import datetime
from unittest import mock
import pytest
from click.testing import CliRunner
from botocore.exceptions import ClientError

from deadline.client.config import config_file
from deadline.client.cli._groups.queue_group import queue_export_credentials


@pytest.fixture
def mock_api():
    """Mock the API module."""
    with mock.patch("deadline.client.cli._groups.queue_group.api") as mock_api:
        # Mock the telemetry client
        mock_api._telemetry.get_deadline_cloud_library_telemetry_client.return_value = (
            mock.MagicMock()
        )
        yield mock_api


def test_queue_export_credentials_user_mode(mock_api, fresh_deadline_config):
    """Test exporting credentials in USER mode."""
    # Setup
    expiration = datetime.datetime(2025, 4, 8, 20, 0, 46)
    mock_api.assume_queue_role_for_user.return_value = {
        "credentials": {
            "accessKeyId": "ASIAEXAMPLE",
            "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "sessionToken": "AQoDYXdzEJr...<remainder of session token>",
            "expiration": expiration,
        }
    }

    # Execute
    runner = CliRunner()
    result = runner.invoke(
        queue_export_credentials,
        ["--farm-id", "f-abcdef12345", "--queue-id", "q-12345abcdef", "--mode", "USER"],
    )

    # Verify
    assert result.exit_code == 0
    output = json.loads(result.output)
    assert output == {
        "Version": 1,
        "AccessKeyId": "ASIAEXAMPLE",
        "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "SessionToken": "AQoDYXdzEJr...<remainder of session token>",
        "Expiration": expiration.isoformat(),
    }
    mock_api.assume_queue_role_for_user.assert_called_once_with(
        farmId="f-abcdef12345", queueId="q-12345abcdef", config=mock.ANY
    )

    # Verify telemetry
    telemetry_client = mock_api._telemetry.get_deadline_cloud_library_telemetry_client.return_value
    telemetry_client.record_event.assert_called_once()
    event_name, event_details = telemetry_client.record_event.call_args[0]
    assert event_name == "com.amazon.rum.deadline.queue_export_credentials"
    assert event_details["mode"] == "USER"
    assert event_details["queue_id"] == "q-12345abcdef"
    assert event_details["is_success"] is True
    assert event_details["error_type"] is None


def test_queue_export_credentials_read_mode(mock_api, fresh_deadline_config):
    """Test exporting credentials in READ mode."""
    # Setup
    expiration = datetime.datetime(2025, 4, 8, 20, 0, 46)
    mock_api.assume_queue_role_for_read.return_value = {
        "credentials": {
            "accessKeyId": "ASIAEXAMPLE",
            "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "sessionToken": "AQoDYXdzEJr...<remainder of session token>",
            "expiration": expiration,
        }
    }

    # Execute
    runner = CliRunner()
    result = runner.invoke(
        queue_export_credentials,
        ["--farm-id", "f-abcdef12345", "--queue-id", "q-12345abcdef", "--mode", "READ"],
    )

    # Verify
    assert result.exit_code == 0
    output = json.loads(result.output)
    assert output == {
        "Version": 1,
        "AccessKeyId": "ASIAEXAMPLE",
        "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "SessionToken": "AQoDYXdzEJr...<remainder of session token>",
        "Expiration": expiration.isoformat(),
    }

    mock_api.assume_queue_role_for_read.assert_called_once_with(
        farmId="f-abcdef12345", queueId="q-12345abcdef", config=mock.ANY
    )


def test_queue_export_credentials_default_queue_id(mock_api, fresh_deadline_config):
    """Test exporting credentials using default farm ID and queue ID from config."""
    # Setup
    expiration = datetime.datetime(2025, 4, 8, 20, 0, 46)
    mock_api.assume_queue_role_for_user.return_value = {
        "credentials": {
            "accessKeyId": "ASIAEXAMPLE",
            "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "sessionToken": "AQoDYXdzEJr...<remainder of session token>",
            "expiration": expiration,
        }
    }

    config_file.set_setting("defaults.farm_id", "f-default")
    config_file.set_setting("defaults.queue_id", "q-default")

    # Execute
    runner = CliRunner()
    result = runner.invoke(queue_export_credentials, [])

    # Verify
    assert result.exit_code == 0, result.output
    mock_api.assume_queue_role_for_user.assert_called_once_with(
        farmId="f-default", queueId="q-default", config=mock.ANY
    )


def test_queue_export_credentials_client_error(mock_api, fresh_deadline_config):
    """Test error handling when API call fails."""
    # Setup

    mock_api.assume_queue_role_for_user.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException", "Message": "Access denied"}},
        "AssumeQueueRoleForUser",
    )

    # Execute
    runner = CliRunner()
    result = runner.invoke(
        queue_export_credentials,
        ["--farm-id", "f-abcdef12345", "--queue-id", "q-12345abcdef"],
    )

    # Verify
    assert result.exit_code != 0
    assert "Insufficient permissions" in result.output

    # Verify telemetry
    telemetry_client = mock_api._telemetry.get_deadline_cloud_library_telemetry_client.return_value
    telemetry_client.record_event.assert_called_once()
    event_name, event_details = telemetry_client.record_event.call_args[0]
    assert event_name == "com.amazon.rum.deadline.queue_export_credentials"
    assert event_details["is_success"] is False
    assert event_details["error_type"] == "ClientError"


def test_queue_export_credentials_auth_error(mock_api, fresh_deadline_config):
    """Test error handling when authentication fails."""
    # Setup

    mock_api.assume_queue_role_for_user.side_effect = ClientError(
        {
            "Error": {
                "Code": "UnrecognizedClientException",
                "Message": "The security token included in the request is invalid",
            }
        },
        "AssumeQueueRoleForUser",
    )

    # Execute
    runner = CliRunner()
    result = runner.invoke(
        queue_export_credentials,
        ["--farm-id", "f-abcdef12345", "--queue-id", "q-12345abcdef"],
    )

    # Verify
    assert result.exit_code != 0
    assert "Authentication failed" in result.output
