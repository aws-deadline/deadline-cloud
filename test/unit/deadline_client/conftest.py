# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for Deadline Client Library tests.
"""

import botocore
from moto import mock_aws
import deadline.client.api
from deadline.client.api._telemetry import TelemetryClient
import tempfile
import os
from datetime import datetime

from unittest.mock import patch, MagicMock
import pytest

from .shared_constants import MOCK_QUEUE_ID


@pytest.fixture(scope="function")
def temp_job_bundle_dir():
    """
    Fixture to provide a temporary job bundle directory.
    """

    with tempfile.TemporaryDirectory() as job_bundle_dir:
        yield job_bundle_dir


@pytest.fixture(scope="function")
def temp_assets_dir():
    """
    Fixture to provide a temporary directory for asset files.
    """

    with tempfile.TemporaryDirectory() as assets_dir:
        yield assets_dir


@pytest.fixture(scope="function")
def temp_cwd():
    """
    Fixture to provide a temporary current working directory.
    """

    with tempfile.TemporaryDirectory() as cwd:
        # Change the current working directory to the temporary directory
        original_cwd = os.getcwd()
        try:
            os.chdir(cwd)
            yield cwd
        finally:
            # Restore the original current working directory
            os.chdir(original_cwd)


@pytest.fixture(scope="function")
def mock_telemetry():
    """
    Fixture to avoid calling telemetry code in unrelated unit tests.
    """

    with patch.object(TelemetryClient, "record_event") as mock_telemetry:
        yield mock_telemetry


@pytest.fixture
def deadline_mock():
    """
    Uses the moto library to create a mock boto3 session for all tests to use.
    Mocks Deadline Cloud via the approach moto recommends for services that it
    lacks an implementation for.

    Returns a MagicMock that handles all the deadline client operations.
    """
    os.environ["AWS_ACCESS_KEY_ID"] = "ACCESSKEY"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

    with mock_aws():
        deadline_magicmock = MagicMock()

        # See https://docs.getmoto.org/en/latest/docs/services/patching_other_services.html
        original_make_api_call = botocore.client.BaseClient._make_api_call

        def mock_make_api_call(self, operation_name, kwarg):
            service_name = self._service_model.service_name

            if service_name == "deadline":
                # Send the "GetQueue" operation, i.e. the get_queue call, to deadline_magicmock.GetQueue()
                return getattr(deadline_magicmock, operation_name)(**kwarg)

            # If we don't want to patch the API call
            return original_make_api_call(self, operation_name, kwarg)

        deadline_magicmock.GetQueue.return_value = {
            "queueId": MOCK_QUEUE_ID,
            "displayName": "Mock Queue",
            "jobAttachmentSettings": {
                "rootPrefix": "MockRootPrefix",
                "s3BucketName": "mock-s3-bucket",
            },
        }
        deadline_magicmock.ListSessions.return_value = {"sessions": []}
        deadline_magicmock.ListSessionActions.return_value = {"sessionActions": []}
        deadline_magicmock.AssumeQueueRoleForUser.return_value = {
            "credentials": {
                "accessKeyId": "ACCESSKEY",
                "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "sessionToken": "testing",
                "expiration": datetime.fromisoformat("2025-08-07T01:01:44+00:00"),
            }
        }

        with patch(
            "botocore.client.BaseClient._make_api_call", new=mock_make_api_call
        ), patch.object(deadline.client.api, "get_deadline_cloud_library_telemetry_client"):
            yield deadline_magicmock
