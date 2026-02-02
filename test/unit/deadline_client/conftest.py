# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for Deadline Client Library tests.
"""

import botocore
import boto3
from moto import mock_aws
import deadline.client.api
from deadline.client.api import _submit_job_bundle
from deadline.client.api._telemetry import TelemetryClient
import tempfile
import os
import re
from datetime import datetime
from typing import Generator

from unittest.mock import patch, MagicMock
import pytest

from .shared_constants import MOCK_FARM_ID, MOCK_QUEUE_ID, MOCK_BUCKET_NAME


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
def deadline_mock() -> Generator[MagicMock, None, None]:
    """
    Uses the moto library to create a mock boto3 session for all tests to use.
    Mocks Deadline Cloud via the approach moto recommends for services that it
    lacks an implementation for.

    As a special case, deadline.client.api.get_deadline_cloud_library_telemetry_client
    is also redirected to deadline_mock.get_deadline_cloud_library_telemetry_client

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
                # Send the "GetQueue" operation, i.e. the get_queue call, to
                # deadline_magicmock.get_queue()
                operation_words = re.findall("[A-Z][a-z]+", operation_name)
                snake_operation = "_".join(word.lower() for word in operation_words)
                return getattr(deadline_magicmock, snake_operation)(**kwarg)

            # If we don't want to patch the API call
            return original_make_api_call(self, operation_name, kwarg)

        # Create a moto mock S3 bucket for job attachments on the queue
        boto3_session = boto3.Session(region_name="us-west-2")
        s3_client = boto3_session.client("s3", region_name="us-west-2")
        s3_client.create_bucket(
            Bucket=MOCK_BUCKET_NAME, CreateBucketConfiguration={"LocationConstraint": "us-west-2"}
        )

        # Mock some defaults into the Deadline Cloud API calls
        deadline_magicmock.get_farm.return_value = {
            "farmId": MOCK_FARM_ID,
            "displayName": "Mock Farm",
            "description": "Farm for mock testing",
            "kmsKeyArn": "",
            "createdAt": datetime.fromisoformat("2024-08-01T01:01:44+00:00"),
            "createdBy": "mock-user-id",
        }
        deadline_magicmock.get_queue.return_value = {
            "queueId": MOCK_QUEUE_ID,
            "displayName": "Mock Queue",
            "jobAttachmentSettings": {
                "rootPrefix": "MockRootPrefix",
                "s3BucketName": MOCK_BUCKET_NAME,
            },
        }
        deadline_magicmock.list_sessions.return_value = {"sessions": []}
        deadline_magicmock.list_session_actions.return_value = {"sessionActions": []}
        deadline_magicmock.assume_queue_role_for_user.return_value = {
            "credentials": {
                "accessKeyId": "ACCESSKEY",
                "secretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "sessionToken": "testing",
                "expiration": datetime.fromisoformat("2125-08-07T01:01:44+00:00"),
            }
        }

        with patch(
            "botocore.client.BaseClient._make_api_call", new=mock_make_api_call
        ), patch.object(
            deadline.client.api,
            "get_deadline_cloud_library_telemetry_client",
            new=deadline_magicmock.get_deadline_cloud_library_telemetry_client,
        ), patch.object(
            _submit_job_bundle.api,
            "get_deadline_cloud_library_telemetry_client",
            new=deadline_magicmock.get_deadline_cloud_library_telemetry_client,
        ):
            yield deadline_magicmock
