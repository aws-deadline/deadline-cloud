# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for aws clients"""
from unittest.mock import Mock, patch
from deadline.job_attachments._aws.aws_clients import (
    get_deadline_client,
    get_s3_client,
    get_sts_client,
)
import deadline
from deadline.job_attachments._aws.aws_config import (
    S3_CONNECT_TIMEOUT_IN_SECS,
    S3_READ_TIMEOUT_IN_SECS,
)


def test_get_deadline_client(boto_config):
    """
    Test that get_deadline_client returns the correct deadline client
    """
    session_mock = Mock()
    with patch(
        f"{deadline.__package__}.job_attachments._aws.aws_clients.get_boto3_session"
    ) as get_session:
        get_session.return_value = session_mock
        session_mock.client.return_value = Mock()
        get_deadline_client()

    session_mock.client.assert_called_with("deadline", endpoint_url=None)


def test_get_deadline_client_non_default_endpoint(boto_config):
    """
    Test that get_deadline_client returns the correct deadline client
    and that the endpoint url is the given one when provided.
    """
    test_endpoint = "https://test.com"
    session_mock = Mock()
    with patch(
        f"{deadline.__package__}.job_attachments._aws.aws_clients.get_boto3_session"
    ) as get_session:
        get_session.return_value = session_mock
        session_mock.client.return_value = Mock()
        get_deadline_client(endpoint_url=test_endpoint)

    session_mock.client.assert_called_with("deadline", endpoint_url=test_endpoint)


def test_get_s3_client(boto_config):
    """
    Test that get_s3_client returns a properly configured S3 client.
    """
    s3_client = get_s3_client()

    assert s3_client.meta.endpoint_url == "https://s3.us-west-2.amazonaws.com"
    assert s3_client.meta.config.signature_version == "s3v4"
    assert s3_client.meta.config.connect_timeout == S3_CONNECT_TIMEOUT_IN_SECS
    assert s3_client.meta.config.read_timeout == S3_READ_TIMEOUT_IN_SECS


def test_get_sts_client(boto_config):
    sts_client = get_sts_client()

    assert sts_client.meta.endpoint_url == "https://sts.us-west-2.amazonaws.com"
