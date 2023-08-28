# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for aws clients"""
from deadline.job_attachments.aws.aws_clients import (
    get_deadline_client,
    get_s3_client,
    get_sts_client,
)
from deadline.job_attachments.aws.aws_config import (
    S3_CONNECT_TIMEOUT_IN_SECS,
    S3_READ_TIMEOUT_IN_SECS,
)


def test_get_deadline_client(boto_config):
    """
    Test that get_deadline_client returns the correct deadline client
    """
    deadline_client = get_deadline_client()

    assert deadline_client.meta.service_model.service_name == "deadline"


def test_get_deadline_client_non_default_endpoint(boto_config):
    """
    Test that get_deadline_client returns the correct deadline client
    and that the endpoint url is the given one when provided.
    """
    test_endpoint = "https://test.com"
    deadline_client = get_deadline_client(endpoint_url=test_endpoint)

    assert deadline_client.meta.service_model.service_name == "deadline"
    assert deadline_client.meta.endpoint_url == test_endpoint


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
