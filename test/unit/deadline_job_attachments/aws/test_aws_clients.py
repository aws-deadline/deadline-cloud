# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for aws clients"""

import os
from unittest.mock import Mock, patch

import pytest

from deadline.job_attachments._aws.aws_clients import (
    get_boto3_session,
    get_botocore_session,
    get_deadline_client,
    get_s3_client,
    get_sts_client,
)
import deadline
from deadline.job_attachments._aws.aws_config import (
    S3_CONNECT_TIMEOUT_IN_SECS,
    S3_READ_TIMEOUT_IN_SECS,
)


def _make_client(service_name, session=None):
    """Create a client using the production factory functions with an optional fresh session."""
    if session is None:
        session = get_boto3_session(get_botocore_session())
    factories = {
        "s3": get_s3_client,
        "sts": get_sts_client,
        "deadline": get_deadline_client,
    }
    return factories[service_name](session=session)


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

    assert s3_client.meta.config.signature_version == "s3v4"
    assert s3_client.meta.config.connect_timeout == S3_CONNECT_TIMEOUT_IN_SECS
    assert s3_client.meta.config.read_timeout == S3_READ_TIMEOUT_IN_SECS


def test_get_sts_client(boto_config):
    sts_client = get_sts_client()

    assert sts_client.meta.service_model.service_name == "sts"


@pytest.mark.parametrize("service_name", ["s3", "sts", "deadline"])
def test_default_regional_endpoint(boto_config, service_name):
    """
    Test that S3 and STS clients (previously global by default) now use regional endpoints by default.
    """
    region = os.environ["AWS_DEFAULT_REGION"]
    client = _make_client(service_name)
    assert client.meta.endpoint_url == f"https://{service_name}.{region}.amazonaws.com"


@pytest.mark.parametrize(
    "service_name, env_var",
    [
        ("s3", "AWS_ENDPOINT_URL_S3"),
        ("sts", "AWS_ENDPOINT_URL_STS"),
        ("deadline", "AWS_ENDPOINT_URL_DEADLINE"),
    ],
)
def test_endpoint_url_override_via_env(boto_config, service_name, env_var):
    """
    Test that clients respect service-specific AWS_ENDPOINT_URL_* environment variables.
    """
    custom_endpoint = f"https://custom-{service_name}-env.example.com"
    with patch.dict(os.environ, {env_var: custom_endpoint}):
        client = _make_client(service_name)
        assert client.meta.endpoint_url == custom_endpoint


@pytest.mark.parametrize(
    "service_name",
    ["s3", "sts", "deadline"],
)
def test_endpoint_url_override_via_config_profile(boto_config, tmp_path, service_name):
    """
    Test that clients respect endpoint_url set in an AWS config profile.
    """
    custom_endpoint = f"https://custom-{service_name}-config.example.com"
    config_file = tmp_path / "config"
    config_file.write_text(f"""
[profile testprofile]
services = testprofile-services

[services testprofile-services]
{service_name} =
    endpoint_url = {custom_endpoint}
""")
    with patch.dict(
        os.environ,
        {
            "AWS_CONFIG_FILE": str(config_file),
            "AWS_PROFILE": "testprofile",
        },
    ):
        client = _make_client(service_name)
        assert client.meta.endpoint_url == custom_endpoint
