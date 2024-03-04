# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for Deadline AWS calls."""
import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

import deadline
from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import Queue


@patch(f"{deadline.__package__}.job_attachments._aws.aws_clients.get_boto3_session")
def test_get_queue(mock_get_boto3_session, default_queue: Queue, create_get_queue_response):
    # Set up the mock session and mock deadline client
    mock_session = MagicMock()
    mock_get_boto3_session.return_value = mock_session
    mock_deadline_client = MagicMock()
    mock_session.client.return_value = mock_deadline_client
    # Simulate a response from get_queue
    mock_deadline_client.get_queue.return_value = create_get_queue_response(default_queue)

    result = get_queue(default_queue.farmId, default_queue.queueId)

    mock_get_boto3_session.assert_called_once()
    mock_session.client.assert_called_with("deadline", endpoint_url=None)
    mock_deadline_client.get_queue.assert_called_once_with(
        farmId=default_queue.farmId, queueId=default_queue.queueId
    )
    assert result == default_queue


@patch(f"{deadline.__package__}.job_attachments._aws.deadline.get_deadline_client")
def test_get_queue_client_error(mock_get_deadline_client, default_queue: Queue):
    # Set up the mock deadline client
    mock_client = mock_get_deadline_client.return_value
    # Simulate a ClientError from get_queue
    mock_client.get_queue.side_effect = ClientError(
        {"Error": {"Code": "SomeErrorCode", "Message": "SomeErrorMessage"}},
        "GetQueue",
    )

    with pytest.raises(JobAttachmentsError) as exc_info:
        get_queue(default_queue.farmId, default_queue.queueId)

    # Check that the correct exception is raised
    assert 'Failed to get queue "queue-01234567890123456789012345678901" from Deadline: ' in str(
        exc_info.value
    )
