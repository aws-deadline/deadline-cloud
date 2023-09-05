# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for Deadline AWS calls."""
import pytest

from deadline.job_attachments._aws.deadline import get_queue
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.models import Queue


def test_get_queue(deadline_stub, default_queue: Queue, create_get_queue_response):
    deadline_stub.add_response(
        "get_queue",
        create_get_queue_response(default_queue),
        {"farmId": default_queue.farmId, "queueId": default_queue.queueId},
    )

    with deadline_stub:
        assert get_queue(default_queue.farmId, default_queue.queueId) == default_queue


def test_get_queue_fail_to_get_queue(
    deadline_stub, default_queue: Queue, create_get_queue_response
):
    deadline_stub.add_client_error("get_queue")

    with deadline_stub, pytest.raises(JobAttachmentsError):
        get_queue(default_queue.farmId, default_queue.queueId)
