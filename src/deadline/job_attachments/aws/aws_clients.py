# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Functions for handling and retrieving AWS clients."""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

import boto3
import botocore
from botocore.client import BaseClient, Config

from .. import version
from .aws_config import S3_CONNECT_TIMEOUT_IN_SECS, S3_READ_TIMEOUT_IN_SECS, VENDOR_CODE

MAX_SIZE_CACHE = 128


# Should create a new botocore session since botocore session may be modified by boto3 session/client using it
# https://github.com/boto/boto3/blob/61de529b5f9a7bdcc8c76debb472a7f934d048e6/boto3/session.py#L79
def get_botocore_session() -> botocore.session.Session:
    return botocore.session.get_session()


@lru_cache(maxsize=MAX_SIZE_CACHE)
def get_boto3_session(
    botocore_session: botocore.session.Session = get_botocore_session(),
) -> boto3.session.Session:
    return boto3.session.Session(botocore_session=botocore_session)


@lru_cache(maxsize=MAX_SIZE_CACHE)
def get_deadline_client(
    session: Optional[boto3.session.Session] = None, endpoint_url: Optional[str] = None
) -> BaseClient:
    """
    Get a boto3 Deadline client to make API calls to Deadline
    """
    if session is None:
        session = get_boto3_session()

    return session.client(VENDOR_CODE, endpoint_url=endpoint_url)


@lru_cache(maxsize=MAX_SIZE_CACHE)
def get_s3_client(session: Optional[boto3.Session] = None) -> BaseClient:
    """
    Get a boto3 S3 client to make API calls to S3
    """
    if session is None:
        session = get_boto3_session()

    # TODO: For max_pool_connections, the default max connections is 10. Since we're multithreading the client,
    # we'll use the number of threads multiplied by the default.
    # https://docs.python.org/3/library/concurrent.futures.html#threadpoolexecutor
    # Or, use the default number for now, and revisit later.

    client = session.client(
        "s3",
        config=Config(
            signature_version="s3v4",
            connect_timeout=S3_CONNECT_TIMEOUT_IN_SECS,
            read_timeout=S3_READ_TIMEOUT_IN_SECS,
            user_agent_extra=f"S3A/Deadline/NA/JobAttachments/{version}",
        ),
        endpoint_url=f"https://s3.{session.region_name}.amazonaws.com",
    )

    def add_expected_bucket_owner(params, model, **kwargs):
        """
        Add the expected bucket owner to the params if the API operation to run can use it.
        """
        if "ExpectedBucketOwner" in model.input_shape.members:
            params["ExpectedBucketOwner"] = get_account_id(session=session)

    client.meta.events.register("provide-client-params.s3.*", add_expected_bucket_owner)

    return client


@lru_cache(maxsize=MAX_SIZE_CACHE)
def get_sts_client(session: Optional[boto3.session.Session] = None) -> BaseClient:
    """
    Get a boto3 sts client to make API calls to STS.
    """
    if session is None:
        session = get_boto3_session()
    return session.client(
        "sts",
        endpoint_url=f"https://sts.{session.region_name}.amazonaws.com",
    )


@lru_cache(maxsize=MAX_SIZE_CACHE)
def get_caller_identity(session: Optional[boto3.session.Session] = None) -> dict[str, str]:
    """
    Get the caller identity for the current session.
    """
    return get_sts_client(session).get_caller_identity()


def get_account_id(session: Optional[boto3.session.Session] = None) -> str:
    """
    Get the account id for the current session.
    """
    return get_caller_identity(session)["Account"]
