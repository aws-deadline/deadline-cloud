# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import pytest


@pytest.fixture()
def external_bucket() -> str:
    """
    Return a bucket that all developers and test accounts have access to, but isn't in the testers account.
    """
    return os.environ.get("INTEG_TEST_JA_CROSS_ACCOUNT_BUCKET", "job-attachment-bucket-snipe-test")
