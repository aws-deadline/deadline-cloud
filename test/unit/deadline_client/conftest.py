# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for Deadline Client Library tests.
"""

import os
import tempfile
from unittest.mock import patch

import pytest

from deadline.client.config import config_file


@pytest.fixture(scope="function")
def fresh_deadline_config():
    """
    Fixture to start with a blank Amazon Deadline Cloud config file.
    """

    try:
        # Create an empty temp file to set as the Amazon Deadline Cloud config
        with tempfile.NamedTemporaryFile(
            mode="w+t", suffix="", encoding="utf8", delete=False
        ) as temp:
            temp.write("")

        # Yield the temp file name with it patched in as the
        # Amazon Deadline Cloud config file
        with patch.object(config_file, "CONFIG_FILE_PATH", temp.name):
            yield temp.name
    finally:
        os.remove(temp.name)


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
