# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for Deadline Client Library tests.
"""

import tempfile
from unittest.mock import patch
from pathlib import Path

import pytest

from deadline.client.config import config_file


@pytest.fixture(scope="function")
def fresh_deadline_config():
    """
    Fixture to start with a blank Amazon Deadline Cloud config file.
    """

    try:
        # Create an empty temp file to set as the Amazon Deadline Cloud config
        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = Path(temp_dir.name)
        temp_file_path = temp_dir_path / "config"
        with open(temp_file_path, "w+t", encoding="utf8") as temp_file:
            temp_file.write("")

        # Yield the temp file name with it patched in as the
        # Amazon Deadline Cloud config file
        with patch.object(config_file, "CONFIG_FILE_PATH", str(temp_file_path)):
            yield str(temp_file_path)
    finally:
        temp_dir.cleanup()


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
