# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

# https://docs.getmoto.org/en/latest/docs/getting_started.html#pesky-imports-section
from moto import mock_aws  # noqa: F401
import getpass
import sys
import pytest
import tempfile

from unittest.mock import patch
from pathlib import Path
from deadline.client.config import config_file


@pytest.fixture(scope="function")
def fresh_deadline_config():
    """
    Fixture to start with a blank AWS Deadline Cloud config file.

    """

    # Clear the session cache. Importing the cache invalidator at runtime is necessary
    # to make sure the import order doesn't bypass moto mocking in other areas.
    from deadline.client.api._session import invalidate_boto3_session_cache

    invalidate_boto3_session_cache()

    try:
        # Create an empty temp file to set as the AWS Deadline Cloud config
        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = Path(temp_dir.name)
        temp_file_path = temp_dir_path / "config"
        with open(temp_file_path, "w+t", encoding="utf8") as temp_file:
            temp_file.write("")

        # Yield the temp file name with it patched in as the
        # AWS Deadline Cloud config file
        with patch.object(config_file, "CONFIG_FILE_PATH", str(temp_file_path)):
            # Write a telemetry id to force it getting saved to the config file. If we don't, then
            # an ID will get generated and force a save of the config file in the middle of a test.
            # Writing the config file may be undesirable in the middle of a test.
            config_file.set_setting("telemetry.identifier", "00000000-0000-0000-0000-000000000000")

            yield str(temp_file_path)
    finally:
        temp_dir.cleanup()


def is_windows_non_admin():
    return sys.platform == "win32" and getpass.getuser() != "Administrator"
