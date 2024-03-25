# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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
            yield str(temp_file_path)
    finally:
        temp_dir.cleanup()


def is_windows_non_admin():
    return sys.platform == "win32" and getpass.getuser() != "Administrator"
