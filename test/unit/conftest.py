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


def pytest_xdist_auto_num_workers(config):
    # Disable xdist on Windows + Python 3.14+ due to module import race conditions in workers
    if sys.platform == "win32" and sys.version_info >= (3, 14):
        return 1


@pytest.fixture(scope="function")
def fresh_deadline_config(monkeypatch):
    """
    Fixture to start with a blank AWS Deadline Cloud config file and isolated cache directories.

    This fixture also overrides the HOME environment variable to ensure cache isolation
    between tests. Both HashCache and S3CheckCache will use temporary directories under
    the isolated HOME directory.
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

        # Create a temporary HOME directory for cache isolation
        temp_home_dir = tempfile.TemporaryDirectory()
        temp_home_path = Path(temp_home_dir.name)

        # Override HOME environment variable to isolate cache directories
        # This affects both:
        # - HashCache default: $HOME/.deadline/job_attachments/
        # - S3CheckCache via config_file.get_cache_directory(): $HOME/.deadline/cache/
        monkeypatch.setenv("HOME", str(temp_home_path))

        # On Windows, os.path.expanduser("~") uses USERPROFILE instead of HOME
        # So we need to override USERPROFILE as well for Windows compatibility
        import sys

        if sys.platform == "win32":
            monkeypatch.setenv("USERPROFILE", str(temp_home_path))

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
        temp_home_dir.cleanup()


@pytest.fixture(scope="function", autouse=True)
def aws_config(monkeypatch):
    """
    Fixture to set the AWS_CONFIG_FILE environment variable to a temporary file.

    This fixture sanitizes the environment, otherwise it's easy to write a test that only works when
    a AWS config file exists(even if it isn't used) that then fails on machines where no such config file exists

    This fixture yields the AWS config file path, so that tests can write to this file if necessary for testing
    """
    try:
        temp_dir = tempfile.TemporaryDirectory()
        temp_dir_path = Path(temp_dir.name)
        temp_file_path = temp_dir_path / "aws_config"
        monkeypatch.setenv("AWS_CONFIG_FILE", str(temp_file_path))
        monkeypatch.delenv("AWS_DEFAULT_PROFILE", raising=False)
        monkeypatch.delenv("AWS_PROFILE", raising=False)

        yield temp_file_path

    finally:
        temp_dir.cleanup()
        monkeypatch.delenv("AWS_CONFIG_FILE", raising=False)


def is_windows_non_admin():
    return sys.platform == "win32" and getpass.getuser() != "Administrator"
