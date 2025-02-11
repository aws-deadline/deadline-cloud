# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Common fixtures for deadline job attachments tests.
"""

from unittest.mock import patch
import os
import time
import pytest
import deadline
import tempfile


@pytest.fixture(scope="function", autouse=True)
def session_hash_db_dir_mock():
    with tempfile.TemporaryDirectory() as tmpdir_path:
        # We have to use time as a seed, otherwise pytest has fixed random for reproducibility.
        tmpdir_path = os.path.join(tmpdir_path, str(int(time.time())))
        with patch(
            f"{deadline.__package__}.client.config.config_file.get_cache_directory",
            return_value=str(tmpdir_path),
        ), patch(
            f"{deadline.__package__}.job_attachments.caches.CacheDB.get_default_cache_db_file_dir",
            return_value=str(tmpdir_path),
        ):
            yield
