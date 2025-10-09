# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import sys
import tempfile
from pathlib import Path

from deadline.job_attachments.caches.hash_cache import HashCache
from deadline.job_attachments.caches.s3_check_cache import S3CheckCache
from deadline.client.config import config_file


def _is_temp_directory(path: str) -> bool:
    """Check if a path is in a temporary directory (cross-platform)"""
    path_obj = Path(path).resolve()

    # Get the system temp directory
    system_temp = Path(tempfile.gettempdir()).resolve()

    try:
        # Check if the path is relative to the system temp directory
        path_obj.relative_to(system_temp)
        return True
    except ValueError:
        # Path is not under system temp directory
        return False


def test_cache_isolation_unit(fresh_deadline_config):
    """Test that demonstrates cache isolation in unit tests"""

    print(f"Config path: {fresh_deadline_config}")
    print(f"HOME environment: {os.environ.get('HOME')}")
    print(f"Platform: {sys.platform}")

    # Test HashCache default location
    with HashCache() as hash_cache:
        print(f"HashCache location: {hash_cache.cache_dir}")
        # Verify it's in a temporary directory (cross-platform)
        assert _is_temp_directory(hash_cache.cache_dir), (
            f"HashCache not in temp dir: {hash_cache.cache_dir}"
        )
        # Verify it has the expected subdirectory structure
        assert ".deadline" in hash_cache.cache_dir
        assert "job_attachments" in hash_cache.cache_dir

    # Test S3CheckCache default location
    with S3CheckCache() as s3_cache:
        print(f"S3CheckCache location: {s3_cache.cache_dir}")
        # Verify it's in a temporary directory (cross-platform)
        assert _is_temp_directory(s3_cache.cache_dir), (
            f"S3CheckCache not in temp dir: {s3_cache.cache_dir}"
        )
        # Verify it has the expected subdirectory structure
        assert ".deadline" in s3_cache.cache_dir
        assert "job_attachments" in s3_cache.cache_dir

    # Test config_file.get_cache_directory()
    cache_dir = config_file.get_cache_directory()
    print(f"config_file.get_cache_directory(): {cache_dir}")
    print(f"USERPROFILE environment: {os.environ.get('USERPROFILE')}")

    # Get the expected home directory
    expected_home = os.environ.get("HOME")
    if sys.platform == "win32":
        # On Windows, also check USERPROFILE
        expected_home = os.environ.get("USERPROFILE", expected_home)

    print(f"Expected home directory: {expected_home}")

    # Verify it has the expected subdirectory structure
    assert ".deadline" in cache_dir
    assert "cache" in cache_dir
