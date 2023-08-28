# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from sqlite3 import OperationalError
from unittest.mock import patch

import pytest

import deadline
from deadline.job_attachments.errors import JobAttachmentsError
from deadline.job_attachments.hash_cache import CACHE_FILE_NAME, HashCache
from deadline.job_attachments.models import HashCacheEntry


class TestHashCache:
    """
    Tests for the local Hash Cache
    """

    def test_init_empty_path(self, tmpdir):
        """
        Tests that when no cache file path is given, the default is used.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.hash_cache.get_default_hash_cache_db_file_dir",
            side_effect=[tmpdir],
        ):
            hc = HashCache()
            assert hc.cache_dir == tmpdir.join(CACHE_FILE_NAME)

    def test_init_empty_path_no_default_throws_error(self):
        """
        Tests that when no cache file path is given, the default is used.
        """
        os.environ.pop("APPDATA", None)
        os.environ.pop("HOME", None)
        os.environ.pop("XDG_CONFIG_HOME", None)

        with pytest.raises(JobAttachmentsError):
            HashCache()
            assert False, "Constructor should raise an error, this assert should not be reached"

    def test_enter_bad_cache_path_throws_error(self, tmpdir):
        """
        Tests that an error is raised when a bad path is provided to the HashCache constructor
        """
        with pytest.raises(JobAttachmentsError) as err:
            hc = HashCache(tmpdir)
            hc.cache_dir = "/some/bad/path"
            with hc:
                assert (
                    False
                ), "Context manager should throw an execption, this assert should not be reached"
        assert isinstance(err.value.__cause__, OperationalError)

    def test_get_entry_returns_valid_entry(self, tmpdir):
        """
        Tests that a valid entry is returned when it exists in the cache already
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = HashCacheEntry("file", "hash", "1234.5678")

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(expected_entry)
            actual_entry = hc.get_entry("file")

            # THEN
            assert actual_entry == expected_entry

    def test_enter_sqlite_import_error(self, tmpdir):
        """
        Tests that the hash cache doesn't throw errors when the SQLite module can't be found
        """
        with patch.dict("sys.modules", {"sqlite3": None}):
            new_dir = tmpdir.join("does_not_exist")
            hc = HashCache(new_dir)
            assert not os.path.exists(new_dir)
            with hc:
                assert hc.get_entry("/no/file") is None
                hc.put_entry(HashCacheEntry("/no/file", "abc", "1234.56"))
                assert hc.get_entry("/no/file") is None
