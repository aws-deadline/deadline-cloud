# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
from datetime import datetime
from sqlite3 import OperationalError
from unittest.mock import patch

import pytest

import deadline
from deadline.job_attachments.asset_manifests import HashAlgorithm
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.caches import (
    CacheDB,
    HashCache,
    HashCacheEntry,
    S3CheckCache,
    S3CheckCacheEntry,
)


class TestCacheDB:
    """
    Tests for the CacheDB abstract base class
    """

    def test_get_default_cache_db_file_dir_env_var_path_exists(self, tmpdir):
        """
        Tests that when an environment variable exists, it uses that path for the hash cache
        """
        expected_path = tmpdir.join(".deadline").join("job_attachments")
        with patch("os.environ.get", side_effect=[tmpdir]):
            assert CacheDB.get_default_cache_db_file_dir() == expected_path

    def test_init_empty_path_no_default_throws_error(self):
        """
        Tests that when no cache file path is given, the default is used.
        """
        os.environ.pop("APPDATA", None)
        os.environ.pop("HOME", None)
        os.environ.pop("XDG_CONFIG_HOME", None)

        with pytest.raises(JobAttachmentsError):
            CacheDB("name", "table", "query")

    def test_enter_bad_cache_path_throws_error(self, tmpdir):
        """
        Tests that an error is raised when a bad path is provided to the CacheDB constructor
        """
        with pytest.raises(JobAttachmentsError) as err:
            cdb = CacheDB("name", "table", "query", tmpdir)
            cdb.cache_dir = "/some/bad/path"
            with cdb:
                assert (
                    False
                ), "Context manager should throw a JobAttachmentsError, this assert should not be reached"
        assert isinstance(err.value.__cause__, OperationalError)

    @pytest.mark.parametrize(
        "cache_name, table_name, create_query",
        [
            pytest.param("", "table", "query"),
            pytest.param("name", "", "query"),
            pytest.param("name", "table", ""),
        ],
    )
    def test_init_throws_error_on_empty_strings(self, cache_name, table_name, create_query):
        """Tests that a JobAttachmentsError is raised if init args are empty"""
        with pytest.raises(JobAttachmentsError):
            CacheDB(cache_name, table_name, create_query)


class TestHashCache:
    """
    Tests for the local Hash Cache
    """

    def test_init_empty_path(self, tmpdir):
        """
        Tests that when no cache file path is given, the default is used.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.caches.CacheDB.get_default_cache_db_file_dir",
            side_effect=[tmpdir],
        ):
            hc = HashCache()
            assert hc.cache_dir == tmpdir.join(f"{HashCache.CACHE_NAME}.db")

    def test_get_entry_returns_valid_entry(self, tmpdir):
        """
        Tests that a valid entry is returned when it exists in the cache already
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = HashCacheEntry(
            file_path="file",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(expected_entry)
            actual_entry = hc.get_entry("file", HashAlgorithm.XXH128)

            # THEN
            assert actual_entry == expected_entry

    def test_enter_sqlite_import_error(self, tmpdir):
        """
        Tests that the cache doesn't throw errors when the SQLite module can't be found
        """
        with patch.dict("sys.modules", {"sqlite3": None}):
            new_dir = tmpdir.join("does_not_exist")
            hc = HashCache(new_dir)
            assert not os.path.exists(new_dir)
            with hc:
                assert hc.get_entry("/no/file", HashAlgorithm.XXH128) is None
                hc.put_entry(
                    HashCacheEntry(
                        file_path="/no/file",
                        hash_algorithm=HashAlgorithm.XXH128,
                        file_hash="abc",
                        last_modified_time="1234.56",
                    )
                )
                assert hc.get_entry("/no/file", HashAlgorithm.XXH128) is None


class TestS3CheckCache:
    """
    Tests for the local S3 Check Hash
    """

    def test_init_empty_path(self, tmpdir):
        """
        Tests that when no cache file path is given, the default is used.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.caches.CacheDB.get_default_cache_db_file_dir",
            side_effect=[tmpdir],
        ):
            s3c = S3CheckCache()
            assert s3c.cache_dir == tmpdir.join(f"{S3CheckCache.CACHE_NAME}.db")

    def test_get_entry_returns_valid_entry(self, tmpdir):
        """
        Tests that a valid entry is returned when it exists in the cache already
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = S3CheckCacheEntry(
            s3_key="bucket/Data/somehash",
            last_seen_time=str(datetime.now().timestamp()),
        )

        # WHEN
        with S3CheckCache(cache_dir) as s3c:
            s3c.put_entry(expected_entry)
            actual_entry = s3c.get_entry("bucket/Data/somehash")

            # THEN
            assert actual_entry == expected_entry

    def test_get_entry_returns_none_with_expired_entry(self, tmpdir):
        """
        Tests that nothing is returned when an existing entry is expired
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = S3CheckCacheEntry(
            s3_key="bucket/Data/somehash",
            last_seen_time="123.456",  # a looong time ago
        )

        # WHEN
        with S3CheckCache(cache_dir) as s3c:
            s3c.put_entry(expected_entry)
            actual_entry = s3c.get_entry("bucket/Data/somehash")

            # THEN
            assert actual_entry is None

    def test_enter_sqlite_import_error(self, tmpdir):
        """
        Tests that the cache doesn't throw errors when the SQLite module can't be found
        """
        with patch.dict("sys.modules", {"sqlite3": None}):
            new_dir = tmpdir.join("does_not_exist")
            s3c = S3CheckCache(new_dir)
            assert not os.path.exists(new_dir)
            with s3c:
                assert s3c.get_entry("bucket/Data/somehash") is None
                s3c.put_entry(
                    S3CheckCacheEntry(
                        s3_key="bucket/Data/somehash",
                        last_seen_time=str(datetime.now().timestamp()),
                    )
                )
                assert s3c.get_entry("bucket/Data/somehash") is None
