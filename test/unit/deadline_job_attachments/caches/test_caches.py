# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import os
import threading
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
                assert False, (
                    "Context manager should throw a JobAttachmentsError, this assert should not be reached"
                )
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

    def test_get_local_connection_same_thread(self, tmpdir):
        """Tests that get_local_connection returns the same connection for a single thread"""
        cache_dir = tmpdir.mkdir("cache")

        with CacheDB(
            "test", "test_table", "CREATE TABLE test_table (id INTEGER)", cache_dir
        ) as cdb:
            # Get connection from main thread
            conn1 = cdb.get_local_connection()
            conn2 = cdb.get_local_connection()

            # Should return same connection for same thread
            assert conn1 is conn2

    def test_get_local_connection_different_threads(self, tmpdir):
        """Tests that get_local_connection creates separate connections for different threads"""
        cache_dir = tmpdir.mkdir("cache")
        connections = {}

        # Create the cache and table first
        with CacheDB(
            "test", "test_table", "CREATE TABLE test_table (id INTEGER)", cache_dir
        ) as cdb:

            def get_connection(thread_id):
                connections[thread_id] = cdb.get_local_connection()

            # Create connections from different threads
            thread1 = threading.Thread(target=get_connection, args=(1,))
            thread2 = threading.Thread(target=get_connection, args=(2,))

            thread1.start()
            thread2.start()
            thread1.join()
            thread2.join()

            # Connections should be different for different threads
            assert connections[1] is not connections[2]

    def test_get_local_connection_handles_sqlite_error(self, tmpdir):
        """Tests that get_local_connection raises JobAttachmentsError on SQLite errors"""
        with CacheDB("test", "test_table", "CREATE TABLE test_table (id INTEGER)", tmpdir) as cdb:
            # Mock sqlite3.connect to raise OperationalError
            with patch("sqlite3.connect", side_effect=OperationalError("test error")):
                with pytest.raises(JobAttachmentsError) as exc_info:
                    cdb.get_local_connection()
                assert "Could not create connection to cache" in str(exc_info.value)


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

    @pytest.mark.parametrize(
        "file_path",
        [
            # Simple ascii filename
            pytest.param("file", id="ascii_name"),
            # Name from test case that was failing on Windows for a user
            pytest.param("ñ/\u00c3\u00b1.txt", id="regression_test_filename"),
            # Name from a generated emoji filename on Windows
            pytest.param("\ude0a.txt", id="surrogate_emoji_example"),
        ],
    )
    def test_get_entry_returns_valid_entry(self, tmpdir, file_path):
        """
        Tests that a valid entry is returned when it exists in the cache already
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = HashCacheEntry(
            file_path=file_path,
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(expected_entry)
            actual_entry = hc.get_entry(file_path, HashAlgorithm.XXH128)

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

    def test_table_creation_idempotent(self, tmpdir):
        """
        Tests that creating the hash cache table multiple times doesn't cause errors
        """
        cache_dir = tmpdir.mkdir("cache")

        # Create the cache and table first time
        with HashCache(cache_dir) as hc1:
            test_entry = HashCacheEntry(
                file_path="/test/file",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="abc123",
                last_modified_time="1234.56",
            )
            hc1.put_entry(test_entry)
            retrieved_entry = hc1.get_entry("/test/file", HashAlgorithm.XXH128)
            assert retrieved_entry == test_entry

        # Create the cache again with the same directory - should not fail
        with HashCache(cache_dir) as hc2:
            # Should be able to retrieve the previously stored entry
            retrieved_entry = hc2.get_entry("/test/file", HashAlgorithm.XXH128)
            assert retrieved_entry == test_entry

            # Should be able to add new entries
            new_entry = HashCacheEntry(
                file_path="/test/file2",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="def456",
                last_modified_time="5678.91",
            )
            hc2.put_entry(new_entry)
            retrieved_new_entry = hc2.get_entry("/test/file2", HashAlgorithm.XXH128)
            assert retrieved_new_entry == new_entry

    def test_table_already_exists_no_error(self, tmpdir):
        """
        Tests that no error is raised when trying to create a table that already exists
        This specifically tests the 'IF NOT EXISTS' clause in the CREATE TABLE statement
        """
        import sqlite3

        cache_dir = tmpdir.mkdir("cache")
        db_path = os.path.join(cache_dir, "hash_cache.db")

        # Create a HashCache instance to get the correct table name and schema
        hc = HashCache(cache_dir)

        with sqlite3.connect(db_path) as conn:
            # Create the database and table first using the create query
            conn.execute(hc.create_query)
            # Running the same create query again to create existing table
            # This is to simulate the case when the query runs concurrently trying to create the same table
            conn.execute(hc.create_query)
            conn.commit()

        # Now verify normal operations work
        with hc:
            test_entry = HashCacheEntry(
                file_path="/test/file",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="abc123",
                last_modified_time="1234.56",
            )
            hc.put_entry(test_entry)
            retrieved_entry = hc.get_entry("/test/file", HashAlgorithm.XXH128)
            assert retrieved_entry == test_entry
            retrieved_entry = hc.get_entry("/test/file", HashAlgorithm.XXH128)
            assert retrieved_entry == test_entry


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

    def test_delete_cache(self, tmpdir):
        """
        Tests if the cache file can be deleted when calling remove_cache
        """
        cache_dir = tmpdir.mkdir("cache")
        with S3CheckCache(cache_dir) as s3c:
            file_name: str = os.path.join(cache_dir, "s3_check_cache.db")
            assert os.path.exists(file_name)
            s3c.remove_cache()

            # Test if the cache file was deleted
            assert not os.path.exists(file_name)

    def test_get_connection_entry_returns_valid_entry(self, tmpdir):
        """Tests that get_connection_entry returns a valid entry with provided connection"""
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = S3CheckCacheEntry(
            s3_key="bucket/Data/somehash",
            last_seen_time=str(datetime.now().timestamp()),
        )

        with S3CheckCache(cache_dir) as s3c:
            s3c.put_entry(expected_entry)
            connection = s3c.get_local_connection()
            actual_entry = s3c.get_connection_entry("bucket/Data/somehash", connection)

            assert actual_entry == expected_entry

    def test_get_connection_entry_returns_none_for_nonexistent_key(self, tmpdir):
        """Tests that get_connection_entry returns None for non-existent key"""
        cache_dir = tmpdir.mkdir("cache")

        with S3CheckCache(cache_dir) as s3c:
            connection = s3c.get_local_connection()
            actual_entry = s3c.get_connection_entry("nonexistent/key", connection)

            assert actual_entry is None

    def test_get_connection_entry_returns_none_for_expired_entry(self, tmpdir):
        """Tests that get_connection_entry returns None for expired entries"""
        cache_dir = tmpdir.mkdir("cache")
        expired_entry = S3CheckCacheEntry(
            s3_key="bucket/Data/somehash",
            last_seen_time="123.456",  # very old timestamp
        )

        with S3CheckCache(cache_dir) as s3c:
            s3c.put_entry(expired_entry)
            connection = s3c.get_local_connection()
            actual_entry = s3c.get_connection_entry("bucket/Data/somehash", connection)

            assert actual_entry is None

    def test_table_creation_idempotent(self, tmpdir):
        """
        Tests that creating the S3 check cache table multiple times doesn't cause errors
        """
        cache_dir = tmpdir.mkdir("cache")

        # Create the cache and table first time
        with S3CheckCache(cache_dir) as s3c1:
            test_entry = S3CheckCacheEntry(
                s3_key="bucket/Data/test-hash",
                last_seen_time=str(datetime.now().timestamp()),
            )
            s3c1.put_entry(test_entry)
            retrieved_entry = s3c1.get_entry("bucket/Data/test-hash")
            assert retrieved_entry == test_entry

        # Create the cache again with the same directory - should not fail
        with S3CheckCache(cache_dir) as s3c2:
            # Should be able to retrieve the previously stored entry
            retrieved_entry = s3c2.get_entry("bucket/Data/test-hash")
            assert retrieved_entry == test_entry

            # Should be able to add new entries
            new_entry = S3CheckCacheEntry(
                s3_key="bucket/Data/another-hash",
                last_seen_time=str(datetime.now().timestamp()),
            )
            s3c2.put_entry(new_entry)
            retrieved_new_entry = s3c2.get_entry("bucket/Data/another-hash")
            assert retrieved_new_entry == new_entry

    def test_table_already_exists_no_error(self, tmpdir):
        """
        Tests that no error is raised when trying to create a table that already exists
        This specifically tests the 'IF NOT EXISTS' clause in the CREATE TABLE statement
        """
        import sqlite3

        cache_dir = tmpdir.mkdir("cache")
        db_path = os.path.join(cache_dir, "s3_check_cache.db")

        # Create a S3CheckCache instance to get the correct table name and schema
        s3c = S3CheckCache(cache_dir)

        with sqlite3.connect(db_path) as conn:
            # Create the database and table first using the create query
            conn.execute(s3c.create_query)
            # Running the same create query again to create existing table
            # This is to simulate the case when the query runs concurrently trying to create the same table
            conn.execute(s3c.create_query)
            conn.commit()

        # Now verify normal operations work
        with s3c:
            test_entry = S3CheckCacheEntry(
                s3_key="bucket/Data/test-hash",
                last_seen_time=str(datetime.now().timestamp()),
            )
            s3c.put_entry(test_entry)
            retrieved_entry = s3c.get_entry("bucket/Data/test-hash")
            assert retrieved_entry == test_entry
