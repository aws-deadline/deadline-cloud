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
    WHOLE_FILE_RANGE_END,
)


class TestCacheDB:
    """
    Tests for the CacheDB abstract base class
    """

    def test_get_default_cache_db_file_dir_env_var_path_exists(self, tmpdir):
        """
        Tests that when an environment variable exists, it uses that path for the hash cache
        """
        expected_path = os.path.join(str(tmpdir), ".deadline", "job_attachments")
        with patch(
            "deadline.job_attachments.caches.cache_db.os.path.expanduser", return_value=str(tmpdir)
        ):
            assert CacheDB.get_default_cache_db_file_dir() == expected_path

    def test_init_empty_path_no_default_throws_error(self):
        """
        Tests that when no cache file path is given and home dir cannot be resolved, an error is raised.
        """
        with patch("deadline.job_attachments.caches.cache_db.os.path.expanduser", return_value="~"):
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

    def test_enter_retries_on_operational_error(self, tmpdir):
        """Tests that __enter__ retries on OperationalError and succeeds on final attempt"""
        from unittest.mock import MagicMock

        # Create a mock connection that will be returned on successful connect
        mock_connection = MagicMock()
        mock_connection.execute.return_value = None

        # Create side effect that fails twice then succeeds
        connect_calls = 0

        def connect_side_effect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            if connect_calls <= 2:
                raise OperationalError("database is locked")
            return mock_connection

        with patch("sqlite3.connect", side_effect=connect_side_effect):
            # This should succeed after 2 retries
            with CacheDB(
                "test", "test_table", "CREATE TABLE test_table (id INTEGER)", tmpdir
            ) as cdb:
                # Verify the connection was established
                assert cdb.db_connection == mock_connection
                # Verify we made the expected number of connection attempts
                assert connect_calls == CacheDB._RETRY_ATTEMPTS

    def test_enter_fails_after_max_retries(self, tmpdir):
        """Tests that __enter__ fails with JobAttachmentsError after max retries"""

        # Track connection attempts
        connect_calls = 0

        def connect_side_effect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            raise OperationalError("database is locked")

        # Mock sqlite3.connect to always raise OperationalError
        with patch("sqlite3.connect", side_effect=connect_side_effect):
            with pytest.raises(JobAttachmentsError) as exc_info:
                with CacheDB("test", "test_table", "CREATE TABLE test_table (id INTEGER)", tmpdir):
                    pass

            # Verify the error message indicates retry exhaustion
            assert (
                f"Could not access cache file after {CacheDB._RETRY_ATTEMPTS} retry attempts"
                in str(exc_info.value)
            )
            # Verify we made the expected number of connection attempts
            assert connect_calls == CacheDB._RETRY_ATTEMPTS

    def test_get_local_connection_retries_on_operational_error(self, tmpdir):
        """Tests that get_local_connection retries on OperationalError and succeeds"""
        from unittest.mock import MagicMock

        # Create a mock connection that will be returned on successful connect
        mock_connection = MagicMock()

        # Create side effect that fails twice then succeeds
        connect_calls = 0

        def connect_side_effect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            if connect_calls <= 2:
                raise OperationalError("database is locked")
            return mock_connection

        with CacheDB("test", "test_table", "CREATE TABLE test_table (id INTEGER)", tmpdir) as cdb:
            with patch("sqlite3.connect", side_effect=connect_side_effect):
                # This should succeed after 2 retries
                connection = cdb.get_local_connection()

                # Verify the connection was established
                assert connection == mock_connection
                # Verify we made the expected number of connection attempts
                assert connect_calls == CacheDB._RETRY_ATTEMPTS


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

    def test_get_entry_with_byte_range(self, tmpdir):
        """
        Tests that a byte range entry is returned when it exists in the cache
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = HashCacheEntry(
            file_path="large_file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="chunk_hash_1",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=268435456,  # 256MB
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(expected_entry)
            actual_entry = hc.get_entry(
                "large_file.bin", HashAlgorithm.XXH128, range_start=0, range_end=268435456
            )

            # THEN
            assert actual_entry == expected_entry
            assert actual_entry.range_start == 0
            assert actual_entry.range_end == 268435456

    def test_get_entry_multiple_byte_ranges_same_file(self, tmpdir):
        """
        Tests that multiple byte range entries for the same file are stored and retrieved correctly
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        chunk_size = 268435456  # 256MB
        entries = [
            HashCacheEntry(
                file_path="large_file.bin",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash=f"chunk_hash_{i}",
                last_modified_time="1234.5678",
                range_start=i * chunk_size,
                range_end=(i + 1) * chunk_size,
            )
            for i in range(4)  # 4 chunks
        ]

        # WHEN
        with HashCache(cache_dir) as hc:
            for entry in entries:
                hc.put_entry(entry)

            # THEN - each chunk should be retrievable independently
            for i, expected_entry in enumerate(entries):
                actual_entry = hc.get_entry(
                    "large_file.bin",
                    HashAlgorithm.XXH128,
                    range_start=i * chunk_size,
                    range_end=(i + 1) * chunk_size,
                )
                assert actual_entry == expected_entry
                assert actual_entry.file_hash == f"chunk_hash_{i}"

    def test_get_entry_byte_range_not_found(self, tmpdir):
        """
        Tests that None is returned when a specific byte range doesn't exist
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="chunk_hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=1000,
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(entry)

            # THEN - different range should return None
            assert (
                hc.get_entry("file.bin", HashAlgorithm.XXH128, range_start=0, range_end=2000)
                is None
            )
            assert (
                hc.get_entry("file.bin", HashAlgorithm.XXH128, range_start=1000, range_end=2000)
                is None
            )

    def test_get_entry_whole_file_vs_byte_range_independent(self, tmpdir):
        """
        Tests that whole-file hashes and byte-range hashes are stored independently
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        whole_file_entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="whole_file_hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=WHOLE_FILE_RANGE_END,
        )
        chunk_entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="chunk_hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=1000,
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(whole_file_entry)
            hc.put_entry(chunk_entry)

            # THEN - both should be retrievable independently
            actual_whole = hc.get_entry("file.bin", HashAlgorithm.XXH128)
            actual_chunk = hc.get_entry(
                "file.bin", HashAlgorithm.XXH128, range_start=0, range_end=1000
            )

            assert actual_whole == whole_file_entry
            assert actual_whole.file_hash == "whole_file_hash"
            assert actual_chunk == chunk_entry
            assert actual_chunk.file_hash == "chunk_hash"

    def test_get_connection_entry_with_byte_range(self, tmpdir):
        """
        Tests that get_connection_entry works with byte range parameters
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        expected_entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="chunk_hash",
            last_modified_time="1234.5678",
            range_start=1000,
            range_end=2000,
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(expected_entry)
            connection = hc.get_local_connection()
            actual_entry = hc.get_connection_entry(
                "file.bin", HashAlgorithm.XXH128, connection, range_start=1000, range_end=2000
            )

            # THEN
            assert actual_entry == expected_entry

    def test_hash_cache_entry_is_whole_file(self):
        """
        Tests the is_whole_file() helper method on HashCacheEntry
        """
        whole_file = HashCacheEntry(
            file_path="file.txt",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
        )
        assert whole_file.is_whole_file() is True

        chunk = HashCacheEntry(
            file_path="file.txt",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=1000,
        )
        assert chunk.is_whole_file() is False

        # Edge case: range_start != 0 but range_end == -1 should not be whole file
        weird_entry = HashCacheEntry(
            file_path="file.txt",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
            range_start=100,
            range_end=WHOLE_FILE_RANGE_END,
        )
        assert weird_entry.is_whole_file() is False

    def test_put_entry_replaces_existing_byte_range(self, tmpdir):
        """
        Tests that put_entry replaces an existing entry with the same byte range
        """
        # GIVEN
        cache_dir = tmpdir.mkdir("cache")
        original_entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="original_hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=1000,
        )
        updated_entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="updated_hash",
            last_modified_time="9999.9999",
            range_start=0,
            range_end=1000,
        )

        # WHEN
        with HashCache(cache_dir) as hc:
            hc.put_entry(original_entry)
            hc.put_entry(updated_entry)
            actual_entry = hc.get_entry(
                "file.bin", HashAlgorithm.XXH128, range_start=0, range_end=1000
            )

            # THEN
            assert actual_entry.file_hash == "updated_hash"
            assert actual_entry.last_modified_time == "9999.9999"

    def test_hash_cache_entry_to_dict_includes_range(self):
        """
        Tests that to_dict() includes range_start and range_end
        """
        entry = HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
            range_start=100,
            range_end=200,
        )
        result = entry.to_dict()

        assert result["file_path"] == "file.bin"
        assert result["hash_algorithm"] == "xxh128"
        assert result["file_hash"] == "hash"
        assert result["last_modified_time"] == "1234.5678"
        assert result["range_start"] == 100
        assert result["range_end"] == 200

    def test_hash_cache_entry_validates_byte_range(self):
        """
        Tests that HashCacheEntry raises ValueError when range_end <= range_start for byte-range entries
        """
        # Valid byte-range entry should work
        HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=100,
        )

        # Whole-file entry (range_end=-1) should work regardless of range_start
        HashCacheEntry(
            file_path="file.bin",
            hash_algorithm=HashAlgorithm.XXH128,
            file_hash="hash",
            last_modified_time="1234.5678",
            range_start=0,
            range_end=WHOLE_FILE_RANGE_END,
        )

        # Invalid: range_end == range_start
        with pytest.raises(ValueError, match="range_end.*must be greater than.*range_start"):
            HashCacheEntry(
                file_path="file.bin",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="hash",
                last_modified_time="1234.5678",
                range_start=100,
                range_end=100,
            )

        # Invalid: range_end < range_start
        with pytest.raises(ValueError, match="range_end.*must be greater than.*range_start"):
            HashCacheEntry(
                file_path="file.bin",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="hash",
                last_modified_time="1234.5678",
                range_start=200,
                range_end=100,
            )


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

    def test_concurrent_read_write_operations_should_not_lock(self, tmpdir):
        """Test that concurrent reads and writes don't cause database locked errors"""
        import threading
        import time

        cache_dir = tmpdir.mkdir("cache")
        errors = {}
        results = {}

        with HashCache(cache_dir) as cache:

            def aggressive_writer_thread(thread_id):
                """Thread that aggressively writes to cache with transactions"""
                try:
                    for i in range(20):  # More operations
                        entry = HashCacheEntry(
                            file_path=f"/test/file_{thread_id}_{i}.txt",
                            hash_algorithm=HashAlgorithm.XXH128,
                            file_hash=f"hash_{thread_id}_{i}",
                            last_modified_time=str(time.time()),
                        )
                        cache.put_entry(entry)
                except Exception as e:
                    errors[f"writer_{thread_id}"] = str(e)

            def aggressive_reader_thread(thread_id):
                """Thread that aggressively reads from cache using thread-local connection"""
                try:
                    conn = cache.get_local_connection()
                    for i in range(50):  # Many more read operations
                        # Try to read - this should never get "database is locked"
                        entry = cache.get_connection_entry(
                            f"/test/file_{i % 4}_{i % 5}.txt", HashAlgorithm.XXH128, conn
                        )
                        results[f"reader_{thread_id}_{i}"] = entry is not None
                except Exception as e:
                    errors[f"reader_{thread_id}"] = str(e)

            # Start many more threads concurrently
            threads = []

            # Start 5 writer threads (more write contention)
            for i in range(5):
                t = threading.Thread(target=aggressive_writer_thread, args=(i,))
                threads.append(t)
                t.start()

            # Start 10 reader threads (more read contention)
            for i in range(10):
                t = threading.Thread(target=aggressive_reader_thread, args=(i,))
                threads.append(t)
                t.start()

            # Wait for all threads
            for t in threads:
                t.join()

        # Should have no "database is locked" errors
        locked_errors = {k: v for k, v in errors.items() if "database is locked" in v}
        assert len(locked_errors) == 0, f"Got database locked errors: {locked_errors}"
        assert len(errors) == 0, f"Got other errors: {errors}"

    def test_large_db_concurrent_operations_expose_timeout_issues(self, tmpdir):
        """Test that large database (~50MB) exposes timeout issues without proper SQLite configuration"""
        import threading
        import time

        cache_dir = tmpdir.mkdir("cache")

        # Prepopulate database to ~50MB (approximately 500K records for more realistic size)
        print("Prepopulating database to ~50MB...")
        with HashCache(cache_dir) as cache:
            # Initialize cache with one entry to ensure table exists
            init_entry = HashCacheEntry(
                file_path="/init.txt",
                hash_algorithm=HashAlgorithm.XXH128,
                file_hash="init_hash",
                last_modified_time=str(time.time()),
            )
            cache.put_entry(init_entry)

            conn = cache.db_connection
            batch_data = []
            batch_size = 10000

            for i in range(500000):  # Increased to 500K records
                batch_data.append(
                    (
                        f"/large/test/file_{i:06d}.txt",
                        HashAlgorithm.XXH128.value,
                        f"hash_{i:032x}" * 2,  # Longer hash to increase record size
                        str(time.time() + i),
                    )
                )

                if len(batch_data) >= batch_size:
                    conn.executemany(
                        f"INSERT OR REPLACE INTO {cache.table_name} (file_path, hash_algorithm, file_hash, last_modified_time) VALUES (?, ?, ?, ?)",
                        batch_data,
                    )
                    conn.commit()
                    batch_data = []
                    if i % 50000 == 0:
                        print(f"  Added {i + 1} records...")

            # Insert remaining records
            if batch_data:
                conn.executemany(
                    f"INSERT OR REPLACE INTO {cache.table_name} (file_path, hash_algorithm, file_hash, last_modified_time) VALUES (?, ?, ?, ?)",
                    batch_data,
                )
                conn.commit()

        print("Database prepopulated. Starting aggressive concurrency test...")

        errors = {}
        results = {}

        with HashCache(cache_dir) as cache:

            def aggressive_writer_thread(thread_id):
                try:
                    for i in range(50):  # More write operations
                        entry = HashCacheEntry(
                            file_path=f"/concurrent/file_{thread_id}_{i}.txt",
                            hash_algorithm=HashAlgorithm.XXH128,
                            file_hash=f"concurrent_hash_{thread_id}_{i}",
                            last_modified_time=str(time.time()),
                        )
                        cache.put_entry(entry)
                except Exception as e:
                    errors[f"writer_{thread_id}"] = str(e)

            def aggressive_reader_thread(thread_id):
                try:
                    conn = cache.get_local_connection()
                    for i in range(100):  # Many more read operations
                        entry = cache.get_connection_entry(
                            f"/large/test/file_{i:06d}.txt", HashAlgorithm.XXH128, conn
                        )
                        results[f"reader_{thread_id}_{i}"] = entry is not None
                except Exception as e:
                    errors[f"reader_{thread_id}"] = str(e)

            # Start many concurrent operations on large database
            threads = []
            for i in range(15):  # More writer threads
                t = threading.Thread(target=aggressive_writer_thread, args=(i,))
                threads.append(t)
                t.start()

            for i in range(25):  # More reader threads
                t = threading.Thread(target=aggressive_reader_thread, args=(i,))
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

        # With proper SQLite configuration (timeout + WAL), should have no lock errors
        locked_errors = {k: v for k, v in errors.items() if "database is locked" in v}
        assert len(locked_errors) == 0, f"Got database locked errors: {locked_errors}"
        assert len(errors) == 0, f"Got other errors: {errors}"
