# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import concurrent.futures
import multiprocessing
import os
from sqlite3 import OperationalError
import threading
from unittest.mock import patch

import pytest

import deadline
from deadline.job_attachments.exceptions import JobAttachmentsError
from deadline.job_attachments.hash_cache import CACHE_FILE_NAME, HashCache
from deadline.job_attachments.models import HashCacheEntry


# This function is used by the bellow function, so it requires to be a top-module function
def parallelization_loop_function(hc, i) -> tuple[HashCacheEntry, HashCacheEntry]:
    filepath = f"/no/file{i}"
    inserted = HashCacheEntry(filepath, f"hash{i}", str(i))
    # print(f"Inserting {i} from {threading.get_ident()}")
    hc.put_entry(inserted)
    retrieved = hc.get_entry(filepath)
    return inserted, retrieved


# requires to be a top-module level function in order to be pickled
def parallelization_process_function(tmpdir, iterations):
    with HashCache(tmpdir) as hc:
        for i in range(iterations):
            result = parallelization_loop_function(hc, i)
            assert result[0] == result[1]


class TestHashCache:
    """
    Tests for the local Hash Cache
    """

    def test_init_empty_path(self, tmpdir):
        """
        Tests that when no cache file path is given, the default is used.
        """
        with patch(
            f"{deadline.__package__}.job_attachments.hash_cache._get_default_hash_cache_db_file_dir",
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

    def test_parallelization(self, tmpdir):
        iterations = 1000

        # Test that we can have multiple threads on the same hashcache
        with HashCache(tmpdir) as hc:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = {
                    executor.submit(parallelization_loop_function, hc, i) for i in range(iterations)
                }
                for future in concurrent.futures.as_completed(futures):
                    assert future.result()[0] == future.result()[1]

        # Test that we can have multiple hashcache across multiple threads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            with HashCache(tmpdir) as hc:
                futures = {
                    executor.submit(parallelization_loop_function, hc, i) for i in range(iterations)
                }
                for future in concurrent.futures.as_completed(futures):
                    assert future.result()[0] == future.result()[1]

        # Test that we can have multiple threads using different hashcache on the same tmpdir
        def thread_function():
            with HashCache(tmpdir) as hc:
                for i in range(iterations):
                    result = parallelization_loop_function(hc, i)
                    assert result[0] == result[1]

        threads = []
        for n in range(multiprocessing.cpu_count()):
            t = threading.Thread(target=thread_function)
            t.start()
            threads.append(t)
        for t in threads:
            t.join()

        # Test that we can have multiple processes using different hashcache on the same tmpdir
        processes = []
        for n in range(multiprocessing.cpu_count()):
            p = multiprocessing.Process(
                target=parallelization_process_function, args=[tmpdir, iterations]
            )
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
