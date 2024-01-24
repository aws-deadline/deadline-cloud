# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for accessing the local file hash cache.
"""

import logging
import os
from threading import Lock
from typing import Optional

from .asset_manifests.hash_algorithms import HashAlgorithm
from .exceptions import JobAttachmentsError
from .models import HashCacheEntry
from ._utils import _get_default_hash_cache_db_file_dir

CACHE_FILE_NAME = "hash_cache.db"
CACHE_DB_VERSION = 2

logger = logging.getLogger("Deadline")


class HashCache:
    """
    Class used to store and retrieve entries in the local file hash cache.

    This class is intended to always be used with a context manager to properly
    close the connection to the hash cache database.

    This class also automatically locks when doing writes, so it can be called
    by multiple threads.
    """

    def __init__(self, cache_dir: Optional[str] = None) -> None:
        try:
            # SQLite is included in Python installers, but might not exist if building python from source.
            import sqlite3  # noqa

            self.enabled = True
        except ImportError:
            logger.warn("SQLite was not found, the Hash Cache will not be used.")
            self.enabled = False
            return

        if cache_dir is None:
            cache_dir = _get_default_hash_cache_db_file_dir()
        if cache_dir is None:
            raise JobAttachmentsError(
                "No default hash cache path found. Please provide a hash cache directory."
            )
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir: str = os.path.join(cache_dir, CACHE_FILE_NAME)
        self.db_lock = Lock()

    def __enter__(self):
        """Called when entering the context manager."""
        if self.enabled:
            import sqlite3

            try:
                self.db_connection: sqlite3.Connection = sqlite3.connect(
                    self.cache_dir, check_same_thread=False
                )
            except sqlite3.OperationalError as oe:
                raise JobAttachmentsError(
                    f"Could not access hash cache file in {self.cache_dir}"
                ) from oe

            try:
                self.db_connection.execute(f"SELECT * FROM hashesV{CACHE_DB_VERSION}")
            except Exception:
                # DB file doesn't have our table, so we need to create it
                logger.info(
                    "No hash cache entries for the current library version were found. Creating a new hash cache."
                )
                self.db_connection.execute(
                    f"CREATE TABLE hashesV{CACHE_DB_VERSION}(file_path text primary key, hash_algorithm text secondary key, file_hash text, last_modified_time timestamp)"
                )
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Called when exiting the context manager."""
        if self.enabled:
            self.db_connection.close()

    def get_entry(
        self, file_path_key: str, hash_algorithm: HashAlgorithm
    ) -> Optional[HashCacheEntry]:
        """
        Returns an entry from the hash cache, if it exists.
        """
        if not self.enabled:
            return None

        with self.db_lock, self.db_connection:
            entry_vals = self.db_connection.execute(
                f"SELECT * FROM hashesV{CACHE_DB_VERSION} WHERE file_path=? AND hash_algorithm=?",
                [file_path_key, hash_algorithm.value],
            ).fetchone()
            if entry_vals:
                return HashCacheEntry(
                    file_path=entry_vals[0],
                    hash_algorithm=HashAlgorithm(entry_vals[1]),
                    file_hash=entry_vals[2],
                    last_modified_time=str(entry_vals[3]),
                )
            else:
                return None

    def put_entry(self, entry: HashCacheEntry) -> None:
        """Inserts or replaces an entry into the hash cache database after acquiring the lock."""
        if self.enabled:
            with self.db_lock, self.db_connection:
                self.db_connection.execute(
                    f"INSERT OR REPLACE INTO hashesV{CACHE_DB_VERSION} VALUES(:file_path, :hash_algorithm, :file_hash, :last_modified_time)",
                    entry.to_dict(),
                )
