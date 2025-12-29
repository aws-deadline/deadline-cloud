# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for accessing the local file hash cache.

Supports two types of hash entries:
1. Whole-file hashes: range_start=0, range_end=-1 (WHOLE_FILE_RANGE_END)
2. Byte-range hashes: range_start >= 0, range_end > 0, defining the range [start, end)

The range parameters allow caching hashes for arbitrary byte ranges of files,
which is useful for caching hashes for any chunking scheme without modifying the cache.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .cache_db import CacheDB
from ..asset_manifests.hash_algorithms import HashAlgorithm


logger = logging.getLogger("Deadline")

# Sentinel value indicating a whole-file hash (no specific byte range)
WHOLE_FILE_RANGE_END = -1


@dataclass
class HashCacheEntry:
    """Represents an entry in the local hash-cache database.

    For whole-file hashes: range_start=0, range_end=-1 (WHOLE_FILE_RANGE_END)
    For chunk hashes: range_start and range_end define the byte range [start, end)
    """

    # The file_path is stored as a BLOB in sqlite, encoded with utf-8 and the "surrogatepass"
    # error handler, as file names encountered in practice require this.
    file_path: str
    hash_algorithm: HashAlgorithm
    file_hash: str
    last_modified_time: str
    range_start: int = 0
    range_end: int = WHOLE_FILE_RANGE_END

    def __post_init__(self) -> None:
        # Validate byte-range entries have range_end > range_start.
        if self.range_end != WHOLE_FILE_RANGE_END and self.range_end <= self.range_start:
            raise ValueError(
                f"For byte-range entries, range_end ({self.range_end}) must be greater than "
                f"range_start ({self.range_start})"
            )

    def is_whole_file(self) -> bool:
        """Returns True if this entry represents a whole-file hash."""
        return self.range_start == 0 and self.range_end == WHOLE_FILE_RANGE_END

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "hash_algorithm": self.hash_algorithm.value,
            "file_hash": self.file_hash,
            "last_modified_time": self.last_modified_time,
            "range_start": self.range_start,
            "range_end": self.range_end,
        }


class HashCache(CacheDB):
    """
    Class used to store and retrieve entries in the local file hash cache.

    This class is intended to always be used with a context manager to properly
    close the connection to the hash cache database.

    This class also automatically locks when doing writes, so it can be called
    by multiple threads.

    Schema (hashesV4):
        - file_path: blob (part of composite primary key)
        - hash_algorithm: text (part of composite primary key)
        - range_start: integer (part of composite primary key)
        - range_end: integer (part of composite primary key)
        - file_hash: text
        - last_modified_time: timestamp

    For whole-file hashes, range_start=0 and range_end=-1.
    For byte-range hashes, range_start and range_end define [start, end).
    """

    CACHE_NAME = "hash_cache"
    CACHE_DB_VERSION = 4

    def __init__(self, cache_dir: Optional[str] = None) -> None:
        table_name: str = f"hashesV{self.CACHE_DB_VERSION}"
        create_query: str = (
            f"CREATE TABLE {table_name}("
            "file_path blob, "
            "hash_algorithm text, "
            "range_start integer, "
            "range_end integer, "
            "file_hash text, "
            "last_modified_time timestamp, "
            "PRIMARY KEY (file_path, hash_algorithm, range_start, range_end))"
        )
        super().__init__(
            cache_name=self.CACHE_NAME,
            table_name=table_name,
            create_query=create_query,
            cache_dir=cache_dir,
        )

    def get_connection_entry(
        self,
        file_path_key: str,
        hash_algorithm: HashAlgorithm,
        connection: Any,
        range_start: int = 0,
        range_end: int = WHOLE_FILE_RANGE_END,
    ) -> Optional[HashCacheEntry]:
        """
        Returns an entry from the hash cache, if it exists.

        This is the "lockless" version of get_entry which expects a connection
        parameter for the connection which will be used to read from the DB - this can generally
        be the thread local connection returned by get_local_connection()

        Args:
            file_path_key: The file path to look up
            hash_algorithm: The hash algorithm used
            connection: SQLite connection to use
            range_start: Start byte offset (0 for whole-file)
            range_end: End byte offset (-1/WHOLE_FILE_RANGE_END for whole-file)

        Returns:
            HashCacheEntry if found, None otherwise
        """
        if not self.enabled:
            return None

        encoded_path = file_path_key.encode(encoding="utf-8", errors="surrogatepass")

        entry_vals = connection.execute(
            f"SELECT * FROM {self.table_name} "
            "WHERE file_path=? AND hash_algorithm=? AND range_start=? AND range_end=?",
            [encoded_path, hash_algorithm.value, range_start, range_end],
        ).fetchone()

        if entry_vals:
            return HashCacheEntry(
                file_path=str(entry_vals[0], encoding="utf-8", errors="surrogatepass"),
                hash_algorithm=HashAlgorithm(entry_vals[1]),
                file_hash=entry_vals[4],
                last_modified_time=str(entry_vals[5]),
                range_start=entry_vals[2],
                range_end=entry_vals[3],
            )

        return None

    def get_entry(
        self,
        file_path_key: str,
        hash_algorithm: HashAlgorithm,
        range_start: int = 0,
        range_end: int = WHOLE_FILE_RANGE_END,
    ) -> Optional[HashCacheEntry]:
        """
        Returns an entry from the hash cache, if it exists.

        Args:
            file_path_key: The file path to look up
            hash_algorithm: The hash algorithm used
            range_start: Start byte offset (0 for whole-file)
            range_end: End byte offset (-1/WHOLE_FILE_RANGE_END for whole-file)

        Returns:
            HashCacheEntry if found, None otherwise
        """
        if not self.enabled:
            return None

        with self.db_lock, self.db_connection:
            return self.get_connection_entry(
                file_path_key, hash_algorithm, self.db_connection, range_start, range_end
            )

    def put_entry(self, entry: HashCacheEntry) -> None:
        """
        Inserts or replaces an entry into the hash cache database after acquiring the lock.

        The entry's range_start and range_end determine whether this is a whole-file
        hash (range_start=0, range_end=-1) or a byte-range hash.
        """
        if self.enabled:
            with self.db_lock, self.db_connection:
                encoded_path = entry.file_path.encode(encoding="utf-8", errors="surrogatepass")

                self.db_connection.execute(
                    f"INSERT OR REPLACE INTO {self.table_name} "
                    "VALUES(:file_path, :hash_algorithm, :range_start, :range_end, "
                    ":file_hash, :last_modified_time)",
                    {
                        "file_path": encoded_path,
                        "hash_algorithm": entry.hash_algorithm.value,
                        "range_start": entry.range_start,
                        "range_end": entry.range_end,
                        "file_hash": entry.file_hash,
                        "last_modified_time": entry.last_modified_time,
                    },
                )
