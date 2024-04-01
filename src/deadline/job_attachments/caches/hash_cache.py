# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for accessing the local file hash cache.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from .cache_db import CacheDB
from ..asset_manifests.hash_algorithms import HashAlgorithm


logger = logging.getLogger("Deadline")


@dataclass
class HashCacheEntry:
    """Represents an entry in the local hash-cache database"""

    file_path: str
    hash_algorithm: HashAlgorithm
    file_hash: str
    last_modified_time: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_path": self.file_path,
            "hash_algorithm": self.hash_algorithm.value,
            "file_hash": self.file_hash,
            "last_modified_time": self.last_modified_time,
        }


class HashCache(CacheDB):
    """
    Class used to store and retrieve entries in the local file hash cache.

    This class is intended to always be used with a context manager to properly
    close the connection to the hash cache database.

    This class also automatically locks when doing writes, so it can be called
    by multiple threads.
    """

    CACHE_NAME = "hash_cache"
    CACHE_DB_VERSION = 2

    def __init__(self, cache_dir: Optional[str] = None) -> None:
        table_name: str = f"hashesV{self.CACHE_DB_VERSION}"
        create_query: str = (
            f"CREATE TABLE hashesV{self.CACHE_DB_VERSION}(file_path text primary key, hash_algorithm text secondary key, file_hash text, last_modified_time timestamp)"
        )
        super().__init__(
            cache_name=self.CACHE_NAME,
            table_name=table_name,
            create_query=create_query,
            cache_dir=cache_dir,
        )

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
                f"SELECT * FROM {self.table_name} WHERE file_path=? AND hash_algorithm=?",
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
                    f"INSERT OR REPLACE INTO {self.table_name} VALUES(:file_path, :hash_algorithm, :file_hash, :last_modified_time)",
                    entry.to_dict(),
                )
