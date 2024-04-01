# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for accessing the local 'last seen on S3' cache.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from .cache_db import CacheDB


logger = logging.getLogger("Deadline")


@dataclass
class S3CheckCacheEntry:
    """Represents an entry in the local s3 check cache database"""

    s3_key: str
    last_seen_time: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "s3_key": self.s3_key,
            "last_seen_time": self.last_seen_time,
        }


class S3CheckCache(CacheDB):
    """
    Maintains a cache of 'last seen on S3' entries in a local database, which
    specifies which full S3 object keys exist in the content-addressed storage
    in the Job Attachments S3 bucket.

    This class is intended to always be used with a context manager to properly
    close the connection to the hash cache database.

    This class also automatically locks when doing writes, so it can be called
    by multiple threads.
    """

    CACHE_NAME = "s3_check_cache"
    CACHE_DB_VERSION = 1
    ENTRY_EXPIRY_DAYS = 30

    def __init__(self, cache_dir: Optional[str] = None) -> None:
        table_name: str = f"s3checkV{self.CACHE_DB_VERSION}"
        create_query: str = (
            f"CREATE TABLE s3checkV{self.CACHE_DB_VERSION}(s3_key text primary key, last_seen_time timestamp)"
        )
        super().__init__(
            cache_name=self.CACHE_NAME,
            table_name=table_name,
            create_query=create_query,
            cache_dir=cache_dir,
        )

    def get_entry(self, s3_key: str) -> Optional[S3CheckCacheEntry]:
        """
        Checks if an entry exists in the cache, and returns it if it hasn't expired.
        """
        if not self.enabled:
            return None

        with self.db_lock, self.db_connection:
            entry_vals = self.db_connection.execute(
                f"SELECT * FROM {self.table_name} WHERE s3_key=?",
                [s3_key],
            ).fetchone()
            if entry_vals:
                entry = S3CheckCacheEntry(
                    s3_key=entry_vals[0],
                    last_seen_time=str(entry_vals[1]),
                )
                try:
                    last_seen = datetime.fromtimestamp(float(entry.last_seen_time))
                    if (datetime.now() - last_seen).days < self.ENTRY_EXPIRY_DAYS:
                        return entry
                except ValueError:
                    logger.warning(f"Timestamp for S3 key {s3_key} is not valid. Ignoring.")

            return None

    def put_entry(self, entry: S3CheckCacheEntry) -> None:
        """Inserts or replaces an entry into the cache database."""
        if self.enabled:
            with self.db_lock, self.db_connection:
                self.db_connection.execute(
                    f"INSERT OR REPLACE INTO {self.table_name} VALUES(:s3_key, :last_seen_time)",
                    entry.to_dict(),
                )
