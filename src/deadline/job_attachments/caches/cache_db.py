# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for defining a local cache file.
"""

import logging
import os
from abc import ABC
from threading import Lock
from typing import Optional

from ..exceptions import JobAttachmentsError

CONFIG_ROOT = ".deadline"
COMPONENT_NAME = "job_attachments"

logger = logging.getLogger("Deadline")


class CacheDB(ABC):
    """
    Abstract base class for connecting to a local SQLite cache database.

    This class is intended to always be used with a context manager to properly
    close the connection to the cache database.
    """

    def __init__(
        self, cache_name: str, table_name: str, create_query: str, cache_dir: Optional[str] = None
    ) -> None:
        if not cache_name or not table_name or not create_query:
            raise JobAttachmentsError("Constructor strings for CacheDB cannot be empty.")
        self.cache_name: str = cache_name
        self.table_name: str = table_name
        self.create_query: str = create_query

        try:
            # SQLite is included in Python installers, but might not exist if building python from source.
            import sqlite3  # noqa

            self.enabled = True
        except ImportError:
            logger.warn(f"SQLite was not found, {cache_name} will not be used.")
            self.enabled = False
            return

        if cache_dir is None:
            cache_dir = self.get_default_cache_db_file_dir()
        if cache_dir is None:
            raise JobAttachmentsError(
                f"No default cache path found. Please provide a directory for {self.cache_name}."
            )
        os.makedirs(cache_dir, exist_ok=True)
        self.cache_dir: str = os.path.join(cache_dir, f"{self.cache_name}.db")
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
                    f"Could not access cache file in {self.cache_dir}"
                ) from oe

            try:
                self.db_connection.execute(f"SELECT * FROM {self.table_name}")
            except Exception:
                # DB file doesn't have our table, so we need to create it
                logger.info(
                    f"No cache entries for the current library version were found. Creating a new cache for {self.cache_name}"
                )
                self.db_connection.execute(self.create_query)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Called when exiting the context manager."""
        if self.enabled:
            self.db_connection.close()

    @classmethod
    def get_default_cache_db_file_dir(cls) -> Optional[str]:
        """
        Gets the expected directory for the cache database file based on OS environment variables.
        If a directory cannot be found, defaults to the working directory.
        """
        default_path = os.environ.get("HOME")
        if default_path:
            default_path = os.path.join(default_path, CONFIG_ROOT, COMPONENT_NAME)
        return default_path
