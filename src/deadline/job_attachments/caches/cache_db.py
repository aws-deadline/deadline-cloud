# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for defining a local cache file.
"""

import logging
import os
import threading as _threading
from abc import ABC
from threading import Lock
from typing import Optional

from ..exceptions import JobAttachmentsError
from .._utils import _retry

CONFIG_ROOT = ".deadline"
COMPONENT_NAME = "job_attachments"

logger = logging.getLogger("Deadline")


class CacheDB(ABC):
    """
    Abstract base class for connecting to a local SQLite cache database.

    This class is intended to always be used with a context manager to properly
    close the connection to the cache database.
    """

    # Number of retry attempts for SQLite operational errors (e.g., database locks)
    _RETRY_ATTEMPTS = 3

    def __init__(
        self, cache_name: str, table_name: str, create_query: str, cache_dir: Optional[str] = None
    ) -> None:
        if not cache_name or not table_name or not create_query:
            raise JobAttachmentsError("Constructor strings for CacheDB cannot be empty.")
        self.cache_name: str = cache_name
        self.table_name: str = table_name
        self.create_query: str = create_query
        self._local = _threading.local()
        self._local_connections: set = set()

        try:
            # SQLite is included in Python installers, but might not exist if building python from source.
            import sqlite3  # noqa

            self.enabled = True
        except ImportError:
            logger.warning(f"SQLite was not found, {cache_name} will not be used.")
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

            @_retry(
                ExceptionToCheck=sqlite3.OperationalError,
                tries=self._RETRY_ATTEMPTS,
                delay=(0.5, 1.5),  # Jitter between 0.5 and 1.5 seconds
                backoff=1.0,
                logger=logger.warning,
            )
            def _connect_to_db():
                """
                Connect to the SQLite database and ensure the table exists.

                Raises:
                    sqlite3.OperationalError: If there is an error connecting to the database.
                """
                connection = sqlite3.connect(self.cache_dir, check_same_thread=False)
                connection.execute("PRAGMA journal_mode=WAL")
                try:
                    # Test the connection by trying to query the table
                    connection.execute(f"SELECT * FROM {self.table_name}")
                except Exception:
                    # DB file doesn't have our table, so we need to create it
                    logger.info(
                        f"No cache entries for the current library version were found. Creating a new cache for {self.cache_name}"
                    )
                    connection.execute(self.create_query)
                return connection

            try:
                self.db_connection = _connect_to_db()
            except sqlite3.OperationalError as oe:
                raise JobAttachmentsError(
                    f"Could not access cache file after {self._RETRY_ATTEMPTS} retry attempts: {self.cache_dir}"
                ) from oe
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Called when exiting the context manager."""

        if self.enabled:
            import sqlite3

            self.db_connection.close()
            for conn in self._local_connections:
                try:
                    conn.close()
                except sqlite3.Error as e:
                    logger.warning(f"SQLite connection failed to close with error {e}")

            self._local_connections.clear()

    def get_local_connection(self):
        """Create and/or returns a thread local connection to the SQLite database."""
        if not self.enabled:
            return None
        import sqlite3

        if not hasattr(self._local, "connection"):

            @_retry(
                ExceptionToCheck=sqlite3.OperationalError,
                tries=self._RETRY_ATTEMPTS,
                delay=(0.5, 1.5),  # Jitter between 0.5 and 1.5 seconds
                backoff=1.0,
                logger=logger.warning,
            )
            def _create_local_connection():
                """
                Create a local SQLite connection.

                Raises:
                    sqlite3.OperationalError: If there is an error connecting to the database.
                """
                connection = sqlite3.connect(self.cache_dir, check_same_thread=False)
                return connection

            try:
                self._local.connection = _create_local_connection()
                self._local_connections.add(self._local.connection)
            except sqlite3.OperationalError as oe:
                raise JobAttachmentsError(
                    f"Could not create connection to cache after {self._RETRY_ATTEMPTS} retry attempts: {self.cache_dir}"
                ) from oe

        return self._local.connection

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

    def remove_cache(self) -> None:
        """
        Removes the underlying cache contents from the file system.
        """

        if self.enabled:
            import sqlite3

            self.db_connection.close()
            conn_list = list(self._local_connections)
            for conn in conn_list:
                try:
                    conn.close()
                    self._local_connections.remove(conn)
                except sqlite3.Error as e:
                    logger.warning(f"SQLite connection failed to close with error {e}")

        logger.debug(f"The cache {self.cache_dir} will be removed")
        try:
            os.remove(self.cache_dir)
        except Exception as e:
            logger.error(f"Error occurred while removing the cache file {self.cache_dir}: {e}")

            raise e
