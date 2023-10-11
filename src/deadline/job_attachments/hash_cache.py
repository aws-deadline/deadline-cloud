# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Module for accessing the local file hash cache.
"""

import logging
import os
import sqlite3
import threading
import time
import random
from typing import Optional

from .exceptions import JobAttachmentsError
from .models import HashCacheEntry
from ._utils import _get_default_hash_cache_db_file_dir

CACHE_FILE_NAME = "hash_cache.db"

logger = logging.getLogger("Deadline")


class FileLocking:
    """
    Handles file locking.
    File locking is necessary to control access to the HashCache database across processes.
    multiprocessing.Lock only works between processes that were spawned through multiprocessing itself.
    threading.Lock does not handle access to the database across proccess, producing "database is locked" errors
    """

    MAX_RETRY_ATTEMPTS = 10
    MAX_BACKOFF_SECONDS = 1.0

    def __init__(self, db_lock_file_path):
        self._db_lock_file_path = db_lock_file_path
        self._db_lock_file = os.open(self._db_lock_file_path, os.O_APPEND | os.O_CREAT)

    def __del__(self):
        os.close(self._db_lock_file)

    def acquire(self):
        attempts = 0
        while True:
            try:
                if os.name == "posix":
                    import fcntl

                    fcntl.flock(self._db_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)  # type: ignore[attr-defined]
                elif os.name == "nt":
                    import msvcrt

                    msvcrt.locking(self._db_lock_file, msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
                else:
                    raise RuntimeError("File locking not supported in current platform")
                break
            except (PermissionError, OSError):
                attempts += 1
                if attempts > self.MAX_RETRY_ATTEMPTS:
                    raise
                time.sleep(random.uniform(0.0, self.MAX_BACKOFF_SECONDS))

    def release(self):
        if os.name == "posix":
            import fcntl

            fcntl.flock(self._db_lock_file, fcntl.LOCK_UN)  # type: ignore[attr-defined]
        elif os.name == "nt":
            import msvcrt

            msvcrt.locking(self._db_lock_file, msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
        else:
            raise RuntimeError("File unlocking not supported in current platform")


class DbConnection:
    """
    Handles a sqlite3.Connection. This class is needed to tie the create/close connection to
    the lifetime of the object (necessary to store it in a thread local object).
    Basically, a RAII class around sqlite3.Connection.
    """

    def __init__(self, cache_db_file):
        try:
            # Using immediate isolation level and increasing the timeout so multiple threads can
            # operate at the same time without erroring out so quickly
            self.con: sqlite3.Connection = sqlite3.connect(
                cache_db_file,
                check_same_thread=True,
                isolation_level="IMMEDIATE",
                timeout=30,
            )
            self.con.execute("pragma journal_mode=wal")
        except sqlite3.OperationalError as oe:
            raise JobAttachmentsError(
                f"Could not access hash cache file in {cache_db_file}"
            ) from oe

    def __del__(self):
        self.con.close()


class ReadDbCursor:
    """
    Class to use the sqlite cursor with context manager (not supported by default). This class is meant
    to be used with queries that only do reads on the database.
    """

    # We maintain a connection per thread, we reuse it and maintain it for the lifetime of the thread.
    # Most compilations of sqlite3 have threadsafety=1 (SQLITE_THREADSAFE=2) which allows to share the
    # module, but connections cannot be shared between threads. This code is compatible with
    # threadsafety=1 and threadsafety=3.
    assert sqlite3.threadsafety in (1, 3)

    thread_local_connection = threading.local()

    @staticmethod
    def _get_db_con(cache_db_file) -> sqlite3.Connection:
        ReadDbCursor.thread_local_connection
        if "db" not in ReadDbCursor.thread_local_connection.__dict__:
            ReadDbCursor.thread_local_connection.__dict__["db"] = DbConnection(cache_db_file)
        return ReadDbCursor.thread_local_connection.db.con

    def __init__(self, db_file_path: str):
        self._db_lock_file = f"{db_file_path}.lock"
        self._db_con = ReadDbCursor._get_db_con(db_file_path)

    def __enter__(self):
        self._db_cur = self._db_con.cursor()
        return self._db_cur

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._db_cur.close()


class WriteDbCursor(ReadDbCursor):
    """
    Class to use the sqlite cursor with context manager (not supported by default)
    """

    # to reduce file locks (since they hare a bit more expensive and only needed per process), we will take
    # a file lock only once per process. We sync threads within the process with a ref counter
    file_locking = None
    ref_counter = 0
    lock = threading.Lock()

    def __init__(self, db_file_path: str):
        super().__init__(db_file_path)

    def __enter__(self):
        with WriteDbCursor.lock:
            if WriteDbCursor.ref_counter == 0:
                # first thread and instance of WriteDbCursor, initialize file locking
                WriteDbCursor.file_locking = FileLocking(self._db_lock_file)
                WriteDbCursor.file_locking.acquire()
            WriteDbCursor.ref_counter += 1
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._db_con.commit()
        super().__exit__(exc_type, exc_value, exc_traceback)
        with WriteDbCursor.lock:
            WriteDbCursor.ref_counter -= 1
            if WriteDbCursor.ref_counter == 0:
                WriteDbCursor.file_locking.release()
                del WriteDbCursor.file_locking


class HashCache:
    """
    Class used to store and retrieve entries in the local file hash cache.

    This class handles multithreading and multiprocessing by creating a connection per
    thread, using cursors, committing immediately, enabling WAL and doing file locking
    per process.

    This class can be used with context manager to close all database connections on
    exit. Closing on exit is optional and enabled by default. If false, connections will
    be closed on thread destruction.
    """

    def __init__(self, cache_dir: Optional[str] = None, close_on_exit: bool = True) -> None:
        self.enabled = False
        self.close_on_exit = close_on_exit
        self.cache_dir = cache_dir
        try:
            # SQLite is included in Python installers, but might not exist if building python from source.
            import sqlite3  # noqa

            self.enabled = True
        except ImportError:
            logger.warn("SQLite was not found, the Hash Cache will not be used.")
            return

        if self.cache_dir is None:
            self.cache_dir = _get_default_hash_cache_db_file_dir()
        if self.cache_dir is None:
            raise JobAttachmentsError(
                "No default hash cache path found. Please provide a hash cache directory."
            )
        os.makedirs(self.cache_dir, exist_ok=True)
        self.cache_db_file: str = os.path.join(self.cache_dir, CACHE_FILE_NAME)

        with WriteDbCursor(self.cache_db_file) as cur:
            cur.execute(
                "CREATE TABLE IF NOT EXISTS hashesV1(file_path text primary key, file_hash text, last_modified_time timestamp)"
            )

    def __enter__(self):
        """Called when entering the context manager."""
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Called when exiting the context manager."""
        if self.close_on_exit:
            ReadDbCursor.thread_local_connection.__dict__.clear()
        return

    def get_entry(self, file_path_key: str) -> Optional[HashCacheEntry]:
        """Returns an entry from the hash cache, if it exists."""
        if not self.enabled:
            return None

        with ReadDbCursor(self.cache_db_file) as cur:
            entry_vals = cur.execute(
                "SELECT * FROM hashesV1 WHERE file_path=?", [file_path_key]
            ).fetchone()
        if entry_vals:
            return HashCacheEntry(
                file_path=entry_vals[0],
                file_hash=entry_vals[1],
                last_modified_time=str(entry_vals[2]),
            )
        else:
            return None

    def put_entry(self, entry: HashCacheEntry) -> None:
        """Inserts or replaces an entry into the hash cache database after acquiring the lock."""
        if not self.enabled:
            return

        with WriteDbCursor(self.cache_db_file) as cur:
            cur.execute(
                "INSERT OR REPLACE INTO hashesV1 VALUES(:file_path, :file_hash, :last_modified_time)",
                entry.to_dict(),
            )
