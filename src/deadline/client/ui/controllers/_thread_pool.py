# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Dedicated thread pool for Deadline Cloud UI operations.

Using a dedicated pool prevents Deadline API calls from competing
with other Qt background work in the application.
"""

from typing import Optional

from qtpy.QtCore import QThreadPool


class DeadlineThreadPool:
    """
    Singleton thread pool dedicated to Deadline Cloud operations.

    This isolates Deadline API calls from other application threads,
    ensuring consistent performance and easier debugging.

    The pool is created lazily on first access and configured with
    a reasonable default thread count for API operations.

    Example::

        from deadline.client.ui.controllers import DeadlineThreadPool

        pool = DeadlineThreadPool.instance()
        pool.start(my_runnable)
    """

    _instance: Optional[QThreadPool] = None

    # Default configuration - reasonable for concurrent API calls
    DEFAULT_MAX_THREADS = 4

    @classmethod
    def instance(cls) -> QThreadPool:
        """
        Get the singleton thread pool instance.

        Creates the pool on first access with default configuration.

        Returns:
            The shared QThreadPool instance for Deadline operations.
        """
        if cls._instance is None:
            cls._instance = QThreadPool()
            cls._instance.setMaxThreadCount(cls.DEFAULT_MAX_THREADS)
        return cls._instance

    @classmethod
    def set_max_threads(cls, count: int) -> None:
        """
        Configure the maximum number of concurrent threads.

        Args:
            count: Maximum thread count (must be >= 1)

        Raises:
            ValueError: If count is less than 1
        """
        if count < 1:
            raise ValueError("Thread count must be at least 1")
        cls.instance().setMaxThreadCount(count)

    @classmethod
    def active_thread_count(cls) -> int:
        """
        Get the number of currently active threads.

        Returns:
            Number of threads currently executing tasks.
        """
        if cls._instance is None:
            return 0
        return cls._instance.activeThreadCount()

    @classmethod
    def shutdown(cls, wait_for_done: bool = True, timeout_ms: int = 5000) -> bool:
        """
        Shutdown the thread pool.

        Args:
            wait_for_done: If True, wait for running tasks to complete
            timeout_ms: Maximum time to wait in milliseconds

        Returns:
            True if all tasks completed (or no pool exists),
            False if timeout occurred while waiting
        """
        if cls._instance is not None:
            if wait_for_done:
                result = cls._instance.waitForDone(timeout_ms)
                cls._instance = None
                return result
            cls._instance.clear()
            cls._instance = None
        return True

    @classmethod
    def reset(cls) -> None:
        """
        Reset the thread pool instance.

        This clears any pending tasks and destroys the pool.
        A new pool will be created on next access.
        Primarily useful for testing.
        """
        if cls._instance is not None:
            cls._instance.clear()
            cls._instance.waitForDone(1000)
            cls._instance = None
