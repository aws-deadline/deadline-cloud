# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
High-level async task management with automatic cancellation.
"""

from logging import getLogger
from typing import Any, Callable, Dict, Optional

from qtpy.QtCore import QObject, Qt, Signal

from ._async_task import AsyncTask
from ._thread_pool import DeadlineThreadPool


logger = getLogger(__name__)


class AsyncTaskRunner(QObject):
    """
    Manages async task execution with automatic cancellation of superseded operations.

    When a new task is started with the same operation_key, any previous task
    with that key is automatically canceled. This prevents race conditions
    where an older, slower request returns after a newer one.

    All signal connections use Qt.QueuedConnection to ensure thread-safe
    delivery from background threads to the main thread.

    Example::

        from deadline.client.ui.controllers import AsyncTaskRunner

        runner = AsyncTaskRunner()
        runner.run(
            operation_key="list_farms",
            fn=api.list_farms,
            on_success=self._handle_farms,
            on_error=self._handle_error,
            config=config,  # kwargs passed to fn
        )

    Signals:
        task_error: Emitted when any task encounters an error.
                   Args: (operation_key, exception)
    """

    task_error = Signal(str, BaseException)

    def __init__(self, parent: Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._thread_pool = DeadlineThreadPool.instance()
        self._active_tasks: Dict[str, AsyncTask] = {}
        self._operation_counter = 0

    def run(
        self,
        operation_key: str,
        fn: Callable[..., Any],
        on_success: Optional[Callable[[Any], None]] = None,
        on_error: Optional[Callable[[BaseException], None]] = None,
        *args: Any,
        **kwargs: Any,
    ) -> int:
        """
        Run a function asynchronously in the dedicated thread pool.

        Args:
            operation_key: Unique key identifying this operation type.
                          If a task with the same key is running, it
                          will be canceled before starting the new one.
            fn: The function to execute in the background
            on_success: Callback invoked with fn's return value on success.
                       Called in the main thread via queued connection.
            on_error: Callback invoked with the exception on failure.
                     Called in the main thread via queued connection.
            *args: Positional arguments passed to fn
            **kwargs: Keyword arguments passed to fn

        Returns:
            Operation ID for tracking this specific task instance
        """
        # Cancel any existing task with the same key
        self.cancel(operation_key)

        # Generate unique operation ID
        self._operation_counter += 1
        operation_id = self._operation_counter

        # Create the task
        task = AsyncTask(fn, *args, operation_id=operation_id, **kwargs)

        # Connect success callback with queued connection for thread safety
        if on_success is not None:
            task.signals.result.connect(on_success, Qt.QueuedConnection)

        # Connect error callbacks
        if on_error is not None:
            task.signals.error.connect(on_error, Qt.QueuedConnection)

        # Always emit to our error signal for centralized logging
        # Use a closure to capture operation_key
        def emit_task_error(e: BaseException) -> None:
            self.task_error.emit(operation_key, e)

        task.signals.error.connect(emit_task_error, Qt.QueuedConnection)

        # Clean up tracking when task finishes
        # Use a closure to capture operation_key
        def cleanup() -> None:
            self._active_tasks.pop(operation_key, None)

        task.signals.finished.connect(cleanup, Qt.QueuedConnection)

        # Track and start
        self._active_tasks[operation_key] = task
        self._thread_pool.start(task)

        logger.debug(f"Started async task: {operation_key} (id={operation_id})")
        return operation_id

    def cancel(self, operation_key: str) -> bool:
        """
        Cancel a running task by operation key.

        Args:
            operation_key: The key of the operation to cancel

        Returns:
            True if a task was canceled, False if no task was running
        """
        task = self._active_tasks.pop(operation_key, None)
        if task is not None:
            task.cancel()
            logger.debug(f"Canceled async task: {operation_key}")
            return True
        return False

    def cancel_all(self) -> int:
        """
        Cancel all running tasks.

        Returns:
            Number of tasks that were canceled
        """
        count = len(self._active_tasks)
        for task in self._active_tasks.values():
            task.cancel()
        self._active_tasks.clear()
        if count > 0:
            logger.debug(f"Canceled {count} async tasks")
        return count

    def is_running(self, operation_key: str) -> bool:
        """Check if a task with the given key is currently running."""
        return operation_key in self._active_tasks

    @property
    def active_task_count(self) -> int:
        """Number of currently active tasks."""
        return len(self._active_tasks)
