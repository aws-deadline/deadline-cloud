# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Async task execution using Qt's threading primitives.

This module provides a clean pattern for running background operations
with proper Qt signal integration and automatic cancellation handling.
"""

from typing import Any, Callable, Optional

from qtpy.QtCore import QObject, QRunnable, Signal


class WorkerSignals(QObject):
    """
    Signals for QRunnable workers.

    QRunnable doesn't inherit from QObject, so we need a separate
    signals class that can be attached to the runnable.

    Signals:
        finished: Emitted when the task completes (success or failure)
        error: Emitted with the exception when the task fails
        result: Emitted with the return value when the task succeeds
    """

    finished = Signal()
    error = Signal(BaseException)
    result = Signal(object)


class AsyncTask(QRunnable):
    """
    A QRunnable that executes a callable and emits signals on completion.

    This class provides a clean way to run functions in a background thread
    while communicating results back to the main thread via Qt signals.

    The task supports cancellation - when canceled, no signals will be
    emitted even if the underlying function completes.

    Signals are emitted from the background thread, so connections should
    use Qt.QueuedConnection for thread-safe delivery to the main thread.

    Example::

        from qtpy.QtCore import Qt
        from deadline.client.ui.controllers import AsyncTask, DeadlineThreadPool

        def fetch_data():
            return api.list_farms()

        task = AsyncTask(fetch_data)
        task.signals.result.connect(handle_result, Qt.QueuedConnection)
        task.signals.error.connect(handle_error, Qt.QueuedConnection)
        DeadlineThreadPool.instance().start(task)

    Args:
        fn: The callable to execute in the background
        *args: Positional arguments for fn
        operation_id: Optional ID for tracking/cancellation
        **kwargs: Keyword arguments for fn
    """

    def __init__(
        self,
        fn: Callable[..., Any],
        *args: Any,
        operation_id: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.operation_id = operation_id
        self.signals = WorkerSignals()
        self._is_canceled = False

        # Allow thread pool to clean up automatically
        self.setAutoDelete(True)

    def cancel(self) -> None:
        """
        Mark this task as canceled.

        The task checks this flag before emitting signals, preventing
        stale results from being delivered after cancellation.

        Note that this does not interrupt the running function - it only
        prevents signal emission after the function completes.
        """
        self._is_canceled = True

    @property
    def is_canceled(self) -> bool:
        """Check if this task has been canceled."""
        return self._is_canceled

    def run(self) -> None:
        """
        Execute the task in the thread pool.

        This method runs in a background thread. All signal emissions
        are guarded by cancellation checks to prevent race conditions.

        The execution flow is:
        1. Check if canceled before starting
        2. Execute the function
        3. Check if canceled before emitting result/error
        4. Emit finished signal (if not canceled)
        """
        if self._is_canceled:
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
            if not self._is_canceled:
                self.signals.result.emit(result)
        except Exception as e:
            if not self._is_canceled:
                self.signals.error.emit(e)
        finally:
            if not self._is_canceled:
                self.signals.finished.emit()
