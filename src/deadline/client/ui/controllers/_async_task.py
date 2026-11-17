# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Async task execution using Qt's threading primitives.

This module provides a clean pattern for running background operations
with proper Qt signal integration and automatic cancellation handling.
"""

from logging import getLogger
from typing import Any, Callable, Iterator, Optional

from qtpy.QtCore import QCoreApplication, QObject, QRunnable, QThread, Signal


logger = getLogger(__name__)

# Substrings of the RuntimeError each Qt binding raises when emitting from a deleted
# signal source:
#   PySide6: "Signal source has been deleted"
#   PyQt5:   "wrapped C/C++ object of type WorkerSignals has been deleted"
#   PySide2: "Internal C++ object (WorkerSignals) already deleted." (from shiboken2)
_DELETED_SOURCE_MESSAGES = ("has been deleted", "already deleted")


def _is_deleted_source_error(exc: RuntimeError) -> bool:
    message = str(exc)
    return any(marker in message for marker in _DELETED_SOURCE_MESSAGES)


class WorkerSignals(QObject):
    """
    Signals for QRunnable workers.

    QRunnable doesn't inherit from QObject, so we need a separate
    signals class that can be attached to the runnable.

    Signals:
        finished: Emitted when the task completes (success or failure)
        error: Emitted with the exception when the task fails
        result: Emitted with the return value when the task succeeds
        progress: Emitted with an intermediate value for progressive/streaming
                  tasks. A streaming worker (one that consumes a generator) emits
                  this once per item as the item arrives, then emits ``result``
                  with a terminal value once the generator is exhausted. Single
                  result tasks never emit ``progress``.
    """

    finished = Signal()
    error = Signal(BaseException)
    result = Signal(object)
    progress = Signal(object)


def _create_worker_signals() -> WorkerSignals:
    """Creates a task's WorkerSignals, owned by Qt rather than by Python.

    A Python-owned QObject is destroyed wherever its last Python reference is dropped.
    For a task's signals that is often the pool thread, which releases the task after
    run() returns, or a garbage collection that happens to run on that thread. Qt
    objects must be destroyed on the thread they belong to, so the signals are parented
    to the application object instead, and the task deletes them with deleteLater once
    it is done. The parent must live on the current thread, so this applies only when
    the task is created on the application's thread, as it is from the GUI.

    The deletion is delivered by the application's event loop. Until then, and for a
    task that is never run, for example one removed by QThreadPool.clear(), the small
    signals object stays attached to the application.
    """
    app = QCoreApplication.instance()
    if app is not None and QThread.currentThread() is app.thread():
        return WorkerSignals(app)
    return WorkerSignals()


class AsyncTask(QRunnable):
    """
    A QRunnable that executes a callable and emits signals on completion.

    This class provides a clean way to run functions in a background thread
    while communicating results back to the main thread via Qt signals.

    The task supports cancellation - when canceled, no signals will be
    emitted even if the underlying function completes.

    Signals are emitted from the background thread, so connections should
    use Qt.QueuedConnection for thread-safe delivery to the main thread.

    Subclasses override ``_execute``, not ``run``: ``run`` also deletes the
    signals object on the main thread once the work is done.

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
        self.signals = _create_worker_signals()
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

    def _safe_emit(self, signal_name: str, *args: Any) -> None:
        """
        Emit ``self.signals.<signal_name>`` unless the task was canceled or the
        signal source has already been deleted.

        ``run()`` executes in a background thread and may still be in-flight when
        the owning runner (or the widget it is parented to) is destroyed. When that
        happens the underlying ``WorkerSignals`` C++ object is deleted out from under
        us, and touching/emitting it raises a ``RuntimeError`` whose message depends
        on the Qt binding (see ``_DELETED_SOURCE_MESSAGES``). A deleted source has no live listeners, so there is nothing to
        deliver - we log at debug level and move on instead of letting the exception
        cascade through the result/error/finished emissions.
        """
        if self._is_canceled:
            return
        try:
            getattr(self.signals, signal_name).emit(*args)
        except RuntimeError as exc:
            # For a DirectConnection, emit() invokes slots synchronously, so a
            # RuntimeError may originate in a slot body rather than from a deleted
            # signal source. Only swallow the "source deleted" case (nothing is
            # listening anymore); re-raise anything else so genuine slot bugs and
            # failed error/finished deliveries aren't silently lost.
            if not _is_deleted_source_error(exc):
                raise
            logger.debug("Skipping '%s' emit; signal source has been deleted", signal_name)

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

        Emissions are routed through :meth:`_safe_emit`, which additionally
        tolerates the signal source being deleted mid-flight (e.g. when the
        owning runner/widget is torn down before a slow task returns).

        Whether or not the task ran, its signals are then scheduled for deletion
        on their own thread, after the signals already emitted are delivered.
        """
        try:
            self._execute()
        finally:
            self._release_signals()

    def _release_signals(self) -> None:
        # deleteLater is thread-safe: it posts the deletion to the thread the signals
        # belong to, behind the queued emissions this task already made.
        try:
            self.signals.deleteLater()
        except RuntimeError:
            # Already deleted, e.g. with the application object.
            pass

    def _execute(self) -> None:
        if self._is_canceled:
            return

        try:
            result = self.fn(*self.args, **self.kwargs)
            self._safe_emit("result", result)
        except Exception as e:
            self._safe_emit("error", e)
        finally:
            self._safe_emit("finished")


class StreamingAsyncTask(AsyncTask):
    """
    A QRunnable that consumes a generator and emits a ``progress`` signal per item.

    This is the progressive-result variant of :class:`AsyncTask`. The wrapped
    callable must return an iterable/generator; each yielded item is delivered via
    ``signals.progress`` (in arrival order, which for a fan-out generator is the
    order regions complete, not request order). Once the generator is exhausted the
    terminal ``signals.result`` is emitted (with ``None``) so existing
    finished/result wiring still fires exactly once at the end.

    Like the base class, all emissions are guarded by the cancellation flag so a
    superseded refresh delivers no stale partial updates.

    Args:
        fn: A callable returning an iterator/generator to consume in the background.
        *args: Positional arguments for fn
        operation_id: Optional ID for tracking/cancellation
        **kwargs: Keyword arguments for fn
    """

    def _execute(self) -> None:
        """
        Execute the streaming task in the thread pool.

        Runs in a background thread. Emits ``progress`` per yielded item, a single
        terminal ``result`` once exhausted, ``error`` if the generator raises, and
        ``finished`` at the end. All emissions are guarded by cancellation checks
        and tolerate the signal source being deleted mid-flight (see
        :meth:`AsyncTask._safe_emit`). :meth:`AsyncTask.run` releases the signals
        afterwards.
        """
        if self._is_canceled:
            return

        try:
            iterator: Iterator[Any] = iter(self.fn(*self.args, **self.kwargs))
            for item in iterator:
                if self._is_canceled:
                    return
                self._safe_emit("progress", item)
            # Terminal result (no aggregate payload; progress already delivered each item).
            self._safe_emit("result", None)
        except Exception as e:
            self._safe_emit("error", e)
        finally:
            self._safe_emit("finished")
