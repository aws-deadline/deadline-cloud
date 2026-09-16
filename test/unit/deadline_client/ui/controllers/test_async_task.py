# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for AsyncTask and WorkerSignals classes.
"""

import pytest
from unittest.mock import Mock

try:
    from deadline.client.ui.controllers._async_task import (
        AsyncTask,
        StreamingAsyncTask,
        WorkerSignals,
    )
except ImportError:
    pytest.importorskip("deadline.client.ui.controllers._async_task")


class TestWorkerSignals:
    """Tests for WorkerSignals class."""

    def test_signals_exist(self, qtbot):
        """Test that all expected signals are defined."""
        signals = WorkerSignals()

        assert hasattr(signals, "finished")
        assert hasattr(signals, "error")
        assert hasattr(signals, "result")

    def test_result_signal_emits(self, qtbot):
        """Test that result signal can be emitted and received."""
        signals = WorkerSignals()

        received = []
        signals.result.connect(lambda x: received.append(x))

        signals.result.emit({"test": "data"})

        assert len(received) == 1
        assert received[0] == {"test": "data"}

    def test_error_signal_emits(self, qtbot):
        """Test that error signal can be emitted and received."""
        signals = WorkerSignals()

        received = []
        signals.error.connect(lambda x: received.append(x))

        test_error = ValueError("test error")
        signals.error.emit(test_error)

        assert len(received) == 1
        assert received[0] is test_error

    def test_finished_signal_emits(self, qtbot):
        """Test that finished signal can be emitted and received."""
        signals = WorkerSignals()

        finished_called = []
        signals.finished.connect(lambda: finished_called.append(True))

        signals.finished.emit()

        assert len(finished_called) == 1


class TestAsyncTask:
    """Tests for AsyncTask class."""

    def test_init_stores_function_and_args(self):
        """Test that constructor stores function and arguments."""
        fn = Mock()
        task = AsyncTask(fn, "arg1", "arg2", kwarg1="value1")

        assert task.fn is fn
        assert task.args == ("arg1", "arg2")
        assert task.kwargs == {"kwarg1": "value1"}

    def test_init_with_operation_id(self):
        """Test that operation_id is stored."""
        fn = Mock()
        task = AsyncTask(fn, operation_id=42)

        assert task.operation_id == 42

    def test_init_default_operation_id_is_none(self):
        """Test that operation_id defaults to None."""
        fn = Mock()
        task = AsyncTask(fn)

        assert task.operation_id is None

    def test_cancel_sets_flag(self):
        """Test that cancel() sets the canceled flag."""
        fn = Mock()
        task = AsyncTask(fn)

        assert task.is_canceled is False
        task.cancel()
        assert task.is_canceled is True

    def test_run_executes_function(self):
        """Test that run() executes the function with correct arguments."""
        fn = Mock(return_value="result")
        task = AsyncTask(fn, "arg1", kwarg1="value1")

        task.run()

        fn.assert_called_once_with("arg1", kwarg1="value1")

    def test_run_emits_result_on_success(self, qtbot):
        """Test that run() emits result signal on success."""
        fn = Mock(return_value={"data": "test"})
        task = AsyncTask(fn)

        received = []
        task.signals.result.connect(lambda x: received.append(x))

        task.run()

        assert len(received) == 1
        assert received[0] == {"data": "test"}

    def test_run_emits_error_on_exception(self, qtbot):
        """Test that run() emits error signal on exception."""
        test_error = ValueError("test error")
        fn = Mock(side_effect=test_error)
        task = AsyncTask(fn)

        received = []
        task.signals.error.connect(lambda x: received.append(x))

        task.run()

        assert len(received) == 1
        assert received[0] is test_error

    def test_run_emits_finished_on_success(self, qtbot):
        """Test that run() emits finished signal on success."""
        fn = Mock(return_value="result")
        task = AsyncTask(fn)

        finished_called = []
        task.signals.finished.connect(lambda: finished_called.append(True))

        task.run()

        assert len(finished_called) == 1

    def test_run_emits_finished_on_error(self, qtbot):
        """Test that run() emits finished signal even on error."""
        fn = Mock(side_effect=ValueError("error"))
        task = AsyncTask(fn)

        finished_called = []
        task.signals.finished.connect(lambda: finished_called.append(True))

        task.run()

        assert len(finished_called) == 1

    def test_run_does_not_execute_if_canceled_before_start(self, qtbot):
        """Test that run() does nothing if task was canceled before starting."""
        fn = Mock(return_value="result")
        task = AsyncTask(fn)

        received = []
        task.signals.result.connect(lambda x: received.append(x))

        task.cancel()
        task.run()

        fn.assert_not_called()
        assert len(received) == 0

    def test_run_does_not_emit_result_if_canceled_during_execution(self, qtbot):
        """Test that run() does not emit result if canceled during execution."""

        def slow_fn():
            # Simulate cancellation during execution
            task.cancel()
            return "result"

        task = AsyncTask(slow_fn)

        received = []
        task.signals.result.connect(lambda x: received.append(x))

        task.run()

        assert len(received) == 0

    def test_run_does_not_emit_error_if_canceled_during_execution(self, qtbot):
        """Test that run() does not emit error if canceled during execution."""

        def failing_fn():
            task.cancel()
            raise ValueError("error")

        task = AsyncTask(failing_fn)

        received = []
        task.signals.error.connect(lambda x: received.append(x))

        task.run()

        assert len(received) == 0

    def test_auto_delete_is_enabled(self):
        """Test that autoDelete is set to True."""
        fn = Mock()
        task = AsyncTask(fn)

        assert task.autoDelete() is True

    def test_run_swallows_deleted_signal_source_on_result(self, qtbot):
        """
        If the signals object is deleted while the task is running, emitting the
        result must not raise - it should be swallowed (no listeners remain).

        This reproduces the "RuntimeError: Signal source has been deleted"
        cascade that occurs when the owning runner/widget is torn down before a
        slow task returns.
        """
        fn = Mock(return_value="result")
        task = AsyncTask(fn)

        # Simulate the WorkerSignals C++ object having been deleted: any access to
        # a signal raises RuntimeError, mirroring PySide behavior.
        deleted_signals = Mock()
        deleted_signals.result.emit.side_effect = RuntimeError("Signal source has been deleted")
        deleted_signals.error.emit.side_effect = RuntimeError("Signal source has been deleted")
        deleted_signals.finished.emit.side_effect = RuntimeError("Signal source has been deleted")
        task.signals = deleted_signals

        # Must not raise despite every emit failing.
        task.run()

        deleted_signals.result.emit.assert_called_once_with("result")
        # finished is always attempted in the finally block.
        deleted_signals.finished.emit.assert_called_once_with()

    def test_run_swallows_deleted_signal_source_on_error(self, qtbot):
        """A deleted signal source during error emission must not raise."""
        fn = Mock(side_effect=ValueError("boom"))
        task = AsyncTask(fn)

        deleted_signals = Mock()
        deleted_signals.error.emit.side_effect = RuntimeError("Signal source has been deleted")
        deleted_signals.finished.emit.side_effect = RuntimeError("Signal source has been deleted")
        task.signals = deleted_signals

        # Must not raise despite the emits failing.
        task.run()

        deleted_signals.error.emit.assert_called_once()
        deleted_signals.finished.emit.assert_called_once_with()

    def test_safe_emit_does_nothing_when_canceled(self):
        """_safe_emit is a no-op for canceled tasks and never touches the signal."""
        task = AsyncTask(Mock())
        signals = Mock()
        task.signals = signals

        task.cancel()
        task._safe_emit("result", "value")

        signals.result.emit.assert_not_called()

    def test_safe_emit_swallows_deleted_source(self):
        """A deleted signal source is swallowed (nothing is listening)."""
        task = AsyncTask(Mock())
        signals = Mock()
        signals.result.emit.side_effect = RuntimeError("Signal source has been deleted")
        task.signals = signals

        task._safe_emit("result", "value")  # must not raise

    def test_safe_emit_reraises_real_slot_error(self):
        """A RuntimeError from a slot body (DirectConnection runs slots inside
        emit()) must surface rather than be downgraded to a debug log."""
        task = AsyncTask(Mock())
        signals = Mock()
        signals.result.emit.side_effect = RuntimeError("boom in slot")
        task.signals = signals

        with pytest.raises(RuntimeError, match="boom in slot"):
            task._safe_emit("result", "value")


class TestStreamingAsyncTask:
    """Tests for StreamingAsyncTask, the progressive-result variant of AsyncTask."""

    def test_run_emits_progress_per_item_then_terminal_result(self, qtbot):
        """Each yielded item is emitted via progress; a single terminal result follows."""
        task = StreamingAsyncTask(lambda: iter(["a", "b", "c"]))

        progress = []
        results = []
        finished = []
        task.signals.progress.connect(lambda x: progress.append(x))
        task.signals.result.connect(lambda x: results.append(x))
        task.signals.finished.connect(lambda: finished.append(True))

        task.run()

        assert progress == ["a", "b", "c"]
        # Terminal result carries no aggregate payload (progress already delivered items).
        assert results == [None]
        assert finished == [True]

    def test_run_does_not_execute_if_canceled_before_start(self, qtbot):
        """A task canceled before start emits nothing and does not consume the generator."""
        consumed = []

        def gen():
            consumed.append("started")
            yield "a"

        task = StreamingAsyncTask(gen)

        progress = []
        task.signals.progress.connect(lambda x: progress.append(x))

        task.cancel()
        task.run()

        assert consumed == []
        assert progress == []

    def test_run_stops_emitting_progress_after_cancel_mid_stream(self, qtbot):
        """
        The in-loop cancellation guard: once canceled mid-stream, no further progress
        items and no terminal result are emitted (no stale partial updates after a
        superseded refresh). This directly exercises the per-item _is_canceled check.
        """
        progress = []
        results = []
        finished = []

        def gen():
            yield "a"
            # Cancel after the first item is yielded; the loop must not emit "b"/"c".
            task.cancel()
            yield "b"
            yield "c"

        task = StreamingAsyncTask(gen)
        task.signals.progress.connect(lambda x: progress.append(x))
        task.signals.result.connect(lambda x: results.append(x))
        task.signals.finished.connect(lambda: finished.append(True))

        task.run()

        # Only the item yielded before cancellation was delivered.
        assert progress == ["a"]
        # No terminal result and no finished emission after cancellation.
        assert results == []
        assert finished == []

    def test_run_does_not_emit_error_if_canceled_during_execution(self, qtbot):
        """A generator that raises after cancellation must not emit an error signal."""

        def gen():
            yield "a"
            task.cancel()
            raise ValueError("boom")

        task = StreamingAsyncTask(gen)

        errors = []
        task.signals.error.connect(lambda e: errors.append(e))

        task.run()

        assert errors == []

    def test_run_emits_error_when_generator_raises(self, qtbot):
        """A generator that raises (without cancellation) routes to the error signal."""
        test_error = ValueError("boom")

        def gen():
            yield "a"
            raise test_error

        task = StreamingAsyncTask(gen)

        progress = []
        errors = []
        task.signals.progress.connect(lambda x: progress.append(x))
        task.signals.error.connect(lambda e: errors.append(e))

        task.run()

        assert progress == ["a"]
        assert errors == [test_error]

    def test_run_swallows_deleted_signal_source(self, qtbot):
        """
        If the signals object is deleted while the streaming task is running,
        emitting progress/result/finished must not raise.
        """
        task = StreamingAsyncTask(lambda: iter(["a", "b"]))

        deleted_signals = Mock()
        deleted_signals.progress.emit.side_effect = RuntimeError("Signal source has been deleted")
        deleted_signals.result.emit.side_effect = RuntimeError("Signal source has been deleted")
        deleted_signals.finished.emit.side_effect = RuntimeError("Signal source has been deleted")
        task.signals = deleted_signals

        # Must not raise despite every emit failing.
        task.run()

        assert deleted_signals.progress.emit.call_count == 2
        deleted_signals.result.emit.assert_called_once_with(None)
        deleted_signals.finished.emit.assert_called_once_with()
