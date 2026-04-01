# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for AsyncTask and WorkerSignals classes.
"""

import pytest
from unittest.mock import Mock

try:
    from deadline.client.ui.controllers._async_task import AsyncTask, WorkerSignals
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
