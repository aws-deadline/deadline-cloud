# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for AsyncTaskRunner class.
"""

import pytest
from unittest.mock import Mock
import time

try:
    from deadline.client.ui.controllers._async_runner import AsyncTaskRunner
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from qtpy.QtCore import Qt, QCoreApplication  # type: ignore[attr-defined]

    # Handle Qt5 vs Qt6 API differences for connection types
    try:
        _QueuedConnection = Qt.ConnectionType.QueuedConnection  # type: ignore[attr-defined]
    except AttributeError:
        _QueuedConnection = Qt.QueuedConnection  # type: ignore[attr-defined]
except ImportError:
    pytest.importorskip("deadline.client.ui.controllers._async_runner")


class TestAsyncTaskRunner:
    """Tests for AsyncTaskRunner class."""

    def setup_method(self):
        """Reset thread pool before each test."""
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
        DeadlineThreadPool.reset()

    def test_init_creates_runner(self, qtbot):
        """Test that AsyncTaskRunner can be instantiated."""
        runner = AsyncTaskRunner()

        assert runner.active_task_count == 0

    def test_run_returns_operation_id(self, qtbot):
        """Test that run() returns an operation ID."""
        runner = AsyncTaskRunner()

        fn = Mock(return_value="result")
        op_id = runner.run("test_op", fn)

        assert isinstance(op_id, int)
        assert op_id > 0

    def test_run_increments_operation_id(self, qtbot):
        """Test that operation IDs increment."""
        runner = AsyncTaskRunner()

        fn = Mock(return_value="result")
        op_id1 = runner.run("op1", fn)
        op_id2 = runner.run("op2", fn)

        assert op_id2 > op_id1

    def test_run_executes_function(self, qtbot):
        """Test that run() executes the function."""
        runner = AsyncTaskRunner()

        result_received = []
        fn = Mock(return_value="test_result")

        runner.run("test_op", fn, on_success=lambda x: result_received.append(x))

        # Wait for the task to complete
        qtbot.waitUntil(lambda: len(result_received) > 0, timeout=2000)

        fn.assert_called_once()
        assert result_received[0] == "test_result"

    def test_run_passes_args_to_function(self, qtbot):
        """Test that run() passes arguments to the function."""
        runner = AsyncTaskRunner()

        result_received = []
        fn = Mock(return_value="result")

        runner.run(
            "test_op",
            fn,
            lambda x: result_received.append(x),  # on_success
            None,  # on_error
            "arg1",  # *args start here
            "arg2",
            kwarg1="value1",
        )

        qtbot.waitUntil(lambda: len(result_received) > 0, timeout=2000)

        fn.assert_called_once_with("arg1", "arg2", kwarg1="value1")

    def test_run_calls_on_success_callback(self, qtbot):
        """Test that on_success callback is called with result."""
        runner = AsyncTaskRunner()

        result_received = []
        fn = Mock(return_value={"data": "test"})

        runner.run("test_op", fn, on_success=lambda x: result_received.append(x))

        qtbot.waitUntil(lambda: len(result_received) > 0, timeout=2000)

        assert result_received[0] == {"data": "test"}

    def test_run_calls_on_error_callback(self, qtbot):
        """Test that on_error callback is called on exception."""
        runner = AsyncTaskRunner()

        error_received = []
        test_error = ValueError("test error")
        fn = Mock(side_effect=test_error)

        runner.run("test_op", fn, on_error=lambda e: error_received.append(e))

        qtbot.waitUntil(lambda: len(error_received) > 0, timeout=2000)

        assert error_received[0] is test_error

    def test_run_emits_task_error_signal(self, qtbot):
        """Test that task_error signal is emitted on exception."""
        runner = AsyncTaskRunner()

        errors_received = []
        test_error = ValueError("test error")
        fn = Mock(side_effect=test_error)

        runner.task_error.connect(
            lambda key, e: errors_received.append((key, e)), _QueuedConnection
        )
        runner.run("test_op", fn)

        qtbot.waitUntil(lambda: len(errors_received) > 0, timeout=2000)

        assert errors_received[0][0] == "test_op"
        assert errors_received[0][1] is test_error

    def test_is_running_returns_true_for_active_task(self, qtbot):
        """Test that is_running returns True for active tasks."""
        runner = AsyncTaskRunner()

        # Use a function that takes some time
        def slow_fn():
            time.sleep(0.5)
            return "result"

        runner.run("slow_op", slow_fn)

        # Check immediately - should be running
        assert runner.is_running("slow_op") is True

    def test_is_running_returns_false_for_unknown_key(self, qtbot):
        """Test that is_running returns False for unknown keys."""
        runner = AsyncTaskRunner()

        assert runner.is_running("unknown_op") is False

    def test_cancel_stops_task(self, qtbot):
        """Test that cancel() prevents result emission."""
        runner = AsyncTaskRunner()

        result_received = []

        def slow_fn():
            time.sleep(0.5)
            return "result"

        runner.run("slow_op", slow_fn, on_success=lambda x: result_received.append(x))

        # Cancel immediately
        canceled = runner.cancel("slow_op")

        assert canceled is True
        assert runner.is_running("slow_op") is False

        # Wait a bit and verify no result was received
        time.sleep(0.7)
        QCoreApplication.processEvents()
        assert len(result_received) == 0

    def test_cancel_returns_false_for_unknown_key(self, qtbot):
        """Test that cancel() returns False for unknown keys."""
        runner = AsyncTaskRunner()

        result = runner.cancel("unknown_op")

        assert result is False

    def test_cancel_all_cancels_all_tasks(self, qtbot):
        """Test that cancel_all() cancels all running tasks."""
        runner = AsyncTaskRunner()

        def slow_fn():
            time.sleep(1)
            return "result"

        runner.run("op1", slow_fn)
        runner.run("op2", slow_fn)

        count = runner.cancel_all()

        assert count == 2
        assert runner.active_task_count == 0

    def test_same_key_cancels_previous_task(self, qtbot):
        """Test that running with same key cancels previous task."""
        runner = AsyncTaskRunner()

        results = []

        def slow_fn(value):
            time.sleep(0.3)
            return value

        # Start first task
        runner.run(
            "same_key",
            slow_fn,
            lambda x: results.append(x),  # on_success
            None,  # on_error
            "first",  # arg passed to slow_fn
        )

        # Start second task with same key - should cancel first
        runner.run(
            "same_key",
            slow_fn,
            lambda x: results.append(x),  # on_success
            None,  # on_error
            "second",  # arg passed to slow_fn
        )

        # Wait for completion
        qtbot.waitUntil(lambda: len(results) > 0, timeout=2000)

        # Only second result should be received
        assert results == ["second"]

    def test_active_task_count(self, qtbot):
        """Test that active_task_count reflects running tasks."""
        runner = AsyncTaskRunner()

        def slow_fn():
            time.sleep(0.5)
            return "result"

        assert runner.active_task_count == 0

        runner.run("op1", slow_fn)
        runner.run("op2", slow_fn)

        assert runner.active_task_count == 2

        runner.cancel_all()

        assert runner.active_task_count == 0
