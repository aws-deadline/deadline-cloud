# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for DeadlineThreadPool class.
"""

import pytest

try:
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from qtpy.QtCore import QThreadPool  # type: ignore[attr-defined]
except ImportError:
    pytest.importorskip("deadline.client.ui.controllers._thread_pool")


class TestDeadlineThreadPool:
    """Tests for DeadlineThreadPool class."""

    def setup_method(self):
        """Reset the singleton before each test."""
        DeadlineThreadPool.reset()

    def teardown_method(self):
        """Clean up after each test."""
        DeadlineThreadPool.reset()

    def test_instance_returns_qthreadpool(self):
        """Test that instance() returns a QThreadPool."""
        pool = DeadlineThreadPool.instance()

        assert isinstance(pool, QThreadPool)

    def test_instance_is_singleton(self):
        """Test that instance() returns the same object on multiple calls."""
        pool1 = DeadlineThreadPool.instance()
        pool2 = DeadlineThreadPool.instance()

        assert pool1 is pool2

    def test_default_max_threads(self):
        """Test that default max thread count is set correctly."""
        pool = DeadlineThreadPool.instance()

        assert pool.maxThreadCount() == DeadlineThreadPool.DEFAULT_MAX_THREADS

    def test_set_max_threads(self):
        """Test that set_max_threads updates the thread count."""
        DeadlineThreadPool.set_max_threads(8)

        pool = DeadlineThreadPool.instance()
        assert pool.maxThreadCount() == 8

    def test_set_max_threads_invalid_value(self):
        """Test that set_max_threads raises ValueError for invalid count."""
        with pytest.raises(ValueError, match="Thread count must be at least 1"):
            DeadlineThreadPool.set_max_threads(0)

        with pytest.raises(ValueError, match="Thread count must be at least 1"):
            DeadlineThreadPool.set_max_threads(-1)

    def test_active_thread_count_initially_zero(self):
        """Test that active_thread_count is 0 when no tasks are running."""
        # Don't create instance yet
        DeadlineThreadPool.reset()

        assert DeadlineThreadPool.active_thread_count() == 0

    def test_shutdown_returns_true_when_no_pool(self):
        """Test that shutdown returns True when no pool exists."""
        DeadlineThreadPool.reset()

        result = DeadlineThreadPool.shutdown()

        assert result is True

    def test_shutdown_clears_instance(self):
        """Test that shutdown clears the singleton instance."""
        # Create the instance
        pool1 = DeadlineThreadPool.instance()

        # Shutdown
        DeadlineThreadPool.shutdown()

        # Get instance again - should be a new one
        pool2 = DeadlineThreadPool.instance()

        assert pool1 is not pool2

    def test_reset_clears_instance(self):
        """Test that reset clears the singleton instance."""
        pool1 = DeadlineThreadPool.instance()

        DeadlineThreadPool.reset()

        pool2 = DeadlineThreadPool.instance()

        assert pool1 is not pool2

    def test_instance_not_global_pool(self):
        """Test that our pool is separate from Qt's global pool."""
        our_pool = DeadlineThreadPool.instance()
        global_pool = QThreadPool.globalInstance()

        assert our_pool is not global_pool
