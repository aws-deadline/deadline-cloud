# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Controllers for Deadline Cloud UI operations.

This module provides the controller layer that separates business logic
from UI components. All async API operations should go through these
controllers to ensure proper ordering and thread safety.

The key components are:

- :class:`DeadlineUIController`: Central controller managing all async API
  operations. Widgets connect to its signals rather than making API calls directly.

- :class:`AsyncTaskRunner`: Manages background task execution with automatic
  cancellation of superseded operations.

- :class:`AsyncTask`: A QRunnable for executing callables in the thread pool
  with proper signal emission.

- :class:`DeadlineThreadPool`: Dedicated thread pool for Deadline operations,
  isolated from other Qt background work.

Example usage::

    from deadline.client.ui.controllers import DeadlineUIController
    from qtpy.QtCore import Qt

    controller = DeadlineUIController.getInstance()
    controller.farms_updated.connect(self._on_farms_updated, Qt.QueuedConnection)
    controller.refresh_farms()
"""

from ._async_task import AsyncTask as AsyncTask
from ._async_task import WorkerSignals as WorkerSignals
from ._async_runner import AsyncTaskRunner as AsyncTaskRunner
from ._deadline_controller import DeadlineUIController as DeadlineUIController
from ._thread_pool import DeadlineThreadPool as DeadlineThreadPool

__all__ = [
    "AsyncTask",
    "AsyncTaskRunner",
    "DeadlineThreadPool",
    "DeadlineUIController",
    "WorkerSignals",
]
