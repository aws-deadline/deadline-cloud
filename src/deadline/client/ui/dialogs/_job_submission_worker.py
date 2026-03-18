# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
QThread-based worker for job submission operations.

This module provides a QThread subclass that handles job submission in the background,
emitting signals for progress updates and completion status. It replaces the previous
Python threading approach with Qt's native threading model.
"""

from __future__ import annotations

import threading as _threading
from typing import Any as _Any, Optional as _Optional

from qtpy.QtCore import QThread as _QThread, Signal as _Signal, QObject

from ... import api as _api
from ....job_attachments.progress_tracker import ProgressReportMetadata as _ProgressReportMetadata

__all__ = ["JobSubmissionWorker"]


class JobSubmissionWorker(_QThread):
    """
    Worker thread for job submission with progress reporting.

    This QThread subclass handles the job submission process, including:
    - Hashing progress reporting
    - Upload progress reporting
    - Interactive confirmation dialogs
    - Cancellation support

    Signals:
        print_message(str): Emitted when the submission process has a message to display.
        hashing_progress(ProgressReportMetadata): Emitted during file hashing.
        upload_progress(ProgressReportMetadata): Emitted during file upload.
        confirmation_requested(str, bool): Emitted when user confirmation is needed.
            The bool indicates the default response.
        succeeded(str): Emitted on successful submission with the job ID.
        failed(BaseException): Emitted when submission fails with an exception.

    Usage:
        worker = JobSubmissionWorker()
        worker.print_message.connect(handle_print)
        worker.succeeded.connect(handle_success)
        worker.failed.connect(handle_error)
        worker.set_submission_kwargs(job_bundle_dir="/path/to/bundle", ...)
        worker.start()
    """

    # Progress signals
    print_message = _Signal(str)
    hashing_progress = _Signal(_ProgressReportMetadata)
    upload_progress = _Signal(_ProgressReportMetadata)

    # Interaction signals
    confirmation_requested = _Signal(str, bool)  # message, default_response

    # Completion signals
    succeeded = _Signal(str)  # job_id
    failed = _Signal(Exception)

    def __init__(self, parent: _Optional[QObject] = None) -> None:
        super().__init__(parent)
        self._canceled = False
        self._confirmation_result: _Optional[bool] = None
        self._confirmation_event = _threading.Event()
        self._kwargs: dict[str, _Any] = {}

    def set_submission_kwargs(self, **kwargs: _Any) -> None:
        """
        Set the keyword arguments for the job submission.

        These are passed directly to api.create_job_from_job_bundle.
        Call this before starting the thread.
        """
        self._kwargs = kwargs

    def cancel(self) -> None:
        """
        Request cancellation of the submission.

        This sets a flag that is checked by the submission callbacks.
        The actual cancellation may not be immediate depending on the
        current operation.
        """
        self._canceled = True
        # Release any waiting confirmation dialog
        self._confirmation_event.set()

    @property
    def is_canceled(self) -> bool:
        """Return whether cancellation has been requested."""
        return self._canceled

    def set_confirmation_result(self, accepted: bool) -> None:
        """
        Set the result of a confirmation dialog.

        Called from the main thread when the user responds to a confirmation
        dialog triggered by the confirmation_requested signal.

        Args:
            accepted: True if the user accepted, False if they canceled.
        """
        self._confirmation_result = accepted
        self._confirmation_event.set()

    def run(self) -> None:
        """
        Execute the job submission in the background thread.

        This method is called automatically when start() is invoked.
        It sets up the callbacks and calls api.create_job_from_job_bundle.
        """
        try:
            # Set up callbacks that emit signals
            self._kwargs["print_function_callback"] = self._print_callback
            self._kwargs["interactive_confirmation_callback"] = self._confirmation_callback
            self._kwargs["hashing_progress_callback"] = self._hashing_callback
            self._kwargs["upload_progress_callback"] = self._upload_callback
            self._kwargs["create_job_result_callback"] = self._check_canceled_callback

            job_id = _api.create_job_from_job_bundle(**self._kwargs)

            if not self._canceled:
                self.succeeded.emit(job_id)
        except Exception as e:
            if not self._canceled:
                self.failed.emit(e)

    def _print_callback(self, message: str) -> None:
        """Callback for print messages from the submission process."""
        if not self._canceled:
            self.print_message.emit(message)

    def _confirmation_callback(self, message: str, default_response: bool) -> bool:
        """
        Callback for interactive confirmation dialogs.

        This blocks the worker thread until the main thread responds
        via set_confirmation_result().
        """
        if self._canceled:
            return False

        # Reset state for new confirmation
        self._confirmation_result = None
        self._confirmation_event.clear()

        # Request confirmation from main thread
        self.confirmation_requested.emit(message, default_response)

        # Block until main thread responds or cancellation
        self._confirmation_event.wait()

        if self._canceled:
            return False

        return self._confirmation_result if self._confirmation_result is not None else False

    def _hashing_callback(self, progress_metadata: _ProgressReportMetadata) -> bool:
        """Callback for hashing progress updates."""
        if not self._canceled:
            self.hashing_progress.emit(progress_metadata)
        return not self._canceled

    def _upload_callback(self, progress_metadata: _ProgressReportMetadata) -> bool:
        """Callback for upload progress updates."""
        if not self._canceled:
            self.upload_progress.emit(progress_metadata)
        return not self._canceled

    def _check_canceled_callback(self) -> bool:
        """Callback to check if submission should continue."""
        return not self._canceled
