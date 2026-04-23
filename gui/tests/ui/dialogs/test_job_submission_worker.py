# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for JobSubmissionWorker class.
"""

import pytest
from unittest.mock import patch, MagicMock

try:
    from deadline.client.ui.dialogs._job_submission_worker import JobSubmissionWorker
    from qtpy.QtCore import Qt  # type: ignore[attr-defined]

    # Handle Qt5 vs Qt6 API differences for connection types
    _QueuedConnection = Qt.ConnectionType.QueuedConnection  # type: ignore[attr-defined]
except AttributeError:
    _QueuedConnection = Qt.QueuedConnection  # type: ignore[attr-defined]


class TestJobSubmissionWorker:
    """Tests for JobSubmissionWorker class."""

    def test_init_creates_worker(self, qtbot):
        """Test that JobSubmissionWorker can be instantiated."""
        worker = JobSubmissionWorker()

        assert worker.is_canceled is False
        assert worker._kwargs == {}

    def test_set_submission_kwargs(self, qtbot):
        """Test that set_submission_kwargs stores kwargs."""
        worker = JobSubmissionWorker()

        worker.set_submission_kwargs(
            job_bundle_dir="/path/to/bundle",
            submitter_name="TestSubmitter",
        )

        assert worker._kwargs["job_bundle_dir"] == "/path/to/bundle"
        assert worker._kwargs["submitter_name"] == "TestSubmitter"

    def test_cancel_sets_flag(self, qtbot):
        """Test that cancel() sets the canceled flag."""
        worker = JobSubmissionWorker()

        assert worker.is_canceled is False
        worker.cancel()
        assert worker.is_canceled is True

    def test_cancel_releases_confirmation_event(self, qtbot):
        """Test that cancel() releases any waiting confirmation."""
        worker = JobSubmissionWorker()

        # Clear the event first
        worker._confirmation_event.clear()
        assert not worker._confirmation_event.is_set()

        worker.cancel()

        assert worker._confirmation_event.is_set()

    def test_set_confirmation_result(self, qtbot):
        """Test that set_confirmation_result sets result and releases event."""
        worker = JobSubmissionWorker()

        worker._confirmation_event.clear()
        worker.set_confirmation_result(True)

        assert worker._confirmation_result is True
        assert worker._confirmation_event.is_set()

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_run_calls_api(self, mock_get_ffi, qtbot):
        """Test that run() calls the API with correct kwargs."""
        worker = JobSubmissionWorker()
        mock_get_ffi.return_value.create_job_from_job_bundle.return_value = {"job_id": "job-123"}

        succeeded_results = []
        worker.succeeded.connect(lambda x: succeeded_results.append(x), _QueuedConnection)

        worker.set_submission_kwargs(
            job_bundle_dir="/path/to/bundle",
            submitter_name="TestSubmitter",
        )
        worker.start()

        # Wait for completion
        qtbot.waitUntil(lambda: len(succeeded_results) > 0, timeout=2000)

        mock_get_ffi.return_value.create_job_from_job_bundle.assert_called_once()
        call_args = mock_get_ffi.return_value.create_job_from_job_bundle.call_args
        params_dict = call_args[0][0]  # first positional arg is the kwargs dict
        assert params_dict["job_bundle_dir"] == "/path/to/bundle"
        assert params_dict["submitter_name"] == "TestSubmitter"
        assert succeeded_results[0] == "job-123"

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_run_emits_succeeded_on_success(self, mock_get_ffi, qtbot):
        """Test that run() emits succeeded signal on success."""
        worker = JobSubmissionWorker()
        mock_get_ffi.return_value.create_job_from_job_bundle.return_value = {"job_id": "job-456"}

        succeeded_results = []
        worker.succeeded.connect(lambda x: succeeded_results.append(x), _QueuedConnection)

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(succeeded_results) > 0, timeout=2000)

        assert succeeded_results[0] == "job-456"

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_run_emits_failed_on_exception(self, mock_get_ffi, qtbot):
        """Test that run() emits failed signal on exception."""
        worker = JobSubmissionWorker()
        test_error = ValueError("Test error")
        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = test_error

        failed_results = []
        worker.failed.connect(lambda x: failed_results.append(x), _QueuedConnection)

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(failed_results) > 0, timeout=2000)

        assert failed_results[0] is test_error

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_run_does_not_emit_succeeded_when_canceled(self, mock_get_ffi, qtbot):
        """Test that run() does not emit succeeded when canceled."""
        worker = JobSubmissionWorker()
        mock_get_ffi.return_value.create_job_from_job_bundle.return_value = {"job_id": "job-789"}

        succeeded_results = []
        worker.succeeded.connect(lambda x: succeeded_results.append(x), _QueuedConnection)

        # Cancel before starting
        worker.cancel()

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()
        worker.wait(2000)

        # Process events to ensure any signals would be delivered
        from qtpy.QtCore import QCoreApplication  # type: ignore[attr-defined]

        QCoreApplication.processEvents()

        assert len(succeeded_results) == 0

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_run_does_not_emit_failed_when_canceled(self, mock_get_ffi, qtbot):
        """Test that run() does not emit failed when canceled."""
        worker = JobSubmissionWorker()
        test_error = ValueError("Test error")
        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = test_error

        failed_results = []
        worker.failed.connect(lambda x: failed_results.append(x), _QueuedConnection)

        # Cancel before starting
        worker.cancel()

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()
        worker.wait(2000)

        # Process events to ensure any signals would be delivered
        from qtpy.QtCore import QCoreApplication  # type: ignore[attr-defined]

        QCoreApplication.processEvents()

        assert len(failed_results) == 0

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_print_callback_emits_signal(self, mock_get_ffi, qtbot):
        """Test that print callback emits print_message signal."""
        worker = JobSubmissionWorker()

        print_messages = []
        worker.print_message.connect(lambda x: print_messages.append(x), _QueuedConnection)

        def capture_callback(params, **kwargs):
            callback = kwargs.get("print_cb")
            if callback:
                callback("Test message")
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(print_messages) > 0, timeout=2000)

        assert "Test message" in print_messages

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_hashing_callback_emits_signal(self, mock_get_ffi, qtbot):
        """Test that hashing callback emits hashing_progress signal."""
        worker = JobSubmissionWorker()

        progress_reports = []
        worker.hashing_progress.connect(lambda x: progress_reports.append(x), _QueuedConnection)

        mock_metadata = MagicMock()
        mock_metadata.progress = 50.0
        mock_metadata.progressMessage = "Hashing..."

        def capture_callback(params, **kwargs):
            callback = kwargs.get("hashing_cb")
            if callback:
                callback(mock_metadata)
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(progress_reports) > 0, timeout=2000)

        assert progress_reports[0] is mock_metadata

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_upload_callback_emits_signal(self, mock_get_ffi, qtbot):
        """Test that upload callback emits upload_progress signal."""
        worker = JobSubmissionWorker()

        progress_reports = []
        worker.upload_progress.connect(lambda x: progress_reports.append(x), _QueuedConnection)

        mock_metadata = MagicMock()
        mock_metadata.progress = 75.0
        mock_metadata.progressMessage = "Uploading..."

        def capture_callback(params, **kwargs):
            callback = kwargs.get("upload_cb")
            if callback:
                callback(mock_metadata)
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(progress_reports) > 0, timeout=2000)

        assert progress_reports[0] is mock_metadata

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_check_canceled_callback_returns_not_canceled(self, mock_get_ffi, qtbot):
        """Test that check_canceled callback returns correct value."""
        worker = JobSubmissionWorker()

        callback_results = []

        def capture_callback(params, **kwargs):
            callback = kwargs.get("continue_cb")
            if callback:
                callback_results.append(callback())
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(callback_results) > 0, timeout=2000)

        # Should return True (not canceled)
        assert callback_results[0] is True

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_hashing_callback_returns_false_when_canceled(self, mock_get_ffi, qtbot):
        """Test that hashing callback returns False when canceled."""
        worker = JobSubmissionWorker()

        callback_results = []
        mock_metadata = MagicMock()

        def capture_callback(params, **kwargs):
            callback = kwargs.get("hashing_cb")
            if callback:
                # Cancel during callback
                worker.cancel()
                result = callback(mock_metadata)
                callback_results.append(result)
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()
        worker.wait(2000)

        # Should return False (canceled)
        assert len(callback_results) > 0
        assert callback_results[0] is False

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_confirmation_callback_emits_signal_and_waits(self, mock_get_ffi, qtbot):
        """Test that confirmation callback emits signal and waits for response."""
        worker = JobSubmissionWorker()

        confirmation_requests = []
        worker.confirmation_requested.connect(
            lambda msg, default: confirmation_requests.append((msg, default)), _QueuedConnection
        )

        def capture_callback(params, **kwargs):
            callback = kwargs.get("confirm_cb")
            if callback:
                # This will block until set_confirmation_result is called
                # We'll set it from the main thread via signal handler
                import threading

                def respond_later():
                    import time

                    time.sleep(0.1)
                    worker.set_confirmation_result(True)

                threading.Thread(target=respond_later).start()
                result = callback("Confirm?", True)
                return {"job_id": "job-123"} if result else {"job_id": ""}
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        succeeded_results = []
        worker.succeeded.connect(lambda x: succeeded_results.append(x), _QueuedConnection)

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()

        qtbot.waitUntil(lambda: len(succeeded_results) > 0, timeout=3000)

        assert len(confirmation_requests) > 0
        assert confirmation_requests[0] == ("Confirm?", True)
        assert succeeded_results[0] == "job-123"

    @patch("deadline.client.ui.dialogs._job_submission_worker._get_ffi")
    def test_confirmation_callback_returns_false_when_canceled(self, mock_get_ffi, qtbot):
        """Test that confirmation callback returns False when canceled during wait."""
        worker = JobSubmissionWorker()

        callback_results = []

        def capture_callback(params, **kwargs):
            callback = kwargs.get("confirm_cb")
            if callback:
                # Cancel while waiting for confirmation
                import threading

                def cancel_later():
                    import time

                    time.sleep(0.1)
                    worker.cancel()

                threading.Thread(target=cancel_later).start()
                result = callback("Confirm?", True)
                callback_results.append(result)
            return "job-123"

        mock_get_ffi.return_value.create_job_from_job_bundle.side_effect = capture_callback

        worker.set_submission_kwargs(job_bundle_dir="/path")
        worker.start()
        worker.wait(2000)

        # Should return False (canceled)
        assert len(callback_results) > 0
        assert callback_results[0] is False
