# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the help button functionality in SubmitJobToDeadlineDialog.

These tests verify the help button behavior and help dialog integration.
"""

import pytest
from unittest.mock import Mock, patch

from deadline.client.dataclasses.submitter_info import SubmitterInfo

try:
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import SubmitJobToDeadlineDialog
except ImportError:
    # The tests in this file should be skipped if Qt UI related modules cannot be loaded
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")


class MockJobSettings:
    """Mock job settings class for testing."""

    def __init__(self):
        self.submitter_name = "TestSubmitter"
        self.name = "TestJob"


class TestSubmitJobDialogHelpButton:
    """Test cases for help button functionality in SubmitJobToDeadlineDialog."""

    def test_on_help_button_clicked_success(self):
        """Test successful execution of _on_help_button_clicked method."""
        submitter_info = SubmitterInfo(submitter_name="Maya")

        # Create a mock dialog instance
        mock_dialog = Mock()
        mock_dialog.submitter_info = submitter_info

        with patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog._HelpDialog"
        ) as mock_help_dialog:
            mock_dialog_instance = Mock()
            mock_help_dialog.return_value = mock_dialog_instance

            # Call the method directly on our mock
            SubmitJobToDeadlineDialog._on_help_button_clicked(mock_dialog)

            # Verify HelpDialog was created with correct submitter_info and parent
            mock_help_dialog.assert_called_once_with(submitter_info, parent=mock_dialog)
            # Verify exec_ was called to show the dialog
            mock_dialog_instance.exec_.assert_called_once()

    def test_on_help_button_clicked_exception_handling(self):
        """Test exception handling in _on_help_button_clicked method."""
        submitter_info = SubmitterInfo(submitter_name="TestApp")

        # Create a mock dialog instance
        mock_dialog = Mock()
        mock_dialog.submitter_info = submitter_info

        test_exception = Exception("Test error")

        with patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog._HelpDialog",
            side_effect=test_exception,
        ), patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.logger"
        ) as mock_logger, patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.QMessageBox"
        ) as mock_msgbox:
            # Call the method directly on our mock
            SubmitJobToDeadlineDialog._on_help_button_clicked(mock_dialog)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call_args = mock_logger.error.call_args[0][0]
            assert "Failed to create HelpDialog" in error_call_args

            # Verify error message box was shown
            mock_msgbox.critical.assert_called_once()
            critical_call_args = mock_msgbox.critical.call_args[0]
            assert critical_call_args[0] == mock_dialog  # parent
            assert "Error" in critical_call_args[1]  # title
            assert "Failed to display Help dialog" in critical_call_args[2]  # message
