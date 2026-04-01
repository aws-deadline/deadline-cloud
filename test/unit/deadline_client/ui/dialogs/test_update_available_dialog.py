# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Tests for the UpdateAvailableDialog."""

from unittest.mock import patch

import pytest

try:
    from deadline.client.ui.dialogs.update_available_dialog import UpdateAvailableDialog
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.update_available_dialog")


class TestUpdateAvailableDialogConstruction:
    """Tests for dialog construction and initial state."""

    def test_basic_construction(self, qtbot):
        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
        )
        qtbot.addWidget(dialog)

        assert dialog.user_downloaded is False
        assert dialog.windowTitle() == "New version available"
        assert hasattr(dialog, "download_button")
        assert hasattr(dialog, "dismiss_button")

    def test_release_notes_button_shown_when_url_provided(self, qtbot):
        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
            release_notes_url="https://github.com/example/releases",
        )
        qtbot.addWidget(dialog)

        assert hasattr(dialog, "release_notes_button")

    def test_no_release_notes_button_when_no_url(self, qtbot):
        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
        )
        qtbot.addWidget(dialog)

        assert not hasattr(dialog, "release_notes_button")


class TestUpdateAvailableDialogActions:
    """Tests for dialog button actions."""

    @patch("deadline.client.ui.dialogs.update_available_dialog.QMessageBox.information")
    @patch("deadline.client.ui.dialogs.update_available_dialog.webbrowser.open")
    def test_download_click_opens_url_and_sets_flag(self, mock_open, mock_msgbox, qtbot):
        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
        )
        qtbot.addWidget(dialog)

        dialog.download_button.click()

        mock_open.assert_called_once_with("https://example.com/installer")
        assert dialog.user_downloaded is True
        mock_msgbox.assert_called_once()

    @patch("deadline.client.ui.dialogs.update_available_dialog.webbrowser.open")
    def test_download_click_handles_browser_error(self, mock_open, qtbot):
        mock_open.side_effect = Exception("browser failed")

        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
        )
        qtbot.addWidget(dialog)

        dialog._on_download_clicked()

        assert dialog.user_downloaded is False

    @patch("deadline.client.ui.dialogs.update_available_dialog.webbrowser.open")
    def test_release_notes_click_opens_url(self, mock_open, qtbot):
        dialog = UpdateAvailableDialog(
            integration_name="Cinema 4D",
            current_version="0.9.1",
            latest_version="0.10.0",
            download_url="https://example.com/installer",
            release_notes_url="https://github.com/example/releases",
        )
        qtbot.addWidget(dialog)

        dialog.release_notes_button.click()

        mock_open.assert_called_once_with("https://github.com/example/releases")
