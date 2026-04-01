# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from configparser import ConfigParser
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

try:
    from qtpy.QtWidgets import QWidget
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")


class MockJobSettingsWidget(QWidget):
    """A mock job settings widget that is a real QWidget."""

    def __init__(self, initial_settings=None, parent=None):
        super().__init__(parent)
        self.initial_settings = initial_settings
        self.parameter_changed = MagicMock()
        self.parameter_changed.connect = MagicMock()

    def update_settings(self, settings):
        pass


@pytest.fixture
def mock_auth_status():
    """Create a mock DeadlineAuthenticationStatus that prevents API calls."""
    mock_instance = MagicMock()
    # Use PropertyMock to ensure these return None, not MagicMock
    type(mock_instance).api_availability = PropertyMock(return_value=None)
    type(mock_instance).creds_source = PropertyMock(return_value=None)
    type(mock_instance).auth_status = PropertyMock(return_value=None)
    # Provide a real ConfigParser for the config attribute
    mock_instance.config = ConfigParser()
    # Mock the signals
    mock_instance.api_availability_changed = MagicMock()
    mock_instance.api_availability_changed.connect = MagicMock()
    mock_instance.creds_source_changed = MagicMock()
    mock_instance.creds_source_changed.connect = MagicMock()
    mock_instance.auth_status_changed = MagicMock()
    mock_instance.auth_status_changed.connect = MagicMock()
    return mock_instance


def test_load_bundle_button_shown_when_browse_enabled(qtbot, mock_auth_status):
    """Test that the Load a different job bundle button is shown when browse_enabled is True."""
    # Reset the singleton
    import deadline.client.ui.deadline_authentication_status as auth_module

    auth_module._deadline_authentication_status = mock_auth_status

    # Also patch the widget's getInstance call
    with patch(
        "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ), patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ):
        from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
            SubmitJobToDeadlineDialog,
        )

        settings = JobBundleSettings(browse_enabled=True)

        dialog = SubmitJobToDeadlineDialog(
            job_setup_widget_type=MockJobSettingsWidget,
            initial_job_settings=settings,
            initial_shared_parameter_values={},
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
            on_create_job_bundle_callback=MagicMock(),
        )
        qtbot.addWidget(dialog)

        assert hasattr(dialog, "load_bundle_button")
        assert dialog.load_bundle_button.text() == "Load Bundle"


def test_load_bundle_button_hidden_when_browse_disabled(qtbot, mock_auth_status):
    """Test that the Load a different job bundle button is not shown when browse_enabled is False."""
    # Reset the singleton
    import deadline.client.ui.deadline_authentication_status as auth_module

    auth_module._deadline_authentication_status = mock_auth_status

    # Also patch the widget's getInstance call
    with patch(
        "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ), patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ):
        from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
            SubmitJobToDeadlineDialog,
        )

        settings = JobBundleSettings(browse_enabled=False)

        dialog = SubmitJobToDeadlineDialog(
            job_setup_widget_type=MockJobSettingsWidget,
            initial_job_settings=settings,
            initial_shared_parameter_values={},
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
            on_create_job_bundle_callback=MagicMock(),
        )
        qtbot.addWidget(dialog)

        assert not hasattr(dialog, "load_bundle_button")
