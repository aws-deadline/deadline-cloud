# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

from unittest.mock import MagicMock, patch

import pytest

try:
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import SubmitJobToDeadlineDialog
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")


@pytest.fixture
def mock_job_settings_widget():
    """Create a mock job settings widget type."""
    widget = MagicMock()
    widget.return_value = MagicMock()
    widget.return_value.parameter_changed = MagicMock()
    widget.return_value.parameter_changed.connect = MagicMock()
    return widget


@patch("deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus")
def test_load_bundle_button_shown_when_browse_enabled(
    mock_auth_status, qtbot, mock_job_settings_widget
):
    """Test that the Load a different job bundle button is shown when browse_enabled is True."""
    mock_auth_status.getInstance.return_value = MagicMock()

    settings = JobBundleSettings(browse_enabled=True)

    dialog = SubmitJobToDeadlineDialog(
        job_setup_widget_type=mock_job_settings_widget,
        initial_job_settings=settings,
        initial_shared_parameter_values={},
        auto_detected_attachments=AssetReferences(),
        attachments=AssetReferences(),
        on_create_job_bundle_callback=MagicMock(),
    )
    qtbot.addWidget(dialog)

    assert hasattr(dialog, "load_bundle_button")
    assert dialog.load_bundle_button.text() == "Load Bundle"


@patch("deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus")
def test_load_bundle_button_hidden_when_browse_disabled(
    mock_auth_status, qtbot, mock_job_settings_widget
):
    """Test that the Load a different job bundle button is not shown when browse_enabled is False."""
    mock_auth_status.getInstance.return_value = MagicMock()

    settings = JobBundleSettings(browse_enabled=False)

    dialog = SubmitJobToDeadlineDialog(
        job_setup_widget_type=mock_job_settings_widget,
        initial_job_settings=settings,
        initial_shared_parameter_values={},
        auto_detected_attachments=AssetReferences(),
        attachments=AssetReferences(),
        on_create_job_bundle_callback=MagicMock(),
    )
    qtbot.addWidget(dialog)

    assert not hasattr(dialog, "load_bundle_button")
