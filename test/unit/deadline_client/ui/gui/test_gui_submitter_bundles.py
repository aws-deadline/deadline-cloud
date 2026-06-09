# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI submitter bundle tests using pytest-qt."""

from configparser import ConfigParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from qtpy.QtCore import QEvent, Qt  # type: ignore[attr-defined]
from qtpy.QtGui import QKeyEvent  # type: ignore[attr-defined]
from qtpy.QtWidgets import QWidget

from deadline.client.ui.dataclasses import JobBundleSettings
from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
)
from deadline.client.job_bundle.submission import AssetReferences


_TEST_DATA = Path(__file__).resolve().parent / "test_data"
SIMPLE_UI_WITH_JA = str(_TEST_DATA / "simple_ui_with_ja")
SIMPLE_UI_NO_JA = str(_TEST_DATA / "simple_ui_no_ja")


class MockJobSettingsWidget(QWidget):
    """A minimal job settings widget for testing."""

    def __init__(self, initial_settings=None, parent=None):
        super().__init__(parent)
        self.initial_settings = initial_settings
        self.parameter_changed = MagicMock()
        self.parameter_changed.connect = MagicMock()

    def update_settings(self, settings):
        pass


@pytest.fixture
def mock_auth_status():
    """Mock DeadlineAuthenticationStatus to prevent real API calls."""
    mock_instance = MagicMock()
    type(mock_instance).api_availability = PropertyMock(return_value=None)
    type(mock_instance).creds_source = PropertyMock(return_value=None)
    type(mock_instance).auth_status = PropertyMock(return_value=None)
    mock_instance.config = ConfigParser()
    mock_instance.api_availability_changed = MagicMock()
    mock_instance.api_availability_changed.connect = MagicMock()
    mock_instance.creds_source_changed = MagicMock()
    mock_instance.creds_source_changed.connect = MagicMock()
    mock_instance.auth_status_changed = MagicMock()
    mock_instance.auth_status_changed.connect = MagicMock()

    import deadline.client.ui.deadline_authentication_status as auth_module

    auth_module._deadline_authentication_status = mock_instance
    yield mock_instance
    auth_module._deadline_authentication_status = None


def _create_dialog(qtbot, mock_auth_status, *, name, bundle_dir):
    """Helper to create a SubmitJobToDeadlineDialog with a given bundle."""
    with (
        patch(
            "deadline.client.ui.widgets.deadline_authentication_status_widget"
            ".DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ),
        patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"
            ".DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ),
    ):
        settings = JobBundleSettings(
            browse_enabled=True,
            input_job_bundle_dir=bundle_dir,
            name=name,
        )
        dialog = SubmitJobToDeadlineDialog(
            job_setup_widget_type=MockJobSettingsWidget,
            initial_job_settings=settings,
            initial_shared_parameter_values={},
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
            on_create_job_bundle_callback=MagicMock(),
        )
        qtbot.addWidget(dialog)
        dialog.show()
        return dialog


@pytest.fixture
def submitter_dialog(qtbot, mock_auth_status):
    """Create a SubmitJobToDeadlineDialog loaded with the simple_ui_with_ja bundle."""
    return _create_dialog(
        qtbot,
        mock_auth_status,
        name="Simple UI with Job Attachments",
        bundle_dir=SIMPLE_UI_WITH_JA,
    )


class TestGuiSubmitterBundles:
    def test_submitter_dialog_structure(self, submitter_dialog):
        """Verify the submitter dialog opens correctly with expected tabs and controls."""
        assert submitter_dialog.isVisible()
        assert submitter_dialog.windowTitle() == "Submit to AWS Deadline Cloud"

        tabs = submitter_dialog.tabs
        tab_names = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Shared job settings" in tab_names
        assert "Job-specific settings" in tab_names

        props = submitter_dialog.shared_job_settings.shared_job_properties_box
        assert props.sub_name_edit.text() == "Simple UI with Job Attachments"

        assert submitter_dialog.load_bundle_button.text() == "Load Bundle"
        assert submitter_dialog.load_bundle_button.isEnabled()

    def test_job_name_matches_bundle_no_ja(self, qtbot, mock_auth_status):
        """Verify the job name matches the simple_ui_no_ja bundle."""
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI - No Job Attachments",
            bundle_dir=SIMPLE_UI_NO_JA,
        )
        props = dialog.shared_job_settings.shared_job_properties_box
        assert props.sub_name_edit.text() == "Simple UI - No Job Attachments"

    def test_job_attachments_tab_exists(self, submitter_dialog):
        """Verify the Job attachments tab is present."""
        tabs = submitter_dialog.tabs
        tab_names = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Job attachments" in tab_names

    def test_submit_button_disabled_when_api_unavailable(self, submitter_dialog):
        """Verify submit button is disabled when API is not available."""
        assert not submitter_dialog.submit_button.isEnabled()

    def test_submit_button_tooltip_when_disabled(self, submitter_dialog):
        """Verify submit button has informative tooltip when disabled."""
        tooltip = submitter_dialog.submit_button.toolTip()
        assert "Cannot submit job" in tooltip

    def test_export_bundle_button_exists(self, submitter_dialog):
        """Verify the Export bundle button is present."""
        assert submitter_dialog.export_bundle_button.text() == "Export bundle"

    def test_settings_button_exists(self, submitter_dialog):
        """Verify the Settings button is present."""
        assert submitter_dialog.settings_button.text() == "Settings..."

    def test_host_requirements_tab_hidden_by_default(self, submitter_dialog):
        """Verify host requirements tab is not shown by default."""
        tabs = submitter_dialog.tabs
        tab_names = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Host requirements" not in tab_names

    def test_host_requirements_tab_shown_when_requested(self, qtbot, mock_auth_status):
        """Verify host requirements tab appears when show_host_requirements_tab=True."""
        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
        ):
            settings = JobBundleSettings(
                input_job_bundle_dir=SIMPLE_UI_WITH_JA,
                name="Test",
            )
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=MockJobSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(),
                show_host_requirements_tab=True,
            )
            qtbot.addWidget(dialog)

            tabs = dialog.tabs
            tab_names = [tabs.tabText(i) for i in range(tabs.count())]
            assert "Host requirements" in tab_names

    def test_enter_key_does_not_close_dialog(self, qtbot, submitter_dialog):
        """Verify pressing Enter/Return doesn't trigger submission."""
        event = QKeyEvent(
            QEvent.Type.KeyPress,
            Qt.Key.Key_Return,
            Qt.KeyboardModifier.NoModifier,
        )
        submitter_dialog.keyPressEvent(event)
        # Dialog should still be visible (not closed/accepted)
        assert submitter_dialog.isVisible()
