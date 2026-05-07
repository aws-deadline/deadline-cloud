# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
pytest-qt proof-of-concept: GUI submitter bundle tests.

This is a pytest-qt equivalent of the Squish tst_verify_gui_submitter_bundles test,
verifying that the Submit to AWS Deadline Cloud dialog correctly loads job bundles
and displays their settings.

Run with:
    hatch run test test/unit/deadline_client/ui/gui/
"""

from configparser import ConfigParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

try:
    from qtpy.QtWidgets import QWidget
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
        SubmitJobToDeadlineDialog,
    )
    from deadline.client.job_bundle.submission import AssetReferences
except ImportError:
    pytest.skip("GUI dependencies not available", allow_module_level=True)


# test/unit/deadline_client/ui/gui/ -> test/
_TEST_ROOT = Path(__file__).resolve().parents[4]
_SAMPLES_DIR = _TEST_ROOT / "squish" / "deadline_gui_test_samples"
SIMPLE_UI_WITH_JA = str(_SAMPLES_DIR / "simple_ui_with_ja")
SIMPLE_UI_NO_JA = str(_SAMPLES_DIR / "simple_ui_no_ja")


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
    with patch(
        "deadline.client.ui.widgets.deadline_authentication_status_widget"
        ".DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ), patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"
        ".DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
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
    """
    pytest-qt equivalent of Squish tst_verify_gui_submitter_bundles.
    """

    def test_submitter_dialog_opens(self, submitter_dialog):
        """Verify the submitter dialog opens with correct title."""
        assert submitter_dialog.isVisible()
        assert submitter_dialog.windowTitle() == "Submit to AWS Deadline Cloud"

    def test_shared_job_settings_tab_exists(self, submitter_dialog):
        """Verify the Shared job settings tab is present."""
        tabs = submitter_dialog.tabs
        tab_names = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Shared job settings" in tab_names

    def test_job_specific_settings_tab_exists(self, submitter_dialog):
        """Verify the Job-specific settings tab is present."""
        tabs = submitter_dialog.tabs
        tab_names = [tabs.tabText(i) for i in range(tabs.count())]
        assert "Job-specific settings" in tab_names

    def test_job_name_matches_bundle_with_ja(self, submitter_dialog):
        """Verify the job name matches the simple_ui_with_ja bundle."""
        props = submitter_dialog.shared_job_settings.shared_job_properties_box
        assert props.sub_name_edit.text() == "Simple UI with Job Attachments"

    def test_load_bundle_button_exists(self, submitter_dialog):
        """Verify the 'Load Bundle' button exists when browse is enabled."""
        assert hasattr(submitter_dialog, "load_bundle_button")
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
