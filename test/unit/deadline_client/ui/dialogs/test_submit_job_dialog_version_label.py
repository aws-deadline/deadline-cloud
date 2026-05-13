# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
Tests for the version info label in SubmitJobToDeadlineDialog.

These tests verify that the compact version label is built correctly
with various combinations of submitter info and environment data,
including version truncation and tooltip behavior.
"""

import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from configparser import ConfigParser

from deadline.client.dataclasses.submitter_info import SubmitterInfo

try:
    from qtpy.QtCore import Qt  # type: ignore
    from qtpy.QtWidgets import QWidget
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
        SubmitJobToDeadlineDialog,
    )
    from deadline.client.ui.dataclasses._environment_info import _EnvironmentInfo
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
    return mock_instance


def _create_dialog(qtbot, mock_auth_status, submitter_info=None, deadline_version="1.2.3"):
    """Helper to create a SubmitJobToDeadlineDialog with mocked dependencies."""
    import deadline.client.ui.deadline_authentication_status as auth_module

    auth_module._deadline_authentication_status = mock_auth_status

    env_info = Mock(spec=_EnvironmentInfo)
    env_info.deadline_dep_versions = {"deadline": deadline_version}

    with patch(
        "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ), patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus.getInstance",
        return_value=mock_auth_status,
    ), patch(
        "deadline.client.ui.dialogs.submit_job_to_deadline_dialog._EnvironmentInfo.collect",
        return_value=env_info,
    ):
        settings = JobBundleSettings()

        dialog = SubmitJobToDeadlineDialog(
            job_setup_widget_type=MockJobSettingsWidget,
            initial_job_settings=settings,
            initial_shared_parameter_values={},
            auto_detected_attachments=AssetReferences(),
            attachments=AssetReferences(),
            on_create_job_bundle_callback=MagicMock(),
            submitter_info=submitter_info,
        )
        qtbot.addWidget(dialog)
        return dialog


class TestVersionInfoLabel:
    """Test cases for the version info label in SubmitJobToDeadlineDialog."""

    def test_version_label_shows_deadline_version(self, qtbot, mock_auth_status):
        """Test that the version label displays the deadline library version."""
        submitter_info = SubmitterInfo(submitter_name="TestApp")
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        assert hasattr(dialog, "version_info_label")
        assert "deadline 1.2.3" in dialog.version_info_label.text()

    def test_version_label_shows_submitter_package_info(self, qtbot, mock_auth_status):
        """Test that the version label displays submitter package name and version."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.6.3",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "deadline-cloud-for-blender 0.6.3" in label_text

    def test_version_label_shows_host_application_info(self, qtbot, mock_auth_status):
        """Test that the version label displays host application name and version."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.6.3",
            host_application_name="Blender",
            host_application_version="4.5.21",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "Blender 4.5.21" in label_text

    def test_version_label_shows_all_parts_with_separator(self, qtbot, mock_auth_status):
        """Test that all version parts are joined with pipe separators."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.6.3",
            host_application_name="Blender",
            host_application_version="4.5.21",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "deadline 1.2.3" in label_text
        assert "deadline-cloud-for-blender 0.6.3" in label_text
        assert "Blender 4.5.21" in label_text
        assert "|" in label_text

    def test_version_label_falls_back_to_submitter_name_when_no_package_name(
        self, qtbot, mock_auth_status
    ):
        """Test that submitter_name is used when package_name is not provided."""
        submitter_info = SubmitterInfo(
            submitter_name="CustomApp",
            submitter_package_version="2.0.0",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "CustomApp 2.0.0" in label_text

    def test_version_label_omits_host_app_when_not_provided(self, qtbot, mock_auth_status):
        """Test that host application info is omitted when not available."""
        submitter_info = SubmitterInfo(
            submitter_name="CLI",
            submitter_package_name="deadline-cloud-cli",
            submitter_package_version="1.0.0",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "deadline 1.2.3" in label_text
        assert "deadline-cloud-cli 1.0.0" in label_text
        assert label_text.count("|") == 1

    def test_version_label_truncates_long_versions(self, qtbot, mock_auth_status):
        """Test that long dev version strings are truncated with ellipsis."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.57.0.post6+a9f1944194",
        )
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            submitter_info=submitter_info,
            deadline_version="1.2.3.post2+abc123",
        )

        label_text = dialog.version_info_label.text()
        # Full dev versions should NOT appear in the display
        assert "0.57.0.post6+a9f1944194" not in label_text
        assert "1.2.3.post2+abc123" not in label_text
        # Should contain ellipsis for truncated versions
        assert "..." in label_text

    def test_version_label_tooltip_shows_full_versions(self, qtbot, mock_auth_status):
        """Test that the tooltip shows full untruncated version strings."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.57.0.post6+a9f1944194",
        )
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            submitter_info=submitter_info,
            deadline_version="1.2.3.post2+abc123",
        )

        tooltip = dialog.version_info_label.toolTip()
        assert "1.2.3.post2+abc123" in tooltip
        assert "0.57.0.post6+a9f1944194" in tooltip

    def test_version_label_short_versions_not_truncated(self, qtbot, mock_auth_status):
        """Test that normal release versions are not truncated."""
        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.6.3",
        )
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        label_text = dialog.version_info_label.text()
        assert "deadline-cloud-for-blender 0.6.3" in label_text
        assert "..." not in label_text

    def test_version_label_handles_environment_info_failure(self, qtbot, mock_auth_status):
        """Test that the label still works when _EnvironmentInfo.collect() fails."""
        import deadline.client.ui.deadline_authentication_status as auth_module

        auth_module._deadline_authentication_status = mock_auth_status

        submitter_info = SubmitterInfo(
            submitter_name="Blender",
            submitter_package_name="deadline-cloud-for-blender",
            submitter_package_version="0.6.3",
        )

        with patch(
            "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ), patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ), patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog._EnvironmentInfo.collect",
            side_effect=RuntimeError("collection failed"),
        ):
            settings = JobBundleSettings()

            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=MockJobSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(),
                submitter_info=submitter_info,
            )
            qtbot.addWidget(dialog)

        assert hasattr(dialog, "version_info_label")
        label_text = dialog.version_info_label.text()
        assert "deadline-cloud-for-blender 0.6.3" in label_text

    def test_version_label_is_center_aligned(self, qtbot, mock_auth_status):
        """Test that the version label is center-aligned."""
        submitter_info = SubmitterInfo(submitter_name="TestApp")
        dialog = _create_dialog(qtbot, mock_auth_status, submitter_info=submitter_info)

        assert hasattr(dialog, "version_info_label")
        assert dialog.version_info_label.alignment() == Qt.AlignmentFlag.AlignCenter
