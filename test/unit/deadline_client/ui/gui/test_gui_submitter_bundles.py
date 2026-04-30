# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI submitter bundle tests using pytest-qt."""

import os
from configparser import ConfigParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from qtpy.QtCore import QEvent, Qt, QTimer  # type: ignore[attr-defined]
from qtpy.QtGui import QKeyEvent  # type: ignore[attr-defined]
from qtpy.QtWidgets import QApplication, QDialog, QLabel, QProgressBar, QWidget

from deadline.client.ui.dataclasses import JobBundleSettings
from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
)
from deadline.client.job_bundle.repository import S3BundleRepository
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


def _create_dialog(qtbot, mock_auth_status, *, name, bundle_dir, callback=None):
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
            on_create_job_bundle_callback=callback or MagicMock(),
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
        assert submitter_dialog.export_bundle_button.text() == "Save bundle as"

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

    def test_export_to_queue_progress_reflects_real_archive_size(
        self, qtbot, mock_auth_status, submitter_dialog, tmp_path
    ):
        """The upload progress total reflects the real archive size.

        Regression test for the upload progress bar. The worker previously
        sized the upload progress from ``buf.tell()``, which is always 0
        because ``archive_bundle_dir`` returns the buffer rewound to position
        0. That collapsed the progress maximum to ``max(1, 0) == 1`` and the
        UI always reported a fixed "1.0 KB" total regardless of bundle size.

        This drives the real ``_export_to_queue`` flow (archiving + a mocked
        S3 upload on the background worker) and asserts the progress dialog's
        maximum ends up equal to the true archive size in KB — not 1.
        """
        # Build a bundle whose archive is comfortably larger than 1 KB. Random
        # bytes are incompressible so zip deflation can't shrink it under 1 KB.
        bundle = tmp_path / "big-bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Big Bundle\nsteps: []\n")
        (bundle / "payload.bin").write_bytes(os.urandom(64 * 1024))

        # Mock the queue repository: no pre-existing bundle, and capture the
        # true size of the buffer handed to the upload.
        uploaded: dict = {}

        def _fake_upload(buf, name, metadata=None, progress_callback=None):
            uploaded["size"] = buf.getbuffer().nbytes
            if progress_callback:
                progress_callback(uploaded["size"])
            return f"s3://bucket/prefix/{name}.ojd"

        queue_repo = MagicMock(spec=S3BundleRepository)
        queue_repo.bundle_exists.return_value = False
        queue_repo.upload_archive.side_effect = _fake_upload

        # The progress dialog is modal (exec_ blocks). On success it waits for
        # the user to click "Close", so poll for completion, capture the
        # progress bar maximum, and accept the dialog to unblock exec_.
        captured: dict = {}
        attempts = {"n": 0}

        def _poll():
            attempts["n"] += 1
            for widget in QApplication.topLevelWidgets():
                if isinstance(widget, QDialog) and widget.windowTitle() == "Save Bundle to Queue":
                    completed = any(
                        "saved to queue" in label.text() for label in widget.findChildren(QLabel)
                    )
                    if completed:
                        bars = widget.findChildren(QProgressBar)
                        if bars:
                            captured["max"] = bars[0].maximum()
                        widget.accept()
                        return
                    # Safety valve so a failure can't hang the test forever.
                    if attempts["n"] > 250:
                        widget.reject()
                        return
            QTimer.singleShot(20, _poll)

        QTimer.singleShot(20, _poll)
        submitter_dialog._export_to_queue(queue_repo, "big-bundle", str(bundle))

        assert queue_repo.upload_archive.called
        expected_kb = uploaded["size"] // 1024
        assert expected_kb > 1, "test bundle should archive to more than 1 KB"
        # Before the fix this was 1 (from buf.tell() == 0); after the fix it
        # equals the true archive size in KB.
        assert captured.get("max") == expected_kb

    def test_generate_export_bundle_aborts_and_notifies_on_failure(self, qtbot, mock_auth_status):
        """A failed bundle generation is surfaced and reported as a failure.

        Regression test: previously, if on_create_job_bundle_callback raised
        during a Queue export, the code silently fell back to the original,
        un-edited input bundle (discarding the user's edits) while still
        reporting success — and would raise inside the upload worker if there
        was no input bundle dir to fall back to.

        _generate_export_bundle is the shared seam both export branches use to
        decide whether to proceed. It must return False (so callers abort
        before uploading / reporting success) and show an error dialog.
        """
        failing_callback = MagicMock(side_effect=RuntimeError("generation boom"))
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI with Job Attachments",
            bundle_dir=SIMPLE_UI_WITH_JA,
            callback=failing_callback,
        )

        module = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"
        with patch(f"{module}.QMessageBox.critical") as mock_critical:
            proceeded = dialog._generate_export_bundle(
                "/tmp/does-not-matter",
                JobBundleSettings(),
                [],
                AssetReferences(),
                None,
            )

        assert proceeded is False, "callers must not proceed after a failure"
        assert failing_callback.called
        mock_critical.assert_called_once()

    def test_generate_export_bundle_returns_true_on_success(self, qtbot, mock_auth_status):
        """On success the bundle is generated into the requested dir for EXPORT."""
        from deadline.client.ui.dialogs._types import JobBundlePurpose

        callback = MagicMock()
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI with Job Attachments",
            bundle_dir=SIMPLE_UI_WITH_JA,
            callback=callback,
        )

        proceeded = dialog._generate_export_bundle(
            "/tmp/output-bundle", JobBundleSettings(), [], AssetReferences(), None
        )

        assert proceeded is True
        callback.assert_called_once()
        args, kwargs = callback.call_args
        # Bundle is generated into the requested output directory...
        assert args[1] == "/tmp/output-bundle"
        # ...for the EXPORT purpose.
        assert kwargs["purpose"] == JobBundlePurpose.EXPORT
