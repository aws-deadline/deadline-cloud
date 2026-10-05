# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI submitter bundle tests using pytest-qt."""

import json
import os
from configparser import ConfigParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from qtpy.QtCore import QEvent, Qt, QTimer  # type: ignore[attr-defined]
from qtpy.QtGui import QKeyEvent  # type: ignore[attr-defined]
from qtpy.QtWidgets import QApplication, QDialog, QLabel, QMessageBox, QProgressBar, QWidget

from deadline.client.ui.dataclasses import JobBundleSettings
from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
    SubmitJobToDeadlineDialog,
)
from deadline.client.job_bundle._repository import S3BundleRepository
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
def mock_auth_status(fresh_deadline_config):
    """Mock DeadlineAuthenticationStatus to prevent real API calls.

    A blank Deadline config goes with it, so the dialog has no configured farm or
    queue and starts no background calls to fetch them.
    """
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

        # The upload thread is owned by the dialog, and deleted once it has finished,
        # so a garbage collection on another thread can't destroy it.
        from qtpy.QtCore import QThread

        workers = submitter_dialog.findChildren(QThread)
        assert [type(w).__name__ for w in workers] == ["_UploadWorker"]
        assert workers[0].isFinished()
        destroyed = []
        workers[0].destroyed.connect(lambda *_: destroyed.append(True))
        qtbot.waitUntil(lambda: bool(destroyed), timeout=5000)
        assert submitter_dialog.findChildren(QThread) == []

        assert queue_repo.upload_archive.called
        expected_kb = uploaded["size"] // 1024
        assert expected_kb > 1, "test bundle should archive to more than 1 KB"
        # Before the fix this was 1 (from buf.tell() == 0); after the fix it
        # equals the true archive size in KB.
        assert captured.get("max") == expected_kb

    def test_export_to_queue_finishing_as_the_dialog_closes(
        self, qtbot, mock_auth_status, submitter_dialog, tmp_path
    ):
        """The upload can finish just as the user closes the progress dialog. Its "done"
        signal is then delivered after the dialog was scheduled for deletion, and must
        not touch the dialog's deleted widgets."""
        import time
        from typing import Any

        from qtpy.QtCore import QThread

        bundle = tmp_path / "bundle"
        bundle.mkdir()
        (bundle / "template.yaml").write_text("name: Bundle\nsteps: []\n")

        def _upload_after_close(buf, name, metadata=None, progress_callback=None):
            # Finish only once the dialog is closed and the worker canceled, without
            # calling the progress callback that would abort the upload.
            worker: Any = QThread.currentThread()
            deadline = time.monotonic() + 10
            while not worker._cancelled and time.monotonic() < deadline:
                time.sleep(0.01)
            return f"s3://bucket/prefix/{name}.ojd"

        queue_repo = MagicMock(spec=S3BundleRepository)
        queue_repo.bundle_exists.return_value = False
        queue_repo.upload_archive.side_effect = _upload_after_close

        def _reject_progress_dialog():
            for widget in QApplication.topLevelWidgets():
                if isinstance(widget, QDialog) and widget.windowTitle() == "Save Bundle to Queue":
                    widget.reject()
                    return
            QTimer.singleShot(20, _reject_progress_dialog)

        QTimer.singleShot(20, _reject_progress_dialog)
        submitter_dialog._export_to_queue(queue_repo, "bundle", str(bundle))

        # Deliver the late "done", the dialog's deletion, and any timer either starts.
        qtbot.wait(200)
        assert submitter_dialog.findChildren(QThread) == []

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

        callback = MagicMock(return_value={})
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

    def test_generate_export_bundle_omits_host_requirements_for_legacy_submitter(
        self, qtbot, mock_auth_status
    ):
        """Submitters without the host-requirements tab get a 5-positional-arg call.

        Regression test: the export path must mirror the submit path's
        backwards-compatibility branch. Passing ``requirements`` as a 6th
        positional arg unconditionally raises ``TypeError`` for submitters
        whose callback only accepts 5 positional args.
        """
        callback = MagicMock(return_value={})
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI with Job Attachments",
            bundle_dir=SIMPLE_UI_WITH_JA,
            callback=callback,
        )
        assert dialog.show_host_requirements_tab is False

        dialog._generate_export_bundle(
            "/tmp/output-bundle", JobBundleSettings(), [], AssetReferences(), {"amounts": []}
        )

        args, _kwargs = callback.call_args
        # (widget, output_dir, settings, queue_parameters, asset_references) — no
        # host_requirements positional arg for a legacy submitter.
        assert len(args) == 5

    def test_generate_export_bundle_persists_returned_job_parameters(self, qtbot, mock_auth_status):
        """Job parameters returned by the callback are written into the export.

        Regression test: dropping the callback's return value produced an
        exported ``parameter_values.yaml`` missing any parameters the submitter
        computed, silently diverging the export from what submission produces.
        """
        callback = MagicMock(return_value={"job_parameters": [{"name": "Frames", "value": "1-10"}]})
        dialog = _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI with Job Attachments",
            bundle_dir=SIMPLE_UI_WITH_JA,
            callback=callback,
        )

        with patch.object(dialog, "save_job_parameters_to_job_bundle") as mock_save:
            proceeded = dialog._generate_export_bundle(
                "/tmp/output-bundle", JobBundleSettings(), [], AssetReferences(), None
            )

        assert proceeded is True
        mock_save.assert_called_once_with(
            "/tmp/output-bundle", [{"name": "Frames", "value": "1-10"}]
        )


class TestExportBundleLocalOwnershipGuard:
    """A local 'Save bundle as' must only ever delete a folder it owns (a job
    bundle), never an arbitrary directory that collides with the bundle name."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    def _dialog(self, qtbot, mock_auth_status):
        return _create_dialog(
            qtbot,
            mock_auth_status,
            name="Simple UI with Job Attachments",
            bundle_dir=SIMPLE_UI_WITH_JA,
            callback=MagicMock(return_value={}),
        )

    def _patch_export_dialog(self, stack, local_directory):
        mock_dialog_cls = stack.enter_context(patch(f"{self.MODULE}._ExportBundleDialog"))
        mock_dialog_cls.Accepted = QDialog.DialogCode.Accepted
        stack.enter_context(
            patch(
                f"{self.MODULE}._S3BundleRepository.from_config", side_effect=Exception("no queue")
            )
        )
        stack.enter_context(patch(f"{self.MODULE}.get_setting", return_value=""))
        instance = mock_dialog_cls.return_value
        instance.exec_.return_value = QDialog.DialogCode.Accepted
        instance.bundle_name = "my-export"
        instance.export_to_queue = False
        instance.local_directory = str(local_directory)
        return instance

    def test_refuses_to_overwrite_non_bundle_collision(self, qtbot, mock_auth_status, tmp_path):
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        collision = tmp_path / "my-export"  # a user's folder, NOT a bundle
        collision.mkdir()
        (collision / "keep.txt").write_text("precious")

        with ExitStack() as stack:
            self._patch_export_dialog(stack, tmp_path)
            warn = stack.enter_context(patch(f"{self.MODULE}.QMessageBox.warning"))
            gen = stack.enter_context(patch.object(dialog, "_generate_export_bundle"))
            dialog.on_export_bundle()

        warn.assert_called_once()
        gen.assert_not_called()
        assert (collision / "keep.txt").exists(), "must not delete a non-bundle folder"

    def test_overwrites_existing_bundle_after_confirm(self, qtbot, mock_auth_status, tmp_path):
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        existing = tmp_path / "my-export"
        existing.mkdir()
        (existing / "template.json").write_text("{}")  # makes it a real bundle
        (existing / "stale.txt").write_text("stale")

        with ExitStack() as stack:
            self._patch_export_dialog(stack, tmp_path)
            stack.enter_context(
                patch(
                    f"{self.MODULE}.QMessageBox.question",
                    return_value=QMessageBox.StandardButton.Yes,
                )
            )
            stack.enter_context(patch(f"{self.MODULE}.QMessageBox.information"))
            gen = stack.enter_context(
                patch.object(dialog, "_generate_export_bundle", return_value=True)
            )
            dialog.on_export_bundle()

        gen.assert_called_once()
        assert not (existing / "stale.txt").exists(), "the prior bundle should be replaced"

    def test_missing_location_is_rejected(self, qtbot, mock_auth_status, tmp_path):
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        missing = tmp_path / "does-not-exist"

        with ExitStack() as stack:
            self._patch_export_dialog(stack, missing)
            warn = stack.enter_context(patch(f"{self.MODULE}.QMessageBox.warning"))
            gen = stack.enter_context(patch.object(dialog, "_generate_export_bundle"))
            dialog.on_export_bundle()

        warn.assert_called_once()
        gen.assert_not_called()
        assert not missing.exists(), "must not create a typo directory tree"

    def test_invalid_bundle_name_warns_and_aborts(self, qtbot, mock_auth_status, tmp_path):
        """An unsafe name (e.g. '..') is reported via a warning, not a traceback."""
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        with ExitStack() as stack:
            instance = self._patch_export_dialog(stack, tmp_path)
            instance.bundle_name = ".."  # sanitize_bundle_name raises ValueError
            warn = stack.enter_context(patch(f"{self.MODULE}.QMessageBox.warning"))
            gen = stack.enter_context(patch.object(dialog, "_generate_export_bundle"))
            dialog.on_export_bundle()

        warn.assert_called_once()
        gen.assert_not_called()

    def test_non_bundle_submitter_settings_do_not_crash(self, qtbot, mock_auth_status, tmp_path):
        """Submitters whose settings lack input_job_bundle_dir (CLI/DCC) must not
        raise AttributeError; the default export name falls back to settings.name."""
        import types
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        # A settings object with no input_job_bundle_dir attribute.
        dialog.job_settings_type = lambda: types.SimpleNamespace(name="MyJob")

        with ExitStack() as stack:
            mock_dialog_cls = stack.enter_context(patch(f"{self.MODULE}._ExportBundleDialog"))
            mock_dialog_cls.Accepted = QDialog.DialogCode.Accepted
            # Reject so we only exercise resolved-name computation + early return.
            mock_dialog_cls.return_value.exec_.return_value = QDialog.DialogCode.Rejected
            stack.enter_context(
                patch(
                    f"{self.MODULE}._S3BundleRepository.from_config",
                    side_effect=Exception("no queue"),
                )
            )
            stack.enter_context(patch(f"{self.MODULE}.get_setting", return_value=""))
            stack.enter_context(patch.object(dialog.shared_job_settings, "update_settings"))

            dialog.on_export_bundle()  # must not raise AttributeError

        _args, kwargs = mock_dialog_cls.call_args
        assert kwargs["default_name"] == "MyJob"

    def test_failed_generation_preserves_existing_bundle(self, qtbot, mock_auth_status, tmp_path):
        """A failed export must not destroy the user's existing bundle: it is
        generated into a staging dir and only swapped in on success."""
        from contextlib import ExitStack

        dialog = self._dialog(qtbot, mock_auth_status)
        existing = tmp_path / "my-export"
        existing.mkdir()
        (existing / "template.json").write_text('{"old": true}')  # a real bundle

        with ExitStack() as stack:
            self._patch_export_dialog(stack, tmp_path)
            stack.enter_context(
                patch(
                    f"{self.MODULE}.QMessageBox.question",
                    return_value=QMessageBox.StandardButton.Yes,
                )
            )
            stack.enter_context(patch(f"{self.MODULE}.QMessageBox.information"))
            # Generation fails (submitter callbacks are allowed to fail).
            stack.enter_context(patch.object(dialog, "_generate_export_bundle", return_value=False))
            dialog.on_export_bundle()

        # The prior bundle is untouched — not deleted, contents intact.
        assert existing.is_dir()
        assert (existing / "template.json").read_text() == '{"old": true}'
        # No leftover staging directories in the parent.
        assert [p.name for p in tmp_path.iterdir()] == ["my-export"]


class TestSubmitButtonParameterValidity:
    """The Submit button is disabled while any job or queue parameter holds an invalid value."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    RANGE_EXPR_TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: Range Bundle
parameterDefinitions:
- name: Frames
  type: RANGE_EXPR
  default: "1-10"
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""

    def test_invalid_range_expr_disables_submit(
        self, qtbot, mock_auth_status, tmp_path, fresh_deadline_config
    ):
        from deadline.client.job_bundle.parameters import read_job_bundle_parameters
        from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget

        (tmp_path / "template.yaml").write_text(self.RANGE_EXPR_TEMPLATE, encoding="utf8")
        type(mock_auth_status).api_availability = PropertyMock(return_value=True)
        settings = JobBundleSettings(input_job_bundle_dir=str(tmp_path), name="Range Bundle")
        settings.parameters = read_job_bundle_parameters(str(tmp_path))

        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                f"{self.MODULE}.DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            # Farm and queue are configured.
            patch(f"{self.MODULE}.get_setting", return_value="configured"),
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=JobBundleSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(return_value={}),
            )
            qtbot.addWidget(dialog)
            # Pretend the queue environments loaded successfully.
            with patch.object(dialog.shared_job_settings, "is_queue_valid", return_value=True):
                dialog._set_submit_button_state()
                assert dialog.submit_button.isEnabled()

                edit = dialog.job_settings.parameters_widget.controls["Frames"].edit_control
                edit.setText("1-10,")
                assert not dialog.submit_button.isEnabled()
                tooltip = dialog.submit_button.toolTip()
                assert "Cannot submit job" in tooltip
                assert "invalid values: Frames" in tooltip

                edit.setText("1-10,15")
                assert dialog.submit_button.isEnabled()
                assert dialog.submit_button.toolTip() == ""


class TestListStringParameterInSubmitDialog:
    """A LIST[STRING] job parameter in the submit dialog gates Submit and saves as a list."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    LIST_STRING_TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: List Bundle
parameterDefinitions:
- name: Cameras
  type: list[string]
  default: ["main", "closeup"]
  minLength: 1
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""

    def test_list_string_validity_and_saved_values(
        self, qtbot, mock_auth_status, tmp_path, fresh_deadline_config
    ):
        from deadline.client.job_bundle.parameters import read_job_bundle_parameters
        from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget

        (tmp_path / "template.yaml").write_text(self.LIST_STRING_TEMPLATE, encoding="utf8")
        type(mock_auth_status).api_availability = PropertyMock(return_value=True)
        settings = JobBundleSettings(input_job_bundle_dir=str(tmp_path), name="List Bundle")
        settings.parameters = read_job_bundle_parameters(str(tmp_path))

        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                f"{self.MODULE}.DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(f"{self.MODULE}.get_setting", return_value="configured"),
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=JobBundleSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(return_value={}),
            )
            qtbot.addWidget(dialog)
            with patch.object(dialog.shared_job_settings, "is_queue_valid", return_value=True):
                dialog._set_submit_button_state()
                assert dialog.submit_button.isEnabled()

                control = dialog.job_settings.parameters_widget.controls["Cameras"]
                control.set_value([])
                assert not dialog.submit_button.isEnabled()
                assert "invalid values: Cameras" in dialog.submit_button.toolTip()

                control.set_value(["wide", "top"])
                assert dialog.submit_button.isEnabled()

            # The job history copy of the bundle stores the value as a JSON list, which
            # read_job_bundle_parameters loads back unchanged.
            history_dir = tmp_path / "history"
            history_dir.mkdir()
            (history_dir / "template.yaml").write_text(self.LIST_STRING_TEMPLATE, encoding="utf8")
            dialog.save_job_parameters_to_job_bundle(
                str(history_dir), dialog.job_settings.parameters_widget.get_parameters()
            )
            saved = json.loads((history_dir / "parameter_values.json").read_text("utf8"))
            assert saved["parameterValues"][0]["value"] == ["wide", "top"]
            (reloaded,) = read_job_bundle_parameters(str(history_dir))
            assert reloaded["value"] == ["wide", "top"]


class TestListNumberParametersInSubmitDialog:
    """LIST[INT] and LIST[FLOAT] job parameters in the submit dialog gate Submit and save as lists."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: Number List Bundle
parameterDefinitions:
- name: Frames
  type: list[int]
  default: [1, 5]
  item:
    maxValue: 100
- name: Scales
  type: list[float]
  default: [0.5]
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""

    def test_list_number_validity_and_saved_values(
        self, qtbot, mock_auth_status, tmp_path, fresh_deadline_config
    ):
        from deadline.client.job_bundle.parameters import read_job_bundle_parameters
        from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget

        (tmp_path / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
        type(mock_auth_status).api_availability = PropertyMock(return_value=True)
        settings = JobBundleSettings(input_job_bundle_dir=str(tmp_path), name="Number List Bundle")
        settings.parameters = read_job_bundle_parameters(str(tmp_path))

        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                f"{self.MODULE}.DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(f"{self.MODULE}.get_setting", return_value="configured"),
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=JobBundleSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(return_value={}),
            )
            qtbot.addWidget(dialog)
            controls = dialog.job_settings.parameters_widget.controls
            with patch.object(dialog.shared_job_settings, "is_queue_valid", return_value=True):
                dialog._set_submit_button_state()
                assert dialog.submit_button.isEnabled()

                controls["Frames"].set_value([1, 500])
                assert not dialog.submit_button.isEnabled()
                assert "invalid values: Frames" in dialog.submit_button.toolTip()

                controls["Frames"].spin_box(1).setValue(20)
                controls["Scales"].add_button.click()
                controls["Scales"].spin_box(1).setValue(2.25)
                assert dialog.submit_button.isEnabled()

            history_dir = tmp_path / "history"
            history_dir.mkdir()
            (history_dir / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
            dialog.save_job_parameters_to_job_bundle(
                str(history_dir), dialog.job_settings.parameters_widget.get_parameters()
            )
            saved = json.loads((history_dir / "parameter_values.json").read_text("utf8"))
            assert {p["name"]: p["value"] for p in saved["parameterValues"]} == {
                "Frames": [1, 20],
                "Scales": [0.5, 2.25],
            }
            frames, scales = read_job_bundle_parameters(str(history_dir))
            assert frames["value"] == [1, 20]
            assert scales["value"] == [0.5, 2.25]


class TestListBoolParametersInSubmitDialog:
    """LIST[BOOL] job parameters in the submit dialog gate Submit and save as lists."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: Bool List Bundle
parameterDefinitions:
- name: Flags
  type: list[bool]
  default: [true, "no"]
  minLength: 1
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""

    def test_list_bool_validity_and_saved_values(
        self, qtbot, mock_auth_status, tmp_path, fresh_deadline_config
    ):
        from deadline.client.job_bundle.parameters import read_job_bundle_parameters
        from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget

        (tmp_path / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
        type(mock_auth_status).api_availability = PropertyMock(return_value=True)
        settings = JobBundleSettings(input_job_bundle_dir=str(tmp_path), name="Bool List Bundle")
        settings.parameters = read_job_bundle_parameters(str(tmp_path))

        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                f"{self.MODULE}.DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(f"{self.MODULE}.get_setting", return_value="configured"),
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=JobBundleSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(return_value={}),
            )
            qtbot.addWidget(dialog)
            flags = dialog.job_settings.parameters_widget.controls["Flags"]
            assert flags.value() == [True, False]
            with patch.object(dialog.shared_job_settings, "is_queue_valid", return_value=True):
                dialog._set_submit_button_state()
                assert dialog.submit_button.isEnabled()

                flags.set_value([])
                assert not dialog.submit_button.isEnabled()
                assert "invalid values: Flags" in dialog.submit_button.toolTip()

                flags.add_button.click()
                flags.add_button.click()
                flags.edit_control.item(1).setCheckState(Qt.CheckState.Checked)
                assert dialog.submit_button.isEnabled()

            history_dir = tmp_path / "history"
            history_dir.mkdir()
            (history_dir / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
            dialog.save_job_parameters_to_job_bundle(
                str(history_dir), dialog.job_settings.parameters_widget.get_parameters()
            )
            saved = json.loads((history_dir / "parameter_values.json").read_text("utf8"))
            assert saved["parameterValues"][0]["value"] == [False, True]
            (reloaded,) = read_job_bundle_parameters(str(history_dir))
            assert reloaded["value"] == [False, True]


class TestListPathParametersInSubmitDialog:
    """LIST[PATH] job parameters in the submit dialog gate Submit and save as lists."""

    MODULE = "deadline.client.ui.dialogs.submit_job_to_deadline_dialog"

    TEMPLATE = """
specificationVersion: 'jobtemplate-2023-09'
extensions: [EXPR]
name: Path List Bundle
parameterDefinitions:
- name: Scenes
  type: list[path]
  objectType: FILE
  dataFlow: IN
  default: ["scenes/a.blend"]
  minLength: 1
- name: OutDirs
  type: list[path]
  dataFlow: OUT
steps:
- name: NoOp
  script:
    actions:
      onRun:
        command: "echo hi"
"""

    def test_list_path_validity_and_saved_values(
        self, qtbot, mock_auth_status, tmp_path, fresh_deadline_config
    ):
        from deadline.client.job_bundle.parameters import read_job_bundle_parameters
        from deadline.client.ui.widgets.job_bundle_settings_tab import JobBundleSettingsWidget
        from deadline.client.ui.widgets.openjd_parameters_widget import (
            _JobTemplateDirectoryListWidget,
            _JobTemplateInputFileListWidget,
        )

        (tmp_path / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
        type(mock_auth_status).api_availability = PropertyMock(return_value=True)
        settings = JobBundleSettings(input_job_bundle_dir=str(tmp_path), name="Path List Bundle")
        settings.parameters = read_job_bundle_parameters(str(tmp_path))

        with (
            patch(
                "deadline.client.ui.widgets.deadline_authentication_status_widget"
                ".DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(
                f"{self.MODULE}.DeadlineAuthenticationStatus.getInstance",
                return_value=mock_auth_status,
            ),
            patch(f"{self.MODULE}.get_setting", return_value="configured"),
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=JobBundleSettingsWidget,
                initial_job_settings=settings,
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(return_value={}),
            )
            qtbot.addWidget(dialog)
            controls = dialog.job_settings.parameters_widget.controls
            scenes, out_dirs = controls["Scenes"], controls["OutDirs"]
            assert isinstance(scenes, _JobTemplateInputFileListWidget)
            assert isinstance(out_dirs, _JobTemplateDirectoryListWidget)
            # The default item is resolved against the job bundle directory.
            bundle_scene = os.path.normpath(str(tmp_path / "scenes" / "a.blend"))
            assert scenes.value() == [bundle_scene]
            with patch.object(dialog.shared_job_settings, "is_queue_valid", return_value=True):
                dialog._set_submit_button_state()
                assert dialog.submit_button.isEnabled()

                scenes.set_value([])
                assert not dialog.submit_button.isEnabled()
                assert "invalid values: Scenes" in dialog.submit_button.toolTip()

                scenes.set_value([bundle_scene])
                # Add opens the row's dialog; picking a path in it fills the new row.
                with patch(
                    "deadline.client.ui.widgets.path_widgets.QFileDialog.getOpenFileName",
                    return_value=(str(tmp_path / "b.blend"), ""),
                ):
                    scenes.add_button.click()
                with patch(
                    "deadline.client.ui.widgets.path_widgets.QFileDialog.getExistingDirectory",
                    return_value=str(tmp_path / "out"),
                ):
                    out_dirs.add_button.click()
                assert dialog.submit_button.isEnabled()

            history_dir = tmp_path / "history"
            history_dir.mkdir()
            (history_dir / "template.yaml").write_text(self.TEMPLATE, encoding="utf8")
            dialog.save_job_parameters_to_job_bundle(
                str(history_dir), dialog.job_settings.parameters_widget.get_parameters()
            )
            saved = json.loads((history_dir / "parameter_values.json").read_text("utf8"))
            expected = {
                "Scenes": [bundle_scene, os.path.normpath(str(tmp_path / "b.blend"))],
                "OutDirs": [os.path.normpath(str(tmp_path / "out"))],
            }
            assert {p["name"]: p["value"] for p in saved["parameterValues"]} == expected
            reloaded = read_job_bundle_parameters(str(history_dir))
            assert {p["name"]: p["value"] for p in reloaded} == expected
