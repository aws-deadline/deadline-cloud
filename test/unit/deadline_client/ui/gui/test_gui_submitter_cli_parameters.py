# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""GUI test for the ``bundle gui-submit --parameter`` validation wiring.

Verifies ``show_job_bundle_submitter`` connects the CLI ``--parameter`` validation callback to
the controller's ``queue_parameters_updated`` signal. Uses a real ``SharedJobSettingsWidget``
(not a MagicMock) so the connection is actually resolved — a mocked dialog would let a broken
connect pass silently.
"""

import os

from unittest.mock import MagicMock, patch

import pytest

from qtpy.QtWidgets import QDialog  # type: ignore

from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
from deadline.client.ui.dataclasses import JobBundleSettings
from deadline.client.ui.job_bundle_submitter import show_job_bundle_submitter
from deadline.client.ui.widgets.shared_job_settings_tab import SharedJobSettingsWidget

MODULE = "deadline.client.ui.job_bundle_submitter"


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Reset UI singletons before/after each test so the controller is clean."""
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.reset()
    yield
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
    DeadlineThreadPool.reset()


def _make_bundle(tmp_path):
    bundle_dir = str(tmp_path / "bundle")
    os.makedirs(bundle_dir)
    with open(os.path.join(bundle_dir, "template.yaml"), "w") as f:
        f.write("name: Bundle Job\nsteps: []\n")
    return bundle_dir


class TestGuiSubmitCliParameterValidationWiring:
    """The --parameter validation callback is wired to the real queue-parameters signal."""

    def _run(self, qtbot, tmp_path, *, job_parameters, validate_side_effect, emit_after=None):
        """Drive show_job_bundle_submitter with a REAL SharedJobSettingsWidget standing in for
        the dialog's shared_job_settings. No farm/queue is configured (fresh_deadline_config),
        so the widget does not kick off a background load.

        If ``emit_after`` is provided, controller signals are emitted *inside* the patched
        context so the connected validation callback runs against the mocked
        ``_validate_and_warn_about_parameters``. It is a list of (kind, payload) tuples
        emitted in sequence, mirroring the controller's real emission patterns
        (see DeadlineUIController._on_queue_parameters_success/_error and the
        clearing emits in select_farm/refresh_queue_parameters):

        - ("load", payload): a successful fetch — loading(True), loading(False),
          queue_parameters_updated(payload), queue_parameters_load_succeeded(payload).
        - ("error", _): a failed fetch — loading(True), loading(False),
          queue_parameters_updated([]); load_succeeded is NOT emitted.
        - ("clear", payload): a bare queue_parameters_updated(payload) with no loading
          prefix, as emitted on farm/queue switch or when nothing is selected.

        Returns (dialog, widget, validate_mock)."""
        bundle_dir = _make_bundle(tmp_path)

        settings = JobBundleSettings(input_job_bundle_dir=bundle_dir, name="n")
        real_widget = SharedJobSettingsWidget(
            initial_settings=settings, initial_shared_parameter_values={}
        )
        qtbot.addWidget(real_widget)

        class FakeDialog(QDialog):
            # A real QDialog so the production code's teardown hooks (``finished``
            # on ordinary close, ``destroyed`` on deletion) behave exactly as they
            # do for SubmitJobToDeadlineDialog, which also does not set
            # WA_DeleteOnClose.
            def __init__(self, **kwargs):
                super().__init__()
                self.shared_job_settings = real_widget
                self.closed = False

            def show(self):
                pass

            def close(self):
                self.closed = True
                return super().close()

        template = {"name": "Bundle Job", "steps": []}
        validate_mock = MagicMock(side_effect=validate_side_effect)

        with (
            patch(f"{MODULE}.validate_directory_symlink_containment"),
            patch(
                f"{MODULE}.read_yaml_or_json_object",
                side_effect=lambda _dir, name, *a, **k: template if name == "template" else None,
            ),
            patch(f"{MODULE}.read_job_bundle_parameters", return_value=[]),
            patch(f"{MODULE}.run_pre_gui_hooks", return_value={}),
            patch(f"{MODULE}.SubmitJobToDeadlineDialog", side_effect=FakeDialog),
            patch(f"{MODULE}.QApplication"),
            patch(f"{MODULE}.QMessageBox"),
            patch(f"{MODULE}._get_setting", side_effect=lambda name, config=None: "false"),
            patch(f"{MODULE}._config_file") as cfg,
            patch(f"{MODULE}._validate_and_warn_about_parameters", validate_mock),
        ):
            cfg.str2bool.side_effect = lambda v: str(v).lower() == "true"
            dialog = show_job_bundle_submitter(
                input_job_bundle_dir=bundle_dir, job_parameters=job_parameters
            )
            # Emitting inside the patched block keeps _validate_and_warn_about_parameters mocked
            # when the connected callback fires.
            if emit_after is not None:
                controller = real_widget._controller
                for kind, payload in emit_after:
                    if kind == "load":
                        # A successful fetch (_on_queue_parameters_success).
                        controller.queue_parameters_loading.emit(True)
                        controller.queue_parameters_loading.emit(False)
                        controller.queue_parameters_updated.emit(payload)
                        controller.queue_parameters_load_succeeded.emit(payload)
                    elif kind == "error":
                        # A failed fetch (_on_queue_parameters_error): no load_succeeded.
                        controller.queue_parameters_loading.emit(True)
                        controller.queue_parameters_loading.emit(False)
                        controller.queue_parameters_updated.emit([])
                    else:
                        assert kind == "clear"
                        controller.queue_parameters_updated.emit(payload)
        return dialog, real_widget, validate_mock

    def test_parameter_path_does_not_raise_attribute_error(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """Wiring the --parameter path must not raise AttributeError (the C6 hard crash)."""
        dialog, _widget, _validate = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
        )
        assert dialog is not None

    def test_queue_parameters_update_invokes_validator_with_param_list(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """A completed queue-parameter load runs the validator with the loaded list."""
        queue_parameters = [{"name": "CondaChannels", "type": "STRING"}]
        _dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[("load", queue_parameters)],
        )

        validate_mock.assert_called_once()
        # Signature: (job_parameters, job_template_parameters, queue_parameters, parent_widget)
        assert validate_mock.call_args.args[2] == queue_parameters

    def test_validator_cancel_closes_dialog(self, qtbot, fresh_deadline_config, tmp_path):
        """When the validator returns False (user cancels), the dialog is closed."""
        dialog, _widget, _validate = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: False,
            emit_after=[("load", [{"name": "Foo"}])],
        )

        assert dialog.closed is True

    def test_clearing_emission_does_not_invoke_validator(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """A clearing emission ([] with no loading prefix) — e.g. farm/queue switch or nothing
        selected — must not run validation (which would spuriously flag queue params as
        unrecognized)."""
        _dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[("clear", [])],
        )

        validate_mock.assert_not_called()

    def test_empty_but_real_load_still_invokes_validator(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """A queue that genuinely has zero queue parameters still validates: a completed load
        (loading True -> False, then updated([])) runs the validator so an unrecognized CLI
        --parameter is flagged rather than slipping through."""
        _dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[("load", [])],
        )

        validate_mock.assert_called_once()
        assert validate_mock.call_args.args[2] == []

    def test_failed_load_does_not_invoke_validator(self, qtbot, fresh_deadline_config, tmp_path):
        """A failed fetch (e.g. transient ResourceNotFoundException during a profile switch)
        must not validate against []; the callback keeps waiting for a successful load."""
        queue_parameters = [{"name": "CondaChannels", "type": "STRING"}]
        _dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[
                ("error", None),
                ("load", queue_parameters),
            ],
        )

        # Only the successful load validates, with its real parameter list.
        validate_mock.assert_called_once()
        assert validate_mock.call_args.args[2] == queue_parameters

    def test_validator_runs_single_shot_on_first_successful_load(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """Validation waits through clearing emissions, runs once on the first successful load,
        and disconnects so later reloads don't re-validate."""
        queue_parameters = [{"name": "CondaChannels", "type": "STRING"}]
        _dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[
                ("clear", []),
                ("load", queue_parameters),
                ("load", [{"name": "Other"}]),
            ],
        )

        validate_mock.assert_called_once()
        assert validate_mock.call_args.args[2] == queue_parameters

    def test_destroy_after_validation_does_not_warn(
        self, qtbot, fresh_deadline_config, tmp_path, recwarn
    ):
        """Dialog destruction after validation already ran (and self-disconnected) must not
        attempt a second disconnect — PySide6 reports that with a RuntimeWarning."""
        dialog, _widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
            emit_after=[("load", [{"name": "CondaChannels"}])],
        )
        validate_mock.assert_called_once()

        dialog.destroyed.emit()

        assert not [w for w in recwarn.list if "disconnect" in str(w.message)]

    def test_dialog_closed_before_load_disconnects_validator(
        self, qtbot, fresh_deadline_config, tmp_path
    ):
        """Ordinarily closing the dialog (user hits X/Esc; no WA_DeleteOnClose, so the
        QObject stays alive and ``destroyed`` never fires) before any successful load
        tears down the connection: a later success emission from the long-lived
        singleton must not run the validator against the closed dialog."""
        dialog, widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
        )
        qtbot.addWidget(dialog)

        # Make the dialog visible (as show_job_bundle_submitter does for real) and
        # close it the ordinary way — closeEvent -> reject() -> finished.
        QDialog.show(dialog)
        assert dialog.isVisible()
        QDialog.close(dialog)
        assert not dialog.isVisible()

        with patch(
            "deadline.client.ui.job_bundle_submitter._validate_and_warn_about_parameters",
            validate_mock,
        ):
            # A successful-load emission (e.g. triggered by another consumer of the
            # singleton), which would run the validator if the connection were live.
            widget._controller.queue_parameters_load_succeeded.emit([{"name": "CondaChannels"}])

        validate_mock.assert_not_called()

    def test_dialog_destroyed_disconnects_validator(self, qtbot, fresh_deadline_config, tmp_path):
        """Destroying the dialog before queue params load tears down the connection, so the
        stale closure never fires against the singleton controller."""
        dialog, widget, validate_mock = self._run(
            qtbot,
            tmp_path,
            job_parameters=[{"name": "Foo", "value": "bar"}],
            validate_side_effect=lambda *a, **k: True,
        )

        # Simulate the dialog being destroyed before any load completes.
        dialog.destroyed.emit()

        with patch(
            "deadline.client.ui.job_bundle_submitter._validate_and_warn_about_parameters",
            validate_mock,
        ):
            # A successful-load emission, which would run the validator if the
            # connection were still live.
            widget._controller.queue_parameters_load_succeeded.emit([{"name": "CondaChannels"}])

        validate_mock.assert_not_called()
