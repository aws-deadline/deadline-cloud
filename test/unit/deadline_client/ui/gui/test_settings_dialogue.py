# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Settings dialogue tests using pytest-qt."""

import contextlib
import sys
from configparser import ConfigParser
from unittest.mock import MagicMock, patch

import pytest

try:
    from deadline.client.ui.dialogs.deadline_config_dialog import (
        DeadlineConfigDialog,
        DeadlineWorkstationConfigWidget,
    )
    from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
    from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
    from deadline.client import api
    from qtpy.QtWidgets import QApplication as QApplication
except ImportError:
    pytest.skip("GUI dependencies not available", allow_module_level=True)


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Reset UI singletons before/after each test."""
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.reset()
    yield
    DeadlineUIController.resetInstance()
    DeadlineThreadPool.shutdown(wait_for_done=True, timeout_ms=2000)
    DeadlineThreadPool.reset()


@pytest.fixture
def mock_backend(mock_deadline_backend):
    """Seed a MockDeadlineBackend with farms, queues, and storage profiles."""
    backend = mock_deadline_backend

    farm = backend.create_farm(
        displayName="Test Farm",
        description="Automated GUI Test Farm",
    )
    farm_id = farm["farmId"]

    queue = backend.create_queue(farmId=farm_id, displayName="Test Queue")
    queue_id = queue["queueId"]

    for name, os_family in [
        ("Linux Storage Profile", "LINUX"),
        ("Windows Storage Profile", "WINDOWS"),
        ("macOS Storage Profile", "MACOS"),
    ]:
        backend.create_storage_profile(
            farmId=farm_id, queueId=queue_id, displayName=name, osFamily=os_family
        )

    return backend, farm_id, queue_id


@pytest.fixture
def deadline_config(mock_backend, tmp_path):
    """Create a deadline config pointing at the mock backend's seeded resources."""
    _, farm_id, queue_id = mock_backend
    config = ConfigParser()
    config["defaults"] = {
        "aws_profile_name": "(default)",
        "farm_id": farm_id,
        "queue_id": queue_id,
    }
    config["settings"] = {
        "storage_profile_id": "",
        "job_history_dir": str(tmp_path / "job_history"),
        "auto_accept": "false",
        "conflict_resolution": "NOT_SELECTED",
        "log_level": "WARNING",
    }
    config["telemetry"] = {"opt_out": "false"}
    return config


@pytest.fixture
def mock_api(mock_backend):
    """Patch API functions and auth checks to use MockDeadlineBackend."""
    backend, _, _ = mock_backend
    deadline_mock = MagicMock()
    backend.set_mock_methods(deadline_mock)

    with contextlib.ExitStack() as stack:
        stack.enter_context(
            patch(
                "deadline.client.api.list_farms",
                side_effect=lambda **kw: deadline_mock.list_farms(
                    **{k: v for k, v in kw.items() if k != "config"}
                ),
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api.list_queues",
                side_effect=lambda **kw: deadline_mock.list_queues(
                    **{k: v for k, v in kw.items() if k != "config"}
                ),
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api.list_storage_profiles_for_queue",
                side_effect=lambda **kw: deadline_mock.list_storage_profiles_for_queue(
                    **{k: v for k, v in kw.items() if k != "config"}
                ),
            )
        )
        stack.enter_context(
            patch(
                "deadline.client.api._session.get_user_and_identity_store_id",
                return_value=(None, None),
            )
        )
        stack.enter_context(patch("deadline.client.api._session.get_boto3_session"))
        stack.enter_context(
            patch(
                "deadline.client.api.check_authentication_status",
                return_value=api.AwsAuthenticationStatus.AUTHENTICATED,
            )
        )
        stack.enter_context(
            patch("deadline.client.api.check_deadline_api_available", return_value=True)
        )
        stack.enter_context(
            patch(
                "deadline.client.api.get_credentials_source",
                return_value=api.AwsCredentialsSource.HOST_PROVIDED,
            )
        )
        mock_cf = stack.enter_context(
            patch("deadline.client.ui.deadline_authentication_status.config_file")
        )
        mock_dialog_cf = stack.enter_context(
            patch("deadline.client.ui.dialogs.deadline_config_dialog.config_file")
        )
        mock_boto_session = stack.enter_context(patch("boto3.Session"))
        mock_cf.read_config.return_value = {}
        mock_cf.get_setting.return_value = "(default)"
        mock_dialog_cf.read_config.return_value = {}
        mock_dialog_cf.get_setting.side_effect = lambda name, config=None: {
            "defaults.aws_profile_name": "(default)",
        }.get(name, "")
        mock_dialog_cf.set_setting = lambda name, value, config: None

        session_instance = MagicMock()
        session_instance._session.full_config = {"profiles": {"default": {}, "test-profile": {}}}
        mock_boto_session.return_value = session_instance

        yield deadline_mock


@pytest.fixture
def config_widget(qtbot, mock_api, deadline_config):
    """Create the DeadlineWorkstationConfigWidget with mocked backend."""
    widget = DeadlineWorkstationConfigWidget()
    qtbot.addWidget(widget)
    widget.config = deadline_config
    widget.show()
    return widget


class TestSettingsDialogue:
    def test_dialog_opens(self, qtbot, mock_api, deadline_config):
        """Verify the config dialog can be created and is visible."""
        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)
        dialog.show()

        assert dialog.isVisible()
        assert dialog.windowTitle() == "AWS Deadline Cloud workstation configuration"

    def test_aws_profile_dropdown_populated(self, config_widget):
        """Verify AWS profile dropdown contains expected profiles."""
        combo = config_widget.aws_profiles_box
        items = [combo.itemText(i) for i in range(combo.count())]

        assert "(default)" in items

    def test_auto_accept_checkbox_is_checkable(self, config_widget):
        """Verify auto accept prompt defaults checkbox is checkable."""
        assert config_widget.auto_accept.isCheckable()

    def test_telemetry_opt_out_checkbox_is_checkable(self, config_widget):
        """Verify telemetry opt out checkbox is checkable."""
        assert config_widget.telemetry_opt_out.isCheckable()

    def test_conflict_resolution_dropdown(self, qtbot, config_widget):
        """Verify conflict resolution option can be set."""
        from qtpy.QtWidgets import QComboBox

        combos = config_widget.general_settings_group.findChildren(QComboBox)
        combo = next(c for c in combos if c.findText("NOT_SELECTED") >= 0)
        items = [combo.itemText(i) for i in range(combo.count())]

        assert "NOT_SELECTED" in items
        assert "CREATE_COPY" in items
        assert "OVERWRITE" in items
        assert "SKIP" in items

    def test_log_level_dropdown(self, qtbot, config_widget):
        """Verify logging level dropdown contains expected levels."""
        from qtpy.QtWidgets import QComboBox

        combos = config_widget.general_settings_group.findChildren(QComboBox)
        combo = next(c for c in combos if c.findText("WARNING") >= 0)
        items = [combo.itemText(i) for i in range(combo.count())]

        assert "WARNING" in items
        assert "DEBUG" in items
        assert "INFO" in items
        assert "ERROR" in items

    def test_log_level_can_be_changed(self, qtbot, config_widget):
        """Verify logging level can be changed to WARNING."""
        from qtpy.QtWidgets import QComboBox

        combos = config_widget.general_settings_group.findChildren(QComboBox)
        combo = next(c for c in combos if c.findText("WARNING") >= 0)
        combo.setCurrentText("WARNING")

        assert combo.currentText() == "WARNING"

    def test_job_attachments_filesystem_options(self, config_widget):
        """Verify job attachments filesystem options dropdown has COPIED and VIRTUAL."""
        from qtpy.QtWidgets import QComboBox

        combos = config_widget.farm_settings_group.findChildren(QComboBox)
        combo = next(c for c in combos if c.findText("COPIED") >= 0)
        items = [combo.itemText(i) for i in range(combo.count())]

        assert "COPIED" in items
        assert "VIRTUAL" in items

    def test_job_attachments_copied_tooltip(self, config_widget):
        """Verify COPIED option has correct tooltip."""
        from qtpy.QtWidgets import QComboBox

        combos = config_widget.farm_settings_group.findChildren(QComboBox)
        combo = next(c for c in combos if c.findText("COPIED") >= 0)
        combo.setCurrentText("COPIED")

        assert combo.toolTip() == (
            "When selected, the worker downloads all job attachments to disk "
            "before rendering begins."
        )

    def test_job_history_dir_editable(self, qtbot, config_widget, tmp_path):
        """Verify job history directory widget exists and accepts input."""
        edit = config_widget.job_history_dir_edit
        assert edit is not None
        assert edit.directory_edit.isEnabled()

    def test_farm_dropdown_populated_from_backend(self, qtbot, config_widget):
        """Verify farm dropdown gets populated when list is refreshed."""
        controller = DeadlineUIController.getInstance()
        combo = config_widget.default_farm_box.box

        with qtbot.waitSignal(controller.farms_updated, timeout=5000):
            controller.refresh_farms()

        QApplication.processEvents()

        items = [combo.itemText(i) for i in range(combo.count())]
        assert "Test Farm" in items

    def test_queue_dropdown_populated_from_backend(self, qtbot, config_widget, mock_backend):
        """Verify queue dropdown gets populated for a given farm."""
        _, farm_id, _ = mock_backend
        controller = DeadlineUIController.getInstance()

        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            controller.refresh_queues(farm_id=farm_id)

        QApplication.processEvents()

        queue_combo = config_widget.default_queue_box.box
        items = [queue_combo.itemText(i) for i in range(queue_combo.count())]
        assert "Test Queue" in items

    def test_storage_profile_dropdown_populated_from_backend(
        self, qtbot, config_widget, mock_backend
    ):
        """Verify storage profile dropdown gets populated for a given farm+queue."""
        _, farm_id, queue_id = mock_backend
        controller = DeadlineUIController.getInstance()

        with qtbot.waitSignal(controller.storage_profiles_updated, timeout=5000):
            controller.refresh_storage_profiles(farm_id=farm_id, queue_id=queue_id)

        QApplication.processEvents()

        sp_combo = config_widget.default_storage_profile_box.box
        items = [sp_combo.itemText(i) for i in range(sp_combo.count())]

        if sys.platform.startswith("linux"):
            assert "Linux Storage Profile" in items
        elif sys.platform.startswith("darwin"):
            assert "macOS Storage Profile" in items
        elif sys.platform.startswith("win"):
            assert "Windows Storage Profile" in items

    def test_ok_cancel_apply_buttons_exist(self, qtbot, mock_api, deadline_config):
        """Verify the dialog has Ok, Cancel, and Apply buttons."""
        from qtpy.QtWidgets import QDialogButtonBox

        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)

        ok_btn = dialog.button_box.button(QDialogButtonBox.StandardButton.Ok)  # type: ignore[attr-defined]
        cancel_btn = dialog.button_box.button(QDialogButtonBox.StandardButton.Cancel)  # type: ignore[attr-defined]
        apply_btn = dialog.button_box.button(QDialogButtonBox.StandardButton.Apply)  # type: ignore[attr-defined]

        assert ok_btn is not None
        assert cancel_btn is not None
        assert apply_btn is not None

    def test_queue_resets_when_farm_changes(self, qtbot, config_widget, mock_backend):
        """Verify queue dropdown resets when a different farm is selected."""
        backend, farm_id, _ = mock_backend
        controller = DeadlineUIController.getInstance()

        # Populate farm dropdown
        with qtbot.waitSignal(controller.farms_updated, timeout=5000):
            controller.refresh_farms()
        QApplication.processEvents()

        # Populate queues for the first farm
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            controller.refresh_queues(farm_id=farm_id)
        QApplication.processEvents()

        queue_combo = config_widget.default_queue_box.box
        assert queue_combo.count() > 0

        # Create a second farm with no queues
        farm2 = backend.create_farm(displayName="Empty Farm")
        farm2_id = farm2["farmId"]

        # Switch to the new farm — queue list should be empty
        with qtbot.waitSignal(controller.queues_updated, timeout=5000):
            controller.refresh_queues(farm_id=farm2_id)
        QApplication.processEvents()

        items = [queue_combo.itemText(i) for i in range(queue_combo.count())]
        assert "Test Queue" not in items

    def test_single_farm_auto_selected_on_profile_change(self, qtbot, config_widget, mock_backend):
        """When a profile has exactly one farm, switching profiles should auto-select it.

        Repro for the manual-test bug: open config GUI, switch profiles -> the
        single available farm was NOT auto-selected (combo stayed on the
        '<none selected>' placeholder and the cascade stopped).
        """
        _, farm_id, queue_id = mock_backend

        # Start from a profile with nothing selected (as a fresh profile switch would).
        config_widget.changes.clear()
        config_widget.changes["defaults.farm_id"] = ""
        config_widget.changes["defaults.queue_id"] = ""
        config_widget.refresh()

        controller = DeadlineUIController.getInstance()

        # Simulate the profile-change cascade entry point: farms get refreshed.
        config_widget._awaiting_farms_for_cascade = True
        with qtbot.waitSignal(controller.farms_updated, timeout=5000):
            config_widget.default_farm_box.refresh_list()
        QApplication.processEvents()

        # The single farm should now be recorded as the pending farm change, which is
        # what gets persisted on Apply. (currentData on the combo can't be asserted
        # here because config_file is mocked, so refresh() can't read the value back.)
        assert config_widget.changes.get("defaults.farm_id") == farm_id

    def test_single_farm_auto_selected_on_signin(self, qtbot, config_widget, mock_backend):
        """Signing in (not a profile switch) must also auto-select a lone farm.

        Repro for the manual-test bug: a profile was selected but not signed in;
        clicking sign-in repopulated the farm list via refresh_lists() - which does
        NOT set the cascade flags - yet the single farm must still be selected.
        Because auto-select lives in the combo (the one place every refresh funnels
        through), it works here without a sign-in-specific hook.
        """
        _, farm_id, _ = mock_backend

        config_widget.changes.clear()
        config_widget.changes["defaults.farm_id"] = ""
        config_widget.changes["defaults.queue_id"] = ""
        config_widget.refresh()

        controller = DeadlineUIController.getInstance()

        # Sign-in path: refresh_lists() refreshes the farm list WITHOUT the cascade
        # flags, but only when the API is available - so stub auth as signed in.
        auth_stub = MagicMock()
        auth_stub.api_availability = True
        assert not config_widget._awaiting_farms_for_cascade
        with patch(
            "deadline.client.ui.dialogs.deadline_config_dialog.DeadlineAuthenticationStatus.getInstance",
            return_value=auth_stub,
        ):
            with qtbot.waitSignal(controller.farms_updated, timeout=5000):
                config_widget.refresh_lists()
        QApplication.processEvents()

        # The lone farm is auto-selected (combo fires currentIndexChanged ->
        # default_farm_changed records it). We assert on the pending change, which is
        # what gets persisted on Apply; the combo's own currentData can't be asserted
        # because config_file is mocked so refresh() can't read the value back.
        assert config_widget.changes.get("defaults.farm_id") == farm_id

    def test_multiple_farms_not_auto_selected_on_signin(self, qtbot, config_widget, mock_backend):
        """With more than one farm, sign-in must not auto-select any farm."""
        backend, _, _ = mock_backend
        backend.create_farm(displayName="Second Farm")

        config_widget.changes.clear()
        config_widget.changes["defaults.farm_id"] = ""
        config_widget.changes["defaults.queue_id"] = ""
        config_widget.refresh()

        controller = DeadlineUIController.getInstance()
        auth_stub = MagicMock()
        auth_stub.api_availability = True
        with patch(
            "deadline.client.ui.dialogs.deadline_config_dialog.DeadlineAuthenticationStatus.getInstance",
            return_value=auth_stub,
        ):
            with qtbot.waitSignal(controller.farms_updated, timeout=5000):
                config_widget.refresh_lists()
        QApplication.processEvents()

        # Nothing auto-selected: no farm change recorded, and the combo sits on the
        # "<none selected>" placeholder.
        assert config_widget.changes.get("defaults.farm_id", "") == ""
        assert config_widget.default_farm_box.box.currentData() == ""
