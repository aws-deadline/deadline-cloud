# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""Settings dialogue tests using pytest-qt."""

import contextlib
from configparser import ConfigParser
from unittest.mock import MagicMock, patch

import pytest

from deadline.client import api
from deadline.client.ui.controllers._deadline_controller import DeadlineUIController
from deadline.client.ui.controllers._thread_pool import DeadlineThreadPool
from deadline.client.ui.dialogs.deadline_config_dialog import (
    DeadlineConfigDialog,
    DeadlineWorkstationConfigWidget,
)


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

        # The farm combo box now populates incrementally from the per-region
        # streaming generator. Yield the mock backend's farms as a single
        # "us-west-2" region so they surface with a (region) displayName label.
        def _fake_iter_farms_by_region(config=None, regions=None, **kw):
            farms = deadline_mock.list_farms(**{k: v for k, v in kw.items() if k != "config"})[
                "farms"
            ]
            yield ("us-west-2", [{**farm, "region": "us-west-2"} for farm in farms], None)

        stack.enter_context(
            patch(
                "deadline.client.ui.controllers._deadline_controller._iter_farms_by_region",
                side_effect=_fake_iter_farms_by_region,
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

    def test_aws_profile_change_does_not_stage_farm_queue_clears(self, config_widget):
        """Switching profiles must not stage farm/queue/storage clears.

        Regression: farm/queue/storage are profile-scoped. If aws_profile_changed
        staged empty values for them, applying the change would write those empties
        into the *new* profile's config section, destroying the defaults the user is
        switching to. The profile change must stage ONLY the profile name.
        """
        config_widget.changes.clear()

        config_widget.aws_profile_changed("some-other-profile")

        assert config_widget.changes.get("defaults.aws_profile_name") == "some-other-profile"
        assert "defaults.farm_id" not in config_widget.changes
        assert "defaults.queue_id" not in config_widget.changes
        assert "settings.storage_profile_id" not in config_widget.changes

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


class TestKnownAssetPathEditing:
    """Regression tests for the known-asset-path edit flow (C11 data loss)."""

    def test_editing_known_path_to_duplicate_does_not_drop_original(self, config_widget):
        """Editing a known-asset path to a value that already exists must NOT
        silently delete the original row.

        Regression (C11): ``_on_edit_known_path`` unconditionally staged the
        rebuilt path list (which excludes the row being edited) even when the
        re-insert was skipped because the new value duplicated an existing
        entry. That dropped the original row from the config = data loss. The
        write/refresh must be guarded on the re-insert actually happening.
        """
        import os
        from qtpy.QtWidgets import QFileDialog

        lw = config_widget.known_paths_list
        lw.clear()
        lw.addItems([os.path.normpath("/path/a"), os.path.normpath("/path/b")])
        lw.setCurrentRow(0)  # editing "/path/a"
        config_widget.changes.clear()

        # User picks a directory that duplicates the OTHER existing entry.
        with patch.object(
            QFileDialog, "getExistingDirectory", return_value=os.path.normpath("/path/b")
        ):
            config_widget._on_edit_known_path()

        staged = config_widget.changes.get("settings.known_asset_paths")
        # The original "/path/a" row must not be silently dropped.
        assert staged is None or os.path.normpath("/path/a") in staged.split(os.pathsep)

    def test_editing_known_path_to_new_value_updates_config(self, config_widget):
        """A genuine (non-duplicate) edit still stages the change normally."""
        import os
        from qtpy.QtWidgets import QFileDialog

        lw = config_widget.known_paths_list
        lw.clear()
        lw.addItems([os.path.normpath("/path/a"), os.path.normpath("/path/b")])
        lw.setCurrentRow(0)  # editing "/path/a"
        config_widget.changes.clear()

        with patch.object(
            QFileDialog, "getExistingDirectory", return_value=os.path.normpath("/path/c")
        ):
            config_widget._on_edit_known_path()

        staged = config_widget.changes.get("settings.known_asset_paths")
        assert staged is not None
        paths = staged.split(os.pathsep)
        assert os.path.normpath("/path/c") in paths
        assert os.path.normpath("/path/b") in paths
        assert os.path.normpath("/path/a") not in paths


class TestCorruptBooleanSetting:
    """Regression test for corrupt boolean coercion in checkbox refresh."""

    def test_corrupt_boolean_falls_back_to_setting_default(self, config_widget):
        """A garbage stored boolean must fall back to the setting's declared
        default, not blindly to False.

        ``settings.submitter_update_notification`` defaults to 'true', so a
        corrupt stored value must leave the checkbox CHECKED.
        """
        import deadline.client.ui.dialogs.deadline_config_dialog as dcd

        # ``dcd.config_file`` is a MagicMock here (patched by the mock_api
        # fixture); patch.object scopes the corrupt-value side_effect to this
        # test and restores the fixture's side_effect afterward.
        with patch.object(
            dcd.config_file,
            "get_setting",
            side_effect=lambda name, config=None: (
                "notabool"
                if name == "settings.submitter_update_notification"
                else "(default)"
                if name == "defaults.aws_profile_name"
                else ""
            ),
        ):
            config_widget.refresh()

        assert config_widget.submitter_update_notification.isChecked() is True


def test_profile_switch_preserves_each_profiles_farm_queue(fresh_deadline_config):
    """End-to-end regression for the profile-switch clobber bug.

    Farm/queue are profile-scoped. Switching profile-1 -> profile-2 -> profile-1
    through the real config_file must leave each profile's stored farm/queue
    intact. The previous aws_profile_changed cleared farm/queue in the same
    ``changes`` batch as the new profile name, so applying the switch wrote empty
    values into the *target* profile's section, wiping its saved defaults.

    This drives the actual ``aws_profile_changed`` -> ``apply`` flow against the
    real config_file (not the mock), so it exercises the genuine bug path: it fails
    if aws_profile_changed reintroduces the farm/queue/storage clears.
    """
    from deadline.client.config import config_file
    from deadline.client.ui.dialogs.deadline_config_dialog import DeadlineWorkstationConfigWidget

    # Seed two profiles, each with its own farm + queue.
    config_file.set_setting("defaults.aws_profile_name", "profile-1")
    config_file.set_setting("defaults.farm_id", "farm-1")
    config_file.set_setting("defaults.queue_id", "queue-1")
    config_file.set_setting("defaults.aws_profile_name", "profile-2")
    config_file.set_setting("defaults.farm_id", "farm-2")
    config_file.set_setting("defaults.queue_id", "queue-2")

    # Build a config widget and drive the real profile-switch + apply path. Patch
    # out only the UI-refresh side effects that need a populated dialog; the
    # persistence (set_setting/write_config via apply) runs for real.
    with (
        patch.object(DeadlineWorkstationConfigWidget, "_build_ui"),
        patch.object(DeadlineWorkstationConfigWidget, "_fill_aws_profiles_box"),
        patch.object(DeadlineWorkstationConfigWidget, "refresh"),
    ):
        widget = DeadlineWorkstationConfigWidget.__new__(DeadlineWorkstationConfigWidget)
        widget.changes = {}
        widget.changes_were_applied = False

        # Switch back to profile-1, then apply (writes the staged changes to config).
        widget.aws_profile_changed("profile-1")
        DeadlineWorkstationConfigWidget.apply(widget)

    # Both profiles must still have their own farm/queue.
    config_file.set_setting("defaults.aws_profile_name", "profile-1")
    assert config_file.get_setting("defaults.farm_id") == "farm-1"
    assert config_file.get_setting("defaults.queue_id") == "queue-1"

    config_file.set_setting("defaults.aws_profile_name", "profile-2")
    assert config_file.get_setting("defaults.farm_id") == "farm-2"
    assert config_file.get_setting("defaults.queue_id") == "queue-2"
