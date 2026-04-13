# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Tests for in-memory session config behavior.

Validates that CLI options (--farm-id, --queue-id, etc.) create a session config
that is used by the submission dialog without affecting the on-disk config, and
that the settings dialog correctly separates session vs workstation config.
"""

from configparser import ConfigParser
from typing import Callable, Generator, Optional
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

try:
    from qtpy.QtWidgets import QWidget

    from deadline.client.config import config_file
    from deadline.client.cli._common import _apply_cli_options_to_config
    from deadline.client.ui.dataclasses import JobBundleSettings
    from deadline.client.job_bundle.submission import AssetReferences
    from deadline.client.ui.dialogs.submit_job_to_deadline_dialog import (
        SubmitJobToDeadlineDialog,
    )
    from deadline.client.ui.dialogs.deadline_config_dialog import (
        DeadlineConfigDialog,
        DeadlineWorkstationConfigWidget,
    )
except ImportError:
    pytest.importorskip("deadline.client.ui.dialogs.submit_job_to_deadline_dialog")

DISK_FARM_ID = "farm-disk11111111111111111111111111"
DISK_QUEUE_ID = "queue-disk1111111111111111111111111"
CLI_FARM_ID = "farm-cli111111111111111111111111111"
CLI_QUEUE_ID = "queue-cli11111111111111111111111111"


class MockJobSettingsWidget(QWidget):
    def __init__(
        self, initial_settings: Optional[JobBundleSettings] = None, parent: Optional[QWidget] = None
    ):
        super().__init__(parent)
        self.initial_settings = initial_settings
        self.parameter_changed = MagicMock()
        self.parameter_changed.connect = MagicMock()

    def update_settings(self, settings: JobBundleSettings) -> None:
        pass


def _mock_auth_instance() -> MagicMock:
    """Create a mock DeadlineAuthenticationStatus that won't trigger real API calls."""
    inst = MagicMock()
    inst.api_availability = False
    inst.api_availability_changed = MagicMock()
    inst.api_availability_changed.connect = MagicMock()
    inst.deadline_config_changed = MagicMock()
    inst.deadline_config_changed.connect = MagicMock()
    return inst


@pytest.fixture
def mock_auth_status() -> MagicMock:
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


@pytest.fixture
def disk_config() -> ConfigParser:
    """A ConfigParser representing the on-disk config with known farm/queue."""
    config = ConfigParser()
    config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=config)
    config_file.set_setting("defaults.queue_id", DISK_QUEUE_ID, config=config)
    return config


@pytest.fixture
def submit_dialog_factory(qtbot, mock_auth_status) -> Callable:
    """Factory that creates a SubmitJobToDeadlineDialog with mocked auth."""

    def _create(session_config: Optional[ConfigParser] = None) -> "SubmitJobToDeadlineDialog":
        import deadline.client.ui.deadline_authentication_status as auth_module

        auth_module._deadline_authentication_status = mock_auth_status

        with patch(
            "deadline.client.ui.widgets.deadline_authentication_status_widget.DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ), patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.DeadlineAuthenticationStatus.getInstance",
            return_value=mock_auth_status,
        ):
            dialog = SubmitJobToDeadlineDialog(
                job_setup_widget_type=MockJobSettingsWidget,
                initial_job_settings=JobBundleSettings(),
                initial_shared_parameter_values={},
                auto_detected_attachments=AssetReferences(),
                attachments=AssetReferences(),
                on_create_job_bundle_callback=MagicMock(),
                session_config=session_config,
            )
            qtbot.addWidget(dialog)
            return dialog

    return _create


@pytest.fixture
def config_dialog_mocks() -> Generator[MagicMock, None, None]:
    """Patches config_file, boto3.Session, and auth for DeadlineConfigDialog tests.

    Yields mock_config_file after setting up sane defaults.
    Callers can override mock_config_file.read_config.return_value etc. as needed.
    """
    with patch(
        "deadline.client.ui.dialogs.deadline_config_dialog.DeadlineAuthenticationStatus.getInstance"
    ) as mock_auth, patch(
        "deadline.client.ui.dialogs.deadline_config_dialog.boto3.Session"
    ) as mock_boto3_session, patch(
        "deadline.client.ui.dialogs.deadline_config_dialog.config_file"
    ) as mock_config_file:
        mock_auth.return_value = _mock_auth_instance()
        mock_boto3_session.return_value._session.full_config = {"profiles": {}}
        mock_config_file.read_config.return_value = ConfigParser()
        mock_config_file.get_setting = config_file.get_setting
        mock_config_file.set_setting = config_file.set_setting
        mock_config_file.write_config = MagicMock()
        yield mock_config_file


class TestApplyCliOptionsToConfig:
    """Tests for _apply_cli_options_to_config override and copy behavior."""

    def test_returns_config_when_no_options(self) -> None:
        """When no CLI options are provided, returns a copy of the disk config."""
        result = _apply_cli_options_to_config()
        assert isinstance(result, ConfigParser)

    def test_overrides_farm_id(self, disk_config) -> None:
        session = ConfigParser()
        session.read_dict(disk_config)
        result = _apply_cli_options_to_config(config=session, farm_id=CLI_FARM_ID)
        assert config_file.get_setting("defaults.farm_id", config=result) == CLI_FARM_ID
        # queue_id is scoped to the farm, so changing farm clears the queue
        assert config_file.get_setting("defaults.queue_id", config=result) == ""

    def test_overrides_queue_id(self, disk_config) -> None:
        session = ConfigParser()
        session.read_dict(disk_config)
        result = _apply_cli_options_to_config(config=session, queue_id=CLI_QUEUE_ID)
        assert config_file.get_setting("defaults.queue_id", config=result) == CLI_QUEUE_ID
        assert config_file.get_setting("defaults.farm_id", config=result) == DISK_FARM_ID

    def test_overrides_both(self, disk_config) -> None:
        session = ConfigParser()
        session.read_dict(disk_config)
        result = _apply_cli_options_to_config(
            config=session, farm_id=CLI_FARM_ID, queue_id=CLI_QUEUE_ID
        )
        assert config_file.get_setting("defaults.farm_id", config=result) == CLI_FARM_ID
        assert config_file.get_setting("defaults.queue_id", config=result) == CLI_QUEUE_ID

    def test_does_not_mutate_disk_cache(self) -> None:
        """When no config is passed, reads from disk and returns a fresh copy."""
        with patch.object(config_file, "read_config") as mock_read:
            base = ConfigParser()
            config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=base)
            mock_read.return_value = base

            result = _apply_cli_options_to_config(farm_id=CLI_FARM_ID)

        assert config_file.get_setting("defaults.farm_id", config=result) == CLI_FARM_ID
        assert result is not base
        assert config_file.get_setting("defaults.farm_id", config=base) == DISK_FARM_ID

    def test_does_not_mutate_provided_config(self) -> None:
        """When a config is explicitly passed, it is copied — the original is not mutated."""
        original = ConfigParser()
        config_file.set_setting("defaults.queue_id", DISK_QUEUE_ID, config=original)

        result = _apply_cli_options_to_config(config=original, queue_id=CLI_QUEUE_ID)

        assert result is not original
        assert config_file.get_setting("defaults.queue_id", config=result) == CLI_QUEUE_ID
        assert config_file.get_setting("defaults.queue_id", config=original) == DISK_QUEUE_ID


class TestSubmitDialogSessionConfig:
    """Tests for session config propagation through SubmitJobToDeadlineDialog."""

    def test_no_session_config_when_no_cli_args(self, submit_dialog_factory) -> None:
        dialog = submit_dialog_factory(session_config=None)
        assert dialog._session_config is None

    def test_session_config_stored_when_provided(self, submit_dialog_factory, disk_config) -> None:
        session = _apply_cli_options_to_config(config=disk_config, queue_id=CLI_QUEUE_ID)
        dialog = submit_dialog_factory(session_config=session)

        assert dialog._session_config is not None
        assert (
            config_file.get_setting("defaults.queue_id", config=dialog._session_config)
            == CLI_QUEUE_ID
        )

    def test_session_config_passed_to_shared_job_settings(
        self, submit_dialog_factory, disk_config
    ) -> None:
        session = _apply_cli_options_to_config(config=disk_config, farm_id=CLI_FARM_ID)
        dialog = submit_dialog_factory(session_config=session)

        assert dialog.shared_job_settings._config is session

    def test_session_config_passed_to_display_widgets(
        self, submit_dialog_factory, disk_config
    ) -> None:
        """Session config should reach farm_box and queue_box through the widget hierarchy."""
        session = _apply_cli_options_to_config(config=disk_config, farm_id=CLI_FARM_ID)
        dialog = submit_dialog_factory(session_config=session)

        settings_box = dialog.shared_job_settings.deadline_cloud_settings_box
        assert settings_box._config is session
        assert settings_box.farm_box._config is session
        assert settings_box.queue_box._config is session


@pytest.mark.usefixtures("fresh_deadline_config")
class TestWorkstationConfigWidget:
    """Tests for DeadlineWorkstationConfigWidget session vs disk config behavior."""

    def test_no_session_config_by_default(self, config_dialog_mocks, qtbot) -> None:
        widget = DeadlineWorkstationConfigWidget()
        qtbot.addWidget(widget)
        assert widget._session_config is None

    def test_has_session_config_when_provided(self, config_dialog_mocks, qtbot) -> None:
        session = ConfigParser()
        config_file.set_setting("defaults.queue_id", CLI_QUEUE_ID, config=session)

        widget = DeadlineWorkstationConfigWidget(session_config=session)
        qtbot.addWidget(widget)
        assert widget._session_config is not None

    def test_refresh_uses_session_config(self, config_dialog_mocks, qtbot) -> None:
        """In session mode, refresh() bases the config on the session config, not disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        config_file.set_setting("defaults.farm_id", CLI_FARM_ID, config=session)

        widget = DeadlineWorkstationConfigWidget(session_config=session)
        qtbot.addWidget(widget)
        assert config_file.get_setting("defaults.farm_id", config=widget.config) == CLI_FARM_ID

    def test_refresh_uses_disk_config_without_session(self, config_dialog_mocks, qtbot) -> None:
        """Without session config, refresh() bases the config on disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        widget = DeadlineWorkstationConfigWidget()
        qtbot.addWidget(widget)
        assert config_file.get_setting("defaults.farm_id", config=widget.config) == DISK_FARM_ID

    def test_apply_with_session_does_not_write_to_disk(self, config_dialog_mocks, qtbot) -> None:
        """With session config, apply() mutates in-memory without writing to disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        config_file.set_setting("defaults.farm_id", CLI_FARM_ID, config=session)

        widget = DeadlineWorkstationConfigWidget(session_config=session)
        qtbot.addWidget(widget)

        widget.changes["defaults.farm_id"] = "farm-newvalue1111111111111111111111"
        assert widget.apply() is True
        mock_cf.write_config.assert_not_called()
        assert (
            config_file.get_setting("defaults.farm_id", config=session)
            == "farm-newvalue1111111111111111111111"
        )

    def test_apply_without_session_writes_to_disk(self, config_dialog_mocks, qtbot) -> None:
        """Without session config, apply() writes to disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        widget = DeadlineWorkstationConfigWidget()
        qtbot.addWidget(widget)

        widget.changes["defaults.farm_id"] = "farm-newvalue1111111111111111111111"
        assert widget.apply() is True
        mock_cf.write_config.assert_called_once()


@pytest.mark.usefixtures("fresh_deadline_config")
class TestConfigDialog:
    """Tests for DeadlineConfigDialog button bar and disk I/O actions."""

    def test_disk_actions_disabled_without_session(self, config_dialog_mocks, qtbot) -> None:
        """Without session_config, disk actions start disabled."""
        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)
        assert not dialog._load_from_disk_action.isEnabled()

    def test_config_file_button_exists_with_session(self, config_dialog_mocks, qtbot) -> None:
        """With session_config, Config File button is present."""
        session = ConfigParser()
        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)
        assert dialog._config_file_button is not None

    def test_session_mode_does_not_write_to_disk(self, config_dialog_mocks, qtbot) -> None:
        """With session_config, the widget is in session mode and doesn't write to disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        session.read_dict(disk)

        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)

        assert dialog.config_box._session_config is not None
        mock_cf.write_config.assert_not_called()

    def test_session_config_shows_cli_override(self, config_dialog_mocks, qtbot) -> None:
        """CLI --queue-id override is visible in the dialog's config."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        config_file.set_setting("defaults.queue_id", DISK_QUEUE_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        session.read_dict(disk)
        config_file.set_setting("defaults.queue_id", CLI_QUEUE_ID, config=session)

        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)

        assert (
            config_file.get_setting("defaults.queue_id", config=dialog.config_box.config)
            == CLI_QUEUE_ID
        )

    def test_save_to_disk_with_session_writes_config(self, config_dialog_mocks, qtbot) -> None:
        """Save to Disk applies pending changes to session config and writes to disk."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        session.read_dict(disk)
        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.farm_id"] = CLI_FARM_ID
        dialog._on_save_to_disk()

        mock_cf.write_config.assert_called_once()
        assert config_file.get_setting("defaults.farm_id", config=session) == CLI_FARM_ID
        assert not dialog.config_box.changes

    def test_save_to_disk_without_session_writes_config(self, config_dialog_mocks, qtbot) -> None:
        """Save to Disk reads from disk, applies changes, and writes back."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.farm_id"] = CLI_FARM_ID
        dialog._on_save_to_disk()

        mock_cf.write_config.assert_called_once()
        assert not dialog.config_box.changes

    def test_save_to_disk_blocks_on_not_valid(self, config_dialog_mocks, qtbot) -> None:
        """Save to Disk should not write when changes contain NOT_VALID_MARKER."""
        mock_cf = config_dialog_mocks

        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.aws_profile_name"] = "[NOT VALID] bad"
        with patch("deadline.client.ui.dialogs.deadline_config_dialog.QMessageBox.warning"):
            dialog._on_save_to_disk()

        mock_cf.write_config.assert_not_called()

    def test_load_from_disk_with_session_replaces_session(self, config_dialog_mocks, qtbot) -> None:
        """Load from Disk replaces the session config with disk contents."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        session = ConfigParser()
        session.read_dict(disk)
        config_file.set_setting("defaults.farm_id", CLI_FARM_ID, config=session)

        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)

        dialog._on_load_from_disk()

        assert (
            config_file.get_setting("defaults.farm_id", config=dialog._session_config)
            == DISK_FARM_ID
        )

    def test_load_from_disk_without_session_clears_changes(
        self, config_dialog_mocks, qtbot
    ) -> None:
        """Load from Disk discards pending changes."""
        mock_cf = config_dialog_mocks
        disk = ConfigParser()
        config_file.set_setting("defaults.farm_id", DISK_FARM_ID, config=disk)
        mock_cf.read_config.return_value = disk

        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.farm_id"] = CLI_FARM_ID
        dialog._on_load_from_disk()

        assert not dialog.config_box.changes

    def test_accept_blocks_on_not_valid_with_session(self, config_dialog_mocks, qtbot) -> None:
        """Ok should not close the dialog when session-mode changes are not valid."""
        session = ConfigParser()
        dialog = DeadlineConfigDialog(session_config=session)
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.aws_profile_name"] = "[NOT VALID] bad"
        with patch("deadline.client.ui.dialogs.deadline_config_dialog.QMessageBox.warning"):
            with patch.object(type(dialog).mro()[1], "accept") as mock_super_accept:
                dialog.accept()
                mock_super_accept.assert_not_called()

    def test_accept_blocks_on_not_valid_without_session(self, config_dialog_mocks, qtbot) -> None:
        """Ok should not close the dialog when workstation-mode changes are not valid."""
        mock_cf = config_dialog_mocks

        dialog = DeadlineConfigDialog()
        qtbot.addWidget(dialog)

        dialog.config_box.changes["defaults.aws_profile_name"] = "[NOT VALID] bad"
        with patch("deadline.client.ui.dialogs.deadline_config_dialog.QMessageBox.warning"):
            with patch.object(type(dialog).mro()[1], "accept") as mock_super_accept:
                dialog.accept()
                mock_super_accept.assert_not_called()
        mock_cf.write_config.assert_not_called()


class TestSubmitDialogPersistJobId:
    """Tests for job ID persistence after successful GUI submission."""

    def test_persist_job_id_uses_session_config_values(
        self, submit_dialog_factory, disk_config
    ) -> None:
        """After submission, persist_job_id is called with profile/farm/queue from session config."""
        session = _apply_cli_options_to_config(
            config=disk_config, farm_id=CLI_FARM_ID, queue_id=CLI_QUEUE_ID
        )
        dialog = submit_dialog_factory(session_config=session)

        with patch(
            "deadline.client.ui.dialogs.submit_job_to_deadline_dialog.persist_job_id"
        ) as mock_persist:
            dialog._submission_succeeded_signal_receiver("job-test1111111111111111111111111")

        mock_persist.assert_called_once_with(
            "job-test1111111111111111111111111",
            profile=config_file.get_setting("defaults.aws_profile_name", config=session),
            farm_id=CLI_FARM_ID,
            queue_id=CLI_QUEUE_ID,
        )


class TestSetSessionConfigPropagation:
    """Tests that set_session_config propagates to child display widgets."""

    def test_set_session_config_propagates_to_child_widgets(
        self, submit_dialog_factory, disk_config
    ) -> None:
        """set_session_config should update config on farm_box and queue_box."""
        session = _apply_cli_options_to_config(config=disk_config, farm_id=CLI_FARM_ID)
        dialog = submit_dialog_factory(session_config=session)

        new_session = ConfigParser()
        new_session.read_dict(disk_config)
        config_file.set_setting(
            "defaults.farm_id", "farm-new111111111111111111111111111", config=new_session
        )

        dialog.shared_job_settings.set_session_config(new_session)

        settings_box = dialog.shared_job_settings.deadline_cloud_settings_box
        assert settings_box._config is new_session
        assert settings_box.farm_box._config is new_session
        assert settings_box.queue_box._config is new_session
