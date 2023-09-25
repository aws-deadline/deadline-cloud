# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
"""
Provides a modal dialog box for modifying the Amazon Deadline Cloud
local workstation configuration.

Example code:
    from deadline.client.ui.dialogs import DeadlineConfigDialog
    DeadlineConfigDialog.configure_settings(parent=self)
"""

__all__ = ["DeadlineConfigDialog"]

import sys
import threading
from configparser import ConfigParser
from logging import getLogger, root
from typing import Callable, List, Optional

import boto3  # type: ignore[import]
from botocore.exceptions import ProfileNotFound  # type: ignore[import]
from PySide2.QtCore import QSize, Qt, Signal
from PySide2.QtWidgets import (  # pylint: disable=import-error; type: ignore
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSizePolicy,
    QSpacerItem,
    QStyle,
    QVBoxLayout,
    QWidget,
)

from ... import api
from ..deadline_credentials_status import DeadlineCredentialsStatus
from ...config import config_file, get_setting_default, str2bool
from .. import CancelationFlag, block_signals
from ..widgets import DirectoryPickerWidget
from ..widgets.deadline_credentials_status_widget import DeadlineCredentialsStatusWidget
from .deadline_login_dialog import DeadlineLoginDialog

logger = getLogger(__name__)

NOT_VALID_MARKER = "[NOT VALID]"


class DeadlineConfigDialog(QDialog):
    """
    A modal dialog box for modifying the Amazon Deadline Cloud local workstation
    configuration.

    Example code:
        DeadlineConfigDialog.configure_settings(parent=self)
    """

    @staticmethod
    def configure_settings(parent=None) -> bool:
        """
        Static method that runs the Deadline Config Dialog.

        Returns True if any changes were applied, False otherwise.
        """
        deadline_config = DeadlineConfigDialog(parent=parent)
        deadline_config.exec()
        return deadline_config.changes_were_applied

    def __init__(self, parent=None) -> None:
        super().__init__(
            parent=parent, f=Qt.WindowSystemMenuHint | Qt.WindowTitleHint | Qt.WindowCloseButtonHint
        )

        self.setWindowTitle("Amazon Deadline Cloud Workstation Configuration")
        self.changes_were_applied = False
        self.deadline_credentials_status = DeadlineCredentialsStatus.getInstance()
        self._build_ui()

    def _build_ui(self):
        self.layout = QVBoxLayout(self)

        self.config_box = DeadlineWorkstationConfigWidget(parent=self)
        self.layout.addWidget(self.config_box)
        self.config_box.refreshed.connect(self.on_refresh)

        self.layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))

        self.creds_status_box = DeadlineCredentialsStatusWidget(self)
        self.layout.addWidget(self.creds_status_box)
        self.deadline_credentials_status.deadline_config_changed.connect(self.config_box.refresh)
        self.deadline_credentials_status.api_availability_changed.connect(
            self.on_creds_status_update
        )

        # We only use a Close button, not OK/Cancel, because we live update the settings.
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel | QDialogButtonBox.Apply, Qt.Horizontal
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        self.button_box.clicked.connect(self.on_button_box_clicked)
        self.login_button = QPushButton("Login")
        self.login_button.clicked.connect(self.on_login)
        self.button_box.addButton(self.login_button, QDialogButtonBox.ResetRole)
        self.logout_button = QPushButton("Logout")
        self.logout_button.clicked.connect(self.on_logout)
        self.button_box.addButton(self.logout_button, QDialogButtonBox.ResetRole)
        self.layout.addWidget(self.button_box)

    def accept(self):
        if self.config_box.apply():
            super().accept()

    def on_login(self):
        DeadlineLoginDialog.login(parent=self, config=self.config_box.config)
        self.deadline_credentials_status.refresh_status()
        self.config_box.refresh()

    def on_logout(self):
        api.logout(config=self.config_box.config)
        self.deadline_credentials_status.refresh_status()
        self.config_box.refresh()

    def on_button_box_clicked(self, button):
        if self.button_box.standardButton(button) == QDialogButtonBox.Apply:
            self.config_box.apply()

    def on_refresh(self):
        # Enable the "Apply" button only if there are changes
        self.button_box.button(QDialogButtonBox.Apply).setEnabled(bool(self.config_box.changes))
        # Update the creds status with the refreshed config
        self.deadline_credentials_status.set_config(self.config_box.config)

    def on_creds_status_update(self):
        # If the Amazon Deadline Cloud API is authorized successfully for the AWS profile
        # in the config dialog, refresh the farm/queue lists
        if self.deadline_credentials_status.api_availability and config_file.get_setting(
            "defaults.aws_profile_name", self.deadline_credentials_status.config
        ) == config_file.get_setting("defaults.aws_profile_name", self.config_box.config):
            self.config_box.refresh_lists()


class DeadlineWorkstationConfigWidget(QWidget):
    """
    A widget that displays and edits the Amazon Deadline Cloud local workstation.
    """

    # Signal for when the GUI is refreshed
    refreshed = Signal()

    # Emitted when an async refresh_queues_list thread completes,
    # provides (aws_profile_name, farm_id, [(queue_id, queue_name), ...])
    _queue_list_update = Signal(str, str, list)
    # Emitted when an async refresh_storage_profiles_name_list thread completes,
    # provides (aws_profile_name, farm_id, queue_id, [storage_profile_id, ...])
    _storage_profile_list_update = Signal(str, str, list)
    # This signal is sent when any background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    _background_exception = Signal(str, BaseException)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.changes = {}
        self.config: Optional[ConfigParser] = None

        self._build_ui()
        self._fill_aws_profiles_box()
        self.refresh()

    def minimumSizeHint(self):
        return QSize(500, 200)

    def _build_ui(self):
        self.layout = QVBoxLayout(self)

        self.labels = {}
        self._refresh_callbacks: List[Callable] = []

        # Global settings
        self.global_settings_group = QGroupBox(parent=self, title="Global Settings")
        self.layout.addWidget(self.global_settings_group)
        global_settings_layout = QFormLayout(self.global_settings_group)
        self._build_global_settings_ui(self.global_settings_group, global_settings_layout)

        # AWS Profile-specific settings
        self.profile_settings_group = QGroupBox(parent=self, title="Profile Settings")
        self.layout.addWidget(self.profile_settings_group)
        profile_settings_layout = QFormLayout(self.profile_settings_group)
        self._build_profile_settings_ui(self.profile_settings_group, profile_settings_layout)

        # Farm-specific settings
        self.farm_settings_group = QGroupBox(parent=self, title="Farm Settings")
        self.layout.addWidget(self.farm_settings_group)
        farm_settings_layout = QFormLayout(self.farm_settings_group)
        self._build_farm_settings_ui(self.farm_settings_group, farm_settings_layout)

        # General settings
        self.general_settings_group = QGroupBox(parent=self, title="General Settings")
        self.layout.addWidget(self.general_settings_group)
        general_settings_layout = QFormLayout(self.general_settings_group)
        self._build_general_settings_ui(self.general_settings_group, general_settings_layout)

        self._background_exception.connect(self.handle_background_exception)

    def _build_global_settings_ui(self, group, layout):
        self.aws_profiles_box = QComboBox(parent=group)
        aws_profile_label = self.labels["defaults.aws_profile_name"] = QLabel("AWS Profile")
        layout.addRow(aws_profile_label, self.aws_profiles_box)
        self.aws_profiles_box.currentTextChanged.connect(self.aws_profile_changed)

    def _build_profile_settings_ui(self, group, layout):
        self.job_history_dir_edit = DirectoryPickerWidget(
            initial_directory="",
            directory_label="Job History Dir",
            parent=group,
            collapse_user_dir=True,
        )
        job_history_dir_label = self.labels["settings.job_history_dir"] = QLabel("Job History Dir")
        layout.addRow(job_history_dir_label, self.job_history_dir_edit)
        self.job_history_dir_edit.path_changed.connect(self.job_history_dir_changed)

        self.default_farm_box = DeadlineFarmListComboBox(parent=group)
        default_farm_box_label = self.labels["defaults.farm_id"] = QLabel("Default Farm")
        self.default_farm_box.box.currentIndexChanged.connect(self.default_farm_changed)
        self.default_farm_box.background_exception.connect(self.handle_background_exception)
        layout.addRow(default_farm_box_label, self.default_farm_box)

        self.deadline_endpoint_url_edit = QLineEdit(parent=group)
        deadline_endpoint_url_label = self.labels["settings.deadline_endpoint_url"] = QLabel(
            "Amazon Deadline Cloud Endpoint URL"
        )
        layout.addRow(deadline_endpoint_url_label, self.deadline_endpoint_url_edit)
        self.deadline_endpoint_url_edit.editingFinished.connect(self.deadline_endpoint_url_edited)

    def _build_farm_settings_ui(self, group, layout):
        self.default_queue_box = DeadlineQueueListComboBox(parent=group)
        default_queue_box_label = self.labels["defaults.queue_id"] = QLabel("Default Queue")
        self.default_queue_box.box.currentIndexChanged.connect(self.default_queue_changed)
        self.default_queue_box.background_exception.connect(self.handle_background_exception)
        layout.addRow(default_queue_box_label, self.default_queue_box)

        self.default_storage_profile_box = DeadlineStorageProfileNameListComboBox(parent=group)
        default_storage_profile_box_label = self.labels["settings.storage_profile_id"] = QLabel(
            "Default Storage Profile"
        )
        self.default_storage_profile_box.box.currentIndexChanged.connect(
            self.default_storage_profile_name_changed
        )
        self.default_storage_profile_box.background_exception.connect(
            self.handle_background_exception
        )
        layout.addRow(default_storage_profile_box_label, self.default_storage_profile_box)
        self.file_system_box = self._init_combobox_setting(
            group,
            layout,
            "defaults.job_attachments_file_system",
            "Job Attachments FileSystem Options",
            ["PRELOAD", "ON_DEMAND"],
        )

    def _build_general_settings_ui(self, group, layout):
        self.auto_accept = self._init_checkbox_setting(
            group, layout, "settings.auto_accept", "Auto Accept Confirmation Prompts"
        )

        self._log_levels = ["ERROR", "WARNING", "INFO", "DEBUG"]
        self.log_level_box = self._init_combobox_setting(
            group,
            layout,
            "settings.log_level",
            "Current Logging Level",
            self._log_levels,
        )

    def _init_checkbox_setting(
        self, group: QWidget, layout: QFormLayout, setting_name: str, label_text: str
    ) -> QCheckBox:
        """
        Creates a checkbox setting and adds it to the specified group and layout. This function also connects state
        change logic, refresh logic, and logic to save the setting in the config file.

        Args:
            group (QWidget): The parent of the checkbox
            layout (QFormLayout): The layout to add a row to for the checkbox
            setting_name (str): The setting name as provided to the config. E.g. "settings.foo_bar"
            label_text (str): The displayed description. E.g. "Foo Bar"

        Returns:
            QCheckBox: The created checkbox.
        """
        checkbox = QCheckBox(parent=group)
        label = self.labels[setting_name] = QLabel(label_text)
        layout.addRow(label, checkbox)

        def refresh_checkbox():
            """Function that refreshes the state of the checkbox based on the setting name"""
            with block_signals(checkbox):
                try:
                    state = str2bool(config_file.get_setting(setting_name, config=self.config))
                except ValueError as e:
                    logger.warning(f"{e} for '{setting_name}'")
                    state = False
                checkbox.setChecked(state)

        self._refresh_callbacks.append(refresh_checkbox)

        def checkbox_changed(new_state: int):
            """Callback for if the state of a given checkbox has changed"""
            value = str(checkbox.isChecked())
            self.changes[setting_name] = value
            self.refresh()

        checkbox.stateChanged.connect(checkbox_changed)

        return checkbox

    def _init_combobox_setting(
        self,
        group: QWidget,
        layout: QFormLayout,
        setting_name: str,
        label_text: str,
        values: List[str],
    ):
        """
        Creates a combobox setting and adds it to the specified group and layout. This function also connects state
        change logic, refresh logic, and logic to save the setting in the config file.

        Args:
            group (QWidget): The parent of the combobox
            layout (QFormLayout): The layout to add a row to for the combobox
            setting_name (str): The setting name as provided to the config. E.g. "settings.foo_bar"
            label_text (str): The displayed description. E.g. "Foo Bar"
            values (List[str]): The list of values that can be selected. E.g. ["Option A", "Option B"]

        Returns:
            QComboBox: The created combobox.
        """
        label = QLabel(label_text)
        combo_box = QComboBox(parent=group)
        layout.addRow(label, combo_box)
        combo_box.addItems(values)

        def refresh_combo_box():
            """Function that refreshes the state of the combo box based on the setting name"""
            with block_signals(combo_box):
                value = config_file.get_setting(setting_name, config=self.config)
                if value not in values:
                    default = get_setting_default(setting_name, config=self.config)
                    logger.warning(f"'{value}' is not one of {values}. Defaulting to '{default}'.")
                    value = default
                combo_box.setCurrentText(value)

        def combo_box_changed(new_value):
            """Callback for if the state of a given checkbox has changed"""
            self.changes[setting_name] = new_value
            self.refresh()

        combo_box.currentTextChanged.connect(combo_box_changed)

        self._refresh_callbacks.append(refresh_combo_box)

    def handle_background_exception(self, title, e):
        QMessageBox.warning(self, title, f"Encountered an error:\n{e}")

    def _fill_aws_profiles_box(self):
        # Use boto3 directly with no profile, so we don't get an error
        # if the configured profile does not exist.
        try:
            session = boto3.Session()
            aws_profile_names = [
                "(default)",
                *(
                    name
                    for name in session._session.full_config["profiles"].keys()
                    if name != "default"
                ),
            ]
        except ProfileNotFound:
            logger.exception("Failed to create boto3.Session for AWS profile list")
            aws_profile_names = [f"{NOT_VALID_MARKER} <failed to retrieve AWS profile names>"]

        self.aws_profile_names = aws_profile_names
        with block_signals(self.aws_profiles_box):
            self.aws_profiles_box.addItems(list(self.aws_profile_names))

    def refresh_lists(self):
        self.default_farm_box.refresh_list()
        self.default_queue_box.refresh_list()
        self.default_storage_profile_box.refresh_list()

    def refresh(self):
        """
        Refreshes all the configuration UI elements from the current config.
        """
        # Make self.config be a deep copy of the config, with changes applied
        self.config = ConfigParser()
        self.config.read_dict(config_file.read_config())
        for setting_name, value in self.changes.items():
            config_file.set_setting(setting_name, value, self.config)
        self.default_farm_box.set_config(self.config)
        self.default_queue_box.set_config(self.config)
        self.default_storage_profile_box.set_config(self.config)

        with block_signals(self.aws_profiles_box):
            aws_profile_name = config_file.get_setting(
                "defaults.aws_profile_name", config=self.config
            )
            # Change the values representing the default to the UI value representing the default
            if aws_profile_name in ("(default)", "default", ""):
                aws_profile_name = "(default)"
            elif aws_profile_name not in self.aws_profile_names:
                aws_profile_name = f"{NOT_VALID_MARKER} {aws_profile_name}"
            index = self.aws_profiles_box.findText(aws_profile_name, Qt.MatchFixedString)
            if index >= 0:
                self.aws_profiles_box.setCurrentIndex(index)
            else:
                self.aws_profiles_box.insertItem(0, aws_profile_name)
                self.aws_profiles_box.setCurrentIndex(0)

        with block_signals(self.job_history_dir_edit):
            job_history_dir = config_file.get_setting(
                "settings.job_history_dir", config=self.config
            )
            self.job_history_dir_edit.setText(job_history_dir)

        self.default_farm_box.refresh_selected_id()

        with block_signals(self.deadline_endpoint_url_edit):
            deadline_endpoint_url = config_file.get_setting(
                "settings.deadline_endpoint_url", config=self.config
            )
            self.deadline_endpoint_url_edit.setText(deadline_endpoint_url)

        for refresh_callback in self._refresh_callbacks:
            refresh_callback()

        self.default_queue_box.refresh_selected_id()
        self.default_storage_profile_box.refresh_selected_id()

        # Put an orange box around the labels for any settings that are changed
        for setting_name, label in self.labels.items():
            if setting_name in self.changes:
                label.setStyleSheet("border: 1px solid orange;")
            else:
                label.setStyleSheet("")

        self.refreshed.emit()

    def apply(self) -> bool:
        """
        Apply all the settings that the user has changed into the config file.

        Returns True if the settings were applied, False otherwise.
        """
        for setting_name, value in self.changes.items():
            if value.startswith(NOT_VALID_MARKER):
                QMessageBox.warning(
                    self,
                    "Apply Changes",
                    f"Cannot apply changes, {value} is not valid for setting {setting_name}",
                )
                return False

        self.config = config_file.read_config()

        for setting_name, value in self.changes.items():
            config_file.set_setting(setting_name, value, self.config)
        root.setLevel(config_file.get_setting("settings.log_level"))

        self.changes.clear()

        # The file watcher will see the file modification and call refresh() for us
        config_file.write_config(self.config)

        self.changes_were_applied = False
        return True

    def aws_profile_changed(self, value):
        self.changes["defaults.aws_profile_name"] = value
        self.default_farm_box.clear_list()
        self.default_queue_box.clear_list()
        self.default_storage_profile_box.clear_list()
        self.refresh()

    def job_history_dir_changed(self):
        job_history_dir = self.job_history_dir_edit.text()
        # Only apply the change if the text was actually edited
        if job_history_dir != config_file.get_setting(
            "settings.job_history_dir", config=self.config
        ):
            self.changes["settings.job_history_dir"] = job_history_dir
        self.refresh()

    def default_farm_changed(self, index):
        self.changes["defaults.farm_id"] = self.default_farm_box.box.itemData(index)
        self.refresh()
        self.default_queue_box.refresh_list()
        self.default_storage_profile_box.refresh_list()

    def deadline_endpoint_url_edited(self):
        deadline_endpoint_url = self.deadline_endpoint_url_edit.text()
        # Only apply the change if the text was actually edited
        if deadline_endpoint_url != config_file.get_setting(
            "settings.deadline_endpoint_url", config=self.config
        ):
            self.changes["settings.deadline_endpoint_url"] = deadline_endpoint_url
        self.refresh()

    def default_queue_changed(self, index):
        self.changes["defaults.queue_id"] = self.default_queue_box.box.itemData(index)
        self.refresh()
        self.default_storage_profile_box.refresh_list()

    def default_storage_profile_name_changed(self, index):
        self.changes["settings.storage_profile_id"] = self.default_storage_profile_box.box.itemData(
            index
        )
        self.refresh()


class _DeadlineResourceListComboBox(QWidget):
    """
    A ComboBox for selecting an Amazon Deadline Cloud Id, with a refresh button.

    The caller should connect the `background_exception` signal, e.g.
    to show a message box, and should call `set_config` whenever there is
    a change to the Amazon Deadline Cloud config object.

    Args:
        resource_name (str): The resource name for the list, like "Farm",
                "Queue", "Fleet".
    """

    # Emitted when the background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    background_exception = Signal(str, BaseException)

    # Emitted when an async refresh_farms_list thread completes,
    # provides (refresh_id, [(farm_id, farm_name), ...])
    _list_update = Signal(int, list)

    def __init__(self, resource_name, setting_name, parent=None):
        super().__init__(parent)

        self.__refresh_thread = None
        self.__refresh_id = 0
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)

        self.resource_name = resource_name
        self.setting_name = setting_name

        self._build_ui()

    def _build_ui(self):
        self.box = QComboBox(parent=self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.box)
        self.refresh_button = QPushButton("")
        layout.addWidget(self.refresh_button)
        self.refresh_button.setIcon(self.style().standardIcon(QStyle.SP_BrowserReload))
        self.refresh_button.setFixedSize(QSize(22, 22))  # Make the button square
        self.refresh_button.clicked.connect(self.refresh_list)
        self._list_update.connect(self.handle_list_update)
        self.background_exception.connect(self.handle_background_exception)

    def handle_background_exception(self, e):
        with block_signals(self.box):
            self.box.clear()
        self.refresh_selected_id()

    def count(self) -> int:
        """Returns the number of items in the combobox"""
        return self.box.count()

    def set_config(self, config: ConfigParser) -> None:
        """Updates the Amazon Deadline Cloud config object the control uses."""
        self.config = config

    def clear_list(self):
        """
        Fully clears the list. The caller needs to call either
        `refresh_list` or `refresh_selected_id` at a later point
        to finish it.
        """
        with block_signals(self.box):
            self.box.clear()

    def refresh_list(self):
        """
        Starts a background thread to refresh the resource list.
        """
        config = self.config
        selected_id = config_file.get_setting(self.setting_name, config=config)
        # Reset to a list of just the currently configured id during refresh
        with block_signals(self.box):
            self.box.clear()
            self.box.addItem("<refreshing>", userData=selected_id)

        self.__refresh_id += 1
        self.__refresh_thread = threading.Thread(
            target=self._refresh_thread_function,
            name=f"Amazon Deadline Cloud Refresh {self.resource_name} Thread",
            args=(self.__refresh_id, config),
        )
        self.__refresh_thread.start()

    def handle_list_update(self, refresh_id, items_list):
        # Apply the refresh if it's still for the latest call
        if refresh_id == self.__refresh_id:
            with block_signals(self.box):
                self.box.clear()
                for name, id in items_list:
                    self.box.addItem(name, userData=id)

                self.refresh_selected_id()

    def refresh_selected_id(self):
        """Refreshes the selected id from the config object"""
        selected_id = config_file.get_setting(self.setting_name, config=self.config)
        # Restore the selected Id, inserting a new item if
        # it doesn't exist in the list.
        with block_signals(self.box):
            index = self.box.findData(selected_id)
            if index >= 0:
                self.box.setCurrentIndex(index)
            else:
                self.box.insertItem(0, selected_id or "<none selected>", userData=selected_id)
                self.box.setCurrentIndex(0)

    def _refresh_thread_function(self, refresh_id: int, config: Optional[ConfigParser] = None):
        """
        This function gets started in a background thread to refresh the list.
        """
        try:
            resources = self.list_resources(config=config)
            if not self.canceled:
                self._list_update.emit(refresh_id, resources)
        except BaseException as e:
            if not self.canceled and refresh_id == self.__refresh_id:
                self.background_exception.emit(f"Refresh {self.resource_name}s List", e)


class DeadlineFarmListComboBox(_DeadlineResourceListComboBox):
    def __init__(self, parent=None):
        super().__init__(resource_name="Farm", setting_name="defaults.farm_id", parent=parent)

    def list_resources(self, config: Optional[ConfigParser]):
        response = api.list_farms(config=config)
        return sorted([(item["displayName"], item["farmId"]) for item in response["farms"]])


class DeadlineQueueListComboBox(_DeadlineResourceListComboBox):
    def __init__(self, parent=None):
        super().__init__(resource_name="Queue", setting_name="defaults.queue_id", parent=parent)

    def list_resources(self, config: Optional[ConfigParser]):
        default_farm_id = config_file.get_setting("defaults.farm_id", config=config)
        if default_farm_id:
            response = api.list_queues(config=config, farmId=default_farm_id)
            return sorted([(item["displayName"], item["queueId"]) for item in response["queues"]])
        else:
            return []


class DeadlineStorageProfileNameListComboBox(_DeadlineResourceListComboBox):
    WINDOWS_OS = "windows"
    MAC_OS = "macos"
    LINUX_OS = "linux"

    def __init__(self, parent=None):
        super().__init__(
            resource_name="Storage Profile",
            setting_name="settings.storage_profile_id",
            parent=parent,
        )

    def list_resources(self, config: Optional[ConfigParser]):
        default_farm_id = config_file.get_setting("defaults.farm_id", config=config)
        default_queue_id = config_file.get_setting("defaults.queue_id", config=config)
        if default_farm_id and default_queue_id:
            response = api.list_storage_profiles_for_queue(
                config=config, farmId=default_farm_id, queueId=default_queue_id
            )
            storage_profiles = response.get("storageProfiles", [])
            return sorted(
                (item["displayName"], item["storageProfileId"])
                for item in storage_profiles
                if self._get_current_os() == item["osFamily"]
            )
        else:
            return []

    def _get_current_os(self) -> str:
        """
        Get a string specifying what the OS is, following the format the Deadline storage profile API expects.
        """
        if sys.platform.startswith("linux"):
            return self.LINUX_OS

        if sys.platform.startswith("darwin"):
            return self.MAC_OS

        if sys.platform.startswith("win"):
            return self.WINDOWS_OS

        return "Unknown"
