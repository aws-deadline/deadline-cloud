# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

"""
A UI Widget containing the render setup tab
"""
import sys
import threading
from typing import Any, Dict, Optional

from PySide2.QtCore import Qt, Signal  # type: ignore
from PySide2.QtWidgets import (  # type: ignore
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

from ... import api
from ...config import get_setting
from .. import CancelationFlag


class SharedJobSettingsWidget(QWidget):  # pylint: disable=too-few-public-methods
    """
    Widget that holds Job setup shared across all job types.
    """

    def __init__(self, initial_settings, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)

        self.desc_box = SubmissionDescriptionWidget(initial_settings, self)
        layout.addWidget(self.desc_box)
        self.deadline_settings_box = DeadlineSettingsWidget(initial_settings, self)
        layout.addWidget(self.deadline_settings_box)

        self.installation_requirements_box = InstallationRequirementsWidget(initial_settings, self)
        layout.addWidget(self.installation_requirements_box)

        layout.addItem(QSpacerItem(0, 0, QSizePolicy.Minimum, QSizePolicy.Expanding))


class SubmissionDescriptionWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI element to hold top level description components of the submission

    The settings object should be a dataclass with:
      - `name: str`        The name of the Job to submit.
      - `description: str`  The description of the Job to submit.
    """

    def __init__(self, initial_settings, parent=None):
        super().__init__("Description", parent)

        self._build_ui()
        self._load_initial_settings(initial_settings)

    def _build_ui(self):
        self.layout = QFormLayout(self)

        self.sub_name_edit = QLineEdit()
        self.layout.addRow("Name", self.sub_name_edit)

        self.desc_label = QLabel("Description")
        self.desc_edit = QLineEdit()
        self.layout.addRow(self.desc_label, self.desc_edit)

        # TODO: Re-enable when this option is available in the back end.
        self.desc_label.setEnabled(False)
        self.desc_edit.setEnabled(False)

    def _load_initial_settings(self, settings):
        self.sub_name_edit.setText(settings.name)
        self.desc_edit.setText(settings.description)

    def update_settings(self, settings):
        """
        Update a given instance of scene settings with updated values.
        """
        settings.name = self.sub_name_edit.text()
        settings.description = self.desc_edit.text()


class DeadlineSettingsWidget(QGroupBox):
    """
    UI component for the Deadline Render Manager.
    """

    def __init__(self, initial_settings, parent: Optional[QWidget] = None):
        super().__init__("Deadline Settings", parent=parent)
        self.deadline_settings: Dict[str, Any] = {"counter": -1}
        self.lyt = QFormLayout(self)
        self.lyt.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        self._build_ui()
        self._load_initial_settings(initial_settings)

    def _set_enabled_with_label(self, prop_name: str, enabled: bool):
        """Enable/disable a control w/ its label"""
        getattr(self, prop_name).setEnabled(enabled)
        getattr(self, prop_name + "_label").setEnabled(enabled)

    def _build_ui(self):
        """
        Build the UI for the Deadline settings
        """
        self.farm_box_label = QLabel("Farm")
        self.farm_box = DeadlineFarmDisplay()
        self.lyt.addRow(self.farm_box_label, self.farm_box)

        self.queue_box_label = QLabel("Queue")
        self.queue_box = DeadlineQueueDisplay()
        self.lyt.addRow(self.queue_box_label, self.queue_box)

        self.initial_status_box_label = QLabel("Initial State")
        self.initial_status_box = QComboBox(parent=self)
        self.initial_status_box.addItems(["READY", "SUSPENDED"])
        self.lyt.addRow(self.initial_status_box_label, self.initial_status_box)

        self.failed_tasks_limit_box_label = QLabel("Failed Tasks Limit")
        self.failed_tasks_limit_box_label.setToolTip(
            "Maximum number of Tasks that can fail before the Job will be marked as failed."
        )
        self.failed_tasks_limit_box = QSpinBox(parent=self)
        self.failed_tasks_limit_box.setRange(0, 2147483647)
        self.lyt.addRow(self.failed_tasks_limit_box_label, self.failed_tasks_limit_box)

        self.task_retry_limit_box_label = QLabel("Task Retry Limit")
        self.task_retry_limit_box_label.setToolTip(
            "Maximum number of times that a Task will retry before it's marked as failed."
        )
        self.task_retry_limit_box = QSpinBox(parent=self)
        self.task_retry_limit_box.setRange(0, 2147483647)
        self.lyt.addRow(self.task_retry_limit_box_label, self.task_retry_limit_box)

        self.priority_box_label = QLabel("Priority")
        self.priority_box = QSpinBox(parent=self)
        self.lyt.addRow(self.priority_box_label, self.priority_box)

    def refresh_setting_controls(self, deadline_authorized):
        """
        Refreshes the controls for UI items that depend on the Amazon Deadline Cloud API
        for their values.

        Args:
            deadline_authorized (bool): Should be the result of a call to
                    api.check_deadline_available, for example from
                    a Amazon Deadline Cloud Status Widget.
        """
        self.farm_box.refresh(deadline_authorized)
        self.queue_box.refresh(deadline_authorized)

    def _load_initial_settings(self, settings):
        self.initial_status_box.setCurrentText(settings.initial_status)
        self.failed_tasks_limit_box.setValue(settings.failed_tasks_limit)
        self.task_retry_limit_box.setValue(settings.task_retry_limit)
        self.priority_box.setValue(settings.priority)

    def update_settings(self, settings) -> None:
        """
        Updates a Amazon Deadline Cloud settings object with the latest values.

        The settings object should be a dataclass with:
            initial_status: str (or enum of base str)
            failed_tasks_limit: int
            task_retry_limit: int
            priority: int
        """
        settings.initial_status = self.initial_status_box.currentText()
        settings.failed_tasks_limit = self.failed_tasks_limit_box.value()
        settings.task_retry_limit = self.task_retry_limit_box.value()
        settings.priority = self.priority_box.value()


class InstallationRequirementsWidget(QGroupBox):  # pylint: disable=too-few-public-methods
    """
    UI element to hold list of Installation Requirements

    The settings object should be a dataclass with:
      - `override_installation_requirements: bool`
      - `installation_requirements: str`
    """

    def __init__(self, initial_settings, parent=None):
        super().__init__("Installation Requirements", parent)

        self._build_ui()
        self._load_initial_settings(initial_settings)

    def _build_ui(self):
        self.layout = QGridLayout(self)

        self.requirements_chck = QCheckBox("Override Installation Requirements", self)
        self.requirements_edit = QLineEdit(self)
        self.layout.addWidget(self.requirements_chck, 4, 0)
        self.layout.addWidget(self.requirements_edit, 4, 1)
        self.requirements_chck.stateChanged.connect(self.enable_requirements_override_changed)

    def _load_initial_settings(self, settings):
        self.requirements_chck.setChecked(settings.override_installation_requirements)
        self.requirements_edit.setEnabled(settings.override_installation_requirements)
        self.requirements_edit.setText(settings.installation_requirements)

    def update_settings(self, settings):
        """
        Update a given instance of scene settings with updated values.
        """
        settings.installation_requirements = self.requirements_edit.text()
        settings.override_installation_requirements = self.requirements_chck.isChecked()

    def enable_requirements_override_changed(self, state):
        """
        Set the enabled/disabled status of the requirements override text box
        """
        self.requirements_edit.setEnabled(state == Qt.Checked)


class _DeadlineNamedResourceDisplay(QWidget):
    """
    A Label for displaying a Amazon Deadline Cloud resource, that starts displaying
    it as the Id, but does an async call to Amazon Deadline Cloud to convert it
    to the name.

    Args:
        resource_name (str): The resource name for the list, like "Farm",
                "Queue", "Fleet".
        setting_name (str): The setting name for the item.
    """

    # Emitted when the background refresh thread catches an exception,
    # provides (operation_name, BaseException)
    background_exception = Signal(str, BaseException)

    # Emitted when an async refresh_item thread completes,
    # provides (refresh_id, id, name, description)
    _item_update = Signal(int, str, str, str)

    def __init__(self, resource_name, setting_name, parent=None):
        super().__init__(parent)

        self.__refresh_thread = None
        self.__refresh_id = 0
        self.canceled = CancelationFlag()
        self.destroyed.connect(self.canceled.set_canceled)

        self.resource_name = resource_name
        self.setting_name = setting_name
        self.item_id = get_setting(self.setting_name)
        self.item_name = ""
        self.item_description = ""

        self._build_ui()

        self.label.setText(self.item_display_name())

    def _build_ui(self):
        self.label = QLabel(parent=self)
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.label)
        self._item_update.connect(self.handle_item_update)
        self.background_exception.connect(self.handle_background_exception)

    def handle_background_exception(self, e):
        self.label.setText(self.item_id)
        self.label.setToolTip("")

    def item_display_name(self):
        """Returns the text to display the item name as"""
        return self.item_name or self.item_id or "<not configured>"

    def refresh(self, deadline_authorized):
        """
        Starts a background thread to refresh the item name.

        Args:
            deadline_authorized (bool): Should be the result of a call to
                    api.check_deadline_available, for example from
                    a Amazon Deadline Cloud Status Widget.
        """
        resource_id = get_setting(self.setting_name)
        if resource_id != self.item_id or not self.item_name:
            self.item_id = resource_id
            self.item_name = ""
            self.item_description = ""
            display_name = self.item_display_name()
            # Only call the Amazon Deadline Cloud API if we've confirmed access
            if deadline_authorized:
                display_name = "<refreshing> - " + display_name

                self.__refresh_id += 1
                self.__refresh_thread = threading.Thread(
                    target=self._refresh_thread_function,
                    name=f"Amazon Deadline Cloud Refresh {self.resource_name} Item Thread",
                    args=(self.__refresh_id,),
                )
                self.__refresh_thread.start()

            self.label.setText(display_name)
            self.label.setToolTip(self.item_description)
        else:
            self.label.setText(self.item_display_name())

    def handle_item_update(self, refresh_id, id, name, description):
        # Apply the refresh if it's still for the latest call
        if refresh_id == self.__refresh_id:
            self.item_id = id
            self.item_name = name
            self.item_description = description
            self.label.setText(self.item_display_name())
            self.label.setToolTip(self.item_description)

    def _refresh_thread_function(self, refresh_id: int):
        """
        This function gets started in a background thread to refresh the list.
        """
        try:
            item = self.get_item()
            if not self.canceled:
                self._item_update.emit(refresh_id, *item)
        except BaseException as e:
            if not self.canceled:
                self.background_exception.emit(f"Refresh {self.resource_name} Item", e)


class DeadlineFarmDisplay(_DeadlineNamedResourceDisplay):
    def __init__(self, parent=None):
        super().__init__(resource_name="Farm", setting_name="defaults.farm_id", parent=parent)

    def get_item(self):
        farm_id = get_setting(self.setting_name)
        if farm_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.get_farm(farmId=farm_id)
            return (response["farmId"], response["displayName"], response["description"])
        else:
            return ("", "", "")


class DeadlineQueueDisplay(_DeadlineNamedResourceDisplay):
    def __init__(self, parent=None):
        super().__init__(resource_name="Queue", setting_name="defaults.queue_id", parent=parent)

    def get_item(self):
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting(self.setting_name)
        if farm_id and queue_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.get_queue(farmId=farm_id, queueId=queue_id)
            return (response["queueId"], response["displayName"], response["description"])
        else:
            return ("", "", "")


class DeadlineStorageProfileNameDisplay(_DeadlineNamedResourceDisplay):
    WINDOWS_OS = "Windows"
    MAC_OS = "Macos"
    LINUX_OS = "Linux"

    def __init__(self, parent=None):
        super().__init__(
            resource_name="Storage Profile Name",
            setting_name="defaults.storage_profile_id",
            parent=parent,
        )

    def get_item(self):
        farm_id = get_setting("defaults.farm_id")
        queue_id = get_setting("defaults.queue_id")
        storage_profile_id = get_setting(self.setting_name)

        if farm_id and queue_id and storage_profile_id:
            deadline = api.get_boto3_client("deadline")
            response = deadline.list_storage_profiles_for_queue(farmId=farm_id, queueId=queue_id)
            farm_storage_profiles = response.get("storageProfiles", {})

            if farm_storage_profiles:
                storage_profile = [
                    (item["storageProfileId"], item["displayName"], item["osFamily"])
                    for item in farm_storage_profiles
                    if storage_profile_id == item["storageProfileId"]
                ]
                return storage_profile[0]

        return ("", "", "")

    def _get_default_storage_profile_name(self) -> str:
        """
        Get a string specifying what the OS is, following the format the Deadline storage profile API expects.
        """
        if sys.platform.startswith("linux"):
            return self.LINUX_OS

        if sys.platform.startswith("darwin"):
            return self.MAC_OS

        if sys.platform.startswith("win"):
            return self.WINDOWS_OS

        return ""
